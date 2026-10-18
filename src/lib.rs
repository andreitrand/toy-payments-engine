//! A toy payments engine that processes client transactions and tracks account balances.
//!
//! Transactions flow through the engine in three stages:
//!
//! 1. A [`TransactionProducer`] (e.g.
//!    [`CsvTransactionProducer`](input_csv::CsvTransactionProducer)) streams and
//!    validates input records into [`Transaction`] values.
//! 2. A [`TransactionRouter`](transaction_routing::TransactionRouter) forwards each
//!    transaction to a worker, chosen by client ID, so that all transactions of a
//!    given client are handled by the same worker, in order.
//! 3. Each worker owns a [`TransactionConsumer`] (e.g. [`TransactionProcessor`]) that
//!    applies the transactions to the accounts it is responsible for.
//!
//! Workers share no state, so no locking is needed while processing. Their results
//! are collected once all producers are done; see [`TransactionEngine`].
//! A [`TransactionProcessor`] displays its final account states as CSV rows, and
//! [`write_accounts`] writes those of every processor under a single header.

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::mpsc::SendError;

use crate::transaction::Transaction;
use crate::transaction_engine::{EngineOutput, TransactionEngine};
use crate::transaction_processor::TransactionProcessor;
use crate::transaction_routing::{TransactionConsumer, TransactionProducer};

pub mod input_csv;
pub mod transaction;
pub mod transaction_engine;
pub mod transaction_processor;
pub mod transaction_routing;

/// A failure that stops the engine, as opposed to a single rejected transaction.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The input file could not be opened.
    #[error("Failed to open '{}': {source}", .path.display())]
    Open { path: PathBuf, source: io::Error },

    /// The input's headers were incorrect or it failed to read part-way.
    #[error("Failed to read input: {0}")]
    Read(io::Error),

    /// A transaction could not be delivered because its worker stopped receiving.
    #[error("Worker stopped receiving; could not deliver {:?}", .0.0)]
    WorkerUnavailable(#[from] SendError<Transaction>),

    /// A worker thread panicked, so its consumer's state is lost.
    #[error("Processor {index} panicked")]
    WorkerPanicked { index: usize },

    /// Writing the output failed.
    #[error("Failed to write output: {0}")]
    Write(io::Error),
}

/// The possible reasons why a [`TransactionConsumer`] rejected a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, thiserror::Error)]
pub enum TransactionRejectionReason {
    /// A deposit or withdrawal on an account locked by a chargeback.
    #[error("Account is locked")]
    AccountLocked,

    /// The referenced transaction is not a disputable deposit of this client.
    #[error("Unknown transaction")]
    UnknownTransaction,

    /// A deposit reuses an existing transaction ID, or a dispute repeats one that is still open.
    #[error("Duplicate transaction")]
    DuplicateTransaction,

    /// A withdrawal exceeds the available funds.
    #[error("Insufficient funds")]
    InsufficientFunds,

    /// A resolve or chargeback references a transaction that is not under dispute.
    #[error("Transaction is not under dispute")]
    NotDisputed,

    /// A deposit or withdrawal with an amount that is not positive or exceeds the maximum.
    #[error("Invalid amount")]
    InvalidAmount,

    /// Applying the transaction would lead to a balance overflow.
    #[error("Arithmetic overflow")]
    Overflow,
}

/// Runs a complete engine over a single transaction producer and returns the final state of every
/// transaction consumer, with statistics on processed transactions.
///
/// The engine launches `num_processors` workers, each with a bounded input queue that is fed by
/// the transaction producer. The workers process transactions until the producer is exhausted, at
/// which point the engine gathers the final state of every consumer and returns it.
///
/// Returns the producer's error if [`TransactionProducer::send_all_transactions`] stopped early,
/// or [`EngineError::WorkerPanicked`] if any worker panicked.
pub fn run_with_single_transaction_producer<
    P: TransactionProducer,
    C: TransactionConsumer + Default + Send + 'static,
>(
    mut transaction_producer: P,
    num_processors: usize,
    channel_capacity: usize,
) -> Result<EngineOutput<C>, EngineError> {
    let engine = TransactionEngine::<C>::start(num_processors, channel_capacity);

    transaction_producer.send_all_transactions(engine.router())?;
    engine.wait_for_completion()
}

/// Writes the accounts of every processor to `out` as CSV, under a
/// `client,available,held,total,locked` header. The rows are in no particular order.
pub fn write_accounts(mut out: impl Write, processors: &[TransactionProcessor]) -> io::Result<()> {
    writeln!(out, "client,available,held,total,locked")?;
    for processor in processors {
        write!(out, "{processor}")?;
    }

    out.flush()
}
