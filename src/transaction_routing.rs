//! Moving transactions from producers to consumers.

use std::sync::mpsc::{SendError, SyncSender};

use crate::EngineError;
use crate::transaction::Transaction;

/// Forwards transactions to the worker responsible for their client.
///
/// A client is always assigned to the same worker (`client % number of workers`), so
/// all of a client's transactions end up in one place and keep their submission order.
/// Each producer must receive its own router clone and it is mandatory to drop it when done.
#[derive(Clone)]
pub struct TransactionRouter {
    senders: Vec<SyncSender<Transaction>>,
}

impl TransactionRouter {
    /// Creates a router over one channel per worker. `senders` must not be empty.
    pub(crate) fn new(senders: Vec<SyncSender<Transaction>>) -> Self {
        Self { senders }
    }

    /// Sends `transaction` to the worker responsible for its client. Blocks when a worker's channel is full.
    ///
    /// Returns the transaction back if the worker has stopped receiving, for example
    /// because it panicked.
    pub fn submit(&self, transaction: Transaction) -> Result<(), SendError<Transaction>> {
        let shard = usize::from(transaction.client) % self.senders.len();
        self.senders[shard].send(transaction)
    }
}

/// Sending end of the routing: something that produces transactions and submits them to a router.
///
/// Implementations decide where transactions come from (a file, a network stream, an
/// in-memory list) and how invalid input is handled. The engine only sees the
/// [`Transaction`] values they submit.
pub trait TransactionProducer {
    /// Submits every transaction this producer has to `router`, then returns.
    ///
    /// Returns an [`EngineError`] if producing fails (such as [`EngineError::Read`]) or
    /// if the receiving worker is no longer available ([`EngineError::WorkerUnavailable`]).
    fn send_all_transactions(&mut self, router: TransactionRouter) -> Result<(), EngineError>;
}

/// Receiving end of the routing: something that consumes transactions one at a time.
///
/// Each engine worker owns one consumer and feeds it the transactions routed to it, in order.
pub trait TransactionConsumer {
    /// Processes a single transaction, returning a message if it is invalid in any way.
    fn process_transaction(&mut self, transaction: Transaction) -> Result<(), String>;
}
