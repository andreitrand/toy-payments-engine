//! Per-worker transaction processing.

use crate::transaction::Transaction;
use crate::transaction_routing::TransactionConsumer;

/// Applies transactions to the client accounts owned by a single worker.
///
/// Every client is routed to exactly one processor, so a processor has exclusive
/// access to its accounts and needs no synchronisation.
#[derive(Debug, Default)]
pub struct TransactionProcessor {}

impl TransactionConsumer for TransactionProcessor {
    fn process_transaction(&mut self, _transaction: Transaction) -> Result<(), String> {
        // TODO
        Ok(())
    }
}
