//! Multi-threaded engine that spreads transactions over a pool of workers.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use crate::transaction_routing::{TransactionConsumer, TransactionRouter};
use crate::{EngineError, TransactionRejectionReason};

/// Counts of processed transactions, by outcome.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TransactionStats {
    /// Number of successfully processed transactions.
    pub succeeded: u64,
    /// Number of rejected transactions, grouped by reason.
    pub rejected: BTreeMap<TransactionRejectionReason, u64>,
}

impl TransactionStats {
    /// Records the outcome of a single transaction by incrementing the appropriate counter.
    fn record(&mut self, result: Result<(), TransactionRejectionReason>) {
        match result {
            Ok(()) => self.succeeded += 1,
            Err(reason) => *self.rejected.entry(reason).or_default() += 1,
        }
    }

    /// Merges with another [`TransactionStats`] result from a different worker.
    fn merge(&mut self, other: TransactionStats) {
        self.succeeded += other.succeeded;
        for (reason, count) in other.rejected {
            *self.rejected.entry(reason).or_default() += count;
        }
    }
}

/// Displays the statistics, one line per outcome.
impl fmt::Display for TransactionStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_rejected: u64 = self.rejected.values().sum();

        writeln!(f, "Transactions succeeded: {}", self.succeeded)?;
        writeln!(f, "Transactions rejected: {}", total_rejected)?;
        for (reason, count) in &self.rejected {
            writeln!(f, "- {reason}: {count}")?;
        }

        Ok(())
    }
}

/// The output of the engine once all transactions have been processed.
#[derive(Debug)]
pub struct EngineOutput<T> {
    /// The final consumer states, in worker order.
    pub consumers: Vec<T>,
    /// Outcomes of all processed transactions, across all workers.
    pub stats: TransactionStats,
}

/// A pool of worker threads, each owning one [`TransactionConsumer`] instance.
///
/// Every worker has its own bounded FIFO channel. Transactions are submitted through a
/// [`TransactionRouter`], which picks the worker from the client ID. As a result,
/// each client is handled by exactly one worker, and its transactions are processed
/// in the order they were submitted from any single producer. Workers share no state.
///
/// The bounded channels apply backpressure: when workers fall behind, producers block
/// on submit instead of buffering unbounded input in memory.
pub struct TransactionEngine<T: TransactionConsumer + Default + Send + 'static> {
    router: TransactionRouter,
    workers: Vec<JoinHandle<(T, TransactionStats)>>,
}

impl<T: TransactionConsumer + Default + Send + 'static> TransactionEngine<T> {
    /// Spawns `num_workers` workers which listen for transactions to process.
    /// Each worker counts the outcome of every transaction its consumer processes.
    pub fn start(num_workers: usize, channel_capacity: usize) -> Self {
        assert!(num_workers > 0, "At least one worker is required!");
        assert!(
            channel_capacity > 0,
            "The channel capacity must be greater than zero!"
        );

        let (senders, workers) = (0..num_workers)
            .map(|_| {
                let (sender, receiver) = mpsc::sync_channel(channel_capacity);
                let worker = thread::spawn(move || {
                    let mut processor = T::default();
                    let mut stats = TransactionStats::default();

                    // Process every transaction and also store its outcome.
                    for transaction in receiver {
                        stats.record(processor.process_transaction(transaction));
                    }

                    (processor, stats)
                });

                (sender, worker)
            })
            .unzip();

        Self {
            router: TransactionRouter::new(senders),
            workers,
        }
    }

    /// Returns a new router clone for submitting transactions to this engine.
    pub fn router(&self) -> TransactionRouter {
        self.router.clone()
    }

    /// Waits for every worker to process its remaining transactions and returns
    /// their consumers, in worker order, with the combined statistics of all workers.
    ///
    /// All routers obtained from [`router`](Self::router) must be dropped first,
    /// otherwise this call never returns.
    pub fn wait_for_completion(self) -> Result<EngineOutput<T>, EngineError> {
        drop(self.router);

        let mut output = EngineOutput {
            consumers: Vec::with_capacity(self.workers.len()),
            stats: TransactionStats::default(),
        };

        for (index, worker) in self.workers.into_iter().enumerate() {
            let (consumer, stats) = worker
                .join()
                .map_err(|_| EngineError::WorkerPanicked { index })?;
            output.consumers.push(consumer);
            output.stats.merge(stats);
        }

        Ok(output)
    }
}
