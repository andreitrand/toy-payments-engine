//! Multi-threaded engine that spreads transactions over a pool of workers.

use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use crate::EngineError;
use crate::transaction_routing::{TransactionConsumer, TransactionRouter};

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
    workers: Vec<JoinHandle<T>>,
}

impl<T: TransactionConsumer + Default + Send + 'static> TransactionEngine<T> {
    /// Spawns `num_workers` workers which listen for transactions to process.
    /// Transactions reported as invalid by the consumer are silently rejected.
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

                    for transaction in receiver {
                        let _ = processor.process_transaction(transaction);
                    }

                    processor
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
    /// their consumers, in worker order.
    ///
    /// All routers obtained from [`router`](Self::router) must be dropped first,
    /// otherwise this call never returns.
    ///
    /// Returns [`EngineError::WorkerPanicked`] as soon as a worker is found to have
    /// panicked.
    pub fn wait_for_completion(self) -> Result<Vec<T>, EngineError> {
        drop(self.router);

        self.workers
            .into_iter()
            .enumerate()
            .map(|(index, worker)| {
                worker
                    .join()
                    .map_err(|_| EngineError::WorkerPanicked { index })
            })
            .collect()
    }
}
