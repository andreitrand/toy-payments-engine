//! Tests that the engine routes every transaction to the right worker, intact and in order.

mod common;

use std::collections::HashMap;
use std::thread;

use common::{MockTransactionProcessor, VecTransactionProducer};
use toy_payments_engine::transaction::{Transaction, TransactionPayload};
use toy_payments_engine::transaction_engine::{EngineOutput, TransactionEngine};
use toy_payments_engine::transaction_routing::{TransactionConsumer, TransactionProducer};
use toy_payments_engine::{
    EngineError, TransactionRejectionReason, run_with_single_transaction_producer,
};

/// Seed of the random input, fixed so that every run is reproducible.
const SEED: u64 = 0x34321497;

/// Number of transactions in the random input.
const SMALL_SIZE: usize = 10_000;
const BIG_SIZE: usize = 100_000;

/// Small number of distinct clients, giving each worker long per-client sequences.
const NUM_CLIENTS_SMALL: u16 = 10;

/// Large number of distinct clients, spreading the input over many workers.
const NUM_CLIENTS_LARGE: u16 = 100;

/// Capacity of each worker's channel.
const CHANNEL_CAPACITY: usize = 16;

/// The transaction ID that causes a processor to panic.
const PANICKING_TX: u32 = SMALL_SIZE as u32 / 2; // somewhere in the middle of the input

/// Consumer that panics on the transaction with ID `TX` and accepts all others.
#[derive(Debug, Default)]
struct PanickingTransactionProcessor<const TX: u32>;

impl<const TX: u32> TransactionConsumer for PanickingTransactionProcessor<TX> {
    /// Panics if `transaction` has ID `TX`, otherwise accepts it.
    fn process_transaction(
        &mut self,
        transaction: Transaction,
    ) -> Result<(), TransactionRejectionReason> {
        assert_ne!(transaction.payload.tx, TX, "Simulated worker failure");
        Ok(())
    }
}

/// Runs random transactions through an engine of `num_processors` mock processors and checks
/// that each processor received exactly the transactions of its own clients, in input order,
/// and that all of them were accepted.
fn assert_all_transactions_received(
    num_processors: usize,
    channel_capacity: usize,
    num_transactions: usize,
    num_clients: u16,
) {
    let producer = VecTransactionProducer::from_random(SEED, num_transactions, num_clients);
    let input = producer.transactions().to_vec();

    let EngineOutput { consumers, stats } = run_with_single_transaction_producer::<
        _,
        MockTransactionProcessor,
    >(producer, num_processors, channel_capacity)
    .expect("The engine should complete without errors");

    // The mock accepts every transaction, so all of them count as succeeded.
    assert_eq!(stats.succeeded, num_transactions as u64);
    assert!(stats.rejected.is_empty());
    assert_eq!(consumers.len(), num_processors);

    for (index, consumer) in consumers.iter().enumerate() {
        // A single producer and FIFO channels deliver each worker's share in input order.
        let expected: Vec<Transaction> = input
            .iter()
            .copied()
            .filter(|transaction| usize::from(transaction.client) % num_processors == index)
            .collect();

        assert_eq!(
            consumer.transactions, expected,
            "Processor {index} did not receive exactly its clients' transactions, in order"
        );
    }
}

/// A single worker receives every transaction, in input order.
#[test]
fn partitioning_with_1_worker() {
    assert_all_transactions_received(1, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_LARGE);
}

/// With two workers, each receives exactly its clients' transactions, in order.
#[test]
fn partitioning_with_2_workers() {
    assert_all_transactions_received(2, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_LARGE);
}

/// With four workers, each receives exactly its clients' transactions, in order.
#[test]
fn partitioning_with_4_workers() {
    assert_all_transactions_received(4, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_LARGE);
}

/// With seven workers, which the clients do not divide evenly, routing stays exact.
#[test]
fn partitioning_with_7_workers() {
    assert_all_transactions_received(7, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_LARGE);
}

/// With eight workers, each receives exactly its clients' transactions, in order.
#[test]
fn partitioning_with_8_workers() {
    assert_all_transactions_received(8, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_LARGE);
}

/// A single worker handles both small and large inputs over few clients.
#[test]
fn one_worker() {
    assert_all_transactions_received(1, CHANNEL_CAPACITY, SMALL_SIZE, NUM_CLIENTS_SMALL);
    assert_all_transactions_received(1, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_SMALL * 2);
}

/// Several workers handle both small and large inputs over few clients.
#[test]
fn multiple_workers() {
    assert_all_transactions_received(2, CHANNEL_CAPACITY, SMALL_SIZE, NUM_CLIENTS_SMALL);
    assert_all_transactions_received(4, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_SMALL);
    assert_all_transactions_received(8, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_SMALL);
}

/// Workers left without clients stay idle and still return empty consumers.
#[test]
fn more_workers_than_clients() {
    assert_all_transactions_received(16, CHANNEL_CAPACITY, BIG_SIZE, NUM_CLIENTS_SMALL);
}

/// An engine that receives no transactions still shuts down cleanly.
#[test]
fn empty_input() {
    assert_all_transactions_received(4, CHANNEL_CAPACITY, 0, NUM_CLIENTS_SMALL);
}

/// A single client's transactions all go to one worker, in order.
#[test]
fn single_client() {
    assert_all_transactions_received(4, CHANNEL_CAPACITY, SMALL_SIZE, 1);
}

/// The producer blocks on almost every submit, as each channel holds a single transaction.
#[test]
fn channel_capacity_of_one() {
    assert_all_transactions_received(4, 1, BIG_SIZE, NUM_CLIENTS_SMALL);
}

/// The producer never blocks, as each channel can hold the whole input.
#[test]
fn channel_capacity_larger_than_input() {
    assert_all_transactions_received(4, SMALL_SIZE + 1, SMALL_SIZE, NUM_CLIENTS_SMALL);
}

/// A panicking worker makes the run fail with an error instead of hanging.
#[test]
fn worker_panic_fails_the_run() {
    let num_processors = 4;
    let producer = VecTransactionProducer::from_random(SEED, SMALL_SIZE, NUM_CLIENTS_SMALL);
    let panicking_client = producer.transactions()[PANICKING_TX as usize].client;

    let result = run_with_single_transaction_producer::<
        _,
        PanickingTransactionProcessor<PANICKING_TX>,
    >(producer, num_processors, CHANNEL_CAPACITY);

    // Depending on timing, the producer either finds the worker gone while submitting,
    // or the panic is found when joining the workers.
    match result {
        Err(EngineError::WorkerUnavailable(_)) => {}
        Err(EngineError::WorkerPanicked { index }) => {
            assert_eq!(index, usize::from(panicking_client) % num_processors);
        }
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("The run should fail when a worker panics"),
    }
}

/// Runs `num_producers` concurrent producers of random transactions through an engine of
/// `num_processors` mock processors and checks that, together, the processors received exactly
/// the transactions of all producers, all of them accepted.
fn assert_all_transactions_received_from_producers(
    num_producers: usize,
    num_processors: usize,
    channel_capacity: usize,
    num_transactions: usize,
    num_clients: u16,
) {
    let engine =
        TransactionEngine::<MockTransactionProcessor>::start(num_processors, channel_capacity);

    // Each producer gets its own seed and range of transaction IDs, so all IDs stay unique.
    let inputs: Vec<Vec<Transaction>> = (0..num_producers)
        .map(|index| {
            let seed = SEED.wrapping_add(index as u64);
            let offset = (index * num_transactions) as u32;

            VecTransactionProducer::from_random(seed, num_transactions, num_clients)
                .transactions()
                .iter()
                .map(|&transaction| Transaction {
                    payload: TransactionPayload {
                        tx: transaction.payload.tx + offset,
                        ..transaction.payload
                    },
                    ..transaction
                })
                .collect()
        })
        .collect();

    // Each producer runs on its own thread, with its own router.
    let producers: Vec<_> = inputs
        .iter()
        .cloned()
        .map(|transactions| {
            let router = engine.router();
            thread::spawn(move || {
                VecTransactionProducer::new(transactions).send_all_transactions(router)
            })
        })
        .collect();

    for producer in producers {
        producer
            .join()
            .expect("The producer thread should not panic")
            .expect("The producer should submit all of its transactions");
    }

    let EngineOutput { consumers, stats } = engine
        .wait_for_completion()
        .expect("The engine should complete without errors");

    // The mock accepts every transaction, so all of them count as succeeded.
    assert_eq!(stats.succeeded, (num_producers * num_transactions) as u64);
    assert!(stats.rejected.is_empty());

    // Each producer's IDs increase and occupy their own range, so within a consumer, every
    // client's transactions from the same producer must arrive with increasing IDs.
    for (index, consumer) in consumers.iter().enumerate() {
        let mut last_tx: HashMap<(usize, u16), u32> = HashMap::new();

        for transaction in &consumer.transactions {
            let producer = transaction.payload.tx as usize / num_transactions;
            let key = (producer, transaction.client);

            if let Some(&last) = last_tx.get(&key) {
                assert!(
                    transaction.payload.tx > last,
                    "Processor {index} received transaction {} of client {} from producer \
                     {producer} after transaction {last}",
                    transaction.payload.tx,
                    transaction.client,
                );
            }
            last_tx.insert(key, transaction.payload.tx);
        }
    }

    let mut expected: Vec<Transaction> = inputs.into_iter().flatten().collect();
    let mut received: Vec<Transaction> = consumers
        .into_iter()
        .flat_map(|consumer| consumer.transactions)
        .collect();

    // Producers interleave nondeterministically, so compare both sorted by their unique IDs.
    expected.sort_by_key(|transaction| transaction.payload.tx);
    received.sort_by_key(|transaction| transaction.payload.tx);

    assert_eq!(received, expected);
}

/// Several producers feeding one worker deliver all of their transactions.
#[test]
fn multiple_producers_one_worker() {
    assert_all_transactions_received_from_producers(
        4,
        1,
        CHANNEL_CAPACITY,
        SMALL_SIZE,
        NUM_CLIENTS_SMALL,
    );
}

/// Several producers and workers deliver every transaction, in order per producer.
#[test]
fn multiple_producers_multiple_workers() {
    assert_all_transactions_received_from_producers(
        2,
        4,
        CHANNEL_CAPACITY,
        SMALL_SIZE,
        NUM_CLIENTS_SMALL,
    );
    assert_all_transactions_received_from_producers(
        4,
        7,
        CHANNEL_CAPACITY,
        BIG_SIZE,
        NUM_CLIENTS_LARGE,
    );
    assert_all_transactions_received_from_producers(
        8,
        8,
        CHANNEL_CAPACITY,
        BIG_SIZE,
        NUM_CLIENTS_LARGE,
    );
}

/// Producers contend for the same single-slot channels.
#[test]
fn multiple_producers_channel_capacity_of_one() {
    assert_all_transactions_received_from_producers(8, 4, 1, SMALL_SIZE, NUM_CLIENTS_LARGE);
}

/// Starting an engine without workers panics.
#[test]
#[should_panic(expected = "At least one worker is required!")]
fn zero_workers_is_rejected() {
    TransactionEngine::<MockTransactionProcessor>::start(0, CHANNEL_CAPACITY);
}

/// Starting an engine with a channel capacity of zero panics.
#[test]
#[should_panic(expected = "The channel capacity must be greater than zero!")]
fn zero_channel_capacity_is_rejected() {
    TransactionEngine::<MockTransactionProcessor>::start(4, 0);
}
