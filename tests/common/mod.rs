//! Helpers shared by the integration tests.

// Each test crate uses only some of these helpers.
#![allow(dead_code)]

use rust_decimal::Decimal;
use toy_payments_engine::transaction::{Transaction, TransactionPayload, TxKind};
use toy_payments_engine::transaction_routing::{
    TransactionConsumer, TransactionProducer, TransactionRouter,
};
use toy_payments_engine::{EngineError, TransactionRejectionReason};

/// Builds a transaction of the given kind.
pub fn transaction(client: u16, tx: u32, kind: TxKind) -> Transaction {
    Transaction {
        client,
        payload: TransactionPayload { tx, kind },
    }
}

/// Parses `amount` from its text form, keeping its exact scale.
pub fn parse_amount(amount: &str) -> Decimal {
    amount
        .parse()
        .expect("The test amount should be a valid decimal")
}

/// Builds a deposit of `amount`, given in text form (e.g. `"1.5"`).
pub fn deposit(client: u16, tx: u32, amount: &str) -> Transaction {
    transaction(client, tx, TxKind::Deposit(parse_amount(amount)))
}

/// Builds a withdrawal of `amount`, given in text form (e.g. `"1.5"`).
pub fn withdrawal(client: u16, tx: u32, amount: &str) -> Transaction {
    transaction(client, tx, TxKind::Withdrawal(parse_amount(amount)))
}

/// Builds a dispute of transaction `tx`.
pub fn dispute(client: u16, tx: u32) -> Transaction {
    transaction(client, tx, TxKind::Dispute)
}

/// Builds a resolve of transaction `tx`.
pub fn resolve(client: u16, tx: u32) -> Transaction {
    transaction(client, tx, TxKind::Resolve)
}

/// Builds a chargeback of transaction `tx`.
pub fn chargeback(client: u16, tx: u32) -> Transaction {
    transaction(client, tx, TxKind::Chargeback)
}

/// Produces transactions from an in-memory list, in order.
pub struct VecTransactionProducer {
    transactions: Vec<Transaction>,
}

impl VecTransactionProducer {
    /// Creates a producer that submits `transactions` in the given order.
    pub fn new(transactions: Vec<Transaction>) -> Self {
        Self { transactions }
    }

    /// Creates a producer of `size` pseudo-random transactions over `num_clients` clients.
    ///
    /// Transaction IDs are the sequence indexes `0..size`, so each one is unique and increasing.
    /// Clients are drawn from `0..num_clients`, and kinds and amounts at random, regardless of
    /// whether the transactions make sense together. The same seed yields the same transactions.
    pub fn from_random(seed: u64, size: usize, num_clients: u16) -> Self {
        assert!(num_clients > 0, "At least one client is required!");

        let mut rng = SplitMix64::new(seed);
        let transactions = (0..size)
            .map(|tx| {
                let client = rng.below(u64::from(num_clients)) as u16;

                // A positive amount with 4 decimal places, between 0.0001 and 100.
                let amount = Decimal::new(rng.below(1_000_000) as i64 + 1, 4);
                let kind = match rng.below(5) {
                    0 => TxKind::Deposit(amount),
                    1 => TxKind::Withdrawal(amount),
                    2 => TxKind::Dispute,
                    3 => TxKind::Resolve,
                    _ => TxKind::Chargeback,
                };

                Transaction {
                    client,
                    payload: TransactionPayload {
                        tx: tx as u32,
                        kind,
                    },
                }
            })
            .collect();

        Self::new(transactions)
    }

    /// Returns the transactions this producer submits, in order.
    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }
}

impl TransactionProducer for VecTransactionProducer {
    /// Submits every transaction in order.
    fn send_all_transactions(&mut self, router: TransactionRouter) -> Result<(), EngineError> {
        for &transaction in &self.transactions {
            router.submit(transaction)?;
        }
        Ok(())
    }
}

/// Minimal seeded pseudo-random number generator (SplitMix64).
///
/// The same seed always yields the same sequence, so test runs are reproducible.
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Creates a generator from `seed`. Any value is a valid seed.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next pseudo-random value.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a pseudo-random value in `0..bound`. `bound` must not be zero.
    ///
    /// The modulo bias is negligible for the small bounds used in tests.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }
}

/// Consumer that records every transaction it receives and accepts all of them.
#[derive(Debug, Default)]
pub struct MockTransactionProcessor {
    /// Every received transaction in arrival order, duplicates included.
    pub transactions: Vec<Transaction>,
}

impl TransactionConsumer for MockTransactionProcessor {
    /// Records `transaction` and always accepts it.
    fn process_transaction(
        &mut self,
        transaction: Transaction,
    ) -> Result<(), TransactionRejectionReason> {
        self.transactions.push(transaction);
        Ok(())
    }
}
