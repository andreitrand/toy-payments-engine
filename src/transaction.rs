//! Transaction types and their validation.
//!
//! [`Transaction`] can only be built from valid input. Deposits and withdrawals
//! always carry a positive amount with at most four decimal places, while the other
//! kinds carry none. Deserializing a [`Transaction`] goes through the private
//! `InputRow` type, which mirrors a raw input record.

use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{Deserializer, Error, IntoDeserializer};

/// Maximum number of decimal places accepted for an amount.
const MAX_AMOUNT_SCALE: u32 = 4;

/// Transaction type exactly as named in an input record.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum InputKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// One raw input record, before the checks that depend on more than one field.
///
/// Per-field checks (the type name and the amount format) already happen while
/// deserializing. The rest happen when it is converted into a [`Transaction`].
#[derive(Deserialize)]
struct InputRow {
    #[serde(rename = "type", deserialize_with = "case_insensitive_kind")]
    tx_kind: InputKind,
    #[serde(rename = "client")]
    client_id: u16,
    #[serde(rename = "tx")]
    tx_id: u32,
    #[serde(default, deserialize_with = "validated_amount")]
    amount: Option<Decimal>,
}

/// Deserializes an [`InputKind`] regardless of letter case (`deposit`, `Deposit`, `DEPOSIT`).
fn case_insensitive_kind<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<InputKind, D::Error> {
    let kind = String::deserialize(deserializer)?.to_ascii_lowercase();
    InputKind::deserialize(kind.into_deserializer())
}

/// Deserializes an optional amount from its text form, without going through a float. An empty or missing
/// field yields `None`. A present amount must be positive and have at most [`MAX_AMOUNT_SCALE`] decimal places.
fn validated_amount<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Decimal>, D::Error> {
    let value = rust_decimal::serde::str_option::deserialize(deserializer)?;
    match value {
        Some(amount) if amount <= Decimal::ZERO => Err(D::Error::custom("Amount must be positive")),
        Some(amount) if amount.scale() > MAX_AMOUNT_SCALE => Err(D::Error::custom(format!(
            "Amount has more than {MAX_AMOUNT_SCALE} decimal places"
        ))),
        _ => Ok(value),
    }
}

/// The type of operation performed by a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxKind {
    /// Credits the client's account with the given amount.
    Deposit(Decimal),
    /// Debits the client's account by the given amount, if enough funds are available.
    Withdrawal(Decimal),
    /// Launches a dispute for a previous transaction.
    Dispute,
    /// Ends a dispute without reversing the referenced transaction, releasing the held funds.
    Resolve,
    /// Ends a dispute by reversing the referenced transaction and locking the account.
    Chargeback,
}

/// Everything in a [`Transaction`] except the client ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionPayload {
    /// Transaction ID.
    pub tx: u32,
    /// What the transaction does.
    pub kind: TxKind,
}

/// A validated transaction for a single client.
///
/// Deserializes from a record with `type`, `client`, `tx` and (optionally) `amount`
/// fields. Records that don't describe a valid transaction are rejected with a
/// descriptive error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "InputRow")]
pub struct Transaction {
    /// ID of the client whose account the transaction applies to.
    pub client: u16,
    /// The transaction payload.
    pub payload: TransactionPayload,
}

/// Ensures that deposits and withdrawals have an amount. Any amount given for other
/// kinds is ignored, but not considered an error.
impl TryFrom<InputRow> for Transaction {
    type Error = &'static str;

    fn try_from(row: InputRow) -> Result<Self, Self::Error> {
        let required_amount = || row.amount.ok_or("Amount expected but missing");

        let kind = match row.tx_kind {
            InputKind::Deposit => TxKind::Deposit(required_amount()?),
            InputKind::Withdrawal => TxKind::Withdrawal(required_amount()?),
            InputKind::Dispute => TxKind::Dispute,
            InputKind::Resolve => TxKind::Resolve,
            InputKind::Chargeback => TxKind::Chargeback,
        };

        Ok(Transaction {
            client: row.client_id,
            payload: TransactionPayload {
                tx: row.tx_id,
                kind,
            },
        })
    }
}
