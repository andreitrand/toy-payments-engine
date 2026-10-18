//! Transaction types and their validation.

use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{Deserializer, IntoDeserializer};

/// Maximum number of decimal places accepted for an amount.
const MAX_AMOUNT_SCALE: usize = 4;

/// Largest amount accepted for a single transaction: 10^18. This cap keeps every balance far
/// below [`Decimal::MAX`], as a client's balance is the sum of at most `u32::MAX` deposits.
pub(crate) const MAX_AMOUNT: i128 = 10_i128.pow(18);

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

/// One raw input record, before the checks that depend on the transaction type.
///
/// The type name and the IDs are checked while deserializing. The amount is kept as
/// text and checked when the record is converted into a [`Transaction`].
#[derive(Deserialize)]
struct InputRow {
    #[serde(rename = "type", deserialize_with = "case_insensitive_kind")]
    tx_kind: InputKind,
    #[serde(rename = "client")]
    client_id: u16,
    #[serde(rename = "tx")]
    tx_id: u32,
    /// The amount exactly as written. An empty or missing field yields `None`.
    #[serde(default)]
    amount: Option<String>,
}

/// Deserializes an [`InputKind`] regardless of letter case (`deposit`, `Deposit`, `DEPOSIT`).
fn case_insensitive_kind<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<InputKind, D::Error> {
    let kind = String::deserialize(deserializer)?.to_ascii_lowercase();
    InputKind::deserialize(kind.into_deserializer())
}

/// Parses an amount written as plain digits, optionally followed by a dot and 1 to
/// [`MAX_AMOUNT_SCALE`] decimal digits. The amount must be positive and at most [`MAX_AMOUNT`].
fn parse_amount(text: &str) -> Result<Decimal, &'static str> {
    let is_digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());

    // The text must represent a rational number with up to `MAX_AMOUNT_SCALE` decimal places.
    let well_formed = match text.split_once('.') {
        Some((whole, fraction)) => {
            is_digits(whole) && is_digits(fraction) && fraction.len() <= MAX_AMOUNT_SCALE
        }
        None => is_digits(text),
    };

    if !well_formed {
        return Err("Amount must be digits with at most 4 decimal places");
    }

    // The format is valid so we can attempt a parse.
    let amount = Decimal::from_str_exact(text).map_err(|_| "Amount is too large")?;

    if amount.is_zero() {
        return Err("Amount must be positive");
    }
    if amount > Decimal::from(MAX_AMOUNT) {
        return Err("Amount exceeds the maximum allowed");
    }

    Ok(amount)
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

/// Ensures that deposits and withdrawals have a valid amount, and that the other kinds,
/// which refer to an earlier transaction, have none.
impl TryFrom<InputRow> for Transaction {
    type Error = &'static str;

    fn try_from(row: InputRow) -> Result<Self, Self::Error> {
        let amount = row.amount.as_deref();
        let required_amount = || parse_amount(amount.ok_or("Amount expected but missing")?);
        let without_amount = |kind| match amount {
            Some(_) => Err("Amount not allowed for this transaction type"),
            None => Ok(kind),
        };

        let kind = match row.tx_kind {
            InputKind::Deposit => TxKind::Deposit(required_amount()?),
            InputKind::Withdrawal => TxKind::Withdrawal(required_amount()?),
            InputKind::Dispute => without_amount(TxKind::Dispute)?,
            InputKind::Resolve => without_amount(TxKind::Resolve)?,
            InputKind::Chargeback => without_amount(TxKind::Chargeback)?,
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
