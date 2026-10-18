//! Per-worker transaction processing.

use std::collections::{HashMap, HashSet};
use std::fmt;

use rust_decimal::Decimal;

use crate::TransactionRejectionReason;
use crate::transaction::{MAX_AMOUNT, Transaction, TransactionPayload, TxKind};
use crate::transaction_routing::TransactionConsumer;

/// Balances and status of a single client account.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientAccount {
    /// Funds that can be withdrawn. Negative if disputed funds were already withdrawn.
    pub available: Decimal,
    /// Funds held by open disputes.
    pub held: Decimal,
    /// Whether an account is locked by a chargeback. Affects only deposits and withdrawals.
    pub locked: bool,
}

/// Everything a processor keeps about one client.
#[derive(Debug, Default)]
struct ClientState {
    account: ClientAccount,
    /// Amounts of the deposits that can still be disputed, by transaction ID.
    deposits: HashMap<u32, Decimal>,
    /// IDs of the deposits currently under dispute.
    disputed: HashSet<u32>,
}

impl ClientState {
    /// Performs a deposit.
    fn deposit(&mut self, tx: u32, amount: Decimal) -> Result<(), TransactionRejectionReason> {
        // Deposits are not allowed on locked accounts.
        self.ensure_unlocked()?;

        // Only positive amounts up to the maximum can be deposited.
        Self::ensure_valid_amount(amount)?;

        // Check for duplicate transaction IDs.
        if self.deposits.contains_key(&tx) {
            return Err(TransactionRejectionReason::DuplicateTransaction);
        }

        // Calculate the new available balance.
        let new_available = self
            .account
            .available
            .checked_add(amount)
            .ok_or(TransactionRejectionReason::Overflow)?;

        // Check for overflows on the account's total balance.
        self.account
            .available
            .checked_add(self.account.held)
            .and_then(|total| total.checked_add(amount))
            .ok_or(TransactionRejectionReason::Overflow)?;

        // Update the account and store the deposit.
        self.account.available = new_available;
        self.deposits.insert(tx, amount);

        Ok(())
    }

    /// Performs a withdrawal.
    fn withdraw(&mut self, amount: Decimal) -> Result<(), TransactionRejectionReason> {
        // Withdrawals are not allowed on locked accounts.
        self.ensure_unlocked()?;

        // Only positive amounts up to the maximum can be withdrawn.
        Self::ensure_valid_amount(amount)?;

        // Withdrawals larger than the current available balance are rejected.
        if self.account.available < amount {
            return Err(TransactionRejectionReason::InsufficientFunds);
        }

        // Reduce the available balance by the withdrawal amount.
        self.account.available = self
            .account
            .available
            .checked_sub(amount)
            .ok_or(TransactionRejectionReason::Overflow)?;

        Ok(())
    }

    /// Opens a dispute (allowed on locked accounts).
    fn dispute(&mut self, tx: u32) -> Result<(), TransactionRejectionReason> {
        // Reject an already-open dispute (i.e. a duplicate transaction).
        if self.disputed.contains(&tx) {
            return Err(TransactionRejectionReason::DuplicateTransaction);
        }

        // Get the amount under dispute.
        let amount = self.get_deposit_amount(tx)?;

        // Remove the disputed amount from the available balance and hold it.
        // It is possible for the available balance to become negative at this point.
        let available = self
            .account
            .available
            .checked_sub(amount)
            .ok_or(TransactionRejectionReason::Overflow)?;

        let held = self
            .account
            .held
            .checked_add(amount)
            .ok_or(TransactionRejectionReason::Overflow)?;

        // Update the account and mark the transaction as actively disputed.
        self.account.available = available;
        self.account.held = held;
        self.disputed.insert(tx);

        Ok(())
    }

    /// Resolves a dispute (allowed on a locked account).
    fn resolve(&mut self, tx: u32) -> Result<(), TransactionRejectionReason> {
        // Get the amount under dispute, also checking the dispute is active.
        let amount = self.get_disputed_amount(tx)?;

        // Release the held funds and return them to the available balance.
        let held = self
            .account
            .held
            .checked_sub(amount)
            .ok_or(TransactionRejectionReason::Overflow)?;
        let available = self
            .account
            .available
            .checked_add(amount)
            .ok_or(TransactionRejectionReason::Overflow)?;

        // Update the account and mark the transaction as no longer disputed.
        self.account.held = held;
        self.account.available = available;
        self.disputed.remove(&tx);

        Ok(())
    }

    /// Executes a chargeback (allowed on a locked account).
    fn chargeback(&mut self, tx: u32) -> Result<(), TransactionRejectionReason> {
        // Get the amount under dispute, also checking the dispute is active.
        let amount = self.get_disputed_amount(tx)?;

        // Remove the disputed amount from the held balance.
        let held = self
            .account
            .held
            .checked_sub(amount)
            .ok_or(TransactionRejectionReason::Overflow)?;

        // Update the account, lock it and mark the transaction as no longer disputed.
        self.account.held = held;
        self.account.locked = true;
        self.disputed.remove(&tx);

        // The deposit is forgotten, as a chargeback is final and it can never be disputed again.
        self.deposits.remove(&tx);

        Ok(())
    }

    /// Checks if an account is locked, returning an error if so.
    fn ensure_unlocked(&self) -> Result<(), TransactionRejectionReason> {
        if self.account.locked {
            Err(TransactionRejectionReason::AccountLocked)
        } else {
            Ok(())
        }
    }

    /// Gets the amount that was deposited in a given transaction.
    fn get_deposit_amount(&self, tx: u32) -> Result<Decimal, TransactionRejectionReason> {
        self.deposits
            .get(&tx)
            .copied()
            .ok_or(TransactionRejectionReason::UnknownTransaction)
    }

    /// Gets the amount that is disputed by a given transaction.
    fn get_disputed_amount(&self, tx: u32) -> Result<Decimal, TransactionRejectionReason> {
        if !self.disputed.contains(&tx) {
            return Err(TransactionRejectionReason::NotDisputed);
        }
        self.get_deposit_amount(tx)
    }

    /// Rejects an amount that is not positive or exceeds [`MAX_AMOUNT`].
    fn ensure_valid_amount(amount: Decimal) -> Result<(), TransactionRejectionReason> {
        if amount > Decimal::ZERO && amount <= Decimal::from(MAX_AMOUNT) {
            Ok(())
        } else {
            Err(TransactionRejectionReason::InvalidAmount)
        }
    }
}

/// Applies transactions to the client accounts owned by a single worker.
///
/// Every client is routed to exactly one processor, so a processor has exclusive
/// access to its accounts and needs no synchronisation.
///
/// Only deposits can be disputed, so withdrawals are applied but not stored.
/// An account is created by its client's first transaction, even if that transaction is rejected.
#[derive(Debug, Default)]
pub struct TransactionProcessor {
    client_states: HashMap<u16, ClientState>,
}

/// Displays the processor's accounts one row at a time, in the following column order:
/// "client,available,held,total,locked"
impl fmt::Display for TransactionProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (client, account) in self.get_accounts() {
            // Calculate the total balance, which cannot overflow due to capped input amounts.
            let account_total = account.available + account.held;

            writeln!(
                f,
                "{client},{},{},{},{}",
                account.available, account.held, account_total, account.locked
            )?;
        }

        Ok(())
    }
}

impl TransactionConsumer for TransactionProcessor {
    /// Applies `transaction`, or leaves all state unchanged and returns the rejection reason.
    fn process_transaction(
        &mut self,
        transaction: Transaction,
    ) -> Result<(), TransactionRejectionReason> {
        let Transaction {
            client,
            payload: TransactionPayload { tx, kind },
        } = transaction;

        // Gets the client's state, creating an empty account for a new client.
        let client_state = self.client_states.entry(client).or_default();

        match kind {
            TxKind::Deposit(amount) => client_state.deposit(tx, amount),
            TxKind::Withdrawal(amount) => client_state.withdraw(amount),
            TxKind::Dispute => client_state.dispute(tx),
            TxKind::Resolve => client_state.resolve(tx),
            TxKind::Chargeback => client_state.chargeback(tx),
        }
    }
}

impl TransactionProcessor {
    /// Returns every account held by this processor, in no particular order.
    pub fn get_accounts(&self) -> impl Iterator<Item = (u16, &ClientAccount)> {
        self.client_states
            .iter()
            .map(|(&client, state)| (client, &state.account))
    }
}
