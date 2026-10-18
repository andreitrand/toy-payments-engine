//! Tests of the transaction rules, applied one transaction at a time to a single processor.

mod common;

use common::{chargeback, deposit, dispute, parse_amount, resolve, transaction, withdrawal};
use rust_decimal::Decimal;
use toy_payments_engine::TransactionRejectionReason::{
    self, AccountLocked, DuplicateTransaction, InsufficientFunds, InvalidAmount, NotDisputed,
    UnknownTransaction,
};
use toy_payments_engine::transaction::{Transaction, TxKind};
use toy_payments_engine::transaction_processor::{ClientAccount, TransactionProcessor};
use toy_payments_engine::transaction_routing::TransactionConsumer;

/// Applies each transaction in order and checks that it has the expected outcome.
fn apply_transactions(
    processor: &mut TransactionProcessor,
    steps: Vec<(Transaction, Result<(), TransactionRejectionReason>)>,
) {
    for (index, (transaction, expected)) in steps.into_iter().enumerate() {
        assert_eq!(
            processor.process_transaction(transaction),
            expected,
            "Step {index}: unexpected outcome for {transaction:?}"
        );
    }
}

/// Returns the account of `client`, which must exist.
fn get_account(processor: &TransactionProcessor, client: u16) -> ClientAccount {
    processor
        .get_accounts()
        .find(|&(id, _)| id == client)
        .map(|(_, account)| *account)
        .unwrap_or_else(|| panic!("Client {client} should have an account"))
}

/// Builds an expected account from amounts in text form.
fn build_account(available: &str, held: &str, locked: bool) -> ClientAccount {
    ClientAccount {
        available: parse_amount(available),
        held: parse_amount(held),
        locked,
    }
}

/// Returns the processor's output rows, sorted, as their order is unspecified.
fn sorted_rows(processor: &TransactionProcessor) -> Vec<String> {
    let mut rows: Vec<String> = processor.to_string().lines().map(String::from).collect();
    rows.sort();
    rows
}

// =============
// Test deposits
// =============

/// A deposit adds its amount to the available funds.
#[test]
fn deposit_credits_available_funds() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(&mut processor, vec![(deposit(1, 1, "1.5"), Ok(()))]);

    assert_eq!(get_account(&processor, 1), build_account("1.5", "0", false));
}

/// Successive deposits add up in the available funds.
#[test]
fn deposits_accumulate() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.5"), Ok(())),
            (deposit(1, 2, "2.25"), Ok(())),
            (deposit(1, 3, "0.0001"), Ok(())),
        ],
    );

    assert_eq!(
        get_account(&processor, 1),
        build_account("3.7501", "0", false)
    );
}

/// A deposit reusing an earlier deposit's ID is rejected.
#[test]
fn deposit_with_existing_id_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.0"), Ok(())),
            (deposit(1, 1, "5.0"), Err(DuplicateTransaction)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("1.0", "0", false));
}

/// A deposit of a zero or negative amount is rejected.
#[test]
fn deposit_of_non_positive_amount_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.0"), Ok(())),
            (
                transaction(1, 2, TxKind::Deposit(Decimal::ZERO)),
                Err(InvalidAmount),
            ),
            (
                transaction(1, 3, TxKind::Deposit(Decimal::NEGATIVE_ONE)),
                Err(InvalidAmount),
            ),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("1.0", "0", false));
}

/// A deposit above the maximum amount is rejected, while the maximum itself is accepted.
#[test]
fn deposit_above_the_maximum_amount_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1000000000000000000"), Ok(())),
            (
                deposit(1, 2, "1000000000000000000.0001"),
                Err(InvalidAmount),
            ),
            (
                transaction(1, 3, TxKind::Deposit(Decimal::MAX)),
                Err(InvalidAmount),
            ),
        ],
    );

    assert_eq!(
        get_account(&processor, 1),
        build_account("1000000000000000000", "0", false)
    );
}

// ================
// Test withdrawals
// ================

/// A withdrawal subtracts its amount from the available funds.
#[test]
fn withdrawal_debits_available_funds() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "5.0"), Ok(())),
            (withdrawal(1, 2, "1.5"), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("3.5", "0", false));
}

/// A withdrawal may take the available funds down to exactly zero.
#[test]
fn withdrawal_of_all_available_funds_is_allowed() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.5"), Ok(())),
            (withdrawal(1, 2, "2.5"), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("0", "0", false));
}

/// A withdrawal larger than the available funds is rejected.
#[test]
fn withdrawal_exceeding_available_funds_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (withdrawal(1, 2, "2.0001"), Err(InsufficientFunds)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("2.0", "0", false));
}

/// A withdrawal of a zero or negative amount is rejected, so it cannot add funds.
#[test]
fn withdrawal_of_non_positive_amount_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.0"), Ok(())),
            (
                transaction(1, 2, TxKind::Withdrawal(Decimal::ZERO)),
                Err(InvalidAmount),
            ),
            (
                transaction(1, 3, TxKind::Withdrawal(Decimal::NEGATIVE_ONE)),
                Err(InvalidAmount),
            ),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("1.0", "0", false));
}

/// A withdrawal above the maximum amount is rejected, before the funds are even checked.
#[test]
fn withdrawal_above_the_maximum_amount_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.0"), Ok(())),
            (
                withdrawal(1, 2, "1000000000000000000.0001"),
                Err(InvalidAmount),
            ),
            (
                transaction(1, 3, TxKind::Withdrawal(Decimal::MAX)),
                Err(InvalidAmount),
            ),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("1.0", "0", false));
}

/// Held funds are not available, so they cannot be withdrawn.
#[test]
fn withdrawal_of_held_funds_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (deposit(1, 2, "1.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (withdrawal(1, 3, "1.5"), Err(InsufficientFunds)),
            (withdrawal(1, 4, "1.0"), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("0", "2.0", false));
}

/// Only deposits can be disputed, so disputing a withdrawal is rejected.
#[test]
fn withdrawal_cannot_be_disputed() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (withdrawal(1, 2, "1.0"), Ok(())),
            (dispute(1, 2), Err(UnknownTransaction)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("1.0", "0", false));
}

// =============
// Test disputes
// =============

/// A dispute moves the deposit's amount from available to held funds.
#[test]
fn dispute_moves_deposit_to_held_funds() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (deposit(1, 2, "3.0"), Ok(())),
            (dispute(1, 1), Ok(())),
        ],
    );

    assert_eq!(
        get_account(&processor, 1),
        build_account("3.0", "2.0", false)
    );
}

/// A dispute of a transaction that does not exist is rejected.
#[test]
fn dispute_of_unknown_transaction_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (dispute(1, 2), Err(UnknownTransaction)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("2.0", "0", false));
}

/// A client cannot dispute another client's deposit.
#[test]
fn dispute_of_another_clients_deposit_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (dispute(2, 1), Err(UnknownTransaction)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("2.0", "0", false));
    assert_eq!(get_account(&processor, 2), build_account("0", "0", false));
}

/// A dispute of a deposit that is already disputed is rejected.
#[test]
fn dispute_already_open_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (dispute(1, 1), Err(DuplicateTransaction)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("0", "2.0", false));
}

/// Disputing funds that were already withdrawn makes the available funds negative.
#[test]
fn dispute_after_withdrawal_makes_available_funds_negative() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "10.0"), Ok(())),
            (withdrawal(1, 2, "8.0"), Ok(())),
            (dispute(1, 1), Ok(())),
        ],
    );

    assert_eq!(
        get_account(&processor, 1),
        build_account("-8.0", "10.0", false)
    );
}

// =============
// Test resolves
// =============

/// A resolve moves the disputed amount from held back to available funds.
#[test]
fn resolve_returns_held_funds() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (deposit(1, 2, "3.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (resolve(1, 1), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("5.0", "0", false));
}

/// A resolve is rejected unless the deposit is currently disputed.
#[test]
fn resolve_without_open_dispute_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (resolve(1, 1), Err(NotDisputed)),
            (resolve(1, 2), Err(NotDisputed)),
            (dispute(1, 1), Ok(())),
            (resolve(1, 1), Ok(())),
            (resolve(1, 1), Err(NotDisputed)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("2.0", "0", false));
}

/// A deposit whose dispute was resolved can be disputed again.
#[test]
fn resolved_deposit_can_be_disputed_again() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (resolve(1, 1), Ok(())),
            (dispute(1, 1), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("0", "2.0", false));
}

// ================
// Test chargebacks
// ================

/// A chargeback removes the disputed amount from held funds and locks the account.
#[test]
fn chargeback_removes_held_funds_and_locks_the_account() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (deposit(1, 2, "3.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (chargeback(1, 1), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("3.0", "0", true));
}

/// A chargeback is rejected unless the deposit is currently disputed.
#[test]
fn chargeback_without_open_dispute_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (chargeback(1, 1), Err(NotDisputed)),
            (chargeback(1, 2), Err(NotDisputed)),
            (dispute(1, 1), Ok(())),
            (resolve(1, 1), Ok(())),
            (chargeback(1, 1), Err(NotDisputed)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("2.0", "0", false));
}

/// A charged-back deposit is forgotten, so nothing can refer to it any more.
#[test]
fn chargeback_is_final() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (chargeback(1, 1), Ok(())),
            (dispute(1, 1), Err(UnknownTransaction)),
            (resolve(1, 1), Err(NotDisputed)),
            (chargeback(1, 1), Err(NotDisputed)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("0", "0", true));
}

/// Charging back funds that were already withdrawn leaves the account in debt.
#[test]
fn chargeback_after_withdrawal_leaves_negative_funds() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "10.0"), Ok(())),
            (withdrawal(1, 2, "8.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (chargeback(1, 1), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("-8.0", "0", true));
}

// ======================
// Test negative balances
// ======================

/// No withdrawal is possible while the available funds are negative.
#[test]
fn withdrawal_with_negative_available_funds_is_rejected() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "10.0"), Ok(())),
            (withdrawal(1, 2, "8.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (withdrawal(1, 3, "0.0001"), Err(InsufficientFunds)),
        ],
    );

    assert_eq!(
        get_account(&processor, 1),
        build_account("-8.0", "10.0", false)
    );
}

/// Deposits pay off negative available funds and can bring them back above zero.
#[test]
fn deposit_reduces_negative_available_funds() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "10.0"), Ok(())),
            (withdrawal(1, 2, "8.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (deposit(1, 3, "5.0"), Ok(())),
        ],
    );
    assert_eq!(
        get_account(&processor, 1),
        build_account("-3.0", "10.0", false)
    );

    apply_transactions(&mut processor, vec![(deposit(1, 4, "4.0"), Ok(()))]);
    assert_eq!(
        get_account(&processor, 1),
        build_account("1.0", "10.0", false)
    );
}

/// Resolving the dispute brings negative available funds back to their earlier value.
#[test]
fn resolve_restores_negative_available_funds() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "10.0"), Ok(())),
            (withdrawal(1, 2, "8.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (resolve(1, 1), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("2.0", "0", false));
}

// ====================
// Test locked accounts
// ====================

/// A locked account rejects deposits and withdrawals, checking the lock first.
#[test]
fn locked_account_rejects_deposits_and_withdrawals() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (deposit(1, 2, "3.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (chargeback(1, 1), Ok(())),
            (deposit(1, 3, "1.0"), Err(AccountLocked)),
            (withdrawal(1, 4, "1.0"), Err(AccountLocked)),
            // The lock is checked first, even for a deposit that reuses an ID.
            (deposit(1, 2, "1.0"), Err(AccountLocked)),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("3.0", "0", true));
}

/// Disputes, resolves and chargebacks of the remaining deposits still apply when locked.
#[test]
fn locked_account_allows_the_dispute_lifecycle() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.0"), Ok(())),
            (deposit(1, 2, "2.0"), Ok(())),
            (deposit(1, 3, "4.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (chargeback(1, 1), Ok(())),
            (dispute(1, 2), Ok(())),
            (resolve(1, 2), Ok(())),
            (dispute(1, 2), Ok(())),
            (dispute(1, 2), Err(DuplicateTransaction)),
            (chargeback(1, 2), Ok(())),
            (dispute(1, 3), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("0", "4.0", true));
}

/// Locking one client's account does not affect other clients.
#[test]
fn lock_applies_only_to_its_own_client() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "2.0"), Ok(())),
            (deposit(2, 2, "3.0"), Ok(())),
            (dispute(1, 1), Ok(())),
            (chargeback(1, 1), Ok(())),
            (deposit(2, 3, "1.0"), Ok(())),
            (withdrawal(2, 4, "0.5"), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("0", "0", true));
    assert_eq!(get_account(&processor, 2), build_account("3.5", "0", false));
}

// =============
// Test accounts
// =============

/// A client's first transaction creates its account, even if it is rejected.
#[test]
fn rejected_transaction_still_creates_the_account() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (withdrawal(1, 1, "1.0"), Err(InsufficientFunds)),
            (dispute(2, 2), Err(UnknownTransaction)),
            (resolve(3, 3), Err(NotDisputed)),
            (chargeback(4, 4), Err(NotDisputed)),
        ],
    );

    for client in 1..=4 {
        assert_eq!(
            get_account(&processor, client),
            build_account("0", "0", false)
        );
    }
}

/// Each client's transactions affect only that client's account.
#[test]
fn clients_are_independent() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "5.0"), Ok(())),
            (deposit(2, 2, "7.0"), Ok(())),
            (withdrawal(1, 3, "1.0"), Ok(())),
            (withdrawal(2, 4, "8.0"), Err(InsufficientFunds)),
            (dispute(2, 2), Ok(())),
        ],
    );

    assert_eq!(get_account(&processor, 1), build_account("4.0", "0", false));
    assert_eq!(get_account(&processor, 2), build_account("0", "7.0", false));
    assert_eq!(processor.get_accounts().count(), 2);
}

// ===========
// Test output
// ===========

/// A processor without accounts displays no rows.
#[test]
fn processor_without_accounts_displays_nothing() {
    assert_eq!(TransactionProcessor::default().to_string(), "");
}

/// An account is displayed as a `client,available,held,total,locked` row.
#[test]
fn account_is_displayed_as_a_csv_row() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.5"), Ok(())),
            (deposit(1, 2, "2.0"), Ok(())),
            (dispute(1, 2), Ok(())),
        ],
    );

    assert_eq!(processor.to_string(), "1,1.5,2.0,3.5,false\n");
}

/// Amounts are displayed as computed, keeping the scale of the input.
#[test]
fn amounts_are_displayed_as_computed() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(&mut processor, vec![(deposit(1, 1, "1.2340"), Ok(()))]);

    assert_eq!(processor.to_string(), "1,1.2340,0,1.2340,false\n");
}

/// Every account is displayed, including locked, empty and negative ones.
#[test]
fn every_account_is_displayed() {
    let mut processor = TransactionProcessor::default();
    apply_transactions(
        &mut processor,
        vec![
            (deposit(1, 1, "1.0"), Ok(())),
            (deposit(2, 2, "2.0"), Ok(())),
            (dispute(2, 2), Ok(())),
            (chargeback(2, 2), Ok(())),
            (withdrawal(3, 3, "1.0"), Err(InsufficientFunds)),
            (deposit(4, 4, "10.0"), Ok(())),
            (withdrawal(4, 5, "3.0"), Ok(())),
            (dispute(4, 4), Ok(())),
        ],
    );

    assert_eq!(
        sorted_rows(&processor),
        vec![
            "1,1.0,0,1.0,false",
            "2,0.0,0.0,0.0,true",
            "3,0,0,0,false",
            "4,-3.0,10.0,7.0,false",
        ]
    );
}
