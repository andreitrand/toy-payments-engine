//! Tests that CSV input is parsed into exactly the expected transactions, skipping invalid rows.

mod common;

use std::io::{self, Read};
use std::path::Path;

use common::{MockTransactionProcessor, chargeback, deposit, dispute, resolve, withdrawal};
use toy_payments_engine::input_csv::CsvTransactionProducer;
use toy_payments_engine::transaction::Transaction;
use toy_payments_engine::transaction_engine::EngineOutput;
use toy_payments_engine::{EngineError, run_with_single_transaction_producer};

/// Capacity of the worker's channel.
const CHANNEL_CAPACITY: usize = 16;

/// Parses `input` as CSV and returns the transactions it produced, in input order.
fn parse(input: &str) -> Vec<Transaction> {
    let producer =
        CsvTransactionProducer::from_reader(input.as_bytes()).expect("The header should be valid");

    // A single worker receives every transaction, in the order they were parsed.
    let EngineOutput { mut consumers, .. } = run_with_single_transaction_producer::<
        _,
        MockTransactionProcessor,
    >(producer, 1, CHANNEL_CAPACITY)
    .expect("Parsing should complete without errors");

    consumers
        .pop()
        .expect("The engine should return one consumer")
        .transactions
}

/// Returns the error for `input`, whose header is expected to be rejected.
fn header_error(input: &str) -> io::Error {
    match CsvTransactionProducer::from_reader(input.as_bytes()) {
        Err(EngineError::Read(err)) => err,
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("The header should be rejected"),
    }
}

/// Asserts that `input` is rejected for having the header `found` (as reported, after trimming).
fn assert_invalid_header(input: &str, found: &str) {
    let err = header_error(input);

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        format!("Expected header 'type,client,tx,amount', found '{found}'")
    );
}

/// Reader that returns `data` and then fails, as a broken file or connection would.
struct FailingReader {
    data: &'static [u8],
}

impl Read for FailingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.data.is_empty() {
            return Err(io::Error::other("Simulated read failure"));
        }
        self.data.read(buf)
    }
}

/// Every transaction type is parsed into the matching transaction.
#[test]
fn all_transaction_kinds_are_parsed() {
    let input = "type,client,tx,amount
                 deposit,1,1,1.0
                 withdrawal,1,2,0.5
                 dispute,1,1,
                 resolve,1,1,
                 chargeback,1,1,";

    assert_eq!(
        parse(input),
        vec![
            deposit(1, 1, "1.0"),
            withdrawal(1, 2, "0.5"),
            dispute(1, 1),
            resolve(1, 1),
            chargeback(1, 1),
        ]
    );
}

/// Spaces and tabs around fields and header names are ignored.
#[test]
fn whitespace_around_fields_and_header_is_ignored() {
    let input = "  type ,   client,tx  , amount
                 deposit ,  1,   1 ,  1.5
                 withdrawal,\t2\t,\t3\t,\t0.25\t";

    assert_eq!(
        parse(input),
        vec![deposit(1, 1, "1.5"), withdrawal(2, 3, "0.25")]
    );
}

/// Windows (CRLF) line endings are accepted.
#[test]
fn crlf_line_endings_are_supported() {
    let input = "type,client,tx,amount\r\ndeposit,1,1,1.0\r\ndispute,1,1,\r\n";

    assert_eq!(parse(input), vec![deposit(1, 1, "1.0"), dispute(1, 1)]);
}

/// Empty lines between rows are skipped.
#[test]
fn empty_lines_are_skipped() {
    let input = "type,client,tx,amount\n\ndeposit,1,1,1.0\n\n\ndeposit,1,2,2.0\n\n";

    assert_eq!(
        parse(input),
        vec![deposit(1, 1, "1.0"), deposit(1, 2, "2.0")]
    );
}

/// Quotes have no special meaning, so quoted fields are invalid and their rows are skipped.
#[test]
fn quoted_fields_are_skipped() {
    let input = "type,client,tx,amount
                 \"deposit\",1,1,1.5
                 deposit,\"1\",2,1.5
                 deposit,1,3,\"2.5\"
                 deposit,1,4,1.0";

    assert_eq!(parse(input), vec![deposit(1, 4, "1.0")]);
}

/// A stray quote invalidates only its own row, not the rows after it.
#[test]
fn stray_quote_affects_only_its_own_row() {
    let input = "type,client,tx,amount
                 deposit,1,1,5.0
                 dispute,1,1,\"
                 deposit,1,2,100.0
                 withdrawal,1,3,1.0";

    assert_eq!(
        parse(input),
        vec![
            deposit(1, 1, "5.0"),
            deposit(1, 2, "100.0"),
            withdrawal(1, 3, "1.0"),
        ]
    );
}

/// Rows with more than four fields are skipped, such as one with an unquoted `1,000`.
#[test]
fn rows_with_more_than_four_fields_are_skipped() {
    let input = "type,client,tx,amount
                 deposit,1,1,1.0,extra
                 deposit,1,2,1,000
                 deposit,1,3,1.0,
                 dispute,1,1,,
                 dispute,1,1,,extra,more
                 deposit,1,4,1.0
                 dispute,1,4,";

    assert_eq!(parse(input), vec![deposit(1, 4, "1.0"), dispute(1, 4)]);
}

/// Rows without a client or transaction ID are skipped.
#[test]
fn rows_with_too_few_fields_are_skipped() {
    let input = "type,client,tx,amount
                 deposit
                 deposit,1
                 deposit,1,1,1.0";

    assert_eq!(parse(input), vec![deposit(1, 1, "1.0")]);
}

/// Transaction types are recognised in any letter case.
#[test]
fn transaction_type_is_case_insensitive() {
    let input = "type,client,tx,amount
                 Deposit,1,1,1.0
                 DEPOSIT,1,2,2.0
                 wItHdRaWaL,1,3,0.5
                 DISPUTE,1,1,
                 Resolve,1,1,
                 ChargeBack,1,2,";

    assert_eq!(
        parse(input),
        vec![
            deposit(1, 1, "1.0"),
            deposit(1, 2, "2.0"),
            withdrawal(1, 3, "0.5"),
            dispute(1, 1),
            resolve(1, 1),
            chargeback(1, 2),
        ]
    );
}

/// Rows with an unknown or empty transaction type are skipped.
#[test]
fn unknown_or_empty_types_are_skipped() {
    let input = "type,client,tx,amount
                 transfer,1,1,1.0
                 ,1,2,1.0
                 deposits,1,3,1.0
                 deposit,1,4,1.0";

    assert_eq!(parse(input), vec![deposit(1, 4, "1.0")]);
}

/// Client and transaction IDs are accepted over their full range, with leading zeros.
#[test]
fn ids_cover_their_full_range() {
    let input = "type,client,tx,amount
                 deposit,0,0,1.0
                 deposit,65535,4294967295,1.0
                 deposit,01,007,1.0";

    assert_eq!(
        parse(input),
        vec![
            deposit(0, 0, "1.0"),
            deposit(u16::MAX, u32::MAX, "1.0"),
            deposit(1, 7, "1.0"),
        ]
    );
}

/// Rows with out-of-range, negative, fractional, non-numeric or empty IDs are skipped.
#[test]
fn invalid_ids_are_skipped() {
    let input = "type,client,tx,amount
                 deposit,65536,1,1.0
                 deposit,-1,2,1.0
                 deposit,abc,3,1.0
                 deposit,1.5,4,1.0
                 deposit,,5,1.0
                 deposit,1,4294967296,1.0
                 deposit,1,-1,1.0
                 deposit,1,abc,1.0
                 deposit,1,,1.0
                 deposit,1,6,1.0";

    assert_eq!(parse(input), vec![deposit(1, 6, "1.0")]);
}

/// Amounts with up to four decimals are accepted, from 0.0001 up to the maximum.
#[test]
fn valid_amount_formats_are_parsed() {
    let input = "type,client,tx,amount
                 deposit,1,1,7
                 deposit,1,2,1.5
                 deposit,1,3,1.2345
                 deposit,1,4,0.0001
                 deposit,1,5,007.5
                 withdrawal,1,6,1000000000000000000
                 withdrawal,1,7,999999999999999999.9999";

    assert_eq!(
        parse(input),
        vec![
            deposit(1, 1, "7"),
            deposit(1, 2, "1.5"),
            deposit(1, 3, "1.2345"),
            deposit(1, 4, "0.0001"),
            deposit(1, 5, "7.5"),
            withdrawal(1, 6, "1000000000000000000"),
            withdrawal(1, 7, "999999999999999999.9999"),
        ]
    );
}

/// Amounts written as anything but plain digits with up to four decimals are skipped.
#[test]
fn malformed_amounts_are_skipped() {
    let input = "type,client,tx,amount
                 deposit,1,1,.5
                 deposit,1,2,5.
                 deposit,1,3,1e3
                 deposit,1,4,1E3
                 deposit,1,5,1_000
                 deposit,1,6,+1.5
                 deposit,1,7,-1.0
                 deposit,1,8,1.2.3
                 deposit,1,9,1. 5
                 deposit,1,10,1 .5
                 deposit,1,11,\"1,5\"
                 deposit,1,12,1.23456
                 deposit,1,13,1.50000
                 deposit,1,14,abc
                 deposit,1,15,0x10
                 deposit,1,16,\u{0663}
                 withdrawal,1,17,-0.5
                 withdrawal,1,18,1e-3
                 deposit,1,19,1.0";

    assert_eq!(parse(input), vec![deposit(1, 19, "1.0")]);
}

/// Zero amounts, however they are written, are skipped.
#[test]
fn non_positive_amounts_are_skipped() {
    let input = "type,client,tx,amount
                 deposit,1,1,0
                 deposit,1,2,000
                 deposit,1,3,0.0
                 withdrawal,1,4,0.0000
                 deposit,1,5,1.0";

    assert_eq!(parse(input), vec![deposit(1, 5, "1.0")]);
}

/// Amounts above the maximum of 10^18 are skipped.
#[test]
fn amounts_above_the_maximum_are_skipped() {
    let input = "type,client,tx,amount
                 deposit,1,1,1000000000000000000.0001
                 deposit,1,2,1000000000000000001
                 withdrawal,1,3,79228162514264337593543950335
                 withdrawal,1,4,79228162514264337593543950336
                 withdrawal,1,5,99999999999999999999999999999999
                 deposit,1,6,1.0";

    assert_eq!(parse(input), vec![deposit(1, 6, "1.0")]);
}

/// Deposits and withdrawals with a missing or empty amount are skipped.
#[test]
fn deposits_and_withdrawals_without_amount_are_skipped() {
    let input = "type,client,tx,amount
                 deposit,1,1
                 deposit,1,2,
                 withdrawal,1,3
                 withdrawal,1,4,
                 deposit,1,5,1.0";

    assert_eq!(parse(input), vec![deposit(1, 5, "1.0")]);
}

/// Disputes, resolves and chargebacks are only kept without an amount, valid or not.
#[test]
fn amounts_on_disputes_resolves_and_chargebacks_are_skipped() {
    let kinds = [
        ("dispute", dispute as fn(u16, u32) -> Transaction),
        ("resolve", resolve),
        ("chargeback", chargeback),
    ];

    for (name, expected_kind) in kinds {
        let input = format!(
            "type,client,tx,amount
             {name},1,1
             {name},1,2,
             {name},1,3,\t\t
             {name},1,4,5.0
             {name},1,5,1
             {name},1,6,0
             {name},1,7,-5
             {name},1,8,1.23456
             {name},1,9,abc
             {name},1,10,1e3"
        );

        assert_eq!(
            parse(&input),
            vec![
                expected_kind(1, 1),
                expected_kind(1, 2),
                expected_kind(1, 3)
            ],
            "Only {name} rows without an amount should be kept"
        );
    }
}

/// A header without rows produces no transactions.
#[test]
fn header_only_produces_nothing() {
    assert_eq!(parse("type,client,tx,amount\n"), vec![]);
}

/// Header names are recognised in any letter case.
#[test]
fn header_names_are_case_insensitive() {
    let input = "TYPE,Client,tX,AmOuNt
                 deposit,1,1,1.0
                 dispute,1,1,";

    assert_eq!(parse(input), vec![deposit(1, 1, "1.0"), dispute(1, 1)]);
}

/// A header missing any of the four columns is rejected.
#[test]
fn missing_columns_are_rejected() {
    assert_invalid_header("client,tx,amount\n1,1,1.0", "client,tx,amount");
    assert_invalid_header("type,tx,amount\ndeposit,1,1.0", "type,tx,amount");
    assert_invalid_header("type,client,amount\ndeposit,1,1.0", "type,client,amount");
    assert_invalid_header("type,client,tx\ndispute,1,1", "type,client,tx");
}

/// A header with a misspelt column name is rejected.
#[test]
fn misspelt_columns_are_rejected() {
    assert_invalid_header("kind,client,tx,amount\n", "kind,client,tx,amount");
    assert_invalid_header("type,clinet,tx,amount\n", "type,clinet,tx,amount");
    assert_invalid_header("type,client,txid,amount\n", "type,client,txid,amount");
    assert_invalid_header("type,client,tx,amounts\n", "type,client,tx,amounts");
}

/// A header with the columns in another order is rejected.
#[test]
fn reordered_columns_are_rejected() {
    assert_invalid_header("amount,tx,type,client\n", "amount,tx,type,client");
    assert_invalid_header("client,type,tx,amount\n", "client,type,tx,amount");
}

/// A header with a repeated column is rejected.
#[test]
fn duplicate_columns_are_rejected() {
    assert_invalid_header("type,type,client,tx\n", "type,type,client,tx");
    assert_invalid_header(
        "type,client,tx,amount,amount\n",
        "type,client,tx,amount,amount",
    );
}

/// A header with an extra column, even an empty one, is rejected.
#[test]
fn extra_columns_are_rejected() {
    assert_invalid_header("type,client,tx,amount,note\n", "type,client,tx,amount,note");
    assert_invalid_header("type,client,tx,amount,\n", "type,client,tx,amount,");
}

/// Input that starts with a data row instead of a header is rejected.
#[test]
fn input_without_header_is_rejected() {
    assert_invalid_header("deposit,1,1,1.0\ndeposit,1,2,2.0\n", "deposit,1,1,1.0");
}

/// Empty input, which has no header, is rejected.
#[test]
fn empty_input_is_rejected() {
    assert_invalid_header("", "");
    assert_invalid_header("\n\n", "");
}

/// A read failure while reading the header is reported as a read error.
#[test]
fn read_error_in_header_is_reported() {
    let result = CsvTransactionProducer::from_reader(FailingReader { data: b"" });

    assert!(matches!(result, Err(EngineError::Read(_))));
}

/// A read failure after the header stops the run with a read error.
#[test]
fn read_error_after_the_header_stops_the_run() {
    let reader = FailingReader {
        data: b"type,client,tx,amount\ndeposit,1,1,1.0\n",
    };
    let producer = CsvTransactionProducer::from_reader(reader).expect("The header should be valid");

    let result = run_with_single_transaction_producer::<_, MockTransactionProcessor>(
        producer,
        1,
        CHANNEL_CAPACITY,
    );

    assert!(matches!(result, Err(EngineError::Read(_))));
}

/// A file that cannot be opened is reported as an open error.
#[test]
fn missing_file_is_reported() {
    let result = CsvTransactionProducer::new(Path::new("this/file/does/not/exist.csv"));

    assert!(matches!(result, Err(EngineError::Open { .. })));
}
