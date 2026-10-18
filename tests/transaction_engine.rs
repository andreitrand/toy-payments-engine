//! End-to-end tests: CSV input runs through the engine, and the written output and statistics
//! are compared with the expected ones.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use toy_payments_engine::TransactionRejectionReason::{
    self, AccountLocked, DuplicateTransaction, InsufficientFunds, NotDisputed, UnknownTransaction,
};
use toy_payments_engine::input_csv::CsvTransactionProducer;
use toy_payments_engine::transaction_engine::{EngineOutput, TransactionStats};
use toy_payments_engine::transaction_processor::TransactionProcessor;
use toy_payments_engine::{run_with_single_transaction_producer, write_accounts};

/// Capacity of each worker's channel.
const CHANNEL_CAPACITY: usize = 16;

/// Worker counts that every scenario runs with. All of them must give the same result.
const WORKER_COUNTS: [usize; 2] = [1, 4];

/// Column names of the output header.
const OUTPUT_HEADER: [&str; 5] = ["client", "available", "held", "total", "locked"];

/// The values of one output row, compared by meaning rather than by text.
#[derive(Debug, PartialEq)]
struct OutputRow {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

/// Returns the path of the scenario data file `<scenario>_<kind>.csv`.
fn data_file(scenario: &str, kind: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(format!("{scenario}_{kind}.csv"))
}

/// Runs the CSV file at `input_path` through an engine of `num_processors` processors and
/// returns the written output, with the transaction statistics.
fn single_test_run(input_path: &Path, num_processors: usize) -> (String, TransactionStats) {
    let producer = CsvTransactionProducer::new(input_path).unwrap_or_else(|err| panic!("{err}"));

    let EngineOutput { consumers, stats } = run_with_single_transaction_producer::<
        _,
        TransactionProcessor,
    >(producer, num_processors, CHANNEL_CAPACITY)
    .expect("The engine should complete without errors");

    let mut output = Vec::new();
    write_accounts(&mut output, &consumers).expect("Writing to memory should not fail");

    (
        String::from_utf8(output).expect("The output should be valid UTF-8"),
        stats,
    )
}

/// Parses CSV output into its rows by client.
///
/// Row order, whitespace around fields and the scale of amounts (`2` or `2.0000`) do not
/// matter. Fails on a wrong header, a malformed row or a client that appears more than once.
fn parse_output(output: &str) -> Result<BTreeMap<u16, OutputRow>, String> {
    let mut lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());

    let header: Vec<&str> = lines
        .next()
        .ok_or("Missing header")?
        .split(',')
        .map(str::trim)
        .collect();
    if header != OUTPUT_HEADER {
        return Err(format!("Unexpected header {header:?}"));
    }

    let mut rows = BTreeMap::new();
    for line in lines {
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        let [client, available, held, total, locked] = fields[..] else {
            return Err(format!("Row '{line}' does not have 5 fields"));
        };

        let amount = |field: &str| {
            field
                .parse::<Decimal>()
                .map_err(|_| format!("Invalid amount '{field}' in row '{line}'"))
        };
        let client: u16 = client
            .parse()
            .map_err(|_| format!("Invalid client in row '{line}'"))?;
        let row = OutputRow {
            available: amount(available)?,
            held: amount(held)?,
            total: amount(total)?,
            locked: locked
                .parse()
                .map_err(|_| format!("Invalid locked flag in row '{line}'"))?,
        };

        if rows.insert(client, row).is_some() {
            return Err(format!("Client {client} appears more than once"));
        }
    }

    Ok(rows)
}

/// Builds the expected statistics from the number of successes and the rejection counts.
fn build_stats(succeeded: u64, rejected: &[(TransactionRejectionReason, u64)]) -> TransactionStats {
    TransactionStats {
        succeeded,
        rejected: rejected.iter().copied().collect(),
    }
}

/// Runs the input file of `scenario` with every worker count and checks that each run produces
/// the accounts of its expected output file and the expected statistics.
fn test_run_across_workers(scenario: &str, expected_stats: &TransactionStats) {
    let input_path = data_file(scenario, "input");
    let expected_path = data_file(scenario, "expected_output");

    let expected_output = fs::read_to_string(&expected_path)
        .unwrap_or_else(|err| panic!("Cannot read '{}': {err}", expected_path.display()));
    let expected_rows =
        parse_output(&expected_output).expect("The expected output should be valid");

    for num_processors in WORKER_COUNTS {
        let (output, stats) = single_test_run(&input_path, num_processors);
        let rows = parse_output(&output).expect("The output should be valid");

        assert_eq!(
            rows, expected_rows,
            "Unexpected output with {num_processors} worker(s):\n{output}"
        );
        assert_eq!(
            &stats, expected_stats,
            "Unexpected statistics with {num_processors} worker(s)"
        );
    }
}

// ======================
// Test output comparison
// ======================

/// The comparison ignores row order, whitespace around fields and the scale of amounts.
#[test]
fn output_comparison_ignores_order_spacing_and_scale() {
    let output = "client,available,held,total,locked
                  1,1.5,0,1.5,false
                  2,2,0,2,true";
    let equivalent = "  client , available,held,  total ,locked
                      2 , 2.0000 , 0.0 , 2.00 , true

                      1,1.50,0.0000,1.5,false";

    assert_eq!(parse_output(output), parse_output(equivalent));
}

/// The comparison still detects any difference in the accounts.
#[test]
fn output_comparison_detects_differences() {
    let header = "client,available,held,total,locked";
    let base = format!("{header}\n1,1.5,0,1.5,false\n2,2,0,2,true");

    for different in [
        format!("{header}\n1,1.5001,0,1.5,false\n2,2,0,2,true"),
        format!("{header}\n1,1.5,0,1.5,false\n2,2,0,2,false"),
        format!("{header}\n1,1.5,0,1.5,false"),
        format!("{header}\n1,1.5,0,1.5,false\n2,2,0,2,true\n3,0,0,0,false"),
        format!("{header}\n1,1.5,0,1.5,false\n3,2,0,2,true"),
    ] {
        assert_ne!(parse_output(&base), parse_output(&different), "{different}");
    }
}

/// The comparison rejects output that is not valid, rather than comparing it.
#[test]
fn output_comparison_rejects_invalid_output() {
    for invalid in [
        "",
        "client,available,held,locked,total\n",
        "client,available,held,total\n",
        "1,1.5,0,1.5,false\n",
        "client,available,held,total,locked\n1,1.5,0,1.5\n",
        "client,available,held,total,locked\n1,abc,0,1.5,false\n",
        "client,available,held,total,locked\n1,1.5,0,1.5,yes\n",
        "client,available,held,total,locked\nx,1.5,0,1.5,false\n",
        "client,available,held,total,locked\n1,1.5,0,1.5,false\n1,2,0,2,false\n",
    ] {
        assert!(parse_output(invalid).is_err(), "{invalid}");
    }
}

// ==============
// Test scenarios
// ==============
//
// Each scenario reads `tests/data/<test name>_input.csv` and expects the accounts in
// `tests/data/<test name>_expected_output.csv`.

/// Deposits and withdrawals over several clients, with a withdrawal exceeding the funds.
#[test]
fn deposits_and_withdrawals() {
    test_run_across_workers(
        "deposits_and_withdrawals",
        &build_stats(6, &[(InsufficientFunds, 1)]),
    );
}

/// Resolves, chargebacks, locked accounts and negative funds over several clients.
#[test]
fn dispute_lifecycle_over_several_clients() {
    test_run_across_workers(
        "dispute_lifecycle_over_several_clients",
        &build_stats(
            10,
            &[
                (AccountLocked, 1),
                (DuplicateTransaction, 1),
                (NotDisputed, 1),
            ],
        ),
    );
}

/// Invalid rows are skipped without being counted or creating accounts, unlike rejections.
#[test]
fn invalid_rows_are_skipped_and_rejections_counted() {
    test_run_across_workers(
        "invalid_rows_are_skipped_and_rejections_counted",
        &build_stats(
            2,
            &[
                (DuplicateTransaction, 1),
                (InsufficientFunds, 1),
                (UnknownTransaction, 1),
            ],
        ),
    );
}

/// Fifty interleaved clients spread over the workers. Even clients end up charged back.
#[test]
fn many_clients_across_workers() {
    test_run_across_workers("many_clients_across_workers", &build_stats(150, &[]));
}

/// Input with only a header produces only the output header.
#[test]
fn header_only_input_produces_no_accounts() {
    test_run_across_workers(
        "header_only_input_produces_no_accounts",
        &build_stats(0, &[]),
    );
}
