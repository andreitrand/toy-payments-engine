//! Application entry point: processes a CSV file of transactions.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use toy_payments_engine::input_csv::CsvTransactionProducer;

use toy_payments_engine::transaction_processor::TransactionProcessor;
use toy_payments_engine::{EngineError, run_with_single_transaction_producer};

const NUM_PROCESSORS: usize = 1;
const CHANNEL_CAPACITY: usize = 1024;

/// Processes a CSV file of client transactions and prints the final account states.
#[derive(Parser)]
struct Args {
    /// Path to the local CSV file.
    transactions_file_path: PathBuf,
}

fn main() -> ExitCode {
    let args: Args = Args::parse();

    match run(&args.transactions_file_path) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Processes the CSV file at `input_file_path` and prints the final account states.
fn run(input_file_path: &Path) -> Result<(), EngineError> {
    // Create a streaming transaction producer from a local CSV file.
    let producer = CsvTransactionProducer::new(input_file_path)?;

    // Run the payments engine on one transaction producer (the CSV file), using a single processor.
    let _ = run_with_single_transaction_producer::<CsvTransactionProducer, TransactionProcessor>(
        producer,
        NUM_PROCESSORS,
        CHANNEL_CAPACITY,
    )?;

    // TODO: print the final account states to stdout in CSV format.
    Ok(())
}
