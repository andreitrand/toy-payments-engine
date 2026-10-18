//! Application entry point: processes a CSV file of transactions.

use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;

use toy_payments_engine::input_csv::CsvTransactionProducer;
use toy_payments_engine::transaction_engine::EngineOutput;
use toy_payments_engine::transaction_processor::TransactionProcessor;
use toy_payments_engine::{EngineError, run_with_single_transaction_producer, write_accounts};

/// The number of independent transaction processors (threads).
const NUM_PROCESSORS: usize = 1;

/// Each processor's communication channel capacity.
const CHANNEL_CAPACITY: usize = 1024;

/// Whether to show transaction processing statistics after the engine runs.
const SHOW_TRANSACTION_STATS: bool = false;

/// Processes a CSV file of client transactions and prints the final account states.
#[derive(Parser)]
struct Args {
    /// Path to the local CSV file.
    transactions_file_path: PathBuf,
}

/// Processes the CSV file at `input_file_path` and prints the final account states.
fn run_payments_engine(input_file_path: &Path) -> Result<(), EngineError> {
    // Create a streaming transaction producer from a local CSV file.
    let producer = CsvTransactionProducer::new(input_file_path)?;

    // Run the payments engine on one transaction producer (the CSV file).
    let EngineOutput { consumers, stats } = run_with_single_transaction_producer::<
        CsvTransactionProducer,
        TransactionProcessor,
    >(producer, NUM_PROCESSORS, CHANNEL_CAPACITY)?;

    // Print the final account states of every processor.
    write_accounts(BufWriter::new(io::stdout().lock()), &consumers).map_err(EngineError::Write)?;

    // Show transaction processing statistics, including detailed rejections.
    if SHOW_TRANSACTION_STATS {
        eprint!("{stats}");
    }

    Ok(())
}

fn main() -> ExitCode {
    let args: Args = Args::parse();

    match run_payments_engine(&args.transactions_file_path) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}
