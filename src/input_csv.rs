//! Transaction producer that streams records from a CSV file.

use std::fs::File;
use std::path::Path;

use crate::EngineError;
use crate::transaction::Transaction;
use crate::transaction_routing::{TransactionProducer, TransactionRouter};

/// Streams [`Transaction`]s from a CSV file with a `type, client, tx, amount` header.
///
/// Records are read and validated one at a time. The reader accepts:
/// - Whitespace around fields and headers
/// - Rows without the trailing `amount` column (e.g. `dispute, 1, 1`)
/// - Transaction types in any letter case
pub struct CsvTransactionProducer {
    reader: csv::Reader<File>,
}

impl CsvTransactionProducer {
    /// Opens the CSV file at `path`. Returns [`EngineError::Open`] if the file cannot be opened.
    pub fn new(path: &Path) -> Result<Self, EngineError> {
        let file = File::open(path).map_err(|source| EngineError::Open {
            path: path.to_path_buf(),
            source,
        })?;

        let reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .from_reader(file);

        Ok(Self { reader })
    }
}

impl TransactionProducer for CsvTransactionProducer {
    /// Reads every record and routes each valid transaction. Invalid records are silently skipped.
    fn send_all_transactions(&mut self, router: TransactionRouter) -> Result<(), EngineError> {
        for transaction in self.reader.deserialize::<Transaction>() {
            match transaction {
                Ok(transaction) => router.submit(transaction)?,
                Err(err) => {
                    if let csv::ErrorKind::Io(source) = err.into_kind() {
                        return Err(EngineError::Read(source));
                    }
                }
            }
        }
        Ok(())
    }
}
