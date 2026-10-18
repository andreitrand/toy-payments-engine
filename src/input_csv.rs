//! Transaction producer that streams records from CSV input.

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use crate::EngineError;
use crate::transaction::Transaction;
use crate::transaction_routing::{TransactionProducer, TransactionRouter};

/// The columns of the header, which must appear exactly once each and in this order.
const EXPECTED_HEADER: [&str; 4] = ["type", "client", "tx", "amount"];

/// Streams [`Transaction`]s from CSV input with a `type, client, tx, amount` header.
///
/// Records are read and validated one at a time. The reader accepts:
/// - Whitespace around fields and header names
/// - Header names and transaction types in any letter case
/// - Rows without the trailing `amount` field (e.g. `dispute, 1, 1`)
///
/// Rows with more than four fields or those that contain quotes are skipped.
pub struct CsvTransactionProducer<R: Read = File> {
    reader: csv::Reader<R>,
}

impl CsvTransactionProducer {
    /// Opens the CSV file at `path` and reads its header.
    ///
    /// Returns [`EngineError::Open`] if the file cannot be opened, or [`EngineError::Read`]
    /// if its header cannot be read or is invalid.
    pub fn new(path: &Path) -> Result<Self, EngineError> {
        let file = File::open(path).map_err(|source| EngineError::Open {
            path: path.to_path_buf(),
            source,
        })?;

        Self::from_reader(file)
    }
}

impl<R: Read> CsvTransactionProducer<R> {
    /// Reads CSV records from `reader`, such as an in-memory buffer or a network stream.
    ///
    /// The header is read immediately. Returns [`EngineError::Read`] if it cannot be read,
    /// or if it is not exactly `type, client, tx, amount` (in any letter case).
    pub fn from_reader(reader: R) -> Result<Self, EngineError> {
        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .quoting(false)
            .from_reader(reader);

        let headers = reader
            .headers()
            .map_err(|err| EngineError::Read(err.into()))?;

        let is_expected = headers.len() == EXPECTED_HEADER.len()
            && headers
                .iter()
                .zip(EXPECTED_HEADER)
                .all(|(header, expected)| header.eq_ignore_ascii_case(expected));

        if !is_expected {
            let found: Vec<&str> = headers.iter().collect();
            return Err(EngineError::Read(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected header '{}', found '{}'",
                    EXPECTED_HEADER.join(","),
                    found.join(",")
                ),
            )));
        }

        Ok(Self { reader })
    }
}

impl<R: Read> TransactionProducer for CsvTransactionProducer<R> {
    /// Reads every record and routes each valid transaction. Invalid records, including those
    /// with more than four fields, are silently skipped.
    fn send_all_transactions(&mut self, router: TransactionRouter) -> Result<(), EngineError> {
        let mut record = csv::StringRecord::new();

        loop {
            match self.reader.read_record(&mut record) {
                Ok(true) => {}
                Ok(false) => return Ok(()),
                Err(err) => match err.into_kind() {
                    // An I/O error prevents reading the rest of the file so we have to stop.
                    csv::ErrorKind::Io(source) => return Err(EngineError::Read(source)),
                    // Other errors (invalid UTF-8 etc.) only affect this record, so we skip it.
                    _ => continue,
                },
            }

            // The header was validated on construction, so fields are matched by position.
            if record.len() <= EXPECTED_HEADER.len()
                && let Ok(transaction) = record.deserialize::<Transaction>(None)
            {
                router.submit(transaction)?;
            }
        }
    }
}
