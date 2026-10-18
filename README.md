# Toy Payments Engine

## Summary

This program reads a CSV file of client transactions (deposits, withdrawals, disputes, resolves
and chargebacks), applies them to the clients' accounts, and prints the final state of every
account as a CSV.

### *A simple solution*

A payments engine for one file could be implemented relatively easily: a CSV reader streams the
transactions, one at a time, to a single transaction processor, so the file is never loaded into
memory as a whole. The processor keeps, per client:

- the account - its balances (available and held; the total is their sum) and whether it is locked
- the deposits and withdrawals, by transaction ID
- the transactions currently under dispute

After the last transaction, the processor prints every account's balances.

This handles files of any size, as memory only grows with the number of transactions kept for later
disputes, not with the size of the input. However, it processes a single source of transactions, on
a single thread.

### *A multi-threaded producer-consumer solution*

A natural next step is running the engine in a server, with transactions coming from multiple
concurrent sources. The simple design cannot serve that efficiently, since all sources would have
to contend with a single transaction processor. We therefore chose a producer-consumer design that
keeps the simple processor but runs several of them in parallel:

- Any number of **producers** (a CSV file here, with TCP streams in a potential server) turn their input
  into a standard transaction form
- Any number of **consumers** (worker threads) listen for transactions and process them
- A **router** sends every transaction for a given client ID always to the same **consumer**
- As a client's transactions all go in order to one consumer, consumers never share accounts,
  so no synchronization is needed - this effectively shards the transaction space by client ID
- Once all producers have finished, the consumers return their accounts for display

The current solution could be upgraded in the future to also support the following:

- Async producers (for example with Tokio), so that thousands of streams do not need a thread each
- Memory off-loading to storage, if a consumer's state becomes too large to reside in main memory

## Architecture

```text
                     +--------------------------+
transactions.csv --> |  CsvTransactionProducer  |  Reads and validates one row at a time
                     +------------+-------------+
                                  | Transaction
                     +------------v-------------+
                     |    TransactionRouter     |  Worker = client_id % number_of_workers
                     +-----+--------------+-----+
                           |              |        One bounded channel per worker
                     +-----v-----+  +-----v-----+
                     | worker 0  |  | worker N  |  Each owns a TransactionProcessor
                     +-----+-----+  +-----+-----+  for its own client group
                           |              |
                     +-----v--------------v-----+
                     |    TransactionEngine     |  Joins the workers, merges statistics
                     +------------+-------------+
                                  |
                 write_accounts --> stdout, statistics --> stderr (optional)
```

### *Modules*

| Module | Contents |
|---|---|
| `transaction.rs` | The definition and validation of a `Transaction` (containing client ID, transaction ID and `TxKind`). |
| `input_csv.rs` | The `CsvTransactionProducer` checks the CSV header, then validates and streams individual rows from the CSV file. |
| `transaction_routing.rs` | The `TransactionProducer` and `TransactionConsumer` traits, together with the `TransactionRouter`. |
| `transaction_engine.rs` | The `TransactionEngine` (the worker pool) and its output, `EngineOutput`. |
| `transaction_processor.rs` | The `TransactionProcessor` enforces the business rules and updates the client accounts. |
| `lib.rs` | Error enums (`EngineError`, `TransactionRejectionReason`) and support functions. |
| `main.rs` | The application entry point: runs the engine on one CSV file and prints the result. |

### *How the components interact*

- **Producers** implement `TransactionProducer`. The application uses only the CSV producer; the
  tests add an in-memory one.
- **The router** picks the worker as `client % workers` and sends over that worker's bounded
  channel, so each client's transactions keep their input order.
- **Consumers** implement `TransactionConsumer`; the application uses one `TransactionProcessor`
  per worker.
- **The engine** starts the workers, hands out routers, and joins the workers once every router has
  been dropped. It returns each worker's consumer and the merged statistics (`EngineOutput`).
- **Output**: `write_accounts` writes the CSV header, then the rows of every processor.

### *Per-client state*

Each processor keeps a `HashMap<u16, ClientState>`, created on a client's first transaction:

| Field | Purpose |
|---|---|
| `account: ClientAccount` | Contains the `available`, `held` and `locked` fields, with `total` computed as `available + held`. |
| `deposits: HashMap<u32, Decimal>` | Holds the amounts of the client's deposits which can still be disputed, by transaction ID. |
| `disputed: HashSet<u32>` | The IDs of the client's deposit transactions that are currently under dispute. |

*Important:* Unlike in the simple solution, here we apply withdrawals but do not store them, since the business rules for withdrawal disputes are not currently defined. We therefore save memory by storing only the deposit transactions.

### *Application configuration parameters*

The following parameters are available in `src/main.rs` to change the application's configuration (after a rebuild):

| Constant | Default | Meaning |
|---|---|---|
| `NUM_PROCESSORS` | `1` | The number of worker threads - affects performance, not correctness (any value will output the same result). |
| `CHANNEL_CAPACITY` | `1024` | Transactions that can wait in each worker's channel before the producer blocks. |
| `SHOW_TRANSACTION_STATS` | `false` | Print processing statistics to `stderr` after the engine has run. |

The statistics look like this:

```text
Transactions succeeded: 10
Transactions rejected: 3
- Account is locked: 1
- Duplicate transaction: 1
- Transaction is not under dispute: 1
```

## Application Behavior

### 1) *Basics*

- Builds with `cargo build` on Rust 2024. `cargo clippy --all-targets`, `cargo fmt --check` and `cargo doc` report no warnings.
- Exposes the CLI: `cargo run -- transactions.csv > accounts.csv`.
- Reads a CSV with the header `type, client, tx, amount` (case-insensitive), accepting whitespace around fields and up
  to four decimal places for amounts.
- Writes a CSV with the header `client,available,held,total,locked` and one row per client.
- Rows are printed in no particular order, and amounts are printed as computed (e.g. `1.5`, `1.2340`, `0`).

### 2) *Completeness*

All five transaction types are handled:

| Type | Effect |
|---|---|
| `deposit` | Adds the amount to `available`. |
| `withdrawal` | Subtracts the amount from `available`, if enough is available. |
| `dispute` | Moves a deposit's amount from `available` to `held`. |
| `resolve` | Moves a disputed amount from `held` back to `available`. |
| `chargeback` | Removes a disputed amount from `held` and locks the account. |

*Note*: A locked account only prevents deposits and withdrawals - the other 3 transactions are always allowed.

References to transactions that do not exist, or are not under dispute, are rejected. See
[Assumptions](#assumptions) for the cases where the expected behavior is open to interpretation.

### 3) *Correctness*

Correctness is checked by 92 integration tests in four suites, which follow the structure of the code:

| Suite | Tests | What it checks |
|---|---|---|
| `tests/transaction_processor.rs` | 36 | Every business rule, step by step: each transaction's exact outcome (accepted, or the rejection reason) and the account afterwards; covers duplicates, negative balances, locked accounts, the dispute lifecycle and the output format. |
| `tests/input.rs` | 30 | CSV parsing: header validation, whitespace, case, CRLF, quotes, field counts, ID ranges and the exact amount format; tests mix valid and invalid rows. |
| `tests/transaction_routing.rs` | 18 | The engine and router with a mock consumer: every transaction delivered once, to the right worker, in order, for 1 to 16 workers, channel capacities from 1 to above the input size, several concurrent producers, and a panicking worker. |
| `tests/transaction_engine.rs` | 8 | End-to-end tests: CSV files through the engine with real processors, with 1 and 4 workers, compared with expected output files and statistics. |

The entire test suite can be run via:
```sh
cargo test
```

**Sample data.** The end-to-end inputs and their expected outputs are in `tests/data/`, as
`<scenario>_input.csv` and `<scenario>_expected_output.csv`. They can also be run directly:

```sh
cargo run -- tests/data/dispute_lifecycle_over_several_clients_input.csv
```

The expected outputs are compared by meaning: row order, spacing and the number of decimals of
round values do not matter. Three tests check the comparison itself to ensure it actually catches real differences.

**Randomized routing tests** use a small seeded generator (`SplitMix64`) to allow for reproducible runs. Inputs of up to 100,000 transactions are checked for exact delivery and ordering.

Inside the application itself, all balance arithmetic uses checked operations, and every change is computed before any state is updated, so a rejected transaction never leaves a partial change.

**Type system enforcements:**

- A `Transaction` carries an amount only where one is meaningful: `TxKind::Deposit(Decimal)` and
  `TxKind::Withdrawal(Decimal)`. Disputes, resolves and chargebacks cannot carry one.
- Every rejection is a `TransactionRejectionReason` variant rather than a string, and every fatal error is an `EngineError` variant.
- Amounts are `rust_decimal::Decimal`, so there is no floating-point rounding.

### 4) *Safety and Robustness*

- No `unsafe`, and no `unwrap` or `expect` in the library or binary. The only panics are two
  assertions on the engine's configuration (at least one worker and a non-zero channel capacity).
- Errors are handled in three tiers: I/O problems or panicked workers stop the run with exit
  code 1; invalid CSV input rows are skipped; transactions that break a rule are rejected and counted.
- **Amount cap.** Amounts above `10^18` are rejected, both when parsing and in the processor. Without
  it, crafted amounts close to `Decimal::MAX` could make `Decimal` round results instead of failing
  (silently creating or losing money) or overflow when the total is computed.
- **Quotes are not special.** No field ever needs quoting, so quote handling is disabled. A stray
  `"` then invalidates only its own row, instead of swallowing the rest of the file into one field.
- **Strict input.** The header must be a case-insensitive match for exactly `type, client, tx, amount`,
  so a file in an unexpected format fails loudly instead of producing empty output. Amounts must be
  plain digits with up to four decimals (see [Assumptions](#input)).
- **Workers.** A panicking worker makes the run fail with an error instead of hanging or printing
  partial results.

### 5) *Efficiency*

- **Streaming.** The input is read one row at a time and never loaded as a whole.
- **Backpressure.** Each worker has a bounded channel (`1,024` transactions by default). When workers
  fall behind, the reader waits instead of buffering input.
- **Memory.** The only state that grows with the input is the deposit history: about 20 bytes per
  deposit plus the hash map's overhead. Withdrawals are not stored, disputes only while they are
  open, and charged-back deposits are removed. Accounts are bounded by the 65,536 possible clients.
  See [Known limitations](#known-limitations) for the worst case.
- **Many concurrent sources.** Any number of producers, each with its own router, can feed the same
  workers (e.g. one per TCP stream in a server), and adding workers spreads the clients over more
  cores.

### 6) *Maintainability*

- Each module has one responsibility: parsing, routing, the worker pool and the business rules are
  separate, and connected only through the `TransactionProducer` and `TransactionConsumer` traits.
  The business rules can be read in one file (`transaction_processor.rs`) without any threading code.
- Every public module, type and function has a doc comment (`cargo doc --no-deps --open`).
- Every test has a one-line summary of what it checks, and each test suite matches one part of the code.

## Assumptions

Where the expected behavior is open to interpretation, we followed what (we believe) a bank would do.

### *Input*

| Assumption | Reason |
|---|---|
| The header must be exactly `type, client, tx, amount` (case-insensitive, surrounding whitespace allowed). Anything else, including an empty file, is a fatal error. | See [Strict input](#4-safety-and-robustness). |
| Transaction types are accepted in any letter case. | Harmless, and more forgiving towards partners. |
| A row may omit the `amount` column (`dispute, 1, 1`). A row with more than four fields is skipped. | Disputes have no amount. An extra field means the row is not what it seems (an unquoted `1,000` would otherwise be read as `1`). |
| Quotes have no special meaning, so quoted fields are invalid. | See [Quotes are not special](#4-safety-and-robustness). |
| Amounts must be positive plain digits with up to four decimals (`7`, `1.5`, `0.0001`), and at most `10^18`. | Anything else (`1e3`, `1_000`, `+1.5`, `.5`, `-1`) is a malformed amount, not one to reinterpret. The cap is explained under [Amount cap](#4-safety-and-robustness). |
| Deposits and withdrawals need an amount. Disputes, resolves and chargebacks must have none. | A dispute refers to a deposit by ID. A row that also gives an amount is malformed, whatever the value. |
| Invalid rows are skipped silently and do not generate a transaction. | A malformed row is an error on the sender's side. Processing the rest of the input is more useful than stopping. |
| An I/O error while reading stops the run. Other read problems (e.g. invalid UTF-8) skip the row. | An I/O error means the rest of the input is unknown, so the result would be incomplete. |

### *Transaction processing*

| Assumption | Reason |
|---|---|
| An account is created by a client's first transaction, even if it is rejected, and is printed with zero balances. | Every client that appears in the input gets an account, so the output covers every client mentioned. |
| Only deposits can be disputed. A dispute, resolve or chargeback that references a withdrawal is rejected. | Disputing a withdrawal would mean crediting funds that already left the bank. Business rules for this scenario would have to be defined first. |
| A dispute holds the full deposited amount, even if part of it has been withdrawn. The `available` balance may then become negative. | This is a known fraud pattern: deposit, withdraw, then reverse the deposit. The client owes the difference. |
| While `available` is negative, withdrawals are rejected and deposits are accepted (reducing the debt). | A client can never withdraw more than they have. |
| A dispute repeated while still open is rejected as a duplicate. | It would hold the same funds twice. |
| After a resolve, the deposit can be disputed again. | A resolved dispute is closed, and a new claim can be raised later under specific circumstances. |
| A chargeback is final: the deposit is forgotten and cannot be disputed again. | A chargeback reverses the deposit, which ends its dispute for good. |
| A locked account rejects deposits and withdrawals, but disputes, resolves and chargebacks of its other deposits still apply. Nothing unlocks it. | Freezing an account stops the client from moving money, but claims already raised with partners still have to be settled. |
| Transaction IDs are expected to be globally unique. Duplicates are only detected where it costs nothing: a deposit reusing one of the client's deposit IDs, and a repeated open dispute. | Detecting every duplicate would mean remembering every ID, including withdrawals. |
| A rejected transaction leaves the account unchanged. | A failed withdrawal must not change the funds, and the same holds for any rejection. |

## Known limitations

- **Deposit history grows without bound.** Every deposit is kept so that it can be disputed later.
  In the worst case (all `u32` transaction IDs used as deposits), that is over 100 GB. A real system
  would only keep deposits for a dispute window, or move older ones to storage.
- **Skipped rows are not counted.** The statistics count rejected transactions, but not rows that
  failed to parse.
- **Not every duplicate ID is detected** (see [Assumptions](#transaction-processing)).
- **Precision at extreme balances.** Balances are exact up to about `7.9 * 10^24`. Beyond that, `Decimal` starts to round the fractional part.
- **Producers block.** Each producer runs on its own thread and blocks when a channel is full. For
  thousands of concurrent streams, producers should become async tasks (e.g. with Tokio). Ordering is
  guaranteed within each stream, but not across streams.
- **A panicking worker** is usually reported as unavailable (`WorkerUnavailable`) rather than as
  panicked, as the producer notices it first.
