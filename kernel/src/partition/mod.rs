//! Partition utilities for Delta table writes.
//!
//! A partition value goes through multiple transformations before it reaches the Delta
//! log. Each step is handled by a separate submodule. The differences between steps are
//! subtle and easy to get wrong, so this documentation is thorough with examples observed
//! from real Spark-written Delta tables.
//!
//! # Steps
//!
//! ```text
//! Scalar value (e.g., String("US/East"))
//!         |
//!         |  Step 1: validation     -->  (type/key checks against table schema)
//!         |  Step 2: serialization  -->  "US/East"      (add.partitionValues)
//!         |  Step 3: path encoding  -->  "US%2FEast"    (filesystem directory name)
//!         |  Step 4: URI encoding   -->  "US%252FEast"  (add.path in the Delta log)
//!         v
//!   AddFile { partitionValues: {"region": "US/East"}, path: "region=US%252FEast/..." }
//! ```
//!
//! - **Step 1: Validation** (`validation` module): checks partition column
//!   key completeness and value types against the table schema. Runs first to reject bad
//!   input before doing any serialization work.
//!
//! - **Step 2: Serialization** (`serialization` module): converts validated
//!   typed `Scalar` values to protocol-compliant strings for `add.partitionValues`. Null,
//!   empty string, and empty binary all serialize as JSON `null`.
//!
//! - **Step 3: Path encoding** (`hive` module): percent-encodes serialized strings for
//!   Hive-style filesystem directory names. Only a small set of ASCII characters is
//!   encoded; spaces and non-ASCII pass through raw. Hive-style paths are optional; the
//!   Delta protocol does not require them. Kernel uses them for partitioned tables without
//!   column mapping enabled. Happens lazily in the write path.
//!
//! - **Step 4: URI encoding**: an additional layer of URI encoding per RFC 2396 applied
//!   when constructing `add.path` in the Delta log. More aggressive than step 3:
//!   characters like space (0x20), `<`, `>`, `{`, `}`, and `|` that survive Hive
//!   encoding get percent-encoded. Characters already percent-encoded by step 3
//!   (e.g., `%2F`) get double-encoded (e.g., `%252F`).
//!
//! # Encoding by type
//!
//! Shows how each data type serializes across all three forms. Numeric and date types are
//! straightforward (no special characters to encode), but are included here for
//! completeness since the string representation in `partitionValues` must match exactly.
//!
//! ```text
//! Row | Type           | Input Value                  | Filesystem Dir                            | add.path           | partitionValues.p
//! ----+----------------+------------------------------+-------------------------------------------+--------------------+----------------------------
//!  1  | INT            | 0                            | p=0                                       | same               | "0"
//!  2  |                | -1                           | p=-1                                      | same               | "-1"
//!  3  |                | 2147483647 (max)             | p=2147483647                              | same               | "2147483647"
//!  4  |                | -2147483648 (min)            | p=-2147483648                             | same               | "-2147483648"
//!  5  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//!  6  | BIGINT         | 9223372036854775807 (max)    | p=9223372036854775807                     | same               | "9223372036854775807"
//!  7  |                | -9223372036854775808 (min)   | p=-9223372036854775808                    | same               | "-9223372036854775808"
//!  8  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//!  9  | TINYINT        | 127 (max)                    | p=127                                     | same               | "127"
//! 10  |                | -128 (min)                   | p=-128                                    | same               | "-128"
//! 11  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 12  | SMALLINT       | 32767 (max)                  | p=32767                                   | same               | "32767"
//! 13  |                | -32768 (min)                 | p=-32768                                  | same               | "-32768"
//! 14  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 15  | DOUBLE         | 0.0                          | p=0.0                                     | same               | "0.0"
//! 16  |                | -0.0                         | p=0.0                                     | same               | "0.0"
//! 17  |                | 1.7976931348623157E308 (max) | p=1.7976931348623157E308                  | same               | "1.7976931348623157E308"
//! 18  |                | 4.9E-324 (min positive)      | p=4.9E-324                                | same               | "4.9E-324"
//! 19  |                | NaN                          | p=NaN                                     | same               | "NaN"
//! 20  |                | Infinity                     | p=Infinity                                | same               | "Infinity"
//! 21  |                | -Infinity                    | p=-Infinity                               | same               | "-Infinity"
//! 22  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 23  | FLOAT          | 0.0                          | p=0.0                                     | same               | "0.0"
//! 24  |                | NaN                          | p=NaN                                     | same               | "NaN"
//! 25  |                | Infinity                     | p=Infinity                                | same               | "Infinity"
//! 26  |                | -Infinity                    | p=-Infinity                               | same               | "-Infinity"
//! 27  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 28  | BOOLEAN        | true                         | p=true                                    | same               | "true"
//! 29  |                | false                        | p=false                                   | same               | "false"
//! 30  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 31  | DECIMAL(38,18) | 0                            | p=0.000000000000000000                    | same               | "0.000000000000000000"
//! 32  |                | 1.23                         | p=1.230000000000000000                    | same               | "1.230000000000000000"
//! 33  |                | -1.23                        | p=-1.230000000000000000                   | same               | "-1.230000000000000000"
//! 34  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 35  | DATE           | 2024-01-01                   | p=2024-01-01                              | same               | "2024-01-01"
//! 36  |                | 1970-01-01 (epoch)           | p=1970-01-01                              | same               | "1970-01-01"
//! 37  |                | 0001-01-01                   | p=0001-01-01                              | same               | "0001-01-01"
//! 38  |                | 9999-12-31                   | p=9999-12-31                              | same               | "9999-12-31"
//! 39  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 40  | TIMESTAMP      | 2024-06-15 12:30:45          | p=2024-06-15 12%3A30%3A45                 | p=...%2012%253A... | "2024-06-15T19:30:45.000000Z"
//! 41  |                | 1970-01-01 00:00:00          | p=1970-01-01 00%3A00%3A00                 | p=...%2000%253A... | "1970-01-01T08:00:00.000000Z"
//! 42  |                | 2024-06-15 23:59:59.999999   | p=2024-06-15 23%3A59%3A59.999999          | p=...%2023%253A... | "2024-06-16T06:59:59.999999Z"
//! 43  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! 44  | TIMESTAMP_NTZ  | 2024-06-15 12:30:45          | p=2024-06-15 12%3A30%3A45                 | p=...%2012%253A... | "2024-06-15 12:30:45"
//! 45  |                | 1970-01-01 00:00:00          | p=1970-01-01 00%3A00%3A00                 | p=...%2000%253A... | "1970-01-01 00:00:00"
//! 46  |                | NULL                         | p=__HIVE_DEFAULT_PARTITION__              | same               | null
//! ```
//!
//! Notable behaviors:
//! - DOUBLE `-0.0` normalizes to `"0.0"` everywhere.
//! - DECIMAL always includes the full scale (18 trailing digits for `DECIMAL(38,18)`).
//! - TIMESTAMP `partitionValues` are UTC ISO-8601 with microsecond precision. The
//!   filesystem dir and `add.path` use local time without timezone.
//! - TIMESTAMP_NTZ `partitionValues` omit the timezone and use space-separated format.
//!
//! # String and binary encoding
//!
//! String values are the interesting case because they can contain characters that
//! require encoding. Binary values are interpreted as raw bytes and treated as strings
//! for path construction.
//!
//! ```text
//! Row | Input Value              | Filesystem Dir                 | add.path                     | partitionValues.p
//! ----+--------------------------+--------------------------------+------------------------------+------------------
//! 47  | chr(0) (NUL)             | WRITE FAILS*                   | N/A                          | N/A
//! 48  | before\0after            | WRITE FAILS*                   | N/A                          | N/A
//! 49  | X'' (empty binary)       | p=__HIVE_DEFAULT_PARTITION__   | same                         | null
//! 50  | X'DEADBEEF'              | p=<raw high bytes>             | same                         | <invalid UTF-8>**
//! 51  | X'48454C4C4F' ("HELLO")  | p=HELLO                        | same                         | "HELLO"
//! 52  | X'00FF' (has NUL)        | WRITE FAILS*                   | N/A                          | N/A
//! 53  | X'2F3D25' (/=%)          | p=%2F%3D%25                    | p=%252F%253D%2525            | "/=%"
//! 54  | a{b                      | p=a%7Bb                        | p=a%257Bb                    | "a{b"
//! 55  | a}b                      | p=a}b                          | p=a%7Db                      | "a}b"
//! 56  | hello world              | p=hello world                  | p=hello%20world              | "hello world"
//! 57  | M\u{00FC}nchen           | p=M\u{00FC}nchen               | same                         | "M\u{00FC}nchen"
//! 58  | 日本語                    | p=日本語                        | same                         | "日本語"
//! 59  | 🎵🎶                      | p=🎵🎶                         | same                         | "🎵🎶"
//! 60  | a<b>c|d                  | p=a<b>c|d                      | p=a%3Cb%3Ec%7Cd              | "a<b>c|d"
//! 61  | a@b!c(d)                 | p=a@b!c(d)                     | same                         | "a@b!c(d)"
//! 62  | a&b+c$d;e,f              | p=a&b+c$d;e,f                  | same                         | "a&b+c$d;e,f"
//! 63  | Serbia/srb%              | p=Serbia%2Fsrb%25              | p=Serbia%252Fsrb%2525        | "Serbia/srb%"
//! 64  | 100%25 (literal)         | p=100%2525                     | p=100%252525                 | "100%25"
//! 65  | '' (empty string)        | p=__HIVE_DEFAULT_PARTITION__   | same                         | null
//! 66  | NULL                     | p=__HIVE_DEFAULT_PARTITION__   | same                         | null
//! 67  | ' ' (1 space)            | p= (trailing space)            | p=%20                        | " "
//! 68  | '  ' (2 spaces)          | p=  (trailing spaces)          | p=%20%20                     | "  "
//! ```
//!
//! *WRITE FAILS: Delta-Spark fails at the filesystem `mkdirs` stage when the
//! Hive-encoded directory name contains NUL bytes. The encoding itself succeeds
//! (NUL encodes to `%00`), but the filesystem rejects the path.
//!
//! Key observations:
//!
//! - `add.path` applies an additional layer of URI encoding on top of the Hive-encoded
//!   directory name. Characters that Hive leaves raw (space, `<`, `>`, `}`, `|`) get
//!   percent-encoded in `add.path`. Characters that Hive already encoded (`/` -> `%2F`)
//!   get double-encoded in `add.path` (`%2F` -> `%252F`).
//! - `partitionValues` stores the raw value with no encoding. Null and empty string are
//!   both represented as JSON `null`.
//! - NUL bytes cause write failures at the filesystem level (`mkdirs` error).
//! - Non-empty binary values are interpreted as raw bytes. If the bytes happen to be
//!   valid UTF-8, they work (e.g., `X'48454C4C4F'` = "HELLO").
//!
//! **Spark bug: non-UTF-8 binary (e.g., `X'DEADBEEF'`) passes raw bytes through to the
//! filesystem path and `partitionValues` JSON, producing invalid UTF-8 in both. The JSON
//! becomes unparseable. In kernel, `escape_partition_value` takes `&str`, so Rust's type
//! system prevents non-UTF-8 bytes from reaching the encoding layer.

pub(crate) mod hive;
pub(crate) mod serialization;
pub(crate) mod validation;
