# Delta Kernel Go

go bindings for delta-kernel-rs via FFI. reads delta tables, extracts schemas, scans parquet files.

## quick start

```bash
# build rust ffi + go bindings
make build

# build example binary
make example

# run tests
make test-acceptance
```

## basic usage

```go
// open table at latest version
snapshot, _ := delta.NewSnapshot("/path/to/table")
defer snapshot.Close()

// or open at specific version (time travel)
snapshotV0, _ := delta.NewSnapshotAtVersion("/path/to/table", 0)
defer snapshotV0.Close()

// get metadata
version := snapshot.Version()
partitions, _ := snapshot.PartitionColumns()

// scan files
scan, _ := snapshot.Scan()
defer scan.Close()

schema, _ := scan.LogicalSchema()
fmt.Println(schema.String())

// iterate files
iter, _ := scan.MetadataIterator(snapshot.Engine())
for {
    hasMore, _ := iter.Next(visitor)
    if !hasMore { break }
}

// read data
readIter, _ := scan.ReadFile(snapshot.Engine(), fileMeta)
for {
    hasMore, _ := readIter.Next(dataVisitor)
    if !hasMore { break }
}
```

## what works

- ✅ snapshot creation + metadata (version, partitions, table root)
- ✅ time travel (read table at specific version)
- ✅ schema extraction (all types including nested struct/array/map)
- ✅ scan metadata iteration (file lists, stats, partition values)
- ✅ parquet data reading via arrow c interface
- ✅ arrow data access (columns, rows, values)
- ✅ 16/19 acceptance tests passing

## what's missing

**reading limitations:**
- ❌ no predicate pushdown - reads all files
- ❌ no schema projection - reads all columns
- ❌ no deletion vectors - can't handle row deletes

**not implemented:**
- ❌ writes (transactions, commits)
- ❌ CDC (table changes)
- ❌ expression evaluation
- ❌ custom engine config

## FFI coverage

**implemented (12 functions):**
- `snapshot`, `snapshot_at_version`, `get_default_engine`, `scan`
- `scan_logical_schema`, `scan_physical_schema`
- `scan_metadata_iter_init`, `scan_metadata_next`
- `read_parquet_file`, `read_result_next`
- `get_raw_arrow_data`, `visit_field_*`

**missing high priority:**
- `selection_vector_from_dv` - deletion vectors
- `row_indexes_from_dv` - apply deletes
- predicate/projection in `scan()`

**missing medium priority:**
- `table_changes_*` - CDC
- `new_expression_evaluator` - predicates
- `evaluate_expression` - filters

**missing low priority:**
- `transaction`, `commit` - writes
- `get_domain_metadata` - custom metadata
- `with_engine_info` - engine config

## architecture

```
Go API (delta/)
  ↓ cgo
C helpers (delta/c/)
  ↓ FFI
Rust kernel
```

**design patterns:**
- handles wrap rust pointers, must call `Close()`
- visitor pattern for iteration (zero-copy where possible)
- arrow c interface for data exchange

## structure

```
ffi/go/
├── delta/                 # go package, main go files
│   └── c/                 # cgo helpers
└── examples/main.go       # examplar cli tool
```

## building

```bash
make build          # rust + go
make example        # cli tool
make test           # all tests
make test-acceptance # acceptance only
make fmt && make vet # format + lint
```

## example tool

```bash
# show schema
./bin/example describe -table /path/to/table

# scan files
./bin/example read -table /path/to/table

# read data (shows up to 50 rows/batch in table format)
./bin/example read -table /path/to/table -read-data
```

## memory management

always close resources:
```go
snapshot, _ := delta.NewSnapshot(path)
defer snapshot.Close()  // must call

scan, _ := snapshot.Scan()
defer scan.Close()  // must call

iter, _ := scan.MetadataIterator(engine)
defer iter.Close()  // must call
```

## known issues

**standalone binary segfault:** example crashes on startup. workaround: use `go test` instead of `go build`. tests work fine.

**no predicate pushdown:** `scan()` ignores filter params, reads everything.

**limited errors:** only error codes exposed, no messages from rust.

## acceptance tests

runs official delta acceptance test suite. location: `delta/acceptance_test.go`

results: 16/19 pass, 3 skip (cdf, deletion_vectors, iceberg_compat_v1)

test discovery: auto-scans `acceptance/tests/dat/out/reader_tests/generated/`

## next steps

- add predicate pushdown (high priority)
- add schema projection (high priority)
- add deletion vectors (high priority)
- fix standalone binary segfault
- implement CDC
- implement writes
