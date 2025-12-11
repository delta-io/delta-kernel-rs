# Delta Kernel Go Bindings

✅ **Minimal Working Example** - Successfully reads Delta tables and retrieves version information!

Go CGO wrapper for the Delta Kernel Rust FFI library.

## Quick Start

### Build Everything:
```bash
cd ffi/go

# Option 1: Use the build script
./build.sh

# Option 2: Use Make
make all
```

### Run the Example:
```bash
export DYLD_LIBRARY_PATH=$PWD/../../target/release:$DYLD_LIBRARY_PATH  # macOS
export LD_LIBRARY_PATH=$PWD/../../target/release:$LD_LIBRARY_PATH      # Linux

./examples/describe_schema /path/to/delta/table
```

### Example Output:
```
Opening Delta table at: ../../test_table

✓ Successfully opened table
  Version: 1

Table root: file:///Users/andrei.tserakhau/RustroverProjects/delta-kernel-rs/test_table/

Table has no partition columns

Creating scan...
Scan table root: file:///Users/andrei.tserakhau/RustroverProjects/delta-kernel-rs/test_table/

Logical Schema:
- logical_schema: visitor_pattern_not_implemented

Physical Schema:
- physical_schema: visitor_pattern_not_implemented

Note: Full schema extraction via visitor pattern will be implemented next.
```

**With partitioned table**:
```
Opening Delta table at: ../../acceptance/tests/dat/out/reader_tests/generated/multi_partitioned/delta

✓ Successfully opened table
  Version: 2

Table root: file:///Users/andrei.tserakhau/RustroverProjects/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/multi_partitioned/delta/

Partition columns:
  - letter
  - date
  - data

Creating scan...
Scan table root: file:///Users/andrei.tserakhau/RustroverProjects/delta-kernel-rs/acceptance/tests/dat/out/reader_tests/generated/multi_partitioned/delta/

Logical Schema:
- logical_schema: visitor_pattern_not_implemented

Physical Schema:
- physical_schema: visitor_pattern_not_implemented

Note: Full schema extraction via visitor pattern will be implemented next.
```

## Structure

```
ffi/go/
├── delta/
│   ├── schema.go          # Schema types (basic implementation)
│   ├── snapshot.go        # Snapshot operations (✅ WORKING!)
│   └── scan.go           # Scan operations (✅ WORKING!)
├── examples/
│   └── describe_schema.go # Working example
├── go.mod
├── Makefile              # Build automation
├── build.sh              # One-liner build script
└── README.md
```

**Key Point**: No `c/` directory - references `target/` directly:
- Headers: `../../target/ffi-headers/`
- Library: `../../target/release/libdelta_kernel_ffi.{so,dylib}`

## What Works ✅

1. **Snapshot Creation**: `delta.NewSnapshot(tablePath)` and `delta.NewSnapshotAtVersion(tablePath, version)`
2. **Version Retrieval**: `snapshot.Version()`
3. **Table Root Path**: `snapshot.TableRoot()` - returns the absolute path to the table
4. **Partition Columns**: `snapshot.PartitionColumns()` - returns list of partition column names
5. **Scan Creation**: `snapshot.Scan()` - creates a scan operation for reading table data
6. **Schema Access**: `scan.LogicalSchema()` and `scan.PhysicalSchema()` - get logical/physical schemas (handles retrieved, visitor pattern pending)
7. **Scan Table Root**: `scan.TableRoot()` - get table root from scan
8. **Engine Management**: Automatic default engine creation
9. **Resource Cleanup**: `snapshot.Close()` and `scan.Close()`
10. **Error Handling**: Basic error propagation from FFI

## Implementation Details

### CGO Configuration

The key to making it work was defining `DEFINE_DEFAULT_ENGINE_BASE`:

```go
/*
#cgo CFLAGS: -I${SRCDIR}/../../../target/ffi-headers -DDEFINE_DEFAULT_ENGINE_BASE
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldelta_kernel_ffi
#include "delta_kernel_ffi.h"
*/
```

This macro exposes engine creation functions that are conditionally compiled in the FFI header.

### Union Access

C unions are accessed via inline helper functions:

```go
// Helper to extract ok value from union
static inline HandleSharedSnapshot get_ok_snapshot(struct ExternResultHandleSharedSnapshot result) {
    return result.ok;
}
```

### Build Requirements

**Rust FFI must be built with**:
```bash
cargo build --release -p delta_kernel_ffi --all-features
```

The `--all-features` flag is critical - it enables:
- `default-engine-rustls` or `default-engine-native-tls`
- Which triggers `DEFINE_DEFAULT_ENGINE_BASE` in the FFI header

## Current Limitations

- ⚠️ Schema extraction not yet implemented (visitor pattern complexity)
- ⚠️ Error handling needs improvement (crashes on invalid paths)
- ⚠️ Only basic snapshot operations work

## Next Steps

To complete the wrapper:

1. **Improve Error Handling**: Properly handle builder/snapshot errors
2. **Implement Schema Visitor**: Complex callback pattern for schema extraction
3. **Add Scan Operations**: Table scanning functionality
4. **File Reading**: Read actual data from tables
5. **Tests**: Unit and integration tests

## Development

### Build just Rust FFI:
```bash
cargo build --release -p delta_kernel_ffi --all-features
```

### Build just Go:
```bash
go build ./...
```

### Run with a test table:
```bash
export DYLD_LIBRARY_PATH=$PWD/../../target/release:$DYLD_LIBRARY_PATH
./examples/describe_schema ../../acceptance/tests/dat/out/reader_tests/generated/basic_append/delta
```

## API Example

```go
package main

import (
    "fmt"
    "github.com/delta-io/delta-kernel-go/delta"
)

func main() {
    // Create snapshot
    snapshot, err := delta.NewSnapshot("/path/to/table")
    if err != nil {
        panic(err)
    }
    defer snapshot.Close()

    // Get version
    version := snapshot.Version()
    fmt.Printf("Table version: %d\n", version)

    // Get table root path
    tableRoot, err := snapshot.TableRoot()
    if err != nil {
        panic(err)
    }
    fmt.Printf("Table root: %s\n", tableRoot)

    // Get partition columns
    partitions, err := snapshot.PartitionColumns()
    if err != nil {
        panic(err)
    }
    fmt.Printf("Partition columns: %v\n", partitions)

    // Create a scan
    scan, err := snapshot.Scan()
    if err != nil {
        panic(err)
    }
    defer scan.Close()

    // Get logical schema
    logicalSchema, err := scan.LogicalSchema()
    if err != nil {
        panic(err)
    }
    fmt.Printf("Logical schema: %v\n", logicalSchema)

    // Get physical schema
    physicalSchema, err := scan.PhysicalSchema()
    if err != nil {
        panic(err)
    }
    fmt.Printf("Physical schema: %v\n", physicalSchema)

    // TODO: Implement schema visitor pattern to extract actual field information
    // TODO: Implement scan metadata iterator to read data files
}
```

## Success! 🎉

We now have a **working Go CGO wrapper** that:
- ✅ Compiles successfully
- ✅ Links to Rust FFI library
- ✅ Calls FFI functions correctly
- ✅ Reads real Delta tables
- ✅ Retrieves version information
- ✅ Gets table root paths
- ✅ Lists partition columns
- ✅ Creates scan operations
- ✅ Retrieves logical and physical schema handles
- ✅ Manages resources properly with proper cleanup
- ✅ Uses C helper functions for complex FFI patterns (allocators, iterators)

This provides a solid foundation for implementing the remaining Delta Kernel functionality like schema visitor pattern for field extraction, scan metadata iteration, and actual data reading!
