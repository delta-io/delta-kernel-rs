#!/bin/bash
set -e

echo "Building delta-kernel-rs FFI..."
cd ../.. && cargo build --release -p delta_kernel_ffi --all-features

echo "Building Go wrapper..."
cd ffi/go && go build ./...

echo "Building example..."
go build -o examples/describe_schema ./examples/describe_schema.go

echo "✓ Build complete!"
echo ""
echo "To run the example:"
echo "  export DYLD_LIBRARY_PATH=\$PWD/../../target/release:\$DYLD_LIBRARY_PATH  # macOS"
echo "  export LD_LIBRARY_PATH=\$PWD/../../target/release:\$LD_LIBRARY_PATH      # Linux"
echo "  ./examples/describe_schema /path/to/delta/table"
