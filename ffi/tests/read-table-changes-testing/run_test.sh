#!/bin/bash

set -euxo pipefail

# Usage: run_test.sh <table_path> <expected_file> [extra read_table_changes args...]
TABLE_PATH="$1"
EXPECTED="$2"
shift 2

OUT_FILE=$(mktemp)
trap 'rm -f "$OUT_FILE"' EXIT

# table_root lines contain machine-specific absolute paths. Replace everything up to and
# including ".../acceptance/tests/dat/" with "<DAT>" so expected files are portable.
./read_table_changes "$@" "$TABLE_PATH" \
    | sed -E 's|file://.*/acceptance/tests/dat/|file://<DAT>/|g' \
    | tee "$OUT_FILE"
diff "$OUT_FILE" "$EXPECTED"
