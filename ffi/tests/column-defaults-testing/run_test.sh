#!/bin/bash

set -euxo pipefail

# Usage: run_test.sh <table_dir> <expected_file>
#
# Runs ./column_defaults against a read-only fixture Delta table and diffs the output against
# the expected file.
TABLE="$1"
EXPECTED="$2"

OUT_FILE=$(mktemp)
trap 'rm -f "$OUT_FILE"' EXIT

./column_defaults "$TABLE" | tee "$OUT_FILE"
diff -s "$OUT_FILE" "$EXPECTED"
