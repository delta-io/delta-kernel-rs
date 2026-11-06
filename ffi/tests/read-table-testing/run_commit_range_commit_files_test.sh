#!/bin/bash

set -euxo pipefail

# Usage: run_commit_range_commit_files_test.sh <table_path> <expected_file>
TABLE_PATH="$1"
EXPECTED="$2"

OUT_FILE=$(mktemp)
trap 'rm -f "$OUT_FILE"' EXIT

# Commit file locations contain machine-specific absolute paths, so normalize the prefix up to
# ".../acceptance/tests/dat/". Modification times depend on when the fixture was checked out, so
# they are omitted from the golden comparison.
./commit_range_commit_files "$TABLE_PATH" \
    | sed -E \
        -e 's|file://.*/acceptance/tests/dat/|file://<DAT>/|g' \
        -e '/^  last_modified:/d' \
    | tee "$OUT_FILE"
diff "$OUT_FILE" "$EXPECTED"
