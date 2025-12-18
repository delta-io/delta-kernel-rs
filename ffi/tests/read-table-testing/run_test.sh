#!/bin/bash

set -euxo pipefail

OUT_FILE=$(mktemp)
# if we've passed a third arg, use it as the first arg to ./read_table, otherwise set it to the
# empty string
COLS="${3:-}"
./read_table $COLS "$1" | tee "$OUT_FILE"
diff -s "$OUT_FILE" "$2"
DIFF_EXIT_CODE=$?
echo "Diff exited with $DIFF_EXIT_CODE"
rm "$OUT_FILE"
exit "$DIFF_EXIT_CODE"

