#!/bin/bash

set -euxo pipefail

OUT_FILE=$(mktemp)
./visit_expression | tee "$OUT_FILE"
diff -s "$OUT_FILE" "$1"
DIFF_EXIT_CODE=$?
echo "Diff exited with $DIFF_EXIT_CODE"
rm "$OUT_FILE"
exit "$DIFF_EXIT_CODE"

