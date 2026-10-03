#!/bin/bash

set -euxo pipefail

OUT_FILE=$(mktemp)
NORMALIZED_OUT_FILE=$(mktemp)
./schema_roundtrip "$1" | tee "$OUT_FILE"
sed '${/^$/d;}' "$OUT_FILE" > "$NORMALIZED_OUT_FILE"
diff -s "$NORMALIZED_OUT_FILE" "$2"
DIFF_EXIT_CODE=$?
echo "Diff exited with $DIFF_EXIT_CODE"
rm "$NORMALIZED_OUT_FILE"
rm "$OUT_FILE"
exit "$DIFF_EXIT_CODE"
