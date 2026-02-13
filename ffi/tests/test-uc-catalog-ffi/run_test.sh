#!/bin/bash

set -euxo pipefail

OUT_FILE=$(mktemp)
TMP_TABLE_DIR=$(mktemp -d)
cp -r ../../../../kernel/tests/data/table-with-dv-small/ "$TMP_TABLE_DIR"

./uc_catalog_example "$TMP_TABLE_DIR/table-with-dv-small"
CATALOG_EXIT_CODE=$?

rm "$OUT_FILE"
rm -r "$TMP_TABLE_DIR"

exit "$CATALOG_EXIT_CODE"

