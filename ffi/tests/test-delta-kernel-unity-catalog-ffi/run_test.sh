#!/bin/bash

set -euxo pipefail

OUT_FILE=$(mktemp "${TMPDIR:-/tmp}/catalog_test.out.XXXX")
TMP_TABLE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/catalog_test.table.XXXX")
cp -r ../../../../delta-kernel-unity-catalog/tests/data/catalog_managed_0 "$TMP_TABLE_DIR"

./delta_kernel_unity_catalog_example "$TMP_TABLE_DIR/catalog_managed_0"
CATALOG_EXIT_CODE=$?

rm "$OUT_FILE"
rm -r "$TMP_TABLE_DIR"

exit "$CATALOG_EXIT_CODE"

