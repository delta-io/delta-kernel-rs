#!/bin/bash

set -euxo pipefail

# Usage: run_test.sh <create_table_binary> <expected_file>
#
# Seeds a temp dir with a freshly-created Delta table using the create-table example,
# runs ./write_table against it, then diffs output against the expected file.
CREATE_BIN="$1"
EXPECTED="$2"
shift 2

if [ ! -x "$CREATE_BIN" ]; then
    echo "ERROR: create_table binary not found at $CREATE_BIN" >&2
    echo "Build the create-table example first (it seeds the test table)." >&2
    exit 1
fi

TABLE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/write_table_example.XXXXXX")
# macOS resolves /var -> /private/var when canonicalising paths inside the kernel, so the
# write_path the binary prints looks like file:///private/var/... while $TABLE_DIR is still
# /var/.... Resolve TABLE_DIR up front (and feed the resolved form to both binaries) so the
# sed substitution below has a literal match. `cd && pwd -P` is portable to both BSD and GNU.
TABLE_DIR=$(cd "$TABLE_DIR" && pwd -P)
OUT_FILE=$(mktemp)
trap 'rm -rf "$TABLE_DIR" "$OUT_FILE"' EXIT

# Seed with an empty Delta table (v0). We discard create_table's output -- this example's
# expected output covers only what write_table prints.
"$CREATE_BIN" "$TABLE_DIR" > /dev/null

./write_table "$TABLE_DIR" \
    | sed -E "s|$TABLE_DIR|<TABLE>|g" \
    | tee "$OUT_FILE"
diff "$OUT_FILE" "$EXPECTED"
