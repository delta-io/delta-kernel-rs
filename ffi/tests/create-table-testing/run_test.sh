#!/bin/bash

set -euxo pipefail

# Usage: run_test.sh <expected_file>
#
# Creates a fresh temp directory, runs ./create_table against it, and diffs output against
# the expected file. The temp dir is cleaned up regardless of success.
EXPECTED="$1"
shift

TABLE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/create_table_example.XXXXXX")
# Resolve symlinks (e.g. macOS /var -> /private/var) so the sed substitution matches paths
# the kernel canonicalises in any printed output. `cd && pwd -P` is portable to BSD/GNU.
TABLE_DIR=$(cd "$TABLE_DIR" && pwd -P)
OUT_FILE=$(mktemp)
trap 'rm -rf "$TABLE_DIR" "$OUT_FILE"' EXIT

./create_table "$TABLE_DIR" \
    | sed -E "s|$TABLE_DIR|<TABLE>|g" \
    | tee "$OUT_FILE"
diff "$OUT_FILE" "$EXPECTED"
