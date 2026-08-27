#!/bin/bash

set -euxo pipefail

# Usage: run_test.sh <seed> <expected_file> [write_table_args...]
#
# Seeds a temp dir with a Delta table, runs ./write_table against it, then diffs output against the
# expected file. <seed> selects how the table is seeded:
#
#   - a path to the create-table example binary: run it to create a fresh empty table (v0).
#   - "fixture:<dir>": copy a checked-in table from <dir>. Needed for tables create-table cannot
#     produce -- the FFI schema builder has no field-metadata slot, so a feature declared through
#     column metadata (e.g. allowColumnDefaults' CURRENT_DEFAULT) can only come from a fixture.
#
# Either way the table is a throwaway copy, because write_table commits a new version: pointing it
# at a checked-in fixture would dirty the working tree and make a second run disagree with the
# expected version.
SEED="$1"
EXPECTED="$2"
shift 2

case "$SEED" in
    fixture:*)
        FIXTURE="${SEED#fixture:}"
        if [ ! -d "$FIXTURE/_delta_log" ]; then
            echo "ERROR: no Delta table fixture at $FIXTURE" >&2
            exit 1
        fi
        ;;
    *)
        if [ ! -x "$SEED" ]; then
            echo "ERROR: create_table binary not found at $SEED" >&2
            echo "Build the create-table example first (it seeds the test table)." >&2
            exit 1
        fi
        ;;
esac

TABLE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/write_table_example.XXXXXX")
# macOS resolves /var -> /private/var when canonicalising paths inside the kernel, so the
# write_path the binary prints looks like file:///private/var/... while $TABLE_DIR is still
# /var/.... Resolve TABLE_DIR up front (and feed the resolved form to both binaries) so the
# sed substitution below has a literal match. `cd && pwd -P` is portable to both BSD and GNU.
TABLE_DIR=$(cd "$TABLE_DIR" && pwd -P)
OUT_FILE=$(mktemp)
trap 'rm -rf "$TABLE_DIR" "$OUT_FILE"' EXIT

# Seed the table. create_table's own output is discarded -- this example's expected output covers
# only what write_table prints.
case "$SEED" in
    fixture:*) cp -R "${SEED#fixture:}/." "$TABLE_DIR"/ ;;
    *) "$SEED" "$TABLE_DIR" > /dev/null ;;
esac

./write_table "$@" "$TABLE_DIR" \
    | sed -E "s|$TABLE_DIR|<TABLE>|g" \
    | tee "$OUT_FILE"
diff "$OUT_FILE" "$EXPECTED"
