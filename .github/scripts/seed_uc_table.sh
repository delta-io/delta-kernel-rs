#!/usr/bin/env bash
# Seed a catalog-managed Delta table in the running UC OSS server via its `bin/uc` CLI,
# so the gated `load_table` live test has a table to read.
#
# Usage: seed_uc_table.sh <catalog> <schema> <table>
# Overridable via env: UC_DIR.
set -euo pipefail

UC_DIR="${UC_DIR:-$HOME/unitycatalog}"
catalog="${1:?catalog required}"
schema="${2:?schema required}"
table="${3:?table required}"

cd "$UC_DIR"
bin/uc table create \
  --full_name "$catalog.$schema.$table" \
  --columns "id INT, name STRING" \
  --table_type MANAGED
