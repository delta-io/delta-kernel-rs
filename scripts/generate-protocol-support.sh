#!/usr/bin/env bash
# Regenerates docs/protocol-support.md from the ProtocolImplement matrix in
# kernel/src/table_features/mod.rs. The doc reflects default cargo features.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test -p delta_kernel --lib generate_protocol_support_md -- --ignored --nocapture
echo "Regenerated docs/protocol-support.md"
