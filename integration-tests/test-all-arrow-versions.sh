#!/bin/bash

set -eu -o pipefail

test_arrow_version() {
  ARROW_VERSION="$1"
  echo "== Testing version $ARROW_VERSION =="
  cargo clean
  rm -f Cargo.lock
  cargo update
  cat Cargo.toml
  cargo run --features ${ARROW_VERSION}
}

FEATURES=$(cat ../kernel/Cargo.toml | grep -e ^arrow_ | awk '{ print $1 }' | sort -u)


echo "[features]" >> Cargo.toml

for ARROW_VERSION in ${FEATURES}
do
  echo "${ARROW_VERSION} = [\"delta_kernel/${ARROW_VERSION}\"]" >> Cargo.toml
  test_arrow_version $ARROW_VERSION
done

git checkout Cargo.toml
