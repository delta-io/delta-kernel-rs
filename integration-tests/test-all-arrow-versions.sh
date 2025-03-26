#!/bin/bash

set -eu -o pipefail

clean_up () {
  CODE=$?
  git checkout HEAD Cargo.toml
  exit $CODE
}

# ensure we checkout the clean version of Cargo.toml no matter how we exit
trap clean_up EXIT

test_arrow_version() {
  ARROW_VERSION="$1"
  echo "== Testing version $ARROW_VERSION =="
  cargo clean
  rm -f Cargo.lock
  cargo update
  echo "Cargo.toml is:"
  cat Cargo.toml
  echo ""
  if [ "$ARROW_VERSION" = "ALL_ENABLED" ]; then
    echo "testing with --all-features"
    cargo run --all-features
  else
    echo "testing with --features ${ARROW_VERSION}"
    cargo run --features ${ARROW_VERSION}
  fi
}

FEATURES=$(cat ../kernel/Cargo.toml | grep -e ^arrow_ | awk '{ print $1 }' | sort -u)


echo "[features]" >> Cargo.toml

for ARROW_VERSION in ${FEATURES}
do
  echo "${ARROW_VERSION} = [\"delta_kernel/${ARROW_VERSION}\"]" >> Cargo.toml
  test_arrow_version $ARROW_VERSION
done

test_arrow_version "ALL_ENABLED"

