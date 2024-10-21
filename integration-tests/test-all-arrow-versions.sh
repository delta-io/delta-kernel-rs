#!/bin/bash

set -eu -o pipefail

is_version_le() {
    [  "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]
}

is_version_lt() {
  if [ "$1" = "$2" ]
  then
    return 1
  else
    is_version_le "$1" "$2"
  fi
}

test_arrow_version() {
  ARROW_VERSION="$1"
  echo "== Testing version $ARROW_VERSION =="
  sed -i'' -e "s/\(arrow[^\"]*=[^\"]*\).*/\1\"=$ARROW_VERSION\"/" Cargo.toml
  sed -i'' -e "s/\(parquet[^\"]*\).*/\1\"=$ARROW_VERSION\"/" Cargo.toml
  cargo clean
  rm -f Cargo.lock
  cargo update
  cat Cargo.toml
  cargo run
}

MIN_ARROW_VER="52.0.0"
MAX_ARROW_VER="54.0.0"

for ARROW_VERSION in $(curl -s https://crates.io/api/v1/crates/arrow | jq -r '.versions[].num' | tr -d '\r')
do
  if ! is_version_lt "$ARROW_VERSION" "$MIN_ARROW_VER" && is_version_lt "$ARROW_VERSION" "$MAX_ARROW_VER"
  then
    test_arrow_version "$ARROW_VERSION"
  fi
done
