#!/usr/bin/env bash
# Clone and build the Unity Catalog OSS server at a pinned commit so the gated
# delta-kernel-unity-catalog live integration tests can run against a real server in CI.
#
# Pinned to the commit that introduced the Delta REST (v1) endpoints the kernel UC crate
# targets. The published `:latest` artifacts/images predate those endpoints, so building from
# this commit is required. Bump UC_COMMIT when the server contract advances.
#
# Overridable via env: UC_REPO, UC_COMMIT, UC_DIR.
set -euo pipefail

UC_REPO="${UC_REPO:-https://github.com/unitycatalog/unitycatalog.git}"
UC_COMMIT="${UC_COMMIT:-0f1445227bd251b386420c90136515daefa4e03d}"
UC_DIR="${UC_DIR:-$HOME/unitycatalog}"

if [[ ! -d "$UC_DIR/.git" ]]; then
  git init -q "$UC_DIR"
  git -C "$UC_DIR" remote add origin "$UC_REPO"
fi

git -C "$UC_DIR" fetch --depth 1 origin "$UC_COMMIT"
git -C "$UC_DIR" checkout -q --force FETCH_HEAD

cd "$UC_DIR"
# Build the server jar up front so the launch step starts fast and a cache can capture it.
# `bin/start-uc-server` will reuse this jar instead of rebuilding when present.
build/sbt -info clean package
