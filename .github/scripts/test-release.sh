#!/usr/bin/env bash

set -euo pipefail

REPOSITORY_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/delta-kernel-release-test.XXXXXX")
trap 'rm -rf "$TEST_ROOT"' EXIT

fail() {
    echo "release tooling test failed: $1" >&2
    exit 1
}

assert_contains() {
    local path="$1" expected="$2"
    grep -Fq -- "$expected" "$path" || fail "$path does not contain: $expected"
}

assert_count() {
    local path="$1" expected="$2" value="$3"
    local actual
    actual=$(grep -Fc "$value" "$path")
    [[ "$actual" == "$expected" ]] || \
        fail "$path contains '$value' $actual times; expected $expected"
}

test_registry_override() {
    local capture="$TEST_ROOT/cargo-args"

    # shellcheck source=release.sh
    source "$REPOSITORY_ROOT/release.sh"
    cargo() {
        printf '%s\n' "$@" > "$capture"
    }

    DELTA_KERNEL_RELEASE_REGISTRY=databricks-proxy run_cargo_release 0.29.0
    assert_contains "$capture" "--registry"
    assert_contains "$capture" "databricks-proxy"

    unset DELTA_KERNEL_RELEASE_REGISTRY
    run_cargo_release 0.29.0
    if grep -Fq -- "--registry" "$capture"; then
        fail "cargo release received --registry without an override"
    fi
}

commit_file() {
    local message="$1" contents="$2"
    printf '%s\n' "$contents" > tracked.txt
    git add tracked.txt
    git commit -q -m "$message"
}

test_changelog_refresh_and_verification() {
    local repository="$TEST_ROOT/repository"
    mkdir -p "$repository"
    cp "$REPOSITORY_ROOT/release.sh" "$REPOSITORY_ROOT/cliff.toml" "$repository/"

    cd "$repository"
    git init -q
    git config user.email release-test@example.com
    git config user.name "Release Test"
    git config core.hooksPath /dev/null

    printf '# Changelog\n' > CHANGELOG.md
    git add CHANGELOG.md
    git commit -q -m "chore: previous release"
    git tag v0.28.0

    commit_file "chore: publish DAT artifact" "dat"
    git tag v0.0.1_dat
    commit_file "fix: include first change (#101)" "first"

    ./release.sh changelog 0.29.0
    assert_contains CHANGELOG.md "([#101])"
    assert_contains CHANGELOG.md "v0.28.0...v0.29.0"
    git add CHANGELOG.md
    git commit -q -m "release 0.29.0"

    commit_file "fix: include late change (#102)" "late"
    if ./release.sh verify-changelog 0.29.0 > verification.log 2>&1; then
        fail "stale changelog verification unexpectedly passed"
    fi
    assert_contains verification.log "missing PR #102"

    ./release.sh changelog 0.29.0
    assert_count CHANGELOG.md 1 "([#101])"
    assert_count CHANGELOG.md 1 "([#102])"
    ./release.sh verify-changelog 0.29.0
}

test_registry_override
test_changelog_refresh_and_verification
