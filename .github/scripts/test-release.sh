#!/usr/bin/env bash

set -euo pipefail

REPOSITORY_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/delta-kernel-release-test.XXXXXX")
trap 'rm -rf "$TEST_ROOT"' EXIT
export TMPDIR="$TEST_ROOT"

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

    DELTA_KERNEL_RELEASE_REGISTRY=mirror run_cargo_release 0.29.0
    assert_contains "$capture" "--registry"
    assert_contains "$capture" "mirror"

    unset DELTA_KERNEL_RELEASE_REGISTRY
    run_cargo_release 0.29.0
    if grep -Fq -- "--registry" "$capture"; then
        fail "cargo release received --registry without an override"
    fi
}

test_release_command_dispatch() {
    local capture="$TEST_ROOT/release-command"

    (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        check_requirements() { :; }
        is_main_branch() { return 1; }
        handle_release_branch() { printf 'branch %s\n' "$1" > "$capture"; }

        main release 0.29.0
    )
    assert_contains "$capture" "branch 0.29.0"

    (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        check_requirements() { :; }
        is_main_branch() { return 0; }
        handle_main_branch() { printf 'main\n' > "$capture"; }

        main release
    )
    assert_contains "$capture" "main"
}

commit_file() {
    local message="$1" contents="$2"
    printf '%s\n' "$contents" > tracked.txt
    git add tracked.txt
    git commit -q -m "$message"
}

test_changelog_refresh_and_verification() {
    local repository="$TEST_ROOT/repository"
    local failing_bin="$TEST_ROOT/failing-bin"
    local saved_changelog="$TEST_ROOT/changelog-before-failure"
    local backup refresh_log failure_backup
    mkdir -p "$repository"
    cp "$REPOSITORY_ROOT/release.sh" "$REPOSITORY_ROOT/cliff.toml" "$repository/"

    cd "$repository"
    git init -q -b main
    git config user.email release-test@example.com
    git config user.name "Release Test"
    git config core.hooksPath /dev/null

    printf '%s\n' \
        '# Changelog' \
        '' \
        '## [v0.28.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.28.0/)' \
        '' \
        'Previous release notes' > CHANGELOG.md
    git add CHANGELOG.md
    git commit -q -m "chore: previous release"
    git tag v0.28.0

    git switch -q -c divergent-release
    commit_file "release 100.0.0" "divergent"
    git tag v100.0.0
    git switch -q main

    commit_file "chore: publish DAT artifact" "dat"
    git tag v999.0.0_dat
    commit_file "fix: include first change (#101)" "first"

    # The artifact tag sorts above the real release numerically, but cliff.toml still defines
    # v0.28.0 as the latest Kernel release boundary.
    # shellcheck source=release.sh
    source ./release.sh
    [[ "$(latest_kernel_release_tag)" == "v0.28.0" ]] || \
        fail "artifact tag was selected as the latest Kernel release"

    if (
        latest_kernel_release_tag() { :; }
        verify_release_changelog 0.29.0
    ) > no-tag.log 2>&1; then
        fail "verification unexpectedly passed without a prior Kernel release tag"
    fi
    assert_contains no-tag.log "No prior Kernel release tag found"

    ./release.sh > help.log
    assert_contains help.log "./release.sh release [version]"

    refresh_log="$TEST_ROOT/refresh.log"
    ./release.sh changelog 0.29.0 > "$refresh_log"
    assert_contains "$refresh_log" "backup retained at"
    backup=$(sed -n 's/.*backup retained at //p' "$refresh_log")
    [[ -f "$backup" ]] || fail "successful changelog refresh did not retain its backup"
    assert_contains CHANGELOG.md "([#101])"
    assert_contains CHANGELOG.md "v0.28.0...v0.29.0"
    assert_count CHANGELOG.md 1 "## [v0.28.0]"
    assert_contains CHANGELOG.md "Previous release notes"
    git add CHANGELOG.md
    git commit -q -m "release 0.29.0 (#999)"

    # Exercise the no-argument path used by CI. A release commit cannot mention its own PR in the
    # changelog it introduced, and cliff.toml deliberately skips it.
    get_current_version() {
        [[ "$1" == "delta_kernel" ]] || fail "unexpected crate name: $1"
        echo 0.29.0
    }
    verify_release_changelog

    mkdir -p "$failing_bin"
    printf '%s\n' '#!/usr/bin/env bash' 'exit 1' > "$failing_bin/git-cliff"
    chmod +x "$failing_bin/git-cliff"
    if PATH="$failing_bin:$PATH" verify_release_changelog 0.29.0 \
        > tag-failure.log 2>&1; then
        fail "tag lookup failure unexpectedly passed verification"
    fi
    assert_contains tag-failure.log "Could not resolve the latest Kernel release tag"

    commit_file "chore: refresh release changelog (#998)" "housekeeping"
    ./release.sh verify-changelog 0.29.0

    commit_file "fix: include late change (#102) [skip ci]" "late"
    if ./release.sh verify-changelog 0.29.0 > verification.log 2>&1; then
        fail "stale changelog verification unexpectedly passed"
    fi
    assert_contains verification.log "missing PR #102"
    if grep -Fq "missing PR #998" verification.log; then
        fail "verification required a commit skipped by cliff.toml"
    fi

    ./release.sh changelog 0.29.0
    assert_count CHANGELOG.md 1 "([#101])"
    assert_count CHANGELOG.md 1 "([#102])"
    assert_count CHANGELOG.md 1 "## [v0.29.0]"
    assert_count CHANGELOG.md 1 "## [v0.28.0]"
    assert_contains CHANGELOG.md "Previous release notes"
    ./release.sh verify-changelog 0.29.0

    cp CHANGELOG.md "$saved_changelog"
    if PATH="$failing_bin:$PATH" ./release.sh changelog 0.29.0 > refresh-failure.log 2>&1; then
        fail "changelog refresh unexpectedly passed with a failing git-cliff"
    fi
    assert_contains refresh-failure.log "Failed to refresh CHANGELOG.md"
    cmp -s CHANGELOG.md "$saved_changelog" || \
        fail "failed changelog refresh did not restore CHANGELOG.md"
    failure_backup=$(sed -n 's/.*original saved at //p' refresh-failure.log)
    [[ -f "$failure_backup" ]] || fail "failed changelog refresh did not retain its backup"
    cmp -s "$failure_backup" "$saved_changelog" || \
        fail "failed changelog refresh retained the wrong backup contents"
}

test_registry_override
test_release_command_dispatch
test_changelog_refresh_and_verification
