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

assert_not_contains() {
    local path="$1" unexpected="$2"
    if grep -Fq -- "$unexpected" "$path"; then
        fail "$path unexpectedly contains: $unexpected"
    fi
}

test_registry_override() {
    local capture="$TEST_ROOT/cargo-args"

    # shellcheck source=release.sh
    source "$REPOSITORY_ROOT/release.sh"
    cargo() {
        printf '%s\n' "$@" > "$capture"
    }

    DELTA_KERNEL_RELEASE_REGISTRY=mirror run_cargo_release 0.29.0
    assert_contains "$capture" "--workspace"
    assert_contains "$capture" "--no-publish"
    assert_contains "$capture" "--no-push"
    assert_contains "$capture" "--no-tag"
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
    local failure_log="$TEST_ROOT/release-command-failure"

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

    if (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        main unexpected
    ) > "$failure_log" 2>&1; then
        fail "unknown release command unexpectedly succeeded"
    fi
    assert_contains "$failure_log" "Unknown command: unexpected"
    assert_contains "$failure_log" "release [version]"

    if (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        main changelog
    ) > "$failure_log" 2>&1; then
        fail "changelog command unexpectedly succeeded without a version"
    fi
    assert_contains "$failure_log" "changelog <version>"

    if (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        main verify-changelog 0.29.0 extra
    ) > "$failure_log" 2>&1; then
        fail "verify-changelog unexpectedly accepted an extra argument"
    fi
    assert_contains "$failure_log" "verify-changelog [version]"

    if (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        check_requirements() { :; }
        is_main_branch() { return 0; }
        main release 0.29.0
    ) > "$failure_log" 2>&1; then
        fail "main-branch release unexpectedly accepted a version"
    fi
    assert_contains "$failure_log" "Version argument not expected on main branch"

    if (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        check_requirements() { :; }
        is_main_branch() { return 1; }
        main release
    ) > "$failure_log" 2>&1; then
        fail "release-branch release unexpectedly omitted its version"
    fi
    assert_contains "$failure_log" "Version argument required when on release branch"

    if (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        check_requirements() { :; }
        is_main_branch() { return 0; }
        main crate unity-catalog-delta-client-api 0.2.0
    ) > "$failure_log" 2>&1; then
        fail "crate release unexpectedly succeeded on main"
    fi
    assert_contains "$failure_log" "Create a release branch before bumping a crate"

    if (
        # shellcheck source=release.sh
        source "$REPOSITORY_ROOT/release.sh"
        run_cargo_release() { :; }
        verify_release_changelog() { return 1; }
        handle_release_branch 0.29.0
    ) > "$failure_log" 2>&1; then
        fail "release preparation continued after changelog verification failed"
    fi
    assert_contains "$failure_log" "Generated changelog is incomplete"
}

commit_file() {
    local message="$1" contents="$2" path="${3:-tracked.txt}"
    printf '%s\n' "$contents" > "$path"
    git add "$path"
    git commit -q -m "$message"
}

init_test_repository() {
    mkdir -p "$1"
    cd "$1"
    git init -q -b main
    git config user.email release-test@example.com
    git config user.name "Release Test"
    git config core.hooksPath /dev/null
}

init_release_workspace() {
    init_test_repository "$1"
    cat > Cargo.toml <<'EOF'
[workspace]
members = ["kernel", "ffi", "api", "rest", "integration", "example"]
resolver = "2"
[workspace.package]
version = "0.29.0"
EOF
    local spec directory package
    for spec in kernel:delta_kernel ffi:delta_kernel_ffi api:unity-catalog-delta-client-api \
        rest:unity-catalog-delta-rest-client integration:delta-kernel-unity-catalog \
        example:release_example; do
        directory=${spec%%:*} package=${spec#*:}
        mkdir -p "$directory"
        printf '[package]\nname = "%s"\nedition = "2021"\n' "$package" \
            > "$directory/Cargo.toml"
        if [[ "$directory" == kernel || "$directory" == ffi ]]; then
            printf 'version.workspace = true\n' >> "$directory/Cargo.toml"
        else
            printf 'version = "0.29.0"\n' >> "$directory/Cargo.toml"
        fi
        if [[ "$directory" == example ]]; then
            printf 'publish = false\n' >> "$directory/Cargo.toml"
        fi
        if [[ "$directory" == rest || "$directory" == integration ]]; then
            printf '[dependencies]\nunity-catalog-delta-client-api = { path = "../api", version = "0.29.0" }\n' \
                >> "$directory/Cargo.toml"
        fi
        printf '[lib]\npath = "lib.rs"\n' >> "$directory/Cargo.toml"
        : > "$directory/lib.rs"
    done
    cargo generate-lockfile --offline
    git add .
    git commit -q -m "chore: initialize release workspace"
}

test_working_tree_cleanliness() {
    local repository="$TEST_ROOT/cleanliness-repository"
    init_test_repository "$repository"
    commit_file "chore: initial file" "initial"

    # shellcheck source=release.sh
    source "$REPOSITORY_ROOT/release.sh"
    is_working_tree_clean || fail "clean working tree was rejected"

    printf 'untracked\n' > unrelated.txt
    if is_working_tree_clean; then
        fail "untracked file was accepted by the release cleanliness check"
    fi
    git add unrelated.txt
    if is_working_tree_clean; then
        fail "staged file was accepted by the release cleanliness check"
    fi
    git commit -q -m "chore: add file"
    printf 'modified\n' > tracked.txt
    if is_working_tree_clean; then
        fail "modified file was accepted by the release cleanliness check"
    fi
}

test_tag_commit_validation() {
    local failure_log="$TEST_ROOT/tag-commit-failure"
    local prompt="$TEST_ROOT/tag-prompt" release_commit
    init_release_workspace "$TEST_ROOT/tag-repository"
    commit_file "chore: initial file" "initial"

    # shellcheck source=release.sh
    source "$REPOSITORY_ROOT/release.sh"
    REPO_ROOT="$TEST_ROOT/tag-repository"
    confirm() {
        printf '%s\n' "$1" > "$prompt"
        return 1
    }

    if (tag_release unity-catalog-delta-client-api missing-commit) \
        > "$failure_log" 2>&1; then
        fail "tagging unexpectedly accepted an invalid commit"
    fi
    assert_contains "$failure_log" "Not a valid commit: missing-commit"
    [[ ! -f "$prompt" ]] || fail "invalid commit reached the confirmation prompt"

    if (tag_release unity-catalog-delta-client-api HEAD:tracked.txt) \
        > "$failure_log" 2>&1; then
        fail "tagging unexpectedly accepted a file object as a commit"
    fi
    assert_contains "$failure_log" "Not a valid commit: HEAD:tracked.txt"
    [[ ! -f "$prompt" ]] || fail "file object reached the confirmation prompt"

    tag_release unity-catalog-delta-client-api HEAD
    assert_contains "$prompt" "at $(git rev-parse --short HEAD)?"
    if git rev-parse -q --verify refs/tags/v0.29.0_unity-catalog-delta-client-api >/dev/null; then
        fail "declining confirmation unexpectedly created a tag"
    fi
    release_commit=$(git rev-parse HEAD)
    sed -i 's/version = "0.29.0"/version = "0.30.0"/' api/Cargo.toml
    for state in dirty committed; do
        if [[ "$state" == committed ]]; then
            git add api/Cargo.toml
            git commit -q -m "chore: bump API version"
        fi
        if (tag_release unity-catalog-delta-client-api "$release_commit") \
            > "$failure_log" 2>&1; then
            fail "$state manifest mismatch was accepted"
        fi
        assert_contains "$failure_log" "release manifest must match the commit being tagged"
    done
}

test_tag_publication() {
    local repository="$TEST_ROOT/tag-publication"
    local upstream="$TEST_ROOT/tag-upstream.git"
    local target crate tag failure_log="$TEST_ROOT/tag-refusal"
    init_release_workspace "$repository"
    target=$(git rev-parse HEAD)
    commit_file "fix: later change" "later"
    git init -q --bare "$upstream"
    git remote add upstream "$upstream"

    # shellcheck source=release.sh
    source "$REPOSITORY_ROOT/release.sh"
    REPO_ROOT="$repository"
    confirm() { return 0; }
    for crate in delta_kernel unity-catalog-delta-client-api; do
        if [[ "$crate" == delta_kernel ]]; then
            tag=v0.29.0
        else
            tag=v0.29.0_unity-catalog-delta-client-api
        fi
        tag_release "$crate" "$target"
        [[ "$(git cat-file -t "refs/tags/$tag")" == tag ]] || fail "$tag is not annotated"
        [[ "$(git rev-parse "$tag^{commit}")" == "$target" ]] || fail "$tag has wrong target"
        [[ "$(git --git-dir="$upstream" rev-parse "$tag^{commit}")" == "$target" ]] || \
            fail "$tag was not pushed with the intended target"
        if (tag_release "$crate" HEAD) > "$failure_log" 2>&1; then
            fail "existing tag $tag was accepted"
        fi
        assert_contains "$failure_log" "tag $tag already exists"
    done
    if (tag_release delta_kernel_ffi) > "$failure_log" 2>&1; then
        fail "workspace-versioned FFI crate was allowed its own tag"
    fi
    assert_contains "$failure_log" "Tag 'delta_kernel' for Kernel releases"
}

test_crate_release_guards() {
    local repository="$TEST_ROOT/crate-guards"
    local failure_log="$TEST_ROOT/crate-guard-failure" crate dependents
    init_release_workspace "$repository"
    git switch -q -c uc-crate-release/api

    # shellcheck source=release.sh
    source "$REPOSITORY_ROOT/release.sh"
    REPO_ROOT="$repository"
    cargo() {
        if [[ "$1" == release ]]; then
            fail "invalid crate reached cargo release"
        fi
        command cargo "$@"
    }
    for crate in delta_kernel delta_kernel_ffi release_example missing-crate; do
        if (handle_crate_release "$crate" 0.30.0) > "$failure_log" 2>&1; then
            fail "ineligible crate $crate was accepted"
        fi
        assert_contains "$failure_log" "must be a publishable crate on an independent version line"
        is_working_tree_clean || fail "refusing $crate changed the workspace"
    done
    [[ "$(crate_directory unity-catalog-delta-client-api)" == api ]] || \
        fail "crate directory was inferred from the package name"
    dependents=$(independent_dependents_of unity-catalog-delta-client-api)
    [[ "$dependents" == $'delta-kernel-unity-catalog\nunity-catalog-delta-rest-client' ]] || \
        fail "incorrect independent dependents: $dependents"
    printf 'untracked\n' > unrelated.txt
    if (handle_crate_release unity-catalog-delta-client-api 0.2.0) \
        > "$failure_log" 2>&1; then
        fail "crate preparation accepted a dirty working tree"
    fi
    assert_contains "$failure_log" "Working tree must be clean before releasing"
}

test_crate_changelog_ranges() {
    local crate="unity-catalog-delta-client-api"
    local previous_version next_version changelog crate_path=client-api

    for previous_version in "" 0.1.0 0.2.0-rc.1 0.1.0+build.1; do
        init_test_repository "$TEST_ROOT/crate-${previous_version:-first}"
        cp "$REPOSITORY_ROOT/release.sh" "$REPOSITORY_ROOT/cliff.toml" .
        mkdir -p "$crate_path"
        printf '[package]\nname = "%s"\nversion = "0.1.0"\n[lib]\npath = "lib.rs"\n' \
            "$crate" > "$crate_path/Cargo.toml"
        printf '[workspace]\nmembers = ["%s"]\nresolver = "2"\n' "$crate_path" > Cargo.toml
        git add Cargo.toml "$crate_path/Cargo.toml"
        commit_file "feat: initial API (#100)" "initial" "$crate_path/lib.rs"
        git tag v0.28.0
        if [[ -n "$previous_version" ]]; then
            git tag "v${previous_version}_${crate}"
        fi

        commit_file "feat: API change before Kernel release (#101)" "before" "$crate_path/lib.rs"
        git tag v0.29.0
        commit_file "fix: unrelated change (#102)" "unrelated"
        git tag v9.0.0_unity-catalog-delta-rest-client
        commit_file "fix: API change after Kernel release (#103)" "after" "$crate_path/lib.rs"
        commit_file "release $crate 0.2.0 (#104)" "release" "$crate_path/lib.rs"

        # shellcheck source=release.sh
        source ./release.sh
        next_version=0.2.0
        if [[ "$previous_version" == 0.2.0-rc.1 ]]; then
            next_version=0.2.0-rc.2
        fi
        cd "$crate_path"
        update_crate_changelog "$crate" "$next_version"
        cd ..
        changelog="$crate_path/CHANGELOG.md"
        assert_contains "$changelog" "## [v${next_version}_${crate}]"
        assert_contains "$changelog" "([#101])"
        assert_contains "$changelog" "([#103])"
        assert_not_contains "$changelog" "([#102])"
        assert_not_contains "$changelog" "([#104])"
        if [[ -n "$previous_version" ]]; then
            assert_not_contains "$changelog" "([#100])"
            assert_contains "$changelog" "v${previous_version}_${crate}...v${next_version}_${crate}"
        else
            assert_contains "$changelog" "([#100])"
            assert_not_contains "$changelog" "[Full Changelog]"
        fi
    done
}

test_changelog_refresh_and_verification() {
    local repository="$TEST_ROOT/repository"
    local failing_bin="$TEST_ROOT/failing-bin"
    local section_backup="$TEST_ROOT/changelog-before-section-edit"
    local saved_changelog="$TEST_ROOT/changelog-before-failure"
    local backup refresh_log failure_backup
    init_test_repository "$repository"
    cp "$REPOSITORY_ROOT/release.sh" "$REPOSITORY_ROOT/cliff.toml" "$repository/"

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
    commit_file "release unity-catalog-delta-client-api 0.2.0 (#997)" "UC release"

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
    assert_not_contains CHANGELOG.md "([#997])"
    git add CHANGELOG.md
    git commit -q -m "release 0.29.0 (#999)"

    # Exercise the no-argument path used by CI. A release commit cannot mention its own PR in the
    # changelog it introduced, and cliff.toml deliberately skips it.
    get_current_version() {
        [[ "$1" == "delta_kernel" ]] || fail "unexpected crate name: $1"
        echo 0.29.0
    }
    verify_release_changelog

    git tag v0.29.0
    verify_release_changelog 0.29.0 > already-tagged.log
    assert_contains already-tagged.log "already tagged; no release changelog to verify"
    git tag -d v0.29.0 >/dev/null

    if verify_release_changelog 0.30.0 > missing-section.log 2>&1; then
        fail "verification unexpectedly passed without a release section"
    fi
    assert_contains missing-section.log "CHANGELOG.md has no section for v0.30.0"

    if (
        render_release_changelog() { return 1; }
        verify_release_changelog 0.29.0
    ) > render-failure.log 2>&1; then
        fail "verification unexpectedly passed when changelog rendering failed"
    fi
    assert_contains render-failure.log "Could not render the expected changelog for v0.29.0"

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
    assert_contains verification.log "PR #102"
    if grep -Fq "PR #998" verification.log; then
        fail "verification required a commit skipped by cliff.toml"
    fi

    ./release.sh changelog 0.29.0
    assert_count CHANGELOG.md 1 "([#101])"
    assert_count CHANGELOG.md 1 "([#102])"
    assert_count CHANGELOG.md 1 "## [v0.29.0]"
    assert_count CHANGELOG.md 1 "## [v0.28.0]"
    assert_contains CHANGELOG.md "Previous release notes"
    ./release.sh verify-changelog 0.29.0

    cp CHANGELOG.md "$section_backup"
    sed -i '/Include late change/d' CHANGELOG.md
    if ./release.sh verify-changelog 0.29.0 > missing-bullet.log 2>&1; then
        fail "verification unexpectedly passed with a missing changelog bullet"
    fi
    assert_contains missing-bullet.log "missing the changelog entry for PR #102"
    if grep -Fq "missing the link reference for PR #102" missing-bullet.log; then
        fail "verification treated the retained PR reference as missing"
    fi
    cp "$section_backup" CHANGELOG.md

    sed -i '/^\[#102\]: /d' CHANGELOG.md
    if ./release.sh verify-changelog 0.29.0 > missing-reference.log 2>&1; then
        fail "verification unexpectedly passed with a missing PR reference"
    fi
    assert_contains missing-reference.log "missing the link reference for PR #102"
    if grep -Fq "missing the changelog entry for PR #102" missing-reference.log; then
        fail "verification treated the retained changelog bullet as missing"
    fi
    cp "$section_backup" CHANGELOG.md

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

(test_registry_override)
(test_release_command_dispatch)
(test_working_tree_cleanliness)
(test_tag_commit_validation)
(test_tag_publication)
(test_crate_release_guards)
(test_crate_changelog_ranges)
(test_changelog_refresh_and_verification)
