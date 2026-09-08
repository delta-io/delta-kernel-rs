#!/usr/bin/env bash

###################################################################################################
# USAGE:
# 1. on a release branch: ./release.sh <version> (example: ./release.sh 0.1.0)
# 2. on main branch (after merging release branch): ./release.sh
# 3. refresh a release PR after merging/rebasing main: ./release.sh changelog <version>
# 4. verify that a release changelog covers every merged PR: ./release.sh verify-changelog [version]
#
# Set DELTA_KERNEL_RELEASE_REGISTRY when cargo-release must resolve through another registry:
#   DELTA_KERNEL_RELEASE_REGISTRY=databricks-proxy ./release.sh 0.29.0
###################################################################################################

# This is a script to automate a large portion of the release process for the crates we publish to
# crates.io. Currently `delta_kernel` (in the kernel/ dir), `delta_kernel_derive` (in the
# derive-macros/ dir), and `delta_kernel_default_engine` (in the default-engine/ dir) are released.

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# print commands before executing them for debugging
# set -x

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # no color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

check_requirements() {
    log_info "Checking required tools..."

    command -v cargo >/dev/null 2>&1 || log_error "cargo is required but not installed"
    command -v git >/dev/null 2>&1 || log_error "git is required but not installed"
    command -v cargo-release >/dev/null 2>&1 || log_error "cargo-release is required but not installed. Install with: cargo install cargo-release"
    command -v git-cliff >/dev/null 2>&1 || log_error "git-cliff is required but not installed. Install with: cargo install git-cliff"
    command -v jq >/dev/null 2>&1 || log_error "jq is required but not installed."

    log_success "All required tools are available"
}

check_changelog_requirements() {
    command -v git >/dev/null 2>&1 || log_error "git is required but not installed"
    command -v git-cliff >/dev/null 2>&1 || \
        log_error "git-cliff is required but not installed. Install with: cargo install git-cliff"
}

check_changelog_verification_requirements() {
    command -v cargo >/dev/null 2>&1 || log_error "cargo is required but not installed"
    command -v git >/dev/null 2>&1 || log_error "git is required but not installed"
    command -v git-cliff >/dev/null 2>&1 || \
        log_error "git-cliff is required but not installed. Install with: cargo install git-cliff"
    command -v jq >/dev/null 2>&1 || log_error "jq is required but not installed"
}

is_main_branch() {
    local current_branch
    current_branch=$(git rev-parse --abbrev-ref HEAD)
    [[ "$current_branch" == "main" ]]
}

is_working_tree_clean() {
    git diff --quiet && git diff --cached --quiet
}

# check if the version is already published on crates.io
is_version_published() {
    local crate_name="$1"
    local version
    version=$(get_current_version "$crate_name")

    if [[ -z "$version" ]]; then
        log_error "Could not find crate '$crate_name' in workspace"
    fi

    if cargo search "$crate_name" | grep -q "^$crate_name = \"$version\""; then
        return 0
    else
        return 1
    fi
}

# get current version from Cargo.toml
get_current_version() {
    local crate_name="$1"
    cargo metadata --locked --no-deps --format-version 1 | \
        jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version'
}

# Run cargo-release with an optional read registry. This avoids editing release.sh when the
# maintainer's network requires dependency metadata to come from a registry proxy.
run_cargo_release() {
    local version="$1"
    local args=(
        release --workspace "$version" --no-publish --no-push --no-tag --execute
    )

    if [[ -n "${DELTA_KERNEL_RELEASE_REGISTRY:-}" ]]; then
        args+=(--registry "$DELTA_KERNEL_RELEASE_REGISTRY")
    fi

    cargo "${args[@]}"
}

# Ask git-cliff for the latest Kernel release so changelog generation and verification use the
# same tag grammar from cliff.toml.
latest_kernel_release_tag() {
    git cliff --repository "$REPO_ROOT" --config "$REPO_ROOT/cliff.toml" --use-branch-tags \
        --latest --context | jq -r '.[0].version // empty'
}

release_changelog_heading() {
    local version="$1"
    printf '## [v%s]' "$version"
}

release_changelog_section() {
    local heading
    heading=$(release_changelog_heading "$1")
    awk -v heading="$heading" '
        index($0, heading) == 1 { in_release = 1 }
        in_release && /^## \[v/ && index($0, heading) != 1 { exit }
        in_release { print }
    ' "$REPO_ROOT/CHANGELOG.md"
}

release_changelog_subjects() {
    local version="$1"
    git cliff --repository "$REPO_ROOT" --config "$REPO_ROOT/cliff.toml" --use-branch-tags \
        --unreleased --include-path "*" --tag "$version" --context | \
        jq -r '.[].commits[].message | split("\n")[0]'
}

# Verify that the current release section contains every PR git-cliff would render after the
# previous Kernel release. This check runs against GitHub's merge ref, so it becomes stale whenever
# main moves.
verify_release_changelog() {
    local version="${1:-}"
    local previous_tag section subjects subject pr
    local missing=0

    if [[ -z "$version" ]]; then
        version=$(get_current_version "delta_kernel")
    fi

    previous_tag=$(latest_kernel_release_tag)
    if [[ -z "$previous_tag" ]]; then
        log_warning "No prior Kernel release tag found; skipping changelog verification"
        return 0
    fi
    if [[ "$previous_tag" == "v$version" ]]; then
        log_info "Workspace version $version is already tagged; no release changelog to verify"
        return 0
    fi

    section=$(release_changelog_section "$version")
    if [[ -z "$section" ]]; then
        log_warning "CHANGELOG.md has no section for v$version"
        return 1
    fi

    if ! subjects=$(release_changelog_subjects "$version"); then
        log_warning "Could not determine the changelog entries for v$version"
        return 1
    fi

    while IFS= read -r subject; do
        if [[ "$subject" =~ \(\#([0-9]+)\)$ ]]; then
            pr="${BASH_REMATCH[1]}"
            if ! grep -Fq "[#$pr]:" <<< "$section"; then
                log_warning "CHANGELOG.md v$version is missing PR #$pr: $subject"
                missing=1
            fi
        fi
    done <<< "$subjects"

    if (( missing != 0 )); then
        log_warning "Update from main, then run: ./release.sh changelog $version"
        return 1
    fi

    log_success "CHANGELOG.md v$version covers every merged PR since $previous_tag"
}

strip_release_changelog_section() {
    local output="$2" heading
    heading=$(release_changelog_heading "$1")
    awk -v heading="$heading" '
        index($0, heading) == 1 { skipping = 1; next }
        skipping && /^## \[v/ { skipping = 0 }
        !skipping { print }
    ' "$REPO_ROOT/CHANGELOG.md" > "$output"
}

# Replace, rather than append, the pending release section so this command is safe to rerun after
# the release branch is updated from main.
refresh_release_changelog() {
    local version="$1"
    local changelog="$REPO_ROOT/CHANGELOG.md"
    local backup stripped

    backup=$(mktemp "${TMPDIR:-/tmp}/delta-kernel-changelog-backup.XXXXXX")
    stripped=$(mktemp "${TMPDIR:-/tmp}/delta-kernel-changelog-stripped.XXXXXX")
    cp "$changelog" "$backup"
    strip_release_changelog_section "$version" "$stripped"
    mv "$stripped" "$changelog"

    if ! git cliff --repository "$REPO_ROOT" --config "$REPO_ROOT/cliff.toml" \
        --use-branch-tags --unreleased --prepend "$changelog" --include-path "*" \
        --tag "$version"; then
        mv "$backup" "$changelog"
        log_error "Failed to refresh CHANGELOG.md"
    fi

    rm -f "$backup"
    log_success "Refreshed CHANGELOG.md for v$version"
}

# Prompt user for confirmation
confirm() {
    local prompt="$1"
    local response

    echo -e -n "${YELLOW}${prompt} [y/N]${NC} "
    read -r response

    [[ "$response" =~ ^[Yy] ]]
}

# handle release branch workflow (CHANGELOG updates, README updates, PR to main)
handle_release_branch() {
    local version="$1"

    log_info "Starting release preparation for version $version..."

    # Update CHANGELOG and README
    log_info "Updating CHANGELOG.md and README.md..."
    if ! run_cargo_release "$version"; then
        log_error "Failed to update CHANGELOG and README"
    fi

    if ! verify_release_changelog "$version"; then
        log_error "Generated changelog is incomplete"
    fi

    if confirm "Print diff of CHANGELOG/README changes?"; then
        git diff --stat HEAD^
        git diff HEAD^
    fi

    if confirm "Would you like to push these changes to 'origin' remote?"; then
        local current_branch
        current_branch=$(git rev-parse --abbrev-ref HEAD)

        log_info "Pushing changes to remote..."
        git push origin "$current_branch"

        if confirm "Would you like to create a PR to merge this release into 'main'?"; then
            if command -v gh >/dev/null 2>&1; then
                gh pr create --title "release $version" --body "release $version"
                log_success "PR created successfully"
            else
                log_warning "GitHub CLI not found. Please create a PR manually."
            fi
        fi
    fi
}

# Handle main branch workflow (publish and tag)
handle_main_branch() {
    # could potentially just use full 'cargo release' command here
    # publish order matters: each crate depends on the previous at the same workspace version
    publish "delta_kernel_derive"
    publish "delta_kernel"
    publish "delta_kernel_default_engine"

    # hack: just redo getting the version
    local version
    version=$(get_current_version "delta_kernel")

    if confirm "Would you like to tag this release?"; then
        log_info "Tagging release $version..."
        if confirm "Tagging as v$version. continue?"; then
            git tag -a "v$version" -m "Release v$version"
            git push upstream tag "v$version"
            log_success "Tagged release $version"
        fi
    fi
}

publish() {
    local crate_name="$1"
    local current_version
    current_version=$(get_current_version "$crate_name")

    if is_version_published "$crate_name"; then
        log_error "$crate_name version $current_version is already published to crates.io"
    fi
    log_info "[DRY RUN] Publishing $crate_name version $current_version to crates.io..."
    if ! cargo publish --dry-run -p "$crate_name"; then
        log_error "Failed to publish $crate_name to crates.io"
    fi

    if confirm "Dry run complete. Continue with publishing?"; then
        log_info "Publishing $crate_name version $current_version to crates.io..."
        if ! cargo publish -p "$crate_name"; then
            log_error "Failed to publish $crate_name to crates.io"
        fi
        log_success "Successfully published $crate_name version $current_version to crates.io"
    fi
}


validate_version() {
    local version=$1
    # Check if version starts with a number
    if [[ ! $version =~ ^[0-9] ]]; then
        log_error "Version must start with a number (e.g., '0.1.1'). Got: '$version'"
    fi
}

main() {
    case "${1:-}" in
        changelog)
            if [[ $# -ne 2 ]]; then
                log_error "Usage: $0 changelog <version>"
            fi
            check_changelog_requirements
            validate_version "$2"
            refresh_release_changelog "$2"
            ;;
        verify-changelog)
            if [[ $# -gt 2 ]]; then
                log_error "Usage: $0 verify-changelog [version]"
            fi
            check_changelog_verification_requirements
            if ! verify_release_changelog "${2:-}"; then
                log_error "Release changelog is incomplete"
            fi
            ;;
        *)
            check_requirements
            if is_main_branch; then
                if [[ $# -ne 0 ]]; then
                    log_error "Version argument not expected on main branch\nUsage: $0"
                fi
                handle_main_branch
            else
                if [[ $# -ne 1 ]]; then
                    log_error "Version argument required when on release branch\nUsage: $0 <version>"
                fi
                validate_version "$1"
                handle_release_branch "$1"
            fi
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
