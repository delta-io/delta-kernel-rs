#!/usr/bin/env bash

###################################################################################################
# USAGE:
# 1. on a release branch: ./release.sh <version> (example: ./release.sh 0.1.0)
# 2. on main branch (after merging release branch): ./release.sh
###################################################################################################

# This is a script to automate a large portion of the release process for the crates we publish to
# crates.io. Currently only `delta_kernel` (in the kernel/ dir) and `delta_kernel_derive` (in the
# derive-macros/ dir) are released.

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

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
    cargo metadata --no-deps --format-version 1 | \
        jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version'
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
    if ! cargo release --workspace "$version" --no-publish --no-push --no-tag --execute; then
        log_error "Failed to update CHANGELOG and README"
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
    publish "delta_kernel_derive"
    publish "delta_kernel"

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
