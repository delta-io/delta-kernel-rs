#!/usr/bin/env bash

###################################################################################################
# USAGE:
# Release the kernel crates (they share the workspace version):
#   1. on a release branch: ./release.sh <version>        (example: ./release.sh 0.27.0)
#   2. after merging to main: ./release.sh tag delta_kernel
#
# Release one independently-versioned crate (the Unity Catalog crates):
#   1. on a release branch: ./release.sh crate <crate> <version>
#      (example: ./release.sh crate unity-catalog-delta-client-api 0.2.0)
#   2. after merging to main: ./release.sh tag <crate>
#
# Releasing both means running the kernel steps and then the per-crate steps; a kernel bump
# rewrites what the UC crates require of the kernel, but never their own versions.
###################################################################################################

# This is a script to automate a large portion of the release process for the crates we publish to
# crates.io. Currently `delta_kernel` (in the kernel/ dir), `delta_kernel_derive` (in the
# derive-macros/ dir), and `delta_kernel_default_engine` (in the default-engine/ dir) are released.
#
# The Unity Catalog crates carry their own versions rather than the workspace version. Both their
# literal `version` and their `[package.metadata.release] release = false` are needed to keep a
# `cargo release --workspace <version>` bump from sweeping them to the kernel's version. Their
# dependency requirements on the kernel still get rewritten by that bump.
#
# `release = false` also hides them from package selection, so bump them with
# `cargo release version -p <crate> <version> --isolated`, which rewrites their dependents'
# version requirements too.

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

    warn_dependents "delta_kernel" "$version"

    review_and_open_pr "release $version"
}

# A release rewrites what its dependents require of it, but cannot know whether their own APIs broke
# along with it. List the crates that depend on this one and keep their own versions, with the
# command to bump each, so a breaking release does not leave a dependent claiming the old version.
warn_dependents() {
    local crate_name="$1" version="$2" dependent
    local dependents
    dependents=$(independent_dependents_of "$crate_name")

    [[ -n "$dependents" ]] || return 0

    log_warning "These crates depend on $crate_name and keep their own versions. If $version breaks"
    log_warning "their API, bump each one before tagging:"
    while read -r dependent; do
        [[ -n "$dependent" ]] || continue
        log_warning "  ./release.sh crate $dependent <version>"
    done <<< "$dependents"
}

# Publishable workspace crates that depend on the named crate and do not track the workspace
# version. Derived rather than hardcoded so a new crate on its own version line is picked up
# automatically. The examples also carry literal versions but set `publish = false`, which is what
# distinguishes them.
independent_dependents_of() {
    local crate_name="$1" workspace_version
    workspace_version=$(get_current_version "delta_kernel")
    cargo metadata --no-deps --format-version 1 | \
        jq -r --arg wv "$workspace_version" --arg dep "$crate_name" \
        '.packages[]
         | select(.version != $wv and .publish == null)
         | select(any(.dependencies[]; .kind == null and .name == $dep))
         | .name' | sort
}

# Bump one independently-versioned crate and the requirements its dependents place on it.
# `--isolated` is what lets `-p` select a crate that sets `release = false`; it discards
# release.toml, which costs nothing for a version bump (no tag, publish, or commit happens).
handle_crate_release() {
    local crate_name="$1" version="$2"

    case "$crate_name" in
        delta_kernel | delta_kernel_derive | delta_kernel_default_engine)
            log_error "$crate_name uses the workspace version\nUsage: $0 <version>"
            ;;
    esac
    if [[ -z "$(get_current_version "$crate_name")" ]]; then
        log_error "Could not find crate '$crate_name' in workspace"
    fi

    if ! is_working_tree_clean; then
        log_error "Working tree must be clean before releasing"
    fi

    log_info "Bumping $crate_name to $version..."
    if ! cargo release version -p "$crate_name" "$version" --isolated --execute --no-confirm; then
        log_error "Failed to bump $crate_name"
    fi

    update_crate_changelog "$crate_name" "$version"

    git add -A
    git commit -q -m "release $crate_name $version"

    warn_dependents "$crate_name" "$version"
    review_and_open_pr "release $crate_name $version"
}

# Prepend this crate's commits to its own CHANGELOG. Scoped by path so the changelog only collects
# commits that touched this crate. cliff.toml's template renders the leading `v`, so --tag takes the
# tag name without it.
update_crate_changelog() {
    local crate_name="$1" version="$2"
    local changelog="$crate_name/CHANGELOG.md"

    log_info "Updating $changelog..."
    # --prepend needs the file to exist, and a crate's first release has no changelog yet.
    [[ -f "$changelog" ]] || : > "$changelog"
    if ! git cliff --config cliff.toml --unreleased --prepend "$changelog" \
        --include-path "$crate_name/*" --tag "${version}_${crate_name}"; then
        log_error "Failed to update $changelog"
    fi
}

# Show the pending release commit, then optionally push it and open a PR.
review_and_open_pr() {
    local title="$1"

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
                gh pr create --title "$title" --body "$title"
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

    tag_release "delta_kernel"
}

# Tag name for a crate's release. The kernel crates share the workspace version, so one bare
# `v<version>` covers all of them and `delta_kernel` stands in for the set. Crates on their own
# version line take a `_<crate>` suffix, so a low version number cannot be mistaken for an old
# kernel tag (the kernel really was at 0.1.0 once).
tag_name_for() {
    local crate_name="$1" version="$2"
    case "$crate_name" in
        delta_kernel) echo "v$version" ;;
        *) echo "v${version}_${crate_name}" ;;
    esac
}

# Tag a release and push the tag to upstream. Pass the commit to tag if it is not HEAD.
tag_release() {
    local crate_name="$1" commit="${2:-HEAD}"
    local version tag

    # These are published from the kernel's workspace version, so they carry no tag of their own.
    case "$crate_name" in
        delta_kernel_derive | delta_kernel_default_engine)
            log_error "$crate_name shares the kernel release tag; tag 'delta_kernel' instead"
            ;;
    esac

    version=$(get_current_version "$crate_name")
    if [[ -z "$version" ]]; then
        log_error "Could not find crate '$crate_name' in workspace"
    fi
    tag=$(tag_name_for "$crate_name" "$version")

    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        log_error "tag $tag already exists"
    fi

    if confirm "Tag $crate_name $version as $tag at $(git rev-parse --short "$commit")?"; then
        git tag -a "$tag" "$commit" -m "Release $tag"
        git push upstream tag "$tag"
        log_success "Tagged and pushed $tag"
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

case "${1:-}" in
    crate)
        if [[ $# -ne 3 ]]; then
            log_error "Usage: $0 crate <crate> <version>"
        fi
        validate_version "$3"
        handle_crate_release "$2" "$3"
        ;;
    tag)
        if [[ $# -lt 2 || $# -gt 3 ]]; then
            log_error "Usage: $0 tag <crate> [commit]"
        fi
        tag_release "$2" "${3:-HEAD}"
        ;;
    *)
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
