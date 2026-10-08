#!/usr/bin/env bash

###################################################################################################
# USAGE:
# Release the kernel crates (they share the workspace version):
#   1. prepare on a release branch: ./release.sh release <version>
#   2. publish and tag on main after merging: ./release.sh release
#
# Prepare one independently-versioned crate (the Unity Catalog crates):
#   1. prepare on a release branch: ./release.sh crate <crate> <version>
#      (example: ./release.sh crate unity-catalog-delta-client-api 0.2.0)
#   2. create and push its tag after merging: ./release.sh tag <crate> [commit]
#      The tag command does not publish the crate to crates.io.
#
# Refresh a kernel release PR: ./release.sh changelog <version>
# Verify its changelog covers every merged PR: ./release.sh verify-changelog [version]
#
# A kernel bump rewrites what the UC crates require of the kernel, but never their own versions.
#
# Set DELTA_KERNEL_RELEASE_REGISTRY when cargo-release must use an alternate registry:
#   DELTA_KERNEL_RELEASE_REGISTRY=<registry-name> ./release.sh release 0.29.0
###################################################################################################

# This script prepares Kernel and UC releases, publishes the Kernel crates, and creates release tags.
#
# UC crates have literal versions and `release = false` to exclude them from Kernel version bumps.
# `--isolated` allows selecting them for per-crate bumps and updates dependent requirements.

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
UC_RELEASE_CRATES='["unity-catalog-delta-client-api", "unity-catalog-delta-rest-client", "delta-kernel-unity-catalog"]'

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
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; exit 1; }

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
    local status
    status=$(git status --porcelain --untracked-files=all) || return 1
    [[ -z "$status" ]]
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
    workspace_metadata | \
        jq -r --arg name "$crate_name" '.packages[] | select(.name == $name) | .version'
}

workspace_metadata() {
    cargo metadata --locked --no-deps --format-version 1 --manifest-path "$REPO_ROOT/Cargo.toml"
}

# Run cargo-release with an optional registry selection.
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

kernel_cliff() {
    git cliff --repository "$REPO_ROOT" --config "$REPO_ROOT/cliff.toml" --use-branch-tags "$@"
}

# Ask git-cliff for the latest Kernel release so changelog generation and verification use the
# same tag grammar from cliff.toml.
latest_kernel_release_tag() {
    kernel_cliff --latest --context | jq -r '.[0].version // empty'
}

release_changelog_heading() {
    local version="$1"
    printf '## [v%s]' "$version"
}

# Extract or remove a release section using the same section-boundary rules.
filter_release_changelog_section() {
    local mode="$1"
    local version="$2"
    local heading
    heading=$(release_changelog_heading "$version")

    awk -v heading="$heading" -v mode="$mode" '
        index($0, heading) == 1 {
            in_release = 1
            if (mode == "extract") { print }
            next
        }
        in_release && /^## \[v/ {
            in_release = 0
            if (mode == "extract") { exit }
        }
        mode == "extract" && in_release { print }
        mode == "strip" && !in_release { print }
    ' "$REPO_ROOT/CHANGELOG.md"
}

release_changelog_section() {
    filter_release_changelog_section extract "$1"
}

# Render the pending changelog from git-cliff's context so the template remains the source of truth
# for commit filtering and PR references.
render_release_changelog() {
    local version="$1"
    kernel_cliff --unreleased --include-path "*" --tag "$version" --context | \
        git cliff --config "$REPO_ROOT/cliff.toml" --from-context -
}

changelog_pr_reference_ids() {
    awk 'match($0, /^\[#[0-9]+\]:/) { print substr($0, 3, RLENGTH - 4) }'
}

changelog_pr_bullet_ids() {
    awk '
        {
            line = $0
            while (match(line, /\(\[#[0-9]+\]\)/)) {
                print substr(line, RSTART + 3, RLENGTH - 5)
                line = substr(line, RSTART + RLENGTH)
            }
        }
    '
}

# Verify that the current release section contains every PR git-cliff would render after the
# previous Kernel release. This check runs against GitHub's merge ref, so it becomes stale whenever
# main moves.
verify_release_changelog() {
    local version="${1:-}"
    local previous_tag section rendered expected_prs section_references section_bullets pr
    local missing=0

    if [[ -z "$version" ]]; then
        version=$(get_current_version "delta_kernel")
    fi

    if ! previous_tag=$(latest_kernel_release_tag); then
        log_warning "Could not resolve the latest Kernel release tag"
        return 1
    fi
    if [[ -z "$previous_tag" ]]; then
        log_warning "No prior Kernel release tag found"
        return 1
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

    if ! rendered=$(render_release_changelog "$version"); then
        log_warning "Could not render the expected changelog for v$version"
        return 1
    fi
    expected_prs=$(changelog_pr_reference_ids <<< "$rendered")
    section_references=$(changelog_pr_reference_ids <<< "$section")
    section_bullets=$(changelog_pr_bullet_ids <<< "$section")

    while IFS= read -r pr; do
        [[ -z "$pr" ]] && continue
        if ! grep -Fqx "$pr" <<< "$section_references"; then
            log_warning "CHANGELOG.md v$version is missing the link reference for PR #$pr"
            missing=1
        fi
        if ! grep -Fqx "$pr" <<< "$section_bullets"; then
            log_warning "CHANGELOG.md v$version is missing the changelog entry for PR #$pr"
            missing=1
        fi
    done <<< "$expected_prs"

    if (( missing != 0 )); then
        log_warning "Update from main, then run: ./release.sh changelog $version"
        return 1
    fi

    log_success "CHANGELOG.md v$version covers every merged PR since $previous_tag"
}

# Remove the changelog section for the version specified as the first argument.
strip_release_changelog_section() {
    local output="$2"
    filter_release_changelog_section strip "$1" > "$output"
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

    if ! kernel_cliff --unreleased --prepend "$changelog" --include-path "*" --tag "$version"; then
        cp "$backup" "$changelog"
        log_error "Failed to refresh CHANGELOG.md; original saved at $backup"
    fi

    log_success "Refreshed CHANGELOG.md for v$version; backup retained at $backup"
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

    warn_dependents "delta_kernel" "$version"

    review_and_open_pr "release $version"
}

# Dependency requirement updates do not determine whether dependents need their own version bumps.
warn_dependents() {
    local crate_name="$1" version="$2" dependent
    local dependents
    dependents=$(independent_dependents_of "$crate_name")

    [[ -n "$dependents" ]] || return 0

    log_warning "These crates depend on $crate_name and keep their own versions. If $version breaks"
    log_warning "their API, prepare each dependent release on a separate crate-release/ branch"
    log_warning "before tagging:"
    while read -r dependent; do
        [[ -n "$dependent" ]] || continue
        log_warning "  ./release.sh crate $dependent <version>"
    done <<< "$dependents"
}

# The per-crate flow supports the UC crates; Kernel packages use the shared release flow.
independent_release_packages() {
    workspace_metadata | jq -c --argjson names "$UC_RELEASE_CRATES" \
        '.packages[] | select(.name as $name | $names | index($name))
         | select(.publish == null)'
}

independent_dependents_of() {
    local crate_name="$1"
    independent_release_packages | \
        jq -r --arg dep "$crate_name" '
         select(any(.dependencies[]; .kind == null and .name == $dep))
         | .name' | sort
}

crate_directory() {
    local crate_name="$1" manifest_path
    manifest_path=$(workspace_metadata | \
        jq -r --arg name "$crate_name" \
        '.packages[] | select(.name == $name) | .manifest_path')
    if [[ "$manifest_path" != "$REPO_ROOT/"* ]]; then
        log_error "Could not find crate '$crate_name' inside the repository"
    fi
    manifest_path="${manifest_path#"$REPO_ROOT/"}"
    dirname "$manifest_path"
}

# `--isolated` lets `-p` select crates marked `release = false` for a version-only bump.
handle_crate_release() {
    local crate_name="$1" version="$2" crate_path

    if is_main_branch; then
        log_error "Create a release branch before bumping a crate"
    fi

    if ! is_working_tree_clean; then
        log_error "Working tree must be clean before releasing"
    fi

    if [[ -z "$(independent_release_packages | \
        jq -r --arg name "$crate_name" 'select(.name == $name) | .name')" ]]; then
        log_error "'$crate_name' must be a publishable crate on an independent version line"
    fi
    crate_path=$(crate_directory "$crate_name")

    log_info "Bumping $crate_name to $version..."
    if ! cargo release version -p "$crate_name" "$version" --isolated --execute --no-confirm; then
        log_error "Failed to bump $crate_name"
    fi

    update_crate_changelog "$crate_name" "$version" "$crate_path"

    warn_dependents "$crate_name" "$version"
    git add -A
    git commit -q -m "release $crate_name $version"
    review_and_open_pr "release $crate_name $version"
}

# cliff.toml renders the leading `v`, so --tag takes the tag name without it.
update_crate_changelog() {
    local crate_name="$1" version="$2"
    local crate_path="${3:-}"
    [[ -n "$crate_path" ]] || crate_path=$(crate_directory "$crate_name")
    local changelog="$REPO_ROOT/$crate_path/CHANGELOG.md"

    log_info "Updating $changelog..."
    # --prepend needs the file to exist, and a crate's first release has no changelog yet.
    [[ -f "$changelog" ]] || : > "$changelog"
    if ! git cliff --repository "$REPO_ROOT" --config "$REPO_ROOT/cliff.toml" \
        --use-branch-tags \
        --tag-pattern "^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?_${crate_name}$" \
        --unreleased --prepend "$changelog" --include-path "$crate_path/*" \
        --tag "${version}_${crate_name}"; then
        log_error "Failed to update $changelog"
    fi
}

# Show the pending release commit, then optionally push it and open a PR.
review_and_open_pr() {
    local title="$1"

    if confirm "Print diff of the release commit?"; then
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

handle_main_branch() {
    # Publish dependencies before their dependents.
    publish "delta_kernel_derive"
    publish "delta_kernel"
    publish "delta_kernel_default_engine"

    tag_release "delta_kernel"
}

# The UC suffix separates independent versions from Kernel release tags.
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
    local version tag commit_hash

    if [[ "$crate_name" != delta_kernel && -z "$(independent_release_packages | \
        jq -r --arg name "$crate_name" 'select(.name == $name) | .name')" ]]; then
        log_error "'$crate_name' must be an independent publishable crate;\nTag 'delta_kernel' for Kernel releases"
    fi

    version=$(get_current_version "$crate_name")
    if [[ -z "$version" ]]; then
        log_error "Could not find crate '$crate_name' in workspace"
    fi
    tag=$(tag_name_for "$crate_name" "$version")

    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        log_error "tag $tag already exists"
    fi

    if ! commit_hash=$(git rev-parse --verify --end-of-options "${commit}^{commit}" 2>/dev/null); then
        log_error "Not a valid commit: $commit"
    fi
    local manifest="Cargo.toml"
    [[ "$crate_name" == delta_kernel ]] || manifest="$(crate_directory "$crate_name")/Cargo.toml"
    git -C "$REPO_ROOT" diff --quiet "$commit_hash" -- "$manifest" || \
        log_error "Checkout's release manifest must match the commit being tagged"

    if confirm "Tag $crate_name $version as $tag at $(git rev-parse --short "$commit_hash")?"; then
        git tag -a "$tag" "$commit_hash" -m "Release $tag"
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

usage() {
    printf '%s\n' \
        "Usage:" \
        "  $0 release [version]" \
        "  $0 crate <crate> <version>" \
        "  $0 tag <crate> [commit]" \
        "  $0 changelog <version>" \
        "  $0 verify-changelog [version]" \
        "" \
        "release <version> prepares a kernel release; release on main publishes and tags it." \
        "crate <crate> <version> prepares a crate release." \
        "tag <crate> [commit] creates and pushes a tag without publishing."
}

main() {
    cd "$REPO_ROOT"
    case "${1:-}" in
        crate)
            if [[ $# -ne 3 ]]; then
                log_error "Usage: $0 crate <crate> <version>"
            fi
            check_requirements
            validate_version "$3"
            handle_crate_release "$2" "$3"
            ;;
        tag)
            if [[ $# -lt 2 || $# -gt 3 ]]; then
                log_error "Usage: $0 tag <crate> [commit]"
            fi
            check_requirements
            tag_release "$2" "${3:-HEAD}"
            ;;
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
        release)
            check_requirements
            if is_main_branch; then
                if [[ $# -ne 1 ]]; then
                    usage >&2
                    log_error "Version argument not expected on main branch"
                fi
                handle_main_branch
            else
                if [[ $# -ne 2 ]]; then
                    usage >&2
                    log_error "Version argument required when on release branch"
                fi
                validate_version "$2"
                handle_release_branch "$2"
            fi
            ;;
        "" | help | -h | --help)
            usage
            ;;
        *)
            usage >&2
            log_error "Unknown command: $1"
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
