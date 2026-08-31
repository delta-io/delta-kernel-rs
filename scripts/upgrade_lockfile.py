#!/usr/bin/env python3
"""
upgrade_lockfile: Enforce a "no newer than N days" supply-chain policy on Cargo.lock.

The policy: every crates.io-sourced package in the workspace must be at a
version that was published more than --days days ago. The default is 7 days,
which gives malicious uploads a week to be detected and yanked before they
land in the lock.

== Critical user journeys ==

CUJ 1 - Audit the current lock (read-only, CI-friendly):
    ./scripts/upgrade_lockfile.py --check
  Reports every package younger than --days. Never modifies files. Exits 1
  if any violation exists, 0 if the lock is compliant.

CUJ 2 - Bulk upgrade, capped at the policy:
    ./scripts/upgrade_lockfile.py
  Runs `cargo update` over the whole workspace (bumps everything to latest
  available), then walks back any package that came in too new. Use this to
  refresh the entire lock to the newest versions that still satisfy the policy.

CUJ 3 - Upgrade ONE crate to its newest compliant version:
    ./scripts/upgrade_lockfile.py object_store
  Auto-picks the newest version of `object_store` on crates.io that's >= --days
  old, runs the targeted bump, then walks back any too-new transitives that
  came along for the ride.

CUJ 4 - Upgrade ONE crate to a specific version:
    ./scripts/upgrade_lockfile.py object_store --precise 0.13.2
  Same as CUJ 3, but YOU choose the target version. Fails fast if the version
  you picked is itself too new under the policy.

== Options ==
  --days <N>    Minimum age in days for any package (default: 7)
  --check       Audit-only; pair only with --days. Modifies nothing.
  --dry-run     Show what would change without modifying Cargo.lock
  --strict      Fail (exit 1) if any crate's metadata couldn't be fetched from
                crates.io. Without --strict, unfetchable crates are silently
                treated as compliant. CI uses this for a fail-shut posture.

Exits 0 if the lockfile is compliant after the run, 1 otherwise.

Requires Python 3.7+ (no third-party dependencies; uses `cargo metadata` for
lock parsing instead of `tomllib`).
"""

import sys

if sys.version_info < (3, 7):
    sys.stderr.write(
        "upgrade_lockfile.py requires Python 3.7+ (you have {}). "
        "Try invoking with an explicit interpreter, e.g.:\n"
        "  python3.11 {} ...\n".format(sys.version.split()[0], sys.argv[0])
    )
    sys.exit(1)

import concurrent.futures
import json
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

HEADERS = {"User-Agent": "delta-kernel-rs-fix-lockfile (+https://github.com/delta-io/delta-kernel-rs)"}

# Concurrent crates.io fetches. The endpoint is CDN-fronted; 8 in flight is
# comfortably polite and ~25x faster than serial 0.5s-spaced calls on a
# 500-crate workspace.
FETCH_WORKERS = 8

# Walk-back retries: after downgrading a crate, its dependencies may shift and
# pull in NEW too-new versions. We re-scan and walk back again until either
# the lock is compliant or we hit this cap. 5 is more than enough in practice;
# higher than that usually indicates an unresolvable constraint set.
MAX_ITERATIONS = 5

# Per crate, per iteration: if `cargo update --precise X` rejects the newest
# compliant version (because some other crate's constraint forbids it), try
# the next-newest, and so on, up to this many attempts. Without this, a single
# constraint conflict aborts the walk-back for that crate.
MAX_CANDIDATES_PER_CRATE = 5

# Type aliases for readability.
Snapshot = Dict[str, Set[str]]                          # {name: {version, ...}}
Change = Tuple[str, Set[str], str]                      # (name, old_versions, new_version)
Violation = Tuple[str, str, datetime]                   # (name, version, published_at)


# == Cargo workspace inspection ================================================

def lockfile_packages() -> Snapshot:
    """Return {name: {version, ...}} for every crates.io-sourced package in the
    resolved workspace graph. Uses `cargo metadata --locked` so the lockfile
    is never modified."""
    out = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        text=True,
    )
    meta = json.loads(out)
    packages: Snapshot = {}
    for pkg in meta.get("packages", []):
        # Path deps (workspace members) and git deps have non-registry sources;
        # skip them since they don't have a crates.io publish date.
        source = pkg.get("source") or ""
        if source.startswith("registry+"):
            packages.setdefault(pkg["name"], set()).add(pkg["version"])
    return packages


def diff_snapshots(before: Snapshot, after: Snapshot) -> List[Change]:
    """Returns (name, old_versions, new_version) for every version present in
    `after` but not in `before`."""
    changed = []
    for name, new_versions in after.items():
        old_versions = before.get(name, set())
        for new_ver in sorted(new_versions - old_versions):
            changed.append((name, old_versions, new_ver))
    return changed


# == crates.io helpers =========================================================

_version_cache: Dict[str, List[dict]] = {}

# Crates we tried to fetch from crates.io but couldn't (HTTP error, timeout, etc.).
# Used by --strict to fail the run rather than silently treating unfetchable crates
# as compliant. {crate_name: human-readable reason}.
_fetch_errors: Dict[str, str] = {}


def fetch_versions(crate_name: str) -> List[dict]:
    """Cached fetch of all published versions of a crate."""
    if crate_name in _version_cache:
        return _version_cache[crate_name]
    url = f"https://crates.io/api/v1/crates/{crate_name}/versions"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            versions = json.loads(resp.read()).get("versions", [])
    except urllib.error.HTTPError as e:
        reason = f"HTTP {e.code}"
        print(f"  WARNING: crates.io {reason} for {crate_name}")
        _fetch_errors[crate_name] = reason
        versions = []
    except Exception as e:
        reason = str(e)
        print(f"  WARNING: could not fetch {crate_name}: {reason}")
        _fetch_errors[crate_name] = reason
        versions = []
    _version_cache[crate_name] = versions
    return versions


def prefetch_versions(names: Iterable[str], label: str = "crates") -> None:
    """Concurrently warm `fetch_versions` for every name. Prints progress so the
    user knows the script isn't hung while making hundreds of HTTP calls."""
    todo = sorted({n for n in names if n not in _version_cache})
    if not todo:
        return
    total = len(todo)
    print(f"  Fetching {label} metadata ({total} crate(s) from crates.io)...", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = {ex.submit(fetch_versions, n): n for n in todo}
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            done += 1
            # Progress every 25 crates, plus a final tick at the end.
            if done % 25 == 0 or done == total:
                print(f"    {done}/{total}", flush=True)
            try:
                fut.result()
            except Exception as e:
                print(f"  WARNING: failed to fetch {futures[fut]}: {e}", flush=True)


def release_date(versions: List[dict], version_num: str) -> Optional[datetime]:
    for v in versions:
        if v["num"] == version_num:
            raw = v.get("created_at", "")
            return datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else None
    return None


def compliant_candidates(versions: List[dict], cutoff: datetime) -> List[str]:
    """Non-yanked, non-prerelease versions published on or before cutoff, newest-first."""
    candidates: List[Tuple[datetime, str]] = []
    for v in versions:
        if v.get("yanked"):
            continue
        num = v["num"]
        if "-" in num:  # skip prereleases (1.2.3-alpha etc.)
            continue
        raw = v.get("created_at", "")
        if not raw:
            continue
        created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if created <= cutoff:
            candidates.append((created, num))
    candidates.sort(reverse=True)
    return [num for _, num in candidates]


# == cargo wrapper =============================================================

def semver_compatible(a: str, b: str) -> bool:
    """True if cargo's default caret rule would treat versions `a` and `b` as
    semver-compatible. Cargo's rule: all components up to and INCLUDING the
    first non-zero one must match.

    Examples (matches/doesn't match):
      0.13.1 vs 0.13.2 -> True   (first non-zero is the 13, both match)
      0.12.5 vs 0.13.2 -> False  (13 != 12 at the first non-zero position)
      1.2.3  vs 1.5.0  -> True   (first non-zero is the 1, both match)
      1.2.3  vs 2.0.0  -> False  (1 != 2)
    """
    def components(v: str) -> List[int]:
        # Drop build metadata (1.2.3+build) and prerelease (1.2.3-alpha).
        core = v.split("+", 1)[0].split("-", 1)[0]
        out: List[int] = []
        for p in core.split("."):
            if p.isdigit():
                out.append(int(p))
            else:
                break
        return out

    pa, pb = components(a), components(b)
    for i in range(min(len(pa), len(pb))):
        if pa[i] != pb[i]:
            return False
        if pa[i] != 0:
            return True
    return True


def lock_instance(name: str, target_version: str, snap: Snapshot) -> Optional[str]:
    """Return the version of `name` currently in the lock that is semver-compatible
    with `target_version`, or None if no such instance exists. Used to build
    unambiguous `name@version` cargo specs when the lock has multiple instances
    of the same crate (e.g. object_store 0.12.x AND 0.13.x)."""
    for v in snap.get(name, set()):
        if semver_compatible(v, target_version):
            return v
    return None


def cargo_update(
    spec: Optional[str] = None,
    precise: Optional[str] = None,
    dry_run: bool = False,
) -> Tuple[bool, str]:
    cmd = ["cargo", "update"]
    if spec:
        cmd.append(spec)
    if precise:
        cmd += ["--precise", precise]
    if dry_run:
        cmd.append("--dry-run")
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stderr.strip():
        print(result.stderr.strip())
    return (result.returncode == 0, result.stderr.strip())


# == audit + walk-back =========================================================

def audit_current_lock(cutoff: datetime) -> List[Violation]:
    """Returns (name, version, published) for every too-new package in the current lock."""
    snap = lockfile_packages()
    total = sum(len(v) for v in snap.values())
    print(f"  Auditing {total} package version(s) across {len(snap)} crate(s).", flush=True)
    prefetch_versions(snap.keys(), label="audit")
    too_new: List[Violation] = []
    for name, versions in sorted(snap.items()):
        for version in sorted(versions):
            published = release_date(fetch_versions(name), version)
            if published is not None and published > cutoff:
                too_new.append((name, version, published))
    return too_new


def print_change_table(changed: List[Change], cutoff: datetime) -> List[Tuple[str, str]]:
    """Print a tabular summary of changes; return (name, version) for too-new ones."""
    prefetch_versions((n for n, _, _ in changed), label="change")
    print(f"\n  {'Package':<38} {'Old':>14}  {'New':>14}  {'Released':<12}  Status")
    print(f"  {'-'*38}  {'-'*14}  {'-'*14}  {'-'*12}  ------")

    too_new = []
    for name, old_versions, new_ver in changed:
        old_str = ", ".join(sorted(old_versions)) if old_versions else "(new)"
        date = release_date(fetch_versions(name), new_ver)
        if date is None:
            status, date_str = "[WARN]  date unknown", "unknown"
        elif date > cutoff:
            status, date_str = "[FAIL] TOO NEW", str(date.date())
            too_new.append((name, new_ver))
        else:
            status, date_str = "[OK] ok", str(date.date())
        print(f"  {name:<38} {old_str:>14}  {new_ver:>14}  {date_str:<12}  {status}")
    return too_new


def walk_back(
    too_new: List[Tuple[str, str]],
    cutoff: datetime,
    dry_run: bool,
) -> Tuple[int, List[str]]:
    """Try to downgrade every (name, version) to a compliant version.
    Returns (num_fixed, list_of_stuck_names)."""
    fixed = 0
    stuck: List[str] = []
    for name, bad_ver in too_new:
        candidates = compliant_candidates(fetch_versions(name), cutoff)
        if not candidates:
            print(f"  [WARN]  {name}: no compliant non-prerelease version exists")
            stuck.append(name)
            continue
        succeeded = False
        for candidate in candidates[:MAX_CANDIDATES_PER_CRATE]:
            if candidate == bad_ver:
                continue
            print(f"  {name}: {bad_ver} -> {candidate}")
            if dry_run:
                print(f"    [dry-run] would run: cargo update {name} --precise {candidate}")
                succeeded = True
                break
            # Always use name@bad_ver so we update the right instance even when
            # the lock has multiple versions of `name` (e.g. object_store 0.12 + 0.13).
            ok, _ = cargo_update(spec=f"{name}@{bad_ver}", precise=candidate)
            if ok:
                fixed += 1
                succeeded = True
                break
            print(f"    rejected; trying next candidate")
        if not succeeded:
            stuck.append(name)
    return fixed, stuck


def walk_back_until_stable(
    changed: List[Change],
    cutoff: datetime,
    days: int,
    dry_run: bool,
    exclude: Optional[Set[str]] = None,
) -> int:
    """Walk back too-new packages, re-checking each iteration in case downgrades cascade.
    Returns exit code (0 = compliant, 1 = stuck)."""
    exclude = exclude or set()
    if not changed:
        print("  No packages changed.")
        return 0

    too_new = [(n, v) for n, v in print_change_table(changed, cutoff) if n not in exclude]
    if not too_new:
        print(f"\n  All changes are within the {days}-day policy. [OK]")
        return 0

    for iteration in range(1, MAX_ITERATIONS + 1):
        print(f"\n  Iteration {iteration}: walking back {len(too_new)} too-new package(s)...")
        fixed, stuck = walk_back(too_new, cutoff, dry_run)
        if dry_run:
            return 1 if stuck else 0
        # Re-scan in case downgrades shifted other transitives.
        too_new = [(n, v) for n, v, _ in audit_current_lock(cutoff) if n not in exclude]
        if not too_new:
            print(f"\n  Lockfile now compliant with the {days}-day policy. [OK]")
            return 0
        if fixed == 0:
            print(f"\n  ERROR: no progress this iteration. Stuck on: {', '.join(stuck)}")
            return 1
    print(f"\n  ERROR: gave up after {MAX_ITERATIONS} iterations.")
    return 1


# == modes =====================================================================

def mode_check(cutoff: datetime, days: int) -> int:
    # === CUJ 1: read-only audit. Walks the entire lockfile, fetches publish ===
    # dates from crates.io, and reports every version younger than `days`.
    # Never invokes `cargo update`; never modifies any file.
    print("Mode: audit-only (read-only)")
    print()
    too_new = audit_current_lock(cutoff)
    if not too_new:
        print(f"[OK] Lockfile complies with the {days}-day policy.")
        return 0
    print(f"[FAIL] {len(too_new)} package(s) violate the {days}-day policy:\n")
    now = datetime.now(timezone.utc)
    for name, version, published in too_new:
        age = (now - published).days
        print(f"  {name} {version} (published {age}d ago, {published.date()})")
    return 1


def mode_update_all(cutoff: datetime, days: int, dry_run: bool) -> int:
    # === CUJ 2: bulk upgrade. Step 1 runs `cargo update` to bump every crate ===
    # in the workspace to its newest available version. Step 2 audits the new
    # lock and walks back any package that came in too new under the policy.
    print("Mode: update all packages (then walk back any that overshoot the policy)")
    print()

    before = lockfile_packages()

    print("Step 1/2 - running cargo update...")
    ok, _ = cargo_update(dry_run=dry_run)
    if not ok:
        return 1
    if dry_run:
        print("\n[dry-run] Lockfile not modified; skipping age checks.")
        return 0

    after = lockfile_packages()
    changed = diff_snapshots(before, after)
    print(f"\nStep 2/2 - checking {len(changed)} change(s)...")
    return walk_back_until_stable(changed, cutoff, days, dry_run)


def mode_update_one(spec: str, precise: str, cutoff: datetime, days: int, dry_run: bool) -> int:
    # === CUJ 3 or CUJ 4: single-package upgrade. The caller provides `spec` ===
    # (crate name) and `precise` (target version). Both CUJs reach here:
    #   - CUJ 3 (`./upgrade_lockfile.py PKG`): main() auto-picks `precise`.
    #   - CUJ 4 (`./upgrade_lockfile.py PKG --precise VER`): user picks it.
    # Three steps: (1) validate target isn't itself too new, (2) run the
    # targeted `cargo update`, (3) walk back any too-new transitives that
    # got dragged in (excluding the primary spec, which was chosen on purpose).
    print(f"Mode: update {spec} to {precise}, fixing too-new transitives")
    print()

    print(f"Step 1/3 - validating {spec} {precise}...")
    date = release_date(fetch_versions(spec), precise)
    if date is None:
        print(f"  WARNING: could not verify release date for {spec} {precise}; proceeding")
    elif date > cutoff:
        ready = (date + timedelta(days=days)).date()
        print(
            f"  [FAIL] {spec} {precise} was released {date.date()} (younger than {days}d).\n"
            f"     Pick an older version or wait until {ready}."
        )
        return 1
    else:
        print(f"  [OK] {spec} {precise} released {date.date()} - within policy.")

    before = lockfile_packages()

    # If the lock has multiple instances of `spec` (e.g. object_store 0.12 + 0.13),
    # build a name@version spec pointing at the compatible one so cargo doesn't
    # fail with an ambiguity error.
    update_spec = spec
    if len(before.get(spec, set())) > 1:
        existing = lock_instance(spec, precise, before)
        if existing is None:
            print(f"  [FAIL] No instance of {spec} in the lock is semver-compatible "
                  f"with target {precise}. Available: {sorted(before[spec])}")
            return 1
        update_spec = f"{spec}@{existing}"
        print(f"  Disambiguating: {spec} -> {update_spec}")

    print(f"\nStep 2/3 - running cargo update {update_spec} --precise {precise}...")
    ok, _ = cargo_update(spec=update_spec, precise=precise, dry_run=dry_run)
    if not ok:
        return 1
    if dry_run:
        print("\n[dry-run] Lockfile not modified; skipping transitive checks.")
        return 0

    after = lockfile_packages()
    changed = diff_snapshots(before, after)
    print(f"\nStep 3/3 - checking {len(changed)} change(s)...")
    # Exclude the primary package: the contributor picked that version on purpose.
    return walk_back_until_stable(changed, cutoff, days, dry_run, exclude={spec})


# == CLI =======================================================================

def parse_args(argv: List[str]) -> Tuple[Optional[str], Optional[str], int, bool, bool, bool]:
    spec: Optional[str] = None
    precise: Optional[str] = None
    days = 7
    dry_run = False
    check = False
    strict = False
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--precise" and i + 1 < len(argv):
            precise = argv[i + 1]
            i += 2
        elif a == "--days" and i + 1 < len(argv):
            days = int(argv[i + 1])
            i += 2
        elif a == "--dry-run":
            dry_run = True
            i += 1
        elif a == "--check":
            check = True
            i += 1
        elif a == "--strict":
            strict = True
            i += 1
        elif a in ("-h", "--help"):
            print(__doc__)
            sys.exit(0)
        elif a.startswith("-"):
            print(f"Unknown option: {a}")
            print(__doc__)
            sys.exit(1)
        else:
            spec = a
            i += 1
    return spec, precise, days, dry_run, check, strict


def main() -> None:
    spec, precise, days, dry_run, check, strict = parse_args(sys.argv[1:])
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    if not Path("Cargo.lock").exists():
        print("ERROR: Cargo.lock not found. Run from your workspace root.")
        sys.exit(1)

    prefix = "[DRY RUN] " if dry_run else ""
    print(f"{prefix}upgrade_lockfile  |  cutoff: {cutoff.date()} ({days} days)")
    print()

    # === Dispatch to one of the four CUJs based on which args were supplied ===
    #   --check                  -> CUJ 1: audit-only (read-only).
    #   (no args)                -> CUJ 2: bulk upgrade everything, walk back.
    #   PKG                      -> CUJ 3: auto-pick newest compliant, then bump.
    #   PKG --precise VER        -> CUJ 4: bump to the version YOU named.

    rc: int
    if check:
        # CUJ 1.
        if spec or precise or dry_run:
            print("ERROR: --check is read-only; pass it alone (with optional --days / --strict).")
            sys.exit(1)
        rc = mode_check(cutoff, days)
    else:
        if spec and not precise:
            # CUJ 3: resolve `precise` from the newest crates.io version of `spec`
            # that satisfies the age policy. After this block, `precise` is set
            # and we fall through to the same mode_update_one path as CUJ 4.
            print(f"Resolving newest compliant version of {spec}...")
            candidates = compliant_candidates(fetch_versions(spec), cutoff)
            if not candidates:
                print(f"ERROR: no compliant non-prerelease version of {spec} exists "
                      f"on crates.io within the {days}-day policy.")
                sys.exit(1)
            precise = candidates[0]
            print(f"  -> using {spec} {precise}\n")
        if spec and precise:
            # CUJ 3 (with auto-picked precise) or CUJ 4 (with user-specified precise).
            rc = mode_update_one(spec, precise, cutoff, days, dry_run)
        else:
            # CUJ 2: no spec, no precise -> bulk upgrade.
            rc = mode_update_all(cutoff, days, dry_run)

    # === --strict: any unfetchable crate is a hard failure ===
    # Lenient default: a network blip or a 5xx from crates.io shouldn't break
    # local workflows, so we warn but pass. CI passes --strict so it fails
    # closed instead of silently treating unfetchable crates as compliant.
    # If --strict starts producing too many false positives because crates.io
    # is intermittently down, drop it from CI and accept the lenient behavior.
    if strict and _fetch_errors:
        print(f"\n--strict: {len(_fetch_errors)} crate(s) couldn't be fetched from crates.io:")
        for name, reason in sorted(_fetch_errors.items()):
            print(f"  {name}: {reason}")
        rc = 1

    if rc == 0 and not dry_run:
        print("\nDone. [OK]  Tip: commit both Cargo.toml and Cargo.lock.")
    sys.exit(rc)


if __name__ == "__main__":
    main()
