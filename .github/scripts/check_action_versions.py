"""Enforce one commit per action repository in the verified native workflow lockfile."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

import yaml


def check_versions(report: dict, lock: dict, root: Path) -> list[str]:
    """Return verification, coverage, format, and conflicting-version diagnostics.

    ``report`` must be successful gh-actions-lock v0.3.0 --verify JSON. ``lock`` is the
    parsed native lockfile; ``root`` supplies the workflows that must be covered. Traverse
    lockfile ``uses`` edges because the native JSON inventory omits nested dependencies.
    """
    try:
        return _check_versions(report, lock, root)
    except (KeyError, TypeError, ValueError) as error:
        return [f"Invalid native verification report or lockfile: {error}"]


def _check_versions(report, lock, root):
    if not isinstance(report, dict) or not isinstance(lock, dict):
        raise ValueError("expected report and lockfile objects")
    if (report["cli_version"] != "v0.3.0" or report["lockfile_version"] != "v0.0.3"
            or lock["version"] != "v0.0.3"):
        raise ValueError("unsupported CLI or lockfile version; review the checker before upgrading")
    if report["valid"] is not True:
        raise ValueError("native verification failed; inspect gh actions-lock --verify findings")
    if not isinstance(report["findings"], list):
        raise ValueError("findings must be a list")
    if not isinstance(report["workflows"], list) or not report["workflows"]:
        raise ValueError("workflow inventory is missing or empty")
    if not isinstance(report["dependencies"], list) or not report["dependencies"]:
        raise ValueError("dependency inventory is missing or empty")
    if not isinstance(lock["workflows"], dict) or not isinstance(lock["dependencies"], dict):
        raise ValueError("lockfile workflows and dependencies must be mappings")

    expected = {
        path.relative_to(root).as_posix()
        for pattern in ("*.yml", "*.yaml")
        for path in (root / ".github/workflows").glob(pattern)
    }
    identities = {}
    for key, entry in lock["dependencies"].items():
        if not isinstance(key, str) or "@" not in key or not isinstance(entry, dict):
            raise ValueError(f"invalid lock entry: {key!r}")
        nwo, ref = key.rsplit("@", 1)
        commit = entry["commit"]
        if not isinstance(commit, str) or not commit.startswith("sha1-"):
            raise ValueError(f"{key}: unresolved or invalid commit")
        identities[key] = _identity(dict(nwo=nwo, ref=ref, sha=commit[5:],
                                        hostname=entry.get("hostname", "github.com")))
        _references(entry.get("uses", []))

    errors = []
    paths = set()
    inventory = set()
    contexts = {key: set() for key in identities}
    for workflow in report["workflows"]:
        if not isinstance(workflow, dict):
            raise ValueError("workflow must be an object")
        path = workflow["path"]
        if not isinstance(path, str) or path in paths:
            raise ValueError(f"invalid or duplicate workflow path: {path!r}")
        paths.add(path)
        if workflow["valid"] is not True:
            errors.append(f"{path}: native verification failed")
        dependencies = workflow.get("dependencies", [])
        if not isinstance(dependencies, list):
            raise ValueError(f"{path}: dependencies must be a list")
        reported = {_identity(dep) for dep in dependencies}
        inventory.update(reported)
        roots = _references(lock["workflows"][path])
        if {identities[key] for key in roots} != reported:
            errors.append(f"{path}: native report and lockfile root inventories disagree")
        pending = [(key, None, ()) for key in roots]
        visited = set()
        while pending:
            key, parent, ancestors = pending.pop()
            if key in ancestors:
                raise ValueError(f"cyclic dependency: {' -> '.join((*ancestors, key))}")
            sha = identities[key][2]
            context = f"{path}: {key} -> {sha}"
            if parent:
                context += f" (required by {parent})"
            contexts[key].add(context)
            if key in visited:
                continue
            visited.add(key)
            pending.extend((child, key, (*ancestors, key))
                           for child in lock["dependencies"][key].get("uses", []))

    if paths != expected or set(lock["workflows"]) != expected:
        errors.append("Workflow coverage differs between files, native report, and lockfile: "
                      f"files={sorted(expected)}, report={sorted(paths)}, "
                      f"lockfile={sorted(lock['workflows'])}")
    if {_identity(dep) for dep in report["dependencies"]} != inventory:
        errors.append("Global and per-workflow native inventories disagree")
    versions = {}
    for key, (repository, ref, sha) in identities.items():
        if not contexts[key]:
            errors.append(f"{key}: unused lock entry; regenerate the lockfile")
        versions.setdefault(repository, {}).setdefault(sha, set()).update(contexts[key])
    for repository, commits in sorted(versions.items()):
        if len(commits) > 1:
            errors.append(f"{repository}: multiple commits; align direct and nested dependencies:")
            for sha, uses in sorted(commits.items()):
                errors.extend(f"  {context}" for context in sorted(uses or {sha}))
    return errors


def _references(value):
    if not isinstance(value, list) or any(not isinstance(key, str) for key in value):
        raise ValueError("workflow roots and dependency uses must be lists of action refs")
    return value


def _identity(dependency):
    if not isinstance(dependency, dict):
        raise ValueError("dependency must be an object")
    hostname = dependency.get("hostname", "github.com")
    nwo, ref, sha = (dependency[key] for key in ("nwo", "ref", "sha"))
    if not isinstance(hostname, str) or not re.fullmatch(r"[a-zA-Z0-9.-]+", hostname):
        raise ValueError("invalid dependency hostname")
    if not isinstance(nwo, str) or not re.fullmatch(r"[\w.-]+/[\w.-]+(?:/[\w./-]+)?", nwo):
        raise ValueError("invalid dependency repository")
    if not isinstance(ref, str) or not ref:
        raise ValueError(f"{nwo}: missing ref")
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", sha):
        raise ValueError(f"{nwo}@{ref}: unresolved or invalid SHA")
    return hostname.lower() + "/" + "/".join(nwo.lower().split("/")[:2]), ref, sha.lower()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="JSON output of gh actions-lock --verify")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    try:
        report = json.loads(args.report.read_text())
        lock = yaml.safe_load((args.root / ".github/workflows/actions.lock").read_text())
        errors = check_versions(report, lock, args.root)
    except (OSError, ValueError, yaml.YAMLError) as error:
        errors = [f"Cannot read native verification report or lockfile: {error}"]
    if errors:
        print("\n".join(errors))
        raise SystemExit(1)
    print("All workflows and resolved action dependencies use one commit per repository")
