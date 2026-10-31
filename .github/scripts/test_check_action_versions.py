"""Regression tests for deduplicating the complete native action dependency graph."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml

from check_action_versions import check_versions


SHA = "a" * 40
OTHER_SHA = "b" * 40


def dependency(nwo="actions/checkout", ref="v7.0.1", sha=SHA, **extra):
    return dict(nwo=nwo, ref=ref, sha=sha, direct=True, **extra)


class ActionVersionsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = ".github/workflows/build.yml"
        path = self.root / self.path
        path.parent.mkdir(parents=True)
        path.write_text("name: test\n")
        self.report = dict(
            cli_version="v0.3.0", lockfile_version="v0.0.3", valid=True, findings=[],
            dependencies=[], workflows=[dict(path=self.path, valid=True, dependencies=[])],
        )
        self.lock = dict(version="v0.0.3", workflows={self.path: []}, dependencies={})
        self.add(dependency())

    def add(self, dep, path=None):
        path = path or self.path
        self.report["dependencies"].append(dep)
        if path not in self.lock["workflows"]:
            (self.root / path).write_text("name: test\n")
            self.report["workflows"].append(dict(path=path, valid=True, dependencies=[]))
            self.lock["workflows"][path] = []
        next(w for w in self.report["workflows"] if w["path"] == path)["dependencies"].append(dep)
        key = self.lock_dependency(dep)
        self.lock["workflows"][path].append(key)
        return key

    def lock_dependency(self, dep):
        key = f"{dep['nwo']}@{dep['ref']}"
        self.lock["dependencies"][key] = dict(
            ref=dep["ref"], commit="sha1-" + dep["sha"],
            hostname=dep.get("hostname", "github.com"), owner_id=1, repo_id=2,
        )
        return key

    def check(self):
        return check_versions(self.report, self.lock, self.root)

    def test_same_sha_with_different_refs_case_and_subactions_is_allowed(self):
        self.add(dependency("Actions/Checkout/subaction", ref="v7", sha=SHA.upper()))
        self.assertEqual(self.check(), [])

    def test_conflict_in_checkers_own_checkout_is_rejected(self):
        self.add(dependency(ref="v6.0.0", sha=OTHER_SHA), ".github/workflows/other.yaml")
        errors = "\n".join(self.check())
        for detail in (self.path, "other.yaml", SHA, OTHER_SHA, "v6.0.0", "multiple commits"):
            self.assertIn(detail, errors)

    def test_nested_conflict_is_detected_when_native_inventory_omits_it(self):
        self.add(dependency("Swatinem/rust-cache", ref="v2.9.1"))
        parent = self.add(dependency("actions-rust-lang/setup-rust-toolchain", ref="v1.15.4"))
        child = self.lock_dependency(
            dependency("swatinem/rust-cache", ref=OTHER_SHA, sha=OTHER_SHA)
        )
        self.lock["dependencies"][child]["ref"] = "v2.8.2"
        self.lock["dependencies"][parent]["uses"] = [child]
        errors = "\n".join(self.check())
        for detail in (parent, SHA, OTHER_SHA, "swatinem/rust-cache: multiple commits"):
            self.assertIn(detail, errors)
        self.lock["dependencies"][child]["commit"] = "sha1-" + SHA
        self.assertEqual(self.check(), [])

    def test_deeply_nested_dependencies_missing_pins_and_cycles(self):
        child = self.lock_dependency(dependency("octo/composite", ref="v1"))
        grandchild = self.lock_dependency(dependency("octo/tool", ref="v1"))
        self.lock["dependencies"]["actions/checkout@v7.0.1"]["uses"] = [child]
        self.lock["dependencies"][child]["uses"] = [grandchild]
        self.assertEqual(self.check(), [])
        self.lock["dependencies"][grandchild]["uses"] = [child]
        self.assertIn("cyclic dependency", self.check()[0])
        del self.lock["dependencies"][grandchild]
        self.assertIn(grandchild, self.check()[0])

    def test_hosts_are_separate_repository_identities(self):
        self.add(dependency(ref="v6", sha=OTHER_SHA, hostname="example.ghe.com"))
        self.assertEqual(self.check(), [])

    def test_missing_workflow_and_inconsistent_inventories_are_rejected(self):
        (self.root / ".github/workflows/new.yaml").write_text("name: new\n")
        self.assertIn("new.yaml", self.check()[0])
        self.report["dependencies"].append(dependency(sha=OTHER_SHA))
        self.assertIn("inventories disagree", "\n".join(self.check()))

    def test_report_cannot_disagree_with_lockfile_roots(self):
        self.lock["dependencies"]["actions/checkout@v7.0.1"]["commit"] = "sha1-" + OTHER_SHA
        self.assertIn("root inventories disagree", self.check()[0])

    def test_empty_workflow_may_omit_dependencies(self):
        path = ".github/workflows/disabled.yml"
        (self.root / path).write_text("# disabled\n")
        self.report["workflows"].append(dict(path=path, valid=True))
        self.lock["workflows"][path] = []
        self.assertEqual(self.check(), [])

    def test_native_failure_missing_and_malformed_reports_fail_closed(self):
        cases = [None, [], {}, dict(self.report, valid=False),
                 dict(self.report, cli_version="v999"), dict(self.report, workflows=[]),
                 dict(self.report, dependencies=[])]
        for field in ("valid", "findings", "workflows", "dependencies"):
            report = deepcopy(self.report)
            del report[field]
            cases.append(report)
        for report in cases:
            with self.subTest(report=report):
                self.assertTrue(check_versions(report, self.lock, self.root))
        for lock in (None, {}, dict(self.lock, version="v999")):
            with self.subTest(lock=lock):
                self.assertTrue(check_versions(self.report, lock, self.root))

    def test_unresolved_dependency_workflow_failure_and_unused_pin_are_rejected(self):
        for change in (dict(sha=""), dict(sha="abcd"), dict(nwo=None), dict(ref=None)):
            with self.subTest(change=change):
                report = deepcopy(self.report)
                report["workflows"][0]["dependencies"][0].update(change)
                self.assertIn("Invalid", check_versions(report, self.lock, self.root)[0])
        self.report["workflows"][0]["valid"] = False
        self.assertIn("native verification failed", self.check()[0])
        self.report["workflows"][0]["valid"] = True
        self.lock_dependency(dependency("octo/unused"))
        self.assertIn("unused lock entry", self.check()[0])

    def test_cli_success_conflict_and_unreadable_input(self):
        script = Path(__file__).with_name("check_action_versions.py")
        report_path = self.root / "report.json"
        lock_path = self.root / ".github/workflows/actions.lock"
        for conflict in (False, True):
            if conflict:
                self.add(dependency(ref="v6.0.0", sha=OTHER_SHA))
            report_path.write_text(json.dumps(self.report))
            lock_path.write_text(yaml.safe_dump(self.lock))
            result = subprocess.run(
                [sys.executable, str(script), str(report_path), "--root", str(self.root)],
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, int(conflict), result.stdout + result.stderr)
        for content in ("{", "null"):
            report_path.write_text(content)
            result = subprocess.run(
                [sys.executable, str(script), str(report_path)], capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        report_path.unlink()
        result = subprocess.run(
            [sys.executable, str(script), str(report_path)], capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
