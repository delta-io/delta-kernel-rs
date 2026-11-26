"""Exercise the lint runner's public CLI against small real ast-grep projects."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).resolve().with_name("run.py")
RULE_ID = "fixture-use-new"


@unittest.skipUnless(
    shutil.which(os.environ.get("AST_GREP", "ast-grep")),
    "ast-grep or an AST_GREP executable override is required for runner integration tests",
)
class RunnerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="kernel-lint-runner-")
        self.addCleanup(self.temp.cleanup)
        # Every command traverses a root containing characters requiring shell quoting.
        self.root = Path(self.temp.name) / "repo with spaces; $literal [x]"
        self.source = self.root / "kernel" / "src" / "lib.rs"
        self.source.parent.mkdir(parents=True)
        self.rule_dir = self.root / "tools" / "kernel-lint" / "rules" / "fixture"
        self.rule_dir.mkdir(parents=True)
        (self.root / "sgconfig.yml").write_text(json.dumps({
            "ruleDirs": ["tools/kernel-lint/rules"],
        }))
        self.set_rule(safe_fix=True)
        self.write_source("fn f() { old(1); }\n")

    def set_rule(self, safe_fix):
        (self.rule_dir / "fixture.yml").write_text(json.dumps({
            "id": RULE_ID,
            "language": "Rust",
            "severity": "warning",
            "message": "Prefer new() in this test fixture.",
            "rule": {"pattern": "old($VALUE)"},
            "fix": "new($VALUE)",
        }))
        (self.rule_dir / "metadata.json").write_text(json.dumps([
            {"id": RULE_ID, "safeFix": safe_fix},
        ]))

    def write_source(self, text):
        self.source.write_text(text)

    def cli(self, command, *arguments):
        return subprocess.run(
            [sys.executable, str(RUNNER), "--root", str(self.root), command] + list(arguments),
            cwd=str(self.root), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
        )

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def scan(self):
        result = self.cli("scan", "--format", "json")
        self.assert_success(result)
        data = json.loads(result.stdout)
        return data["findings"]

    def baseline(self):
        result = self.cli("baseline")
        self.assert_success(result)
        path = self.root / "tools" / "kernel-lint" / "baseline.json"
        self.assertTrue(path.is_file(), result.stdout + result.stderr)
        return path

    def reviewed_manifest(self, findings):
        path = self.root / "reviewed edits.json"
        path.write_text(json.dumps({"edits": [
            {"fingerprint": finding["fingerprint"], "file": finding["file"],
             "line": finding["range"]["start"]["line"] + 1,
             "replacement": finding["replacement"]}
            for finding in findings
        ]}))
        return path

    def test_baseline_accepts_line_shifts_but_rejects_duplicate_new_occurrence(self):
        self.baseline()
        self.assert_success(self.cli("check"))
        self.write_source("// Added documentation.\n\nfn f() { old(1); }\n")
        self.assert_success(self.cli("check"))
        self.write_source("// Added documentation.\n\nfn f() { old(1); old(1); }\n")
        result = self.cli("check")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_changed_matched_text_is_a_new_finding(self):
        self.baseline()
        self.write_source("fn f() { old(2); }\n")
        result = self.cli("check")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_removing_an_existing_finding_does_not_fail_on_stale_baseline(self):
        self.baseline()
        self.write_source("fn f() { new(1); }\n")
        self.assert_success(self.cli("check"))
        self.assertEqual(self.scan(), [])

    def test_warning_only_rule_is_not_rewritten_by_default_fix(self):
        self.set_rule(safe_fix=False)
        before = self.source.read_bytes()
        self.assertEqual(len(self.scan()), 1)
        result = self.cli("fix")
        self.assert_success(result)
        self.assertEqual(self.source.read_bytes(), before)
        self.assertEqual(len(self.scan()), 1)
        self.set_rule(safe_fix=True)
        self.assert_success(self.cli("fix"))
        self.assertEqual(self.source.read_text(), "fn f() { new(1); }\n")
        self.assertEqual(self.scan(), [])

    def test_suppression_requires_a_reason_and_applies_only_to_the_next_line(self):
        self.write_source(
            "fn f() {\n"
            "    // kernel-lint: allow " + RULE_ID + " -- compatibility fixture\n"
            "    old(1);\n"
            "    old(2);\n"
            "}\n"
        )
        findings = self.scan()
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0]["range"]["start"]["line"], 3)
        self.write_source(
            "fn f() {\n"
            "    // kernel-lint: allow " + RULE_ID + " --\n"
            "    old(1);\n"
            "}\n"
        )
        self.assertEqual(len(self.scan()), 1)
        self.write_source(
            "fn f() {\n"
            "    // kernel-lint: allow " + RULE_ID + " -- compatibility fixture\n\n"
            "    old(1);\n"
            "}\n"
        )
        self.assertEqual(len(self.scan()), 1)

        self.write_source(
            'fn f() { let text = r#"\n'
            "// kernel-lint: allow " + RULE_ID + ' -- raw text"#;\n'
            "old(1);\n}\n"
        )
        self.assertEqual(len(self.scan()), 1)

    def test_reviewed_manifest_fixes_only_selected_match_and_rejects_stale_source(self):
        self.set_rule(safe_fix=False)
        self.write_source("fn f() { old(1); old(2); }\n")
        findings = sorted(self.scan(), key=lambda f: f["range"]["start"]["column"])
        self.assertEqual(len(findings), 2)
        manifest = self.reviewed_manifest(findings[:1])
        reviewed = json.loads(manifest.read_text())
        before = self.source.read_bytes()
        for replacement in [None, 1, [], {}]:
            invalid = json.loads(json.dumps(reviewed))
            invalid["edits"][0]["replacement"] = replacement
            manifest.write_text(json.dumps(invalid))
            self.assertEqual(self.cli("fix", "--reviewed", str(manifest)).returncode, 2)
            self.assertEqual(self.source.read_bytes(), before)
        invalid = json.loads(json.dumps(reviewed))
        del invalid["edits"][0]["replacement"]
        manifest.write_text(json.dumps(invalid))
        self.assertEqual(self.cli("fix", "--reviewed", str(manifest)).returncode, 2)
        self.assertEqual(self.source.read_bytes(), before)

        rule_path = self.rule_dir / "fixture.yml"
        rule = json.loads(rule_path.read_text())
        rule["fix"] = "changed($VALUE)"
        rule_path.write_text(json.dumps(rule))
        self.assertEqual(self.scan()[0]["fingerprint"], findings[0]["fingerprint"])
        for document in [invalid, reviewed]:
            manifest.write_text(json.dumps(document))
            self.assertEqual(self.cli("fix", "--reviewed", str(manifest)).returncode, 2)
            self.assertEqual(self.source.read_bytes(), before)
        self.set_rule(safe_fix=False)
        manifest.write_text(json.dumps(reviewed))
        self.assert_success(self.cli("fix", "--reviewed", str(manifest)))
        self.assertEqual(self.source.read_text(), "fn f() { new(1); old(2); }\n")
        self.assertEqual(len(self.scan()), 1)
        before = self.source.read_bytes()
        result = self.cli("fix", "--reviewed", str(manifest))
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(self.source.read_bytes(), before)

    def test_overlapping_reviewed_edits_are_rejected_without_partial_changes(self):
        self.write_source("fn f() { old(old(1)); }\n")
        findings = self.scan()
        self.assertEqual(len(findings), 2)
        manifest = self.reviewed_manifest(findings)
        before = self.source.read_bytes()
        for _ in range(2):
            result = self.cli("fix", "--reviewed", str(manifest))
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            self.assertEqual(self.source.read_bytes(), before)

    def test_generated_source_is_never_rewritten(self):
        finding = self.scan()[0]
        # A generated-file header shifts the same matched call onto line two.
        finding["range"]["start"]["line"] = 1
        manifest = self.reviewed_manifest([finding])
        self.write_source("//! Generated by `cargo xtask fixture`.\nfn f() { old(1); }\n")
        before = self.source.read_bytes()
        result = self.cli("fix")
        # A default fix may skip generated files or refuse the attempted batch.
        self.assertIn(result.returncode, (0, 2), result.stdout + result.stderr)
        self.assertEqual(self.source.read_bytes(), before)
        result = self.cli("fix", "--reviewed", str(manifest))
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(self.source.read_bytes(), before)

        self.write_source("//! Generated columns: top-level columns\nfn f() { old(1); }\n")
        self.assert_success(self.cli("fix"))
        self.assertIn("new(1)", self.source.read_text())

    def test_control_flow_arguments_respect_closure_and_branch_boundaries(self):
        rule_id = "kernel-policy-no-control-flow-arguments"
        rule_source = RUNNER.parent / "rules" / "policy" / (rule_id + ".yml")
        (self.rule_dir / (rule_id + ".yml")).write_bytes(rule_source.read_bytes())
        (self.rule_dir / "metadata.json").write_text(json.dumps([
            {"id": RULE_ID, "safeFix": False},
            {"id": rule_id, "safeFix": False},
        ]))
        outer_if = "if first { one() } else if second { two() } else { three() }"
        outer_match = "match input { Some(value) => if flag { value } else { 0 }, None => 0 }"
        inner_match = "match nested { Some(value) => value, None => 0 }"
        borrowed_match = "match borrowed { Some(value) => value, None => 0 }"
        self.write_source(
            "fn f() {\n"
            "    consume(" + outer_if + ");\n"
            "    consume(" + outer_match + ");\n"
            "    consume(|| match allowed { Some(value) => value, None => 0 });\n"
            "    values.map(|value| if flag { value } else { 0 });\n"
            "    consume(|| inner(" + inner_match + "));\n"
            "    consume(&(" + borrowed_match + "));\n"
            "    (if flag { left() } else { right() }).method();\n"
            "    consume({ if flag { left() } else { right() } });\n"
            "    // kernel-lint: allow " + rule_id + " -- preserves argument timing\n"
            "    consume(if flag { left() } else { right() });\n"
            "}\n"
        )
        findings = self.scan()
        self.assertCountEqual(
            [finding["text"] for finding in findings],
            [outer_if, outer_match, inner_match, borrowed_match],
        )
        self.assertTrue(all("replacement" not in finding for finding in findings))
        before = self.source.read_bytes()
        self.assert_success(self.cli("fix"))
        self.assertEqual(self.source.read_bytes(), before)

    def test_native_suppression_directives_cannot_bypass_reason_policy(self):
        directives = [
            "// ast-grep-ignore",
            "// ast-grep-ignore: " + RULE_ID,
            "// ast-grep-ignore\n// ast-grep-ignore: " + RULE_ID,
            "/* ast-grep-ignore */",
            "/// ast-grep-ignore",
            "//! ast-grep-ignore",
            "/** ast-grep-ignore */",
            "/*! ast-grep-ignore */",
            "//// ast-grep-ignore",
            "//!! ast-grep-ignore",
            "/*** ast-grep-ignore */",
            "/*!! ast-grep-ignore */",
            "// prefixed ast-grep-ignore",
        ]
        for directive in directives:
            with self.subTest(directive=directive):
                self.write_source("fn f() {\n" + directive + "\nold(1);\n}\n")
                before = self.source.read_bytes()
                for arguments in [(), ("--rule", "^unrelated-rule$")]:
                    result = self.cli("scan", *arguments)
                    self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                    self.assertEqual(self.source.read_bytes(), before)

        self.write_source(
            'fn f() { let text = "// ast-grep-ignore"; '
            'let raw = r#"/* ast-grep-ignore: fixture-use-new */"#; old(1); }\n'
        )
        self.assertEqual(len(self.scan()), 1)
        self.write_source(
            "fn f() {\n"
            "// kernel-lint: allow " + RULE_ID + " -- compatibility fixture\n"
            "old(1);\n}\n"
        )
        self.assertEqual(self.scan(), [])


if __name__ == "__main__":
    unittest.main()
