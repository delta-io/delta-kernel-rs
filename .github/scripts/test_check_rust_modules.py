"""Exercise Rust module discovery against temporary Git repositories and real rustfmt."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from check_rust_modules import find_orphans


class RustModuleCheckTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repository = Path(self.temporary.name)
        subprocess.run(["git", "init", "--quiet"], cwd=self.repository, check=True)
        self.write("Cargo.toml", '[workspace]\nmembers = ["engine", "kernel"]\nresolver = "2"\n')
        for package in ("engine", "kernel"):
            self.write(
                f"{package}/Cargo.toml",
                f'[package]\nname = "{package}"\nversion = "0.0.0"\nedition = "2021"\n',
            )
        self.write("engine/src/lib.rs", "#[cfg(test)]\nmod stats_verifier_tests;\n")
        self.write("engine/src/stats_verifier_tests.rs", "#[test]\nfn stats() {}\n")
        self.write("kernel/src/lib.rs", "")
        self.write("kernel/tests/integration/main.rs", "mod log;\n")
        self.write("kernel/tests/integration/log/mod.rs", "mod v1_checkpoints;\n")
        self.write("kernel/tests/integration/log/v1_checkpoints.rs", "#[test]\nfn v1() {}\n")

    def write(self, name, contents):
        path = self.repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    def check(self):
        subprocess.run(["git", "add", "--", "."], cwd=self.repository, check=True)
        return [path.as_posix() for path in find_orphans(self.repository)]

    def test_reports_disconnected_unit_and_integration_modules(self):
        self.assertEqual(self.check(), [])
        self.write("engine/src/lib.rs", "")
        self.write("kernel/tests/integration/log/mod.rs", "")
        self.assertEqual(self.check(), [
            "engine/src/stats_verifier_tests.rs",
            "kernel/tests/integration/log/v1_checkpoints.rs",
        ])

    def test_checks_nested_workspaces_and_custom_cargo_targets(self):
        self.write("executor/Cargo.toml", (
            '[workspace]\n[package]\nname = "executor"\nversion = "0.0.0"\n'
            'edition = "2021"\n[lib]\npath = "custom_root.rs"\n'
            '[[test]]\nname = "integration"\npath = "custom_test.rs"\n'
        ))
        self.write("executor/custom_root.rs", "mod expression;\n")
        self.write("executor/expression.rs", "pub fn evaluate() {}\n")
        self.write("executor/custom_test.rs", "#[test]\nfn custom_target() {}\n")
        self.assertEqual(self.check(), [])
        self.write("executor/custom_root.rs", "")
        self.assertEqual(self.check(), ["executor/expression.rs"])

    def test_follows_cfg_path_attributes_and_inline_modules_without_modifying_sources(self):
        self.write("kernel/src/lib.rs", (
            '#[cfg(any())]\nmod disabled;\n'
            '#[path = "relocated.rs"]\nmod renamed;\n'
            'mod outer { mod nested; }\n'
        ))
        self.write("kernel/src/disabled.rs", "pub fn disabled(){}\n")
        self.write("kernel/src/relocated.rs", "pub fn relocated(){}\n")
        self.write("kernel/src/outer/nested.rs", "pub fn nested(){}\n")
        before = {p: p.read_bytes() for p in self.repository.rglob("*.rs")}
        self.assertEqual(self.check(), [])
        self.assertEqual(before, {p: p.read_bytes() for p in self.repository.rglob("*.rs")})

    def test_harness_exception_only_covers_its_direct_fixture_files(self):
        self.write("ffi/tests/invalid-handle-code/expected_failure.rs", "invalid rust!\n")
        self.write("ffi/tests/invalid-handle-code/nested/orphan.rs", "")
        self.write("kernel/src/unexpected.rs", "")
        self.assertEqual(self.check(), [
            "ffi/tests/invalid-handle-code/nested/orphan.rs", "kernel/src/unexpected.rs",
        ])

    def test_propagates_rustfmt_failure(self):
        self.write("engine/src/lib.rs", "mod missing;\n")
        with self.assertRaises(subprocess.CalledProcessError):
            self.check()

    def test_rejects_missing_rustfmt_file_headers(self):
        real_run = subprocess.run

        def run(command, **kwargs):
            if command[0] == "cargo":
                return subprocess.CompletedProcess(command, 0, stdout="")
            return real_run(command, **kwargs)

        with patch("check_rust_modules.subprocess.run", side_effect=run):
            with self.assertRaisesRegex(RuntimeError, "no source file headers"):
                self.check()


if __name__ == "__main__":
    unittest.main()
