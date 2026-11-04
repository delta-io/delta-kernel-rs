"""Exercise the benchmark CI script with mocked cargo and critcmp tools.

Cover output isolation, archival decisions, and failures without running benchmarks or installing
Rust tooling.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


MOCK_TOOL = """\
import json
import os
from pathlib import Path
import sys

tool = Path(sys.argv[0]).name
arguments = sys.argv[1:]
with open(os.environ["TOOL_CALLS"], "a") as calls:
    calls.write(json.dumps({
        "tool": tool,
        "arguments": arguments,
        "criterion_home": os.environ.get("CRITERION_HOME"),
        "tags": os.environ.get("BENCH_TAGS"),
    }) + "\\n")

if tool == "cargo":
    phase = arguments[arguments.index("--save-baseline") + 1]
    failed = os.environ.get("FAIL_PHASE") == phase
    if failed and os.environ.get("FAIL_BEFORE_OUTPUT") == "true":
        sys.exit(17)
    output = Path(os.environ["CRITERION_HOME"]) / "mock_workload"
    duration = float(os.environ["CHANGES_TIME"]) if phase == "changes" else 10.0
    for baseline in (phase, "new"):
        directory = output / baseline
        directory.mkdir(parents=True)
        for filename, data in {
            "sample.json": {"sampling_mode": "Linear", "iters": [1], "times": [duration]},
            "estimates.json": {"mean": {"point_estimate": duration}},
            "benchmark.json": {"full_id": "mock_workload"},
            "tukey.json": [0, 1, 2, 3],
        }.items():
            (directory / filename).write_text(json.dumps(data))
    report = output / "report"
    report.mkdir()
    (report / "index.html").write_text("<html>mock report</html>")
    (report / "pdf.svg").write_text("<svg></svg>")
    (report / ".asset").write_text("mock report asset")
    if failed:
        sys.exit(17)
elif tool == "critcmp":
    if arguments[0] == "--target-dir":
        phase = arguments[3]
        sample = Path(arguments[1]) / "criterion" / "mock_workload" / phase / "sample.json"
        duration = json.loads(sample.read_text())["times"][0]
        print(json.dumps({"name": phase, "duration": duration}))
    else:
        if os.environ.get("FAIL_COMPARISON") == "true":
            sys.exit(19)
        base, changes = [json.loads(Path(argument).read_text()) for argument in arguments]
        assert base["name"] == "base"
        assert changes["name"] == "changes"
        print("group  base  changes")
        print("-----  ----  -------")
        print(
            f"mock_workload  1.0  {base['duration']:.2f}±0.10ms"
            f"  1.0  {changes['duration']:.2f}±0.10ms"
        )
elif tool == "lscpu":
    print("Model name: Mock CPU")
elif tool == "free":
    print("Mem: 1000 100 100 0 0 800")
else:
    raise RuntimeError(f"Unexpected tool: {tool}")
"""


class BenchmarkScriptTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        self.repository = self.root / "repository"
        self.repository.mkdir()
        self.runner_temp = self.root / "runner temp"
        self.runner_temp.mkdir()
        self.output_file = self.root / "github-output"
        self.calls_file = self.root / "tool-calls"
        self.comment_file = self.root / "bench-comment.md"
        self.regression_file = self.root / "bench-regression.txt"
        source_directory = Path(__file__).resolve().parent
        formatter = self.repository / "benchmarks" / "ci" / "parse_critcmp.py"
        formatter.parent.mkdir(parents=True)
        formatter.write_text("raise RuntimeError('base formatter must not run')\n")
        self.git("init", "--quiet", "--initial-branch=main")
        self.git("config", "user.name", "Benchmark test")
        self.git("config", "user.email", "benchmark-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.hooksPath", "/dev/null")
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "base")
        self.base_sha = self.git("rev-parse", "HEAD").strip()
        self.git("checkout", "--quiet", "-b", "changes")
        shutil.copyfile(source_directory / "parse_critcmp.py", formatter)
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "changes")
        self.changes_sha = self.git("rev-parse", "HEAD").strip()
        self.git("remote", "add", "origin", str(self.repository))
        self.script = self.root / "run-benchmarks.sh"
        script = (source_directory / "run-benchmarks.sh").read_text()
        self.assertIn("} > /tmp/bench-comment.md", script)
        self.assertIn("export BENCH_REGRESSION_FILE=/tmp/bench-regression.txt", script)
        self.script.write_text(
            script.replace("/tmp/bench-comment.md", str(self.comment_file))
            .replace("/tmp/bench-regression.txt", str(self.regression_file))
        )
        self.tools = self.root / "tools"
        self.tools.mkdir()
        for tool in ("cargo", "critcmp", "lscpu", "free"):
            executable = self.tools / tool
            executable.write_text(f"#!{sys.executable}\n" + MOCK_TOOL)
            executable.chmod(0o755)

    def git(self, *arguments):
        return subprocess.run(
            ["git", *arguments],
            cwd=self.repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def run_benchmarks(self, **overrides):
        self.output_file.write_text("")
        self.calls_file.write_text("")
        self.comment_file.unlink(missing_ok=True)
        self.regression_file.unlink(missing_ok=True)
        environment = {
            **os.environ,
            "PATH": str(self.tools) + os.pathsep + os.environ["PATH"],
            "BASE_REF": "main",
            "HEAD_SHA": self.changes_sha,
            "RUNNER_TEMP": str(self.runner_temp),
            "GITHUB_OUTPUT": str(self.output_file),
            "TOOL_CALLS": str(self.calls_file),
            "COMMENT": "",
            "CHANGES_TIME": "10",
            "FAIL_PHASE": "",
            "FAIL_BEFORE_OUTPUT": "false",
            "FAIL_COMPARISON": "false",
            "BENCH_IGNORE_FAILURE": "false",
            "CRITERION_HOME": str(self.root / "old-criterion"),
            **overrides,
        }
        environment.pop("BENCH_TAGS", None)
        return subprocess.run(
            ["bash", str(self.script)],
            cwd=self.repository,
            env=environment,
            capture_output=True,
            text=True,
        )

    def outputs(self):
        return dict(line.split("=", 1) for line in self.output_file.read_text().splitlines())

    def results_directory(self):
        return Path(self.outputs()["results_dir"])

    def assert_phase_output(self, results, phase):
        output = results / phase / "criterion" / "mock_workload"
        self.assertEqual(
            {entry.name for entry in output.iterdir()},
            {phase, "new", "report"},
        )
        for baseline in (phase, "new"):
            self.assertEqual(
                {entry.name for entry in (output / baseline).iterdir()},
                {"sample.json", "estimates.json", "benchmark.json", "tukey.json"},
            )
        self.assertEqual(
            {entry.name for entry in (output / "report").iterdir()},
            {"index.html", "pdf.svg", ".asset"},
        )

    def test_isolated_complete_output_and_measured_commit_logging(self):
        stale = self.repository / "target" / "criterion" / "stale.json"
        stale.parent.mkdir(parents=True)
        stale.write_text("stale cache output")
        previous_results = None
        for run_number in range(2):
            with self.subTest(run_number=run_number):
                completed = self.run_benchmarks(
                    COMMENT="/bench --tags base,my-tag --filter snapshotLatest|readMetadata"
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                results = self.results_directory()
                self.assertEqual(results.parent, self.runner_temp)
                self.assertNotEqual(results, previous_results)
                self.assertEqual(
                    {entry.name for entry in results.iterdir()}, {"base", "changes"}
                )
                for phase in ("base", "changes"):
                    self.assert_phase_output(results, phase)
                    measured_sha = self.base_sha if phase == "base" else self.changes_sha
                    self.assertIn(
                        f"Benchmark phase: {phase}, commit: {measured_sha}", completed.stdout
                    )
                calls = [json.loads(line) for line in self.calls_file.read_text().splitlines()]
                benchmarks = [call for call in calls if call["tool"] == "cargo"]
                self.assertEqual(len(benchmarks), 2)
                for phase, call in zip(("changes", "base"), benchmarks):
                    self.assertEqual(
                        call["arguments"],
                        [
                            "bench", "--locked", "--bench", "workload_bench", "--",
                            "--save-baseline", phase, "snapshotLatest|readMetadata",
                        ],
                    )
                    self.assertEqual(call["tags"], "base,my-tag")
                    self.assertEqual(
                        call["criterion_home"], str(results / phase / "criterion")
                    )
                self.assertEqual(self.git("rev-parse", "HEAD").strip(), self.changes_sha)
                self.assertEqual(stale.read_text(), "stale cache output")
                self.assertFalse((self.root / "old-criterion").exists())
                self.assertIn("mock_workload", self.comment_file.read_text())
                self.assertEqual(self.outputs()["archive_results"], "false")
                previous_results = results

    def test_partial_output_survives_benchmark_failure(self):
        for phase in ("changes", "base"):
            for before_output in ("true", "false"):
                with self.subTest(phase=phase, before_output=before_output):
                    self.git("checkout", "--quiet", self.changes_sha)
                    completed = self.run_benchmarks(
                        FAIL_PHASE=phase, FAIL_BEFORE_OUTPUT=before_output
                    )
                    self.assertEqual(completed.returncode, 17, completed.stderr)
                    self.assertEqual(self.outputs()["archive_results"], "true")
                    results = self.results_directory()
                    if phase == "base":
                        self.assert_phase_output(results, "changes")
                    if before_output == "false":
                        self.assert_phase_output(results, phase)
                    else:
                        self.assertFalse((results / phase).exists())
                    self.assertFalse(self.comment_file.exists())

    def test_regression_threshold_and_override_preserve_evidence(self):
        for comment in ("", "/bench"):
            for duration, ignored, regressed in (
                ("8", "false", "false"),
                ("10", "false", "false"),
                ("10", "true", "false"),
                ("10.3", "false", "false"),
                ("10.4", "false", "false"),
                ("11.44", "false", "false"),
                ("11.46", "false", "true"),
                ("11.5", "false", "true"),
                ("12", "true", "true"),
            ):
                with self.subTest(comment=comment, duration=duration, ignored=ignored):
                    completed = self.run_benchmarks(
                        COMMENT=comment, CHANGES_TIME=duration, BENCH_IGNORE_FAILURE=ignored
                    )
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    self.assertEqual(self.regression_file.read_text(), regressed)
                    self.assertEqual(self.outputs()["archive_results"], regressed)
                    verdict = next(
                        line for line in self.comment_file.read_text().splitlines()
                        if line.startswith("## Benchmark results:")
                    )
                    if regressed == "true" and ignored == "true":
                        self.assertIn("slowdown ignored", verdict)
                    elif regressed == "true":
                        self.assertIn("Fail", verdict)
                    else:
                        self.assertIn("Pass", verdict)
                    for phase in ("base", "changes"):
                        self.assert_phase_output(self.results_directory(), phase)

    def test_archive_flag_preserves_benchmark_selection(self):
        for comment, tags, filter_pattern in (
            ("/bench --archive", "base", ""),
            ("/bench --archive --tags base,my-tag", "base,my-tag", ""),
            ("/bench --filter snapshotLatest --archive", None, "snapshotLatest"),
            (
                "/bench --archive --tags base,my-tag --filter snapshotLatest",
                "base,my-tag", "snapshotLatest",
            ),
            (
                "/bench --filter snapshotLatest --archive --tags base,my-tag",
                "base,my-tag", "snapshotLatest",
            ),
        ):
            with self.subTest(comment=comment):
                completed = self.run_benchmarks(COMMENT=comment)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertEqual(self.regression_file.read_text(), "false")
                self.assertEqual(self.outputs()["archive_results"], "true")
                calls = [json.loads(line) for line in self.calls_file.read_text().splitlines()]
                benchmarks = [call for call in calls if call["tool"] == "cargo"]
                self.assertEqual(len(benchmarks), 2)
                for call in benchmarks:
                    self.assertEqual(call["tags"], tags)
                    self.assertEqual(call["arguments"][-1], filter_pattern)
                    self.assertNotIn("--archive", call["arguments"])

    def test_benchmark_script_does_not_require_github_outputs(self):
        completed = self.run_benchmarks(GITHUB_OUTPUT="")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.regression_file.read_text(), "false")
        self.assertTrue(self.comment_file.exists())
        self.assertEqual(self.output_file.read_text(), "")

    def test_comparison_failure_preserves_both_measurements(self):
        completed = self.run_benchmarks(FAIL_COMPARISON="true")
        self.assertEqual(completed.returncode, 19, completed.stderr)
        self.assertEqual(self.outputs()["archive_results"], "true")
        for phase in ("base", "changes"):
            self.assert_phase_output(self.results_directory(), phase)
        self.assertFalse(self.comment_file.exists())


if __name__ == "__main__":
    unittest.main()
