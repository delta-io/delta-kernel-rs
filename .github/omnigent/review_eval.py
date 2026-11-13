"""Replay frozen review cases locally; never publish reviews or execute case source."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import signal
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any

import yaml

from inline_review import build_review_payload, extract_inline_findings, inline_prompt_instructions
from review_publish import extract_marked_review

ROOT = Path(__file__).resolve().parent
MARKER = "e" * 32


def digest(value: Any) -> str:
    """Return a stable content digest for JSON-compatible evaluation inputs."""
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def source_tree(root: Path, files: dict[str, str]) -> None:
    """Materialize only ordinary relative source paths, without labels or Git history."""
    for name, content in files.items():
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or ".git" in path.parts:
            raise ValueError(f"invalid source path: {name}")
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)


def case_context(case: dict[str, Any]) -> tuple[str, str]:
    """Build reviewer-visible context exclusively from frozen inputs, never labels."""
    before, after = case["before"], case["after"]
    parts = []
    for name in sorted(before.keys() | after.keys()):
        if before.get(name) == after.get(name):
            continue
        parts.append(f"diff --git a/{name} b/{name}\n")
        for line in difflib.unified_diff(
            before.get(name, "").splitlines(keepends=True),
            after.get(name, "").splitlines(keepends=True),
            fromfile=f"a/{name}" if name in before else "/dev/null",
            tofile=f"b/{name}" if name in after else "/dev/null",
        ):
            parts.append(line if line.endswith("\n") else line + "\n\\ No newline at end of file\n")
    diff = "".join(parts)
    context = (
        f"PR title: {case['title']}\nPR description: {case['description']}\n"
        f"Frozen source base digest: {digest(before)}\nHead digest: {digest(after)}\n"
        "This is an isolated synthetic review case. Source tools expose only the supplied "
        "current files and reference text. Do not infer missing repository context.\n"
        f"## PR Diff\n```diff\n{diff}\n```\n"
    )
    return context, diff


def prepare_bundle(
    destination: Path, context_path: Path, trace_path: Path, models: dict[str, str]
) -> str:
    """Copy checked-in prompts and install the same bounded source tools used in CI."""
    shutil.copytree(
        ROOT / "reviewer", destination, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
    )
    prompt_files = {
        str(p.relative_to(ROOT)): p.read_text()
        for p in (ROOT / "reviewer").rglob("*")
        if p.is_file() and p.suffix in {".yaml", ".md"}
    }
    for config_path in destination.rglob("config.yaml"):
        config = yaml.safe_load(config_path.read_text())
        alias = config["executor"]["model"]
        config["executor"]["model"] = models[alias]
        policies = config.setdefault("guardrails", {}).setdefault("policies", {})
        if "inject_review_context" in policies:
            policies["inject_review_context"]["function"]["arguments"]["context_path"] = str(
                context_path
            )
        policies["evaluation_trace"] = {
            "type": "function",
            "on": ["tool_call", "tool_result"],
            "function": {
                "path": "review_eval_trace.record_events",
                "arguments": {"path": str(trace_path)},
            },
        }
        config_path.write_text(yaml.safe_dump(config, sort_keys=False))
        bundle = config_path.parent
        (bundle / "lib").mkdir(exist_ok=True)
        shutil.copyfile(ROOT / "source_context.py", bundle / "lib/source_context.py")
        tool_dir = bundle / "tools/python"
        tool_dir.mkdir(parents=True, exist_ok=True)
        for source in (ROOT / "source_tools").glob("*.py"):
            shutil.copyfile(source, tool_dir / source.name)
    return digest(prompt_files)


def validate_trace(path: Path) -> None:
    """Require actual dispatch and result evidence, including the gate when dispatched.

    This checks trace availability, not semantic completeness. Adjudication must account for every
    candidate, verdict, and published finding before scoring.
    """
    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    calls, results = set(), set()
    for event in events:
        phase = event.get("type", "").lower().removeprefix("phase_")
        data = event.get("data")
        request = event.get("request_data") or {}
        if (
            phase == "tool_call"
            and isinstance(data, dict)
            and data.get("name") == "sys_session_send"
        ):
            agent = data.get("arguments", {}).get("agent")
            if isinstance(agent, str):
                calls.add(agent)
        if phase == "tool_result" and request.get("name") == "sys_session_send":
            agent = request.get("args", {}).get("agent")
            if isinstance(agent, str) and data:
                results.add(agent)
    maintainers = {"maintainer-claude-reviewer", "maintainer-codex-reviewer"}
    primary = {
        "delta-protocol-reviewer",
        "architecture-reviewer",
        "test-coverage-reviewer",
        "docs-reviewer",
    }
    completed = calls & results
    if not (completed & maintainers and completed & primary):
        raise ValueError("dispatch trace lacks reviewer quorum results; cannot evaluate pipeline")
    if "disprove-reviewer" in calls and "disprove-reviewer" not in results:
        raise ValueError("dispatch trace lacks the disprove result")


def run_case(
    case: dict[str, Any],
    *,
    mode: str,
    output: Path,
    command: list[str],
    models: dict[str, str],
    runtime: str,
    timeout: int,
) -> dict[str, Any]:
    """Run a bounded replay, retain raw evidence, and validate pipeline publication locally.

    Existing runtime authentication is used by the CLI. No credentials are loaded by this runner.
    Runtime/model identifiers are explicit operator attestations, not inferred from aliases.
    """
    output.mkdir(parents=True, exist_ok=False)
    output = output.resolve()
    context, diff = case_context(case)
    (output / "input.json").write_text(json.dumps(case, indent=2) + "\n")
    trace = output / "events.jsonl"
    with tempfile.TemporaryDirectory(prefix="kernel-review-eval-") as directory:
        workspace = Path(directory)
        source_tree(workspace / "source", case["after"])
        source_tree(workspace / "delta", case.get("reference", {}))
        (workspace / "source").mkdir(exist_ok=True)
        (workspace / "delta").mkdir(exist_ok=True)
        context_path = workspace / "context.md"
        context_path.write_text(context)
        bundle = workspace / "reviewer"
        prompt_digest = prepare_bundle(bundle, context_path, trace, models)
        if mode == "gate":
            target = bundle / "agents/disprove-reviewer"
            prompt = context + "\n## Candidate findings\n" + case["candidates"]
        else:
            target = bundle
            prompt = (
                context
                + "\nRun the checked-in review process. Never publish to GitHub.\n"
                + inline_prompt_instructions(MARKER)
                + f"\nEnclose the final review in <!-- AI_REVIEW_START_{MARKER} --> and "
                f"<!-- AI_REVIEW_END_{MARKER} -->.\n"
            )
        (output / "prompt.txt").write_text(prompt)
        env = dict(os.environ)
        env.update(
            {
                "PR_SOURCE_ROOT": str(workspace / "source"),
                "DELTA_SOURCE_ROOT": str(workspace / "delta"),
                "PYTHONPATH": str(ROOT),
                "HARNESS_CODEX_DISABLE_NATIVE_TOOLS": "1",
                "HARNESS_CODEX_ENABLE_WEB_SEARCH": "0",
                "OMNIGENT_CLAUDE_SDK_NO_SANDBOX": "1",
                "OMNIGENT_SKIP_WEB_UI": "true",
            }
        )
        invocation = [*command, "run", str(target), "-p", prompt, "--no-session"]
        record = {
            "case_id": case["id"],
            "mode": mode,
            "input_digest": digest(case),
            "prompt_digest": prompt_digest,
            "tool_digest": digest(
                {
                    p.name: p.read_text()
                    for p in [
                        ROOT / "source_context.py",
                        ROOT / "review_context_policy.py",
                        ROOT / "review_eval_trace.py",
                        ROOT / "inline_review.py",
                        ROOT / "review_publish.py",
                        *sorted((ROOT / "source_tools").glob("*.py")),
                    ]
                }
            ),
            "reference_digest": digest(case.get("reference", {})),
            "models": models,
            "runtime": runtime,
            "command": command,
            "status": "failed",
            "cost_usd": None,
        }
        started = time.monotonic()
        try:
            with (
                (output / "stdout.txt").open("w") as stdout,
                (output / "stderr.txt").open("w") as stderr,
            ):
                with subprocess.Popen(
                    invocation,
                    cwd=workspace,
                    env=env,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                ) as process:
                    try:
                        process.wait(timeout=timeout)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                        raise
                returncode = process.returncode
            record["returncode"] = returncode
            if returncode:
                raise ValueError(f"review runtime exited {returncode}")
            raw = (output / "stdout.txt").read_text()
            if mode == "pipeline":
                body, findings = extract_inline_findings(extract_marked_review(raw, MARKER), MARKER)
                payload, unmapped, duplicates = build_review_payload(
                    review=body,
                    findings=findings,
                    diff=diff,
                    head_sha=digest(case["after"])[:40],
                    run_url="https://github.com/delta-io/delta-kernel-rs/actions/runs/0",
                )
                (output / "publication.json").write_text(json.dumps(payload, indent=2) + "\n")
                # Missing hook evidence invalidates causal analysis, even if the model printed a review.
                if not trace.exists() or not trace.read_text().strip():
                    raise ValueError(
                        "runtime supplied no dispatch trace; pipeline replay is incomplete"
                    )
                validate_trace(trace)
            elif not re.search(
                r"\*\*Verdict\*\*:\s*(CONFIRMED|DISPROVED|NITPICK|CONTESTED)\b", raw
            ) or not re.search(r"\*\*Evidence\*\*:\s*\S", raw):
                raise ValueError("gate returned no verdict evidence")
            record["status"] = "complete"
        except (OSError, ValueError, subprocess.TimeoutExpired) as error:
            record["error"] = str(error)
        finally:
            record["artifacts"] = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in output.iterdir()
                if path.is_file() and path.name != "run.json"
            }
            record["seconds"] = round(time.monotonic() - started, 3)
            (output / "run.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def verify_artifacts(directory: Path, secret: str) -> None:
    """Withhold an artifact bundle containing the runtime key or credential-like output."""
    patterns = (
        r"(?i)authorization\s*[:=]\s*(?:bearer\s+)?\S+",
        r"(?i)(?:(?:x-)?api[-_ ]?key|access[-_ ]?token)\s*[:=]\s*\S+",
        r"(?i)(?:token|key|sig|signature)=[^&\s]+",
    )
    if not directory.is_dir() or not any(directory.iterdir()):
        raise ValueError("No replay evidence is available for upload")
    for path in directory.rglob("*"):
        if path.is_file():
            text = path.read_text(errors="replace")
            if (secret and secret in text) or any(re.search(pattern, text) for pattern in patterns):
                raise ValueError("Replay evidence contains credential-like output; upload withheld")


def score(
    labels: dict[str, Any], adjudication: dict[str, Any], run: dict[str, Any]
) -> dict[str, Any]:
    """Score independently adjudicated claims; model agreement is not ground truth."""
    if run["status"] != "complete":
        raise ValueError("incomplete runs cannot be scored as successful reviews")
    if adjudication["input_digest"] != run["input_digest"]:
        raise ValueError("adjudication does not match this frozen input")
    if labels["input_digest"] != run["input_digest"]:
        raise ValueError("labels do not match this frozen input")
    known = {item["id"] for item in labels["issues"] if item["truth"] == "true"}
    if adjudication["run_digest"] != digest(run):
        raise ValueError("adjudication does not match this run")
    rows = adjudication["findings"]
    for row in rows:
        if row["truth"] not in {"true", "false", "uncertain"}:
            raise ValueError("invalid truth label")
        if row["kind"] not in {"runtime", "policy"} or not row.get("evidence"):
            raise ValueError("each finding needs a category and independent evidence")
        for field in ("discovered", "published", "harmful_fix"):
            if not isinstance(row[field], bool):
                raise ValueError(f"{field} must be a boolean")
        if row["gate_verdict"] not in {"CONFIRMED", "DISPROVED", "NITPICK", "CONTESTED", None}:
            raise ValueError("invalid gate verdict")
        if run["mode"] == "gate" and (row["discovered"] or row["published"]):
            raise ValueError("gate-only runs have no discovery or publication stage")
    discovered = {
        row.get("issue_id") for row in rows if row["discovered"] and row["truth"] == "true"
    } & known
    published = {
        row.get("issue_id") for row in rows if row["published"] and row["truth"] == "true"
    } & known
    judged_scope = [row for row in rows if row.get("expected_scope") in {"in", "out"}]
    return {
        "known_issues": len(known),
        "discovered_known": len(discovered),
        "published_known": len(published),
        "discovery_recall": len(discovered) / len(known)
        if known and run["mode"] == "pipeline"
        else None,
        "published_recall": len(published) / len(known)
        if known and run["mode"] == "pipeline"
        else None,
        "false_published": sum(row["published"] and row["truth"] == "false" for row in rows),
        "new_valid_published": sum(
            row["published"] and row["truth"] == "true" and row.get("issue_id") not in known
            for row in rows
        ),
        "uncertain_published": sum(
            row["published"] and row["truth"] == "uncertain" for row in rows
        ),
        "harmful_fixes": sum(row["published"] and row["harmful_fix"] for row in rows),
        "scope_correct": sum(
            row.get("reported_scope") == row["expected_scope"] for row in judged_scope
        ),
        "scope_judged": len(judged_scope),
        "true_confirmed": sum(
            row["truth"] == "true" and row["gate_verdict"] == "CONFIRMED" for row in rows
        ),
        "false_disproved": sum(
            row["truth"] == "false" and row["gate_verdict"] == "DISPROVED" for row in rows
        ),
        "true_lost_at_gate": sum(
            row["truth"] == "true" and row["gate_verdict"] in {"DISPROVED", "NITPICK"}
            for row in rows
        ),
        "by_kind": {
            kind: {
                "judged": sum(row["kind"] == kind for row in rows),
                "false_published": sum(
                    row["kind"] == kind and row["published"] and row["truth"] == "false"
                    for row in rows
                ),
            }
            for kind in ("runtime", "policy")
        },
        "seconds": run["seconds"],
        "cost_usd": run["cost_usd"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    run = commands.add_parser("run")
    run.add_argument("case", type=Path)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--mode", choices=["gate", "pipeline"], required=True)
    run.add_argument("--command", default="isaac omni")
    run.add_argument(
        "--models",
        type=Path,
        required=True,
        help="JSON mapping of model aliases to resolved model IDs",
    )
    run.add_argument("--runtime", required=True, help="Exact runtime and harness versions used")
    run.add_argument("--timeout", type=int, default=1800)
    scoring = commands.add_parser("score")
    for name in ("labels", "adjudication", "run"):
        scoring.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.action == "score":
        result = score(
            *(
                json.loads(getattr(args, name).read_text())
                for name in ("labels", "adjudication", "run")
            )
        )
    else:
        result = run_case(
            json.loads(args.case.read_text()),
            mode=args.mode,
            output=args.output,
            command=shlex.split(args.command),
            models=json.loads(args.models.read_text()),
            runtime=args.runtime,
            timeout=args.timeout,
        )
    print(json.dumps(result, indent=2))
    return int(result.get("status") == "failed")


if __name__ == "__main__":
    sys.exit(main())
