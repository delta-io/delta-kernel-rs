#!/usr/bin/env python3
"""Run Kernel's structural Rust lints and ratchet existing findings."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


VERSION = "0.37.0"
DEFAULT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATHS = ["kernel/src", "kernel/tests", "ffi/src"]
SUPPRESSION = re.compile(r"^\s*// kernel-lint: allow ([a-zA-Z0-9_-]+) -- (\S.*)$")
GENERATED_SOURCE = re.compile(r"(?im)^\s*//[!/]?\s*(?:@generated\b|generated\s+by\b)")


class LintError(Exception):
    pass


def read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise LintError("Cannot read {}: {}".format(path, exc))


def is_generated(source):
    return GENERATED_SOURCE.search(source[:1024]) is not None


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def metadata(root):
    result = {}
    for path in sorted((root / "tools/kernel-lint/rules").rglob("metadata.json")):
        entries = read_json(path)
        if isinstance(entries, dict):
            entries = entries["rules"]
        for entry in entries:
            rule_id = entry["id"]
            if rule_id in result:
                raise LintError("Duplicate rule metadata: " + rule_id)
            result[rule_id] = entry
    if not result:
        raise LintError("No rule metadata found under tools/kernel-lint/rules")
    return result


def executable():
    binary = os.environ.get("AST_GREP", "ast-grep")
    resolved = shutil.which(binary)
    if not resolved:
        raise LintError("Install ast-grep {}: npm install --global @ast-grep/cli@{}".format(VERSION, VERSION))
    try:
        process = subprocess.run([resolved, "--version"], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, universal_newlines=True)
    except OSError as exc:
        raise LintError(str(exc))
    if process.returncode or process.stdout.strip() != "ast-grep " + VERSION:
        raise LintError("Expected ast-grep {}; found {}".format(VERSION, process.stdout.strip()))
    return resolved


def contained(root, name):
    path = (root / name).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        raise LintError("Refusing path outside repository: " + str(path))
    return path


def scan(root, binary, paths, rule_filter):
    registry = metadata(root)
    paths = paths or [p for p in DEFAULT_PATHS if (root / p).exists()]
    if not paths:
        raise LintError("Pass at least one source path to scan")
    for name in paths:
        if not contained(root, name).exists():
            raise LintError("Source path does not exist: " + name)
    line_comments = set()
    # `run` returns comments before scan's native directives can suppress them.
    # Select the whole comment; signature matching includes doc-comment forms.
    for kind, pattern in [("line_comment", "//"), ("block_comment", "/* */")]:
        guard = subprocess.run([binary, "run", "--lang", "rust", "--pattern", pattern,
                                "--selector", kind, "--strictness", "signature",
                                "--json=compact"] + paths, cwd=str(root),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               universal_newlines=True)
        if guard.returncode not in (0, 1):
            raise LintError(guard.stderr.strip() or "Suppression-policy scan failed")
        try:
            comments = json.loads(guard.stdout)
        except ValueError:
            raise LintError("Invalid suppression-policy scan output")
        for comment in comments:
            if kind == "line_comment":
                line_comments.add((contained(root, comment["file"]), comment["range"]["start"]["line"]))
            if "ast-grep-ignore" in comment["text"]:
                raise LintError("Native ast-grep suppression at {}:{}; use a reasoned kernel-lint allowance".format(
                    comment["file"], comment["range"]["start"]["line"] + 1))
    command = [binary, "scan", "--config", str(root / "sgconfig.yml"), "--json=compact"]
    if rule_filter:
        command += ["--filter", rule_filter]
    process = subprocess.run(command + paths, cwd=str(root), stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, universal_newlines=True)
    if process.returncode not in (0, 1):
        raise LintError(process.stderr.strip() or "ast-grep scan failed")
    try:
        matches = json.loads(process.stdout)
    except ValueError:
        raise LintError(process.stderr.strip() or "Invalid ast-grep JSON output")
    findings, suppressed, sources = [], [], {}
    for match in matches:
        rule_id = match["ruleId"]
        if rule_id not in registry:
            raise LintError("Rule has no metadata: " + rule_id)
        path = contained(root, match["file"])
        match["file"] = path.relative_to(root).as_posix()
        match["fingerprint"] = digest("\0".join([rule_id, match["file"], match["text"]]))
        match["safeFix"] = registry[rule_id].get("safeFix", False)
        match["limitation"] = registry[rule_id].get("limitation", registry[rule_id].get("notes", ""))
        if path not in sources:
            sources[path] = path.read_text().splitlines()
        line = match["range"]["start"]["line"]
        previous = sources[path][line - 1] if line > 0 else ""
        allowance = SUPPRESSION.fullmatch(previous)
        if allowance and (path, line - 1) in line_comments and allowance.group(1) == rule_id:
            match["suppression"] = allowance.group(2)
            suppressed.append(match)
        else:
            findings.append(match)
    findings.sort(key=lambda m: (m["file"], m["range"]["start"]["line"],
                                 m["range"]["start"]["column"], m["ruleId"]))
    return findings, suppressed


def baseline_path(root, override):
    return contained(root, override or "tools/kernel-lint/baseline.json")


def baseline_counts(path):
    if not path.exists():
        return Counter()
    data = read_json(path)
    if data.get("schemaVersion") != 1 or data.get("astGrepVersion") != VERSION:
        raise LintError("Unsupported baseline schema/tool version: " + str(path))
    return Counter({row["fingerprint"]: row["count"] for row in data["findings"]})


def classify(findings, accepted):
    remaining = accepted.copy()
    for finding in findings:
        key = finding["fingerprint"]
        finding["baseline"] = remaining[key] > 0
        remaining[key] -= 1
    return sum(not finding["baseline"] for finding in findings)


def write_baseline(path, findings):
    counts = Counter(m["fingerprint"] for m in findings)
    representatives = {m["fingerprint"]: m for m in findings}
    rows = [{"fingerprint": key, "ruleId": representatives[key]["ruleId"],
             "file": representatives[key]["file"], "count": counts[key]} for key in sorted(counts)]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schemaVersion": 1, "astGrepVersion": VERSION,
                                "findings": rows}, indent=2) + "\n")


def apply_fixes(root, findings, reviewed):
    if reviewed:
        document = read_json(Path(reviewed))
        entries = document.get("edits", [])
        selected = []
        for entry in entries:
            candidates = [m for m in findings if m["fingerprint"] == entry["fingerprint"]
                          and ("file" not in entry or entry["file"] == m["file"])
                          and ("line" not in entry or entry["line"] == m["range"]["start"]["line"] + 1)]
            if len(candidates) != 1:
                raise LintError("Stale or ambiguous reviewed edit: " + entry["fingerprint"])
            match = candidates[0]
            if "replacement" not in match:
                raise LintError("Rule has no replacement: " + match["ruleId"])
            if not isinstance(entry.get("replacement"), str) or entry["replacement"] != match["replacement"]:
                raise LintError("Reviewed replacement differs from current suggestion")
            selected.append(match)
    else:
        selected = [m for m in findings if m["safeFix"] and "replacement" in m]
    plans = {}
    for match in selected:
        path = contained(root, match["file"])
        source, edits = plans.setdefault(path, (path.read_bytes(), []))
        header = source[:1024].decode("utf-8", errors="replace")
        if is_generated(header):
            raise LintError("Refusing to rewrite generated source: " + match["file"])
        offsets = match.get("replacementOffsets", match["range"]["byteOffset"])
        start, end = offsets["start"], offsets["end"]
        if source[start:end].decode("utf-8") != match["text"]:
            raise LintError("Source changed during scan: " + match["file"])
        edits.append((start, end, match["replacement"].encode("utf-8")))
    writes = {}
    for path, (source, edits) in plans.items():
        boundary = len(source)
        for start, end, replacement in sorted(edits, reverse=True):
            if end > boundary:
                raise LintError("Overlapping fixes; review one at a time: " + str(path))
            source = source[:start] + replacement + source[end:]
            boundary = start
        writes[path] = source
    # Every proposed edit is checked before any source file is written.
    for path, source in writes.items():
        path.write_bytes(source)
    return {"applied": len(selected), "files": len(writes)}


def emit(document, output_format, output):
    if output_format == "json":
        text = json.dumps(document, indent=2) + "\n"
    else:
        lines = []
        for match in document["findings"]:
            line = match["range"]["start"]["line"] + 1
            column = match["range"]["start"]["column"] + 1
            message = match["message"]
            if output_format == "github":
                escape = lambda s: str(s).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A").replace(",", "%2C").replace(":", "%3A")
                lines.append("::warning file={},line={},col={},title={}::{}".format(
                    escape(match["file"]), line, column, escape(match["ruleId"]), escape(message)))
            else:
                lines.append("{}:{}:{}: {}: {}{}".format(match["file"], line, column,
                    match["ruleId"], message, " [baseline]" if match.get("baseline") else ""))
        lines.append("{} findings; {} new; {} suppressed".format(
            document["summary"]["total"], document["summary"]["new"], document["summary"]["suppressed"]))
        if "fixes" in document:
            lines.append("{} fixes applied in {} files".format(document["fixes"]["applied"], document["fixes"]["files"]))
        text = "\n".join(lines) + "\n"
    if output:
        Path(output).write_text(text)
    else:
        sys.stdout.write(text)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["scan", "check", "baseline", "fix"]:
        command = commands.add_parser(name)
        command.add_argument("paths", nargs="*")
        command.add_argument("--format", choices=["text", "json", "github"], default="text")
        command.add_argument("--output")
        command.add_argument("--rule", help="Regular expression selecting rule IDs")
        command.add_argument("--baseline")
        if name == "fix":
            command.add_argument("--reviewed", help="JSON manifest of exact reviewed edits")
    commands.add_parser("test")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        binary = executable()
        if args.command == "test":
            return subprocess.run([binary, "test", "--config", str(root / "sgconfig.yml")], cwd=str(root)).returncode
        findings, suppressed = scan(root, binary, args.paths, args.rule)
        path = baseline_path(root, args.baseline)
        new_count = classify(findings, baseline_counts(path))
        document = {"astGrepVersion": VERSION, "findings": findings,
                    "summary": {"total": len(findings), "new": new_count,
                                "suppressed": len(suppressed),
                                "byRule": dict(sorted(Counter(m["ruleId"] for m in findings).items()))}}
        if args.command == "baseline":
            if args.rule or args.paths:
                raise LintError("Refresh the complete baseline without path/rule filters")
            write_baseline(path, findings)
            document["summary"]["new"] = 0
        elif args.command == "fix":
            document["fixes"] = apply_fixes(root, findings, args.reviewed)
        elif args.command == "check":
            # CI annotations should highlight new findings, not accepted historical debt.
            document["findings"] = [m for m in findings if not m["baseline"]]
        emit(document, args.format, args.output)
        return int(args.command == "check" and new_count > 0)
    except (LintError, OSError, KeyError, TypeError, ValueError) as exc:
        print("kernel-lint: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
