#!/usr/bin/env python3
"""Measure AST block depth inside Rust function bodies, excluding outer module/impl scopes."""

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import subprocess
import sys

from run import DEFAULT_ROOT, LintError, contained, executable, is_generated


RULES = "\n---\n".join(json.dumps({
    "id": name,
    "language": "Rust",
    "severity": "hint",
    "message": "Nesting audit",
    "rule": rule,
}) for name, rule in [
    ("function", {"all": [
        {"kind": "function_item"},
        {"has": {"field": "name", "pattern": "$NAME"}},
        {"has": {"field": "body", "kind": "block", "pattern": "$BODY"}},
    ]}),
    ("scope", {"any": [{"kind": "block"}, {"kind": "match_block"}]}),
])


def offsets(match):
    span = match["range"]["byteOffset"]
    return span["start"], span["end"]


def measure(root, paths):
    for path in paths:
        if not contained(root, path).exists():
            raise LintError("Source path does not exist: " + path)
    result = subprocess.run([executable(), "scan", "--inline-rules", RULES,
                             "--json=compact"] + paths, cwd=str(root),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True)
    if result.returncode not in (0, 1):
        raise LintError(result.stderr.strip() or "Nesting AST scan failed")
    matches = json.loads(result.stdout)
    files, seen = defaultdict(list), set()
    for match in matches:
        path = contained(root, match["file"])
        key = (path, match["ruleId"], offsets(match))
        if key not in seen:
            seen.add(key)
            files[str(path)].append(match)
    rows = []
    for file, nodes in sorted(files.items()):
        path = contained(root, file)
        if is_generated(path.read_text()):
            continue
        functions = sorted((node for node in nodes if node["ruleId"] == "function"),
                           key=lambda node: offsets(node)[0])
        scopes = [offsets(node) for node in nodes if node["ruleId"] == "scope"]
        ordinals = Counter()
        for function in functions:
            variables = function["metaVariables"]["single"]
            start, end = offsets(variables["BODY"])
            nested_functions = [offsets(other) for other in functions
                                if start < offsets(other)[0] < end]
            owned = sorted((a, b) for a, b in scopes
                           if start <= a < b <= end and (a, b) != (start, end)
                           and not any(c <= a and b <= d for c, d in nested_functions))
            stack, maximum = [], 0
            for a, b in owned:
                while stack and a >= stack[-1]:
                    stack.pop()
                stack.append(b)
                maximum = max(maximum, len(stack))
            name = variables["NAME"]["text"]
            ordinals[name] += 1
            relative = path.relative_to(root).as_posix()
            rows.append({"id": "{}::{}#{}".format(relative, name, ordinals[name]),
                         "file": relative, "line": function["range"]["start"]["line"] + 1,
                         "name": name, "bodyMaxDepth": maximum,
                         "nestedBlockCount": len(owned)})
    histogram = Counter(row["bodyMaxDepth"] for row in rows)
    return {"definition": "Rust block/match_block depth inside each function body; outer function/module/impl blocks and nested function bodies excluded; closures included; macro token trees not expanded",
            "summary": {"functions": len(rows), "maxBodyDepth": max(histogram, default=0),
                        "bodyDepthHistogram": dict(sorted(histogram.items()))},
            "functions": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", default=["kernel/src", "kernel/tests", "test-utils/src"])
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        document = measure(args.root.resolve(), args.paths)
    except (LintError, OSError, ValueError, KeyError) as exc:
        print("nesting-audit: " + str(exc), file=sys.stderr)
        return 2
    text = json.dumps(document, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
