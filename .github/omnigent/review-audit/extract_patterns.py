#!/usr/bin/env python3
"""Prepare review discussions and produce source-cited ideas for a later integration pass."""

import argparse
import hashlib
import json
import os
import re
import subprocess
from collections import Counter
from pathlib import Path

from review_feedback import feedback_index, rank_feedback, validate_reply_annotations


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


LANES = ("users", "github-actions", "other-bots")
MAJOR_CATEGORIES = {
    "tests": "Tests",
    "comments": "Comments",
    "design-structure": "Design/structure",
}


def source_lane(event):
    login = event["author"].casefold().removesuffix("[bot]")
    if login == "github-actions":
        return "github-actions"
    return "other-bots" if event["role"] == "bot" else "users"


def is_feedback(event, lane):
    return (
        bool(event["body"].strip())
        and event["in_window"]
        and source_lane(event) == lane
        and (event["role"] == "reviewer" if lane == "users" else event["role"] != "author")
    )


def is_recent_comment(event, lane):
    return bool(event["body"].strip()) and event["in_window"] and source_lane(event) == lane


def compact_event(event):
    result = {
        key: event[key]
        for key in (
            "key",
            "kind",
            "pr",
            "pr_title",
            "author",
            "role",
            "automation_marker",
            "in_window",
            "body",
            "url",
            "path",
            "line",
            "original_line",
            "reply_to",
        )
    }
    for key in (
        "created_at",
        "diff_hunk",
        "commit_id",
        "original_commit_id",
        "review_id",
        "review_state",
    ):
        result[key] = event.get(key)
    result["reactions"] = event.get("reactions", {})
    return result


def guidance_inventory(repo, extra_paths=()):
    command = [
        "rg",
        "--files",
        "--hidden",
        "--no-ignore",
        "-g",
        "AGENTS.md",
        "-g",
        "SKILL.md",
        "-g",
        "REVIEW.md",
        "-g",
        "!**/.git/**",
        "-g",
        "!**/target/**",
        str(repo),
    ]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode not in (0, 1):
        raise ValueError(result.stderr.strip())
    paths = {Path(path).resolve() for path in result.stdout.splitlines()}
    for supplied in extra_paths:
        path = Path(supplied).expanduser().resolve()
        paths.add(path / "SKILL.md" if path.is_dir() else path)
    return [
        {
            "path": str(path.relative_to(repo.resolve()))
            if path.is_relative_to(repo.resolve())
            else str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted(paths)
    ]


def prepare(corpus, output, max_chars=50000, lane="users", repo=None, guidance=()):
    if lane not in LANES:
        raise ValueError(f"unknown source lane: {lane}")
    events = read_jsonl(corpus / "events.jsonl")
    threads = read_jsonl(corpus / "threads.jsonl")
    feedback = feedback_index(threads, source_lane) if lane == "github-actions" else {}
    units = []
    for thread in threads:
        recent_feedback = [e for e in thread["comments"] if is_feedback(e, lane)]
        recent_comments = [e for e in thread["comments"] if is_recent_comment(e, lane)]
        if recent_comments:
            unit = {
                "unit_id": f"pr:{thread['pr']}:thread:{thread['root_id']}",
                "lane": lane,
                "pr": thread["pr"],
                "feedback_keys": [e["key"] for e in recent_feedback],
                "recent_keys": [e["key"] for e in recent_comments],
                "events": [compact_event(e) for e in thread["comments"] if source_lane(e) == lane],
                "other_lane_event_keys": [
                    e["key"] for e in thread["comments"] if source_lane(e) != lane
                ],
            }
            if lane == "github-actions":
                unit["feedback_on_findings"] = [feedback[e["key"]] for e in recent_feedback]
            units.append(unit)
    # Top-level discussions have no reply links; keep the same-lane PR conversation together.
    conversations = {}
    for event in events:
        if event["kind"] != "inline" and is_recent_comment(event, lane):
            conversations.setdefault(event["pr"], []).append(event)
    for pr, conversation in conversations.items():
        units.append(
            {
                "unit_id": f"pr:{pr}:conversation",
                "pr": pr,
                "lane": lane,
                "feedback_keys": [e["key"] for e in conversation if is_feedback(e, lane)],
                "recent_keys": [e["key"] for e in conversation],
                "events": [compact_event(e) for e in conversation],
            }
        )
    units.sort(key=lambda unit: (unit["pr"], unit["unit_id"]))
    output.mkdir(parents=True, exist_ok=True)
    rows = [json.dumps(unit, ensure_ascii=False) + "\n" for unit in units]
    (output / "units.jsonl").write_text("".join(rows))
    if repo:
        write_json(output / "guidance-index.json", guidance_inventory(repo, guidance))
    if lane == "github-actions":
        (output / "feedback.jsonl").write_text(
            "".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in feedback.values())
        )
    batches_dir = output / "batches"
    batches_dir.mkdir(exist_ok=True)
    for old_batch in batches_dir.glob("batch-*.jsonl"):
        old_batch.unlink()
    batch, length, number = [], 0, 0
    for row in rows:
        if batch and length + len(row) > max_chars:
            number += 1
            (batches_dir / f"batch-{number:03d}.jsonl").write_text("".join(batch))
            batch, length = [], 0
        batch.append(row)
        length += len(row)
    if batch:
        number += 1
        (batches_dir / f"batch-{number:03d}.jsonl").write_text("".join(batch))
    write_json(
        output / "preparation.json",
        {
            "units": len(units),
            "batches": number,
            "lane": lane,
            "feedback_events": sum(len(unit["feedback_keys"]) for unit in units),
            "recent_comments": sum(len(unit["recent_keys"]) for unit in units),
            "note": (
                "Review source bodies stay in their lane. Full same-lane inline threads include "
                "author replies, older context, and available diff/commit metadata. Same-lane "
                "top-level PR conversations stay together. Only GitHub Actions inputs include "
                "scored feedback attached to specific findings. User ideas need no reactions or "
                "recurrence, but every idea needs a concrete integration target and code scope. "
                "An oversized unit stays intact. No semantic labeling has occurred."
            ),
        },
    )
    return {"units": len(units), "batches": number}


def validated_integration(integration, repo, lane, guidance_sources):
    """Require a concrete target, allowing scoped proposals for new nested guidance files."""
    strict = lane == "github-actions"
    if not integration:
        raise ValueError("every idea requires a concrete integration target")
    integration = dict(integration)
    if integration["status"] not in {
        "new",
        "strengthen",
        "already-covered",
        "conflict",
        "defer",
        "unassessed",
    }:
        raise ValueError("unknown integration status")
    operation = integration.get(
        "operation", "change" if integration["status"] == "strengthen" else "add"
    )
    if operation not in {"add", "change", "remove"}:
        raise ValueError("unknown integration operation")
    if integration["status"] == "new" and operation != "add":
        raise ValueError("new guidance must be an addition")
    if integration["status"] == "strengthen" and operation not in {"change", "remove"}:
        raise ValueError("existing guidance must be changed or removed, not classified as new")
    if (
        strict
        and integration["status"] in {"new", "strengthen"}
        and operation != "remove"
        and not integration.get("proposed_text", "").strip()
    ):
        raise ValueError("integration suggestion requires proposed_text")
    existing_quote = integration.get("existing_quote", "")
    if strict and operation == "remove" and not existing_quote:
        raise ValueError("removing guidance requires the exact existing text")
    integration["operation"] = operation
    if not integration.get("path") or repo is None:
        raise ValueError("integration requires a repository and target path")
    if not integration.get("rationale", "").strip():
        raise ValueError("integration requires a rationale for the target and scope")
    relative = Path(integration["path"])
    target = (repo / relative).resolve()
    allowed_external = {(repo / source["path"]).resolve() for source in guidance_sources}
    if (
        relative.is_absolute() or not target.is_relative_to(repo.resolve())
    ) and target not in allowed_external:
        raise ValueError("integration path must stay inside the repository")
    code_paths = integration.get("code_paths", [])
    if not isinstance(code_paths, list) or not code_paths:
        raise ValueError("ideas require code_paths identifying the areas where the pattern applies")
    for pattern in code_paths:
        if (
            not isinstance(pattern, str)
            or not pattern.strip()
            or Path(pattern).is_absolute()
            or ".." in Path(pattern).parts
        ):
            raise ValueError("code_paths must be nonempty repository-relative paths or globs")
        matches = [repo] if pattern == "." else list(repo.glob(pattern))
        if not matches or any(
            not path.resolve().is_relative_to(repo.resolve()) for path in matches
        ):
            raise ValueError(f"code scope does not match repository content: {pattern}")
        if (
            target.name == "AGENTS.md"
            and target.is_relative_to(repo.resolve())
            and any(not path.resolve().is_relative_to(target.parent) for path in matches)
        ):
            raise ValueError("nested AGENTS.md scope must stay within its directory subtree")
    integration["code_paths"] = code_paths
    integration["target_exists"] = target.is_file()
    if not target.exists():
        if (
            integration["status"] != "new"
            or operation != "add"
            or target.name not in {"AGENTS.md", "SKILL.md", "REVIEW.md"}
        ):
            raise ValueError(
                "new guidance files must be explicit additions targeting "
                "AGENTS.md, SKILL.md, or REVIEW.md"
            )
        if integration.get("anchor") or existing_quote:
            raise ValueError(
                "a proposed new guidance file cannot quote existing contents or anchors"
            )
        if target.name == "AGENTS.md":
            integration["inherited_guidance"] = [
                str((parent / "AGENTS.md").relative_to(repo.resolve()))
                for parent in reversed(target.parents)
                if parent.is_relative_to(repo.resolve()) and (parent / "AGENTS.md").is_file()
            ]
        return integration
    if strict and not integration.get("anchor"):
        raise ValueError("GitHub Actions integration requires an anchor in an existing target")
    text = target.read_text()
    anchor = integration.get("anchor")
    if anchor and anchor not in text:
        raise ValueError(f"integration anchor not found: {relative}")
    if existing_quote and existing_quote not in text:
        raise ValueError("existing guidance quote is not verbatim")
    integration["source_sha256"] = hashlib.sha256(text.encode()).hexdigest()
    if anchor:
        integration["anchor_line"] = text[: text.index(anchor)].count("\n") + 1
    return integration


def validated_pattern(
    proposal, events, unit_by_key, repo, lane="users", feedback=None, guidance_sources=()
):
    required = (
        "id",
        "major_category",
        "category",
        "principle",
        "scope",
        "exceptions",
        "integration",
        "evidence",
    )
    if any(field not in proposal for field in required):
        raise ValueError(f"pattern missing required fields: {proposal.get('id', '<unnamed>')}")
    if not re.fullmatch(r"[a-z][a-z0-9-]*", proposal["id"]):
        raise ValueError("pattern IDs must be lowercase hyphenated names")
    if proposal["category"] not in {"style", "design", "correctness", "testing"}:
        raise ValueError(f"unknown category: {proposal['category']}")
    if proposal["major_category"] not in MAJOR_CATEGORIES:
        raise ValueError(f"unknown major category: {proposal['major_category']}")
    if not proposal["principle"].strip() or not proposal["scope"].strip():
        raise ValueError("principle and scope must be nonempty")
    if not proposal["evidence"]:
        raise ValueError(f"pattern has no evidence: {proposal['id']}")
    support, opposition, evidence = [], [], []
    seen = set()
    for citation in proposal["evidence"]:
        key, quote, stance = citation["event_key"], citation["quote"], citation["stance"]
        if key not in events:
            raise ValueError(f"unknown evidence event: {key}")
        event = events[key]
        if source_lane(event) != lane:
            raise ValueError(
                f"cross-lane evidence rejected: {key} belongs to {source_lane(event)}, "
                f"expected {lane}"
            )
        if not quote.strip() or quote not in event["body"]:
            raise ValueError(f"quote is not verbatim source text: {key}")
        if stance not in {"support", "oppose", "context"}:
            raise ValueError(f"unknown stance: {stance}")
        if (key, stance) in seen:
            continue
        seen.add((key, stance))
        if is_feedback(event, lane):
            if stance == "support":
                support.append(event)
            elif stance == "oppose":
                opposition.append(event)
        evidence.append(
            {
                **citation,
                "url": event["url"],
                "pr": event["pr"],
                "author": event["author"],
                "role": event["role"],
                "automation_marker": event["automation_marker"],
                "unit_id": unit_by_key.get(key),
            }
        )
    integration = validated_integration(proposal.get("integration"), repo, lane, guidance_sources)
    status = integration["status"]
    action = proposal.get(
        "suggested_action",
        integration["operation"] if status in {"new", "strengthen"} else "unspecified",
    )
    if action not in {"add", "change", "remove", "unspecified"}:
        raise ValueError("unknown suggested action")
    bucket = (
        "add"
        if status == "new"
        else "remove-change"
        if status == "strengthen"
        else (
            "deferred"
            if lane == "github-actions"
            else "add"
            if status == "unassessed" and action == "add"
            else "remove-change"
            if status == "unassessed" and action in {"change", "remove"}
            else "unassessed"
        )
    )
    support_prs = sorted({event["pr"] for event in support})
    reviewers = sorted({event["author"] for event in support})
    recurring = len(support_prs) >= 2
    strength = (
        "multiple-reviewers"
        if recurring and len(reviewers) >= 2
        else (
            "single-reviewer"
            if recurring
            else "single-pr"
            if support_prs
            else "no-reviewer-support"
        )
    )
    recommendation_state = (
        "idea-with-disagreement"
        if opposition or status == "conflict"
        else "idea"
        if support
        else "context-only-idea"
    )
    feedback_summary = None
    if lane == "github-actions":
        annotations = validate_reply_annotations(
            proposal.get("feedback_evidence", []), {e["key"] for e in support}, feedback or {}
        )
        feedback_summary = rank_feedback({e["key"] for e in support}, feedback or {}, annotations)
        direction = "add_score" if bucket == "add" else "change_score"
        recommendation_state = (
            "defer"
            if bucket == "deferred"
            else "needs-review"
            if feedback_summary["mixed_feedback"] or opposition
            else "needs-guidance-match"
            if bucket == "remove-change" and not integration.get("existing_quote")
            else "bot-feedback-candidate"
            if recurring and feedback_summary[direction] > 0
            else "single-pr-bot-idea"
            if feedback_summary[direction] > 0
            else "insufficient-feedback"
        )
    elif proposal.get("feedback_evidence"):
        raise ValueError("bot feedback annotations are only allowed in the GitHub Actions lane")
    return {
        **proposal,
        "lane": lane,
        "recommendation_state": recommendation_state,
        "bucket": bucket,
        "suggested_action": action,
        "feedback": feedback_summary,
        "evidence": evidence,
        "support": {
            "prs": support_prs,
            "reviewers": reviewers,
            "discussion_units": sorted(
                {unit_by_key[e["key"]] for e in support if e["key"] in unit_by_key}
            ),
            "opposing_prs": sorted({event["pr"] for event in opposition}),
            "marked_automation_events": sum(event["automation_marker"] for event in support),
            "recurring": recurring,
            "breadth": strength,
        },
        "integration": integration,
    }


def synthesize(corpus, prepared, proposals_path, repo, output, guidance=()):
    events = {event["key"]: event for event in read_jsonl(corpus / "events.jsonl")}
    unit_by_key = {}
    for unit in read_jsonl(prepared / "units.jsonl"):
        for event in unit["events"]:
            events.setdefault(event["key"], event)
            unit_by_key[event["key"]] = unit["unit_id"]
    document = json.loads(proposals_path.read_text())
    lane = json.loads((prepared / "preparation.json").read_text())["lane"]
    if document["lane"] != lane:
        raise ValueError("proposal lane differs from prepared inputs")
    sources = guidance_inventory(repo, guidance)
    feedback = (
        feedback_index(read_jsonl(corpus / "threads.jsonl"), source_lane)
        if lane == "github-actions"
        else {}
    )
    patterns = [
        validated_pattern(proposal, events, unit_by_key, repo, lane, feedback, sources)
        for proposal in document["patterns"]
    ]
    if len({pattern["id"] for pattern in patterns}) != len(patterns):
        raise ValueError("duplicate pattern IDs; merge equivalent candidates before synthesis")
    patterns.sort(
        key=lambda pattern: (
            list(MAJOR_CATEGORIES).index(pattern["major_category"]),
            {"add": 0, "remove-change": 1, "unassessed": 2, "deferred": 3}[pattern["bucket"]],
            -(
                pattern["feedback"]["add_score" if pattern["bucket"] == "add" else "change_score"]
                if pattern["feedback"]
                else 0
            ),
            -len(pattern["support"]["prs"]),
            pattern["id"],
        )
    )
    output.mkdir(parents=True, exist_ok=True)
    source_summary = json.loads((corpus / "summary.json").read_text())
    result = {
        "lane": lane,
        "source_window": {key: source_summary[key] for key in ("repo", "since", "until")},
        "analysis_scope": document["analysis_scope"],
        "patterns": patterns,
        "guidance_scope": sources,
        "buckets": {
            bucket: [p["id"] for p in patterns if p["bucket"] == bucket]
            for bucket in ("add", "remove-change", "unassessed", "deferred")
        },
        "groups": {
            group: [p["id"] for p in patterns if p["major_category"] == group]
            for group in MAJOR_CATEGORIES
        },
        "note": (
            f"Source lane: {lane}. Only GitHub Actions receives emoji/reply scores. Other lanes "
            "retain one-PR ideas; recurrence and existing-guidance coverage are annotations, "
            "not admission gates. Every idea has a concrete integration target and code paths, "
            "including proposed nested AGENTS.md files for local patterns. PR-author replies "
            "and older comments provide context without inflating recent-reviewer recurrence. "
            "Counts are cited lower bounds, not exhaustive frequencies or proof of correctness. "
            "Add/change/remove labels are proposals for a later integration agent."
        ),
    }
    write_json(output / "patterns.json", result)
    chunks = [
        f"# Review-derived ideas: {lane}\n\n"
        f"{source_summary['since']} through {source_summary['until']}.\n\n",
        f"{document['analysis_scope']}\n\n",
        result["note"]
        + "\n\nExisting guidance classifications are semantic assessments; source hashes and "
        "anchors identify any files assessed. Proposed new files have no existing contents.\n",
    ]
    last_group, last_bucket = None, None
    for pattern in patterns:
        if pattern["major_category"] != last_group:
            last_group, last_bucket = pattern["major_category"], None
            chunks.append(f"\n## {MAJOR_CATEGORIES[last_group]}\n")
        if pattern["bucket"] != last_bucket:
            last_bucket = pattern["bucket"]
            label = {
                "add": "Possible additions",
                "remove-change": "Possible changes/removals",
                "unassessed": "Other targeted ideas for review",
                "deferred": "Already covered or deferred",
            }[last_bucket]
            chunks.append(f"\n### {label}\n")
        support, integration = pattern["support"], pattern["integration"]
        chunks.append(
            f"\n#### {pattern['id']} ({pattern['category']})\n\n{pattern['principle']}\n\n"
            f"Scope: {pattern['scope']}\n\nExceptions: {pattern['exceptions']}\n\n"
            f"Cited evidence: {len(support['prs'])} distinct PRs; "
            f"{len(support['reviewers'])} reviewers; "
            f"{len(support['discussion_units'])} discussions. Recurring: {support['recurring']}. "
            f"Opposing PRs cited: {len(support['opposing_prs'])}. "
            f"State: {pattern['recommendation_state']}.\n\n"
            f"Suggested action: {pattern['suggested_action']}; "
            "final integration belongs to the downstream agent.\n"
        )
        if pattern.get("thread_context"):
            chunks.append(f"\nThread context: {pattern['thread_context']}\n")
        location = integration["path"]
        if "anchor_line" in integration:
            location += f":{integration['anchor_line']}"
        file_state = "existing file" if integration["target_exists"] else "proposed new file"
        chunks.append(
            f"\nIntegration target: `{location}` ({file_state}; {integration['status']}).\n\n"
            f"{integration['rationale']}\n"
        )
        if integration["code_paths"]:
            chunks.append(
                "\nApplicable code: "
                + ", ".join(f"`{path}`" for path in integration["code_paths"])
                + ".\n"
            )
        if integration.get("inherited_guidance"):
            chunks.append(
                "\nInherited guidance: "
                + ", ".join(f"`{path}`" for path in integration["inherited_guidance"])
                + ". Keep this proposal specific to its subtree.\n"
            )
        if pattern["feedback"]:
            score = pattern["feedback"]
            chunks.append(
                f"\nBot feedback ranking: add={score['add_score']}, "
                f"remove/change={score['change_score']}. "
                f"Mixed feedback: {score['mixed_feedback']}.\n\n"
            )
            for finding in score["findings"]:
                votes = finding["reactions"]
                chunks.append(
                    f"- `{finding['finding_key']}`: +1={votes.get('+1', 0)}, "
                    f"-1={votes.get('-1', 0)}, eyes={votes.get('eyes', 0)}; "
                    f"positive replies={finding['positive_reply_units']}, "
                    f"corrections={finding['corrective_reply_units']}, "
                    f"scope replies={finding['out_of_scope_reply_units']}; "
                    f"unclassified replies={finding['unclassified_reply_count']}.\n"
                )
            chunks.append("\n" + score["note"] + "\n")
            for reply in score["annotations"]:
                chunks.append(
                    f"\nFeedback [{reply['event_key']}]({reply['url']}) @{reply['author']} "
                    f"({reply['signal']}): {' '.join(reply['quote'].split())}\n"
                )
        if integration.get("existing_quote"):
            chunks.append(
                f"\nExisting wording to {integration['operation']}:\n\n"
                f"```markdown\n{integration['existing_quote']}\n```\n"
            )
        if integration.get("proposed_text"):
            chunks.append(
                f"\nProposed wording:\n\n```markdown\n{integration['proposed_text']}\n```\n"
            )
        chunks.append("\nCitations:\n\n")
        for citation in pattern["evidence"]:
            quote = " ".join(citation["quote"].split())
            observation = (
                f" Explanation: {citation['observation']}" if citation.get("observation") else ""
            )
            chunks.append(
                f"- [{citation['event_key']}]({citation['url']}) @{citation['author']} "
                f"({citation['role']}; {citation['stance']}): {quote}{observation}\n"
            )
    (output / "recommendations.md").write_text("".join(chunks))
    return {
        "patterns": len(patterns),
        "recurring": sum(p["support"]["recurring"] for p in patterns),
        "groups": dict(Counter(p["major_category"] for p in patterns)),
        "integration_status": dict(Counter(p["integration"]["status"] for p in patterns)),
    }


def overview(reports, output):
    """Group validated ideas across source reports, preserving separate authorship lanes."""
    documents = [(path, json.loads((path / "patterns.json").read_text())) for path in reports]
    windows = {json.dumps(doc["source_window"], sort_keys=True) for _, doc in documents}
    if len(windows) != 1:
        raise ValueError("overview reports must describe the same repository and date window")
    window = documents[0][1]["source_window"]
    chunks = [
        f"# Review ideas by category\n\n{window['repo']}: "
        f"{window['since']} through {window['until']}.\n\n",
        "Selected source-cited ideas for downstream integration review. Counts describe cited "
        "evidence, not an exhaustive semantic classification. Human ideas need no emoji feedback "
        "or recurrence. GitHub Actions scores stay in their own lane.\n",
    ]
    for group, label in MAJOR_CATEGORIES.items():
        chunks.append(f"\n## {label}\n")
        for path, doc in documents:
            patterns = [p for p in doc["patterns"] if p["major_category"] == group]
            if not patterns:
                continue
            chunks.append(f"\n### {doc['lane']}\n\n")
            for p in patterns:
                integration = p["integration"]
                link = os.path.relpath(path / "recommendations.md", output.parent) + f"#{p['id']}"
                # Detailed headings include the secondary category in their Markdown anchor.
                link += f"-{p['category']}"
                chunks.append(
                    f"- [{p['id']}]({link}): {p['principle']} Target: `{integration['path']}`; "
                    f"{len(p['support']['prs'])} cited PRs; {p['suggested_action']}; "
                    f"{p['recommendation_state']}.\n"
                )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(chunks))
    return {
        "reports": len(documents),
        "patterns": sum(len(doc["patterns"]) for _, doc in documents),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare", help="export complete semantic-analysis inputs")
    prepare_parser.add_argument("--corpus", type=Path, required=True)
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--max-chars", type=int, default=50000)
    prepare_parser.add_argument("--lane", choices=LANES, default="users")
    prepare_parser.add_argument("--repo", type=Path)
    prepare_parser.add_argument(
        "--guidance",
        type=Path,
        action="append",
        default=[],
        help="additional applicable skill/guidance file or skill directory",
    )
    build = commands.add_parser(
        "build", help="validate evidence, annotate recurrence, render ideas for later integration"
    )
    for flag in ("corpus", "prepared", "proposals", "repo", "output"):
        build.add_argument(f"--{flag}", type=Path, required=True)
    build.add_argument("--guidance", type=Path, action="append", default=[])
    index = commands.add_parser(
        "overview", help="group source-separated reports under the three major categories"
    )
    index.add_argument("--reports", type=Path, nargs="+", required=True)
    index.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "overview":
        print(json.dumps(overview(args.reports, args.output), indent=2))
        return
    if args.guidance and not args.repo:
        parser.error("--guidance requires --repo")
    if args.command == "prepare":
        if args.max_chars <= 0:
            parser.error("--max-chars must be positive")
        result = prepare(
            args.corpus, args.output, args.max_chars, args.lane, args.repo, args.guidance
        )
    else:
        result = synthesize(
            args.corpus, args.prepared, args.proposals, args.repo, args.output, args.guidance
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
