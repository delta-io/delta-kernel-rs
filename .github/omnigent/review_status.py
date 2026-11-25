"""Compute a commit's advisory AI review status and update the main PR comment.

Use patch-excerpt coverage, Reviewer results, Checker decisions, candidate-to-final mappings,
earlier final Blockers and finding output records. Missing required records give NO VERDICT.
The command updates the main AI review Conversation comment, writes status JSON through
--output and rebuilds the AI Review Summary Check Run report. The workflow creates that
Check Run with GitHub conclusion neutral.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path, PurePosixPath
from typing import Any

from finding_publication import GitHub, PublicationError, publication_check, review_overview
from review_evidence import execution_summary
from review_history import canonical_finding_body
from review_publish import BOT_MARKER, format_review_summary, format_status_check

_SHA = re.compile(r"[0-9a-f]{40}")
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_FINAL_ID = re.compile(r"(Blocker|ShouldFix|Nit)[1-9][0-9]*")
_SEVERITIES = ("Blocker", "Should Fix", "Nit")
_PROTOCOL_MODULES = {
    "actions", "action_reconciliation", "checkpoint", "committer", "crc",
    "log_compaction", "log_reader", "log_replay", "log_segment", "log_segment_files",
    "snapshot", "table_features", "transaction", "content_tree",
}


def compute_review_status(
    *, head_sha: str, run_id: str, coverage: Any, completion: Any, publication: Any,
    history: Any, breaking_change: bool = False,
) -> dict[str, Any]:
    """Return the reviewed commit's advisory status, counted findings and incomplete records.

    PR comment modes require confirmed finding-comment URLs. Summary and artifact modes record
    the selected report placement without comment URLs or delivery claims. These records establish
    where a finding is reported; they do not establish whether its claim is correct.

    Count earlier final Blockers for the same commit when the current orchestrator report omits
    them. An earlier-commit Blocker without a matching current result gives NO VERDICT and stays
    outside the current counts. status_requirements_met means the records satisfy the status rules,
    including an uncontested Checker decision. It does not prove every changed line was examined.
    """
    reasons: list[dict[str, str]] = []
    if not _valid_sha(head_sha) or not isinstance(run_id, str) or not run_id:
        _reason(reasons, "run_identity_invalid", "The reviewed run or commit is unavailable.")
    changed_paths = _coverage_reasons(coverage, head_sha, reasons)
    _completion_reasons(completion, head_sha, run_id, reasons)
    findings = _current_findings(publication, head_sha, run_id, reasons)
    prior = _history_blockers(history, reasons)
    ambiguous = _ambiguous_destinations(prior) | _ambiguous_destinations(findings)
    current_blockers = [finding for finding in findings if finding["severity"] == "Blocker"]
    used_ids = {finding["id"] for finding in findings}
    for blocker in prior:
        if blocker["head_sha"] != head_sha or not blocker["url"]:
            continue
        if any(_same_finding(blocker, current, ambiguous) for current in current_blockers):
            continue
        finding_id = blocker["id"]
        if not re.fullmatch(r"Blocker[1-9][0-9]*", finding_id) or finding_id in used_ids:
            index = 1
            while f"Blocker{index}" in used_ids:
                index += 1
            finding_id = f"Blocker{index}"
        used_ids.add(finding_id)
        retained = {
            **blocker, "id": finding_id, "source_finding_id": blocker["id"],
            "severity": "Blocker", "head_sha": head_sha,
            "body": _historical_claim(blocker.get("body")),
            "placement": "history", "delivery": "reused",
        }
        findings.append(retained)
        current_blockers.append(retained)
    for blocker in prior:
        if blocker["head_sha"] is None or blocker["head_sha"] == head_sha:
            continue
        if not any(_same_finding(blocker, current, ambiguous) for current in current_blockers):
            _reason(
                reasons, "earlier_blocker_unchecked",
                "Earlier Blocker has not been rechecked on this commit.", blocker["url"],
            )
    counts = Counter(finding["severity"] for finding in findings)
    if reasons:
        status = "NO VERDICT"
    elif counts["Blocker"]:
        status = "NOT APPROVED"
    elif counts["Should Fix"] or counts["Nit"]:
        status = "APPROVED WITH FEEDBACK"
    else:
        status = "APPROVED"
    return {
        "head_sha": head_sha, "run_id": run_id, "status": status,
        "status_requirements_met": not reasons, "reasons": reasons, "findings": findings,
        "counts": {severity: counts[severity] for severity in _SEVERITIES},
        "sensitive_paths": _sensitive_paths(changed_paths), "breaking_change": breaking_change,
    }


def _coverage_reasons(coverage, head_sha, reasons):
    if not isinstance(coverage, dict):
        _reason(
            reasons, "input_coverage_unavailable",
            "The patch-excerpt coverage record is unavailable.",
        )
        return []
    if (coverage.get("head_sha") != head_sha or not _valid_sha(coverage.get("head_sha"))
            or not _valid_sha(coverage.get("base_sha"))):
        _reason(
            reasons, "input_scope_mismatch",
            "The patch-excerpt coverage record names a different or unknown review range.",
        )
        return []
    total, delivered = coverage.get("total_bytes"), coverage.get("delivered_bytes")
    valid_bytes = _nonnegative(total) and _nonnegative(delivered) and delivered <= total
    files = coverage.get("files")
    valid_files = isinstance(files, list) and all(
        isinstance(item, dict) and _valid_path(item.get("path"))
        and item.get("status") in ("full", "partial", "omitted")
        for item in files
    )
    if not valid_files:
        files = []
    measured = Counter(item["status"] for item in files)
    valid_totals = (
        valid_files and _nonnegative(coverage.get("total_files"))
        and coverage["total_files"] == len(files) and (total == 0 or bool(files))
    ) and all(
        _nonnegative(coverage.get(name)) and coverage[name] == measured[status]
        for name, status in (("full_files", "full"), ("partial_files", "partial"),
                             ("omitted_files", "omitted"))
    )
    if not valid_bytes or not valid_totals:
        _reason(
            reasons, "coverage_record_incomplete",
            "The patch-excerpt coverage record is incomplete.",
        )
    if (coverage.get("complete") is not True or not valid_bytes or delivered != total
            or measured["partial"] or measured["omitted"]):
        _reason(
            reasons, "diff_incomplete",
            "Subagent task messages did not include the complete PR change patch.",
        )
    return [item["path"] for item in files]


def _completion_reasons(completion, head_sha, run_id, reasons):
    if not isinstance(completion, dict):
        _reason(
            reasons, "execution_unavailable",
            "The Reviewer and Checker result record is unavailable.",
        )
        return
    if completion.get("head_sha") != head_sha or completion.get("run_id") != run_id:
        _reason(
            reasons, "execution_identity_mismatch",
            "The Reviewer and Checker result record names another run or commit.",
        )
    details = completion.get("reasons")
    if completion.get("complete") is not True or not isinstance(details, list) or details:
        messages = (
            [message for message in details if isinstance(message, str) and message]
            if isinstance(details, list) else []
        )
        if messages:
            for message in messages:
                _reason(reasons, "execution_detail", message)
        else:
            _reason(
                reasons, "execution_incomplete",
                "Required Reviewer results or Checker decisions are incomplete.",
            )
    roles, attempts = completion.get("configured_reviewer_roles"), completion.get("attempts")
    if (not isinstance(roles, list) or not roles or not all(isinstance(role, str) for role in roles)
            or len(set(roles)) != len(roles) or not isinstance(attempts, list)):
        _reason(
            reasons, "reviewer_returns_unavailable",
            "The required Reviewer results are unavailable.",
        )
    else:
        for role in roles:
            if not any(
                isinstance(attempt, dict) and attempt.get("role") == role
                and attempt.get("outcome") == "returned" and attempt.get("metadata_valid") is True
                and isinstance(attempt.get("session_id"), str) and attempt["session_id"]
                and isinstance(attempt.get("candidates"), list)
                for attempt in attempts
            ):
                _reason(
                    reasons, "reviewer_missing",
                    f"Required reviewer {role} has no valid task result "
                    "recorded by the runtime observer.",
                )
    candidates = completion.get("candidates")
    if not isinstance(candidates, list):
        _reason(reasons, "candidate_accounting_unavailable", "Candidate accounting is unavailable.")
    else:
        for candidate in candidates:
            if not isinstance(candidate, dict):
                _reason(
                    reasons, "candidate_accounting_unavailable",
                    "Candidate accounting is unavailable.",
                )
                continue
            checker = candidate.get("checker")
            if candidate.get("severity") not in _SEVERITIES:
                _reason(
                    reasons, "candidate_accounting_unavailable",
                    "Candidate accounting is unavailable.",
                )
                continue
            if isinstance(checker, dict) and checker.get("decision") == "CONTESTED":
                _reason(reasons, "correctness_contested", "A candidate finding remains contested.")
            if candidate.get("severity") in ("Blocker", "Should Fix") and (
                not isinstance(checker, dict)
                or checker.get("decision") not in ("CONFIRMED", "DISPROVED", "NITPICK", "CONTESTED")
            ):
                _reason(
                    reasons, "checker_incomplete",
                    "A required Checker decision is unavailable.",
                )
    if "rule_evaluation" in completion:
        rules = completion["rule_evaluation"]
        if not isinstance(rules, dict) or rules.get("complete") is not True:
            _reason(reasons, "rules_incomplete", "Selected-rule evaluation is incomplete.")


def _current_findings(publication, head_sha, run_id, reasons):
    if not isinstance(publication, dict):
        _reason(
            reasons, "publication_unavailable",
            "The final finding output record is unavailable.",
        )
        return []
    run_url = publication.get("run_url")
    identity_valid = (
        publication.get("head_sha") == head_sha
        and publication.get("run_id", run_id) == run_id
        and isinstance(run_url, str)
        and re.fullmatch(r"https://github\.com/[^/\s]+/[^/\s]+/actions/runs/[^/\s]+", run_url)
        and run_url.endswith(f"/actions/runs/{run_id}")
    )
    if not identity_valid:
        _reason(
            reasons, "publication_identity_mismatch",
            "The finding output record names another workflow run or reviewed commit.",
        )
    errors = publication.get("errors")
    if publication.get("complete") is not True or not isinstance(errors, list) or errors:
        _reason(
            reasons, "publication_incomplete",
            "The finding output record is incomplete or contains unconfirmed comment writes.",
        )
    raw_findings = publication.get("findings")
    if not isinstance(raw_findings, list):
        _reason(
            reasons, "publication_findings_unavailable",
            "The final finding list is unavailable.",
        )
        return []
    report_mode = publication.get("mode") in ("summary", "artifact")
    findings, seen = [], set()
    for raw in raw_findings:
        if not isinstance(raw, dict):
            _reason(
                reasons, "finding_unconfirmed",
                "A final finding's comment/report record is missing or invalid.",
            )
            continue
        finding_id, severity = raw.get("id"), raw.get("severity")
        match = _FINAL_ID.fullmatch(finding_id) if isinstance(finding_id, str) else None
        output_valid = (
            raw.get("placement") == publication["mode"]
            and raw.get("url") is None and raw.get("delivery") is None
            if report_mode else _valid_url(raw.get("url"))
        )
        if (match is None or severity not in _SEVERITIES
                or match[1].replace("ShouldFix", "Should Fix") != severity
                or raw.get("head_sha") != head_sha or not _valid_sha(raw.get("head_sha"))
                or not output_valid):
            _reason(
                reasons, "finding_unconfirmed",
                "A final finding's comment/report record is missing or invalid.",
            )
            continue
        if finding_id in seen:
            _reason(
                reasons, "finding_identity_ambiguous",
                "Final finding identities are ambiguous.",
            )
            continue
        seen.add(finding_id)
        finding = dict(raw)
        if not isinstance(finding.get("body"), str) or not finding["body"].strip():
            _reason(
                reasons, "finding_body_unavailable",
                "A final finding's text is unavailable.",
            )
            finding["body"] = "Finding text is unavailable."
        finding["fingerprint"] = _fingerprint(finding)
        findings.append(finding)
    return findings


def _history_blockers(history, reasons):
    if not isinstance(history, dict):
        _reason(reasons, "history_unavailable", "Earlier AI review history is unavailable.")
        return []
    details = history.get("reasons")
    if history.get("complete") is not True or not isinstance(details, list) or details:
        _reason(reasons, "history_incomplete", "Earlier AI review history is incomplete.")
    if isinstance(details, list):
        messages = {
            "history_unavailable": "Earlier review history could not be fetched.",
            "history_query_errors": "The earlier review history request returned errors.",
            "history_pages_missing": "Earlier review history has pages that were not fetched.",
            "history_page_info_missing": (
                "Earlier review history has no complete pagination metadata."
            ),
            "history_prompt_clipped": "Earlier review history was clipped before the review.",
            "history_inline_locations_unavailable": (
                "Earlier inline finding locations are unavailable."
            ),
            "history_connection_missing": "Part of the earlier review history response is missing.",
            "history_record_incomplete": "An earlier review history record is incomplete.",
            "blocker_sha_unknown": "An earlier Blocker's reviewed commit is unknown.",
            "blocker_url_unknown": "An earlier Blocker's discussion URL is unknown.",
        }
        for detail in details:
            if isinstance(detail, str) and detail:
                _reason(reasons, "history_detail", messages.get(detail, detail))
    blockers = history.get("blockers")
    if not isinstance(blockers, list):
        _reason(reasons, "history_blockers_unavailable", "Earlier Blocker sources are unavailable.")
        return []
    result = []
    for blocker in blockers:
        if not isinstance(blocker, dict):
            _reason(
                reasons, "history_blocker_unknown",
                "An earlier Blocker's identity is unavailable.",
            )
            continue
        item = dict(blocker)
        item["id"] = str(item.get("id") or "Blocker")
        item["head_sha"] = item.get("head_sha") if _valid_sha(item.get("head_sha")) else None
        item["url"] = item.get("url") if _valid_url(item.get("url")) else None
        item["fingerprint"] = _fingerprint(item)
        if item["head_sha"] is None or item["url"] is None:
            _reason(
                reasons, "history_blocker_unknown",
                "An earlier Blocker's reviewed commit or comment URL is unknown.", item["url"],
            )
        result.append(item)
    return result


def _ambiguous_destinations(findings):
    identities = defaultdict(set)
    for finding in findings:
        identities[finding.get("url"), finding.get("head_sha")].add(
            (finding.get("id"), finding.get("fingerprint"))
        )
    return {url for (url, _), values in identities.items() if len(values) > 1}


def _same_finding(previous, current, ambiguous):
    if previous.get("fingerprint") and previous["fingerprint"] == current.get("fingerprint"):
        compatible = all(
            previous.get(key) is None or current.get(key) is None
            or previous[key] == current[key]
            for key in ("path", "line", "side")
        )
        if compatible:
            return True
    if not previous.get("url") or previous["url"] != current.get("url"):
        return False
    if previous["url"] not in ambiguous:
        return True
    return (
        previous.get("head_sha") == current.get("head_sha")
        and previous["id"] == current.get("source_finding_id", current.get("id"))
    )


def _fingerprint(finding):
    value = finding.get("fingerprint")
    body = finding.get("body")
    if isinstance(body, str) and body.strip():
        return hashlib.sha256(canonical_finding_body(body).encode("utf-8")).hexdigest()
    return value if isinstance(value, str) and _FINGERPRINT.fullmatch(value) else None


def _historical_claim(body):
    if not isinstance(body, str) or not body.strip():
        return "Blocker retained from an earlier review of this commit."
    return re.sub(
        r"^\s*(?:[-*+]\s+|\d+\.\s+)?(?:#{1,6}\s+)?(?:\*\*)?"
        r"\[?(?:Blocker\s*|B)[1-9][0-9]*\]?(?:\*\*)?\s*[:.-]?\s*",
        "", body.strip(), flags=re.IGNORECASE,
    ).strip() or "Blocker retained from an earlier review of this commit."


def _sensitive_paths(paths):
    matched = set()
    for path in paths:
        parts = PurePosixPath(path).parts
        name = parts[-1]
        if name in {"AGENTS.md", "CLAUDE.md"} or "AGENTS" in parts[:-1]:
            matched.add(path)
        elif name in {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml"}:
            matched.add(path)
        elif path.startswith(".cargo/"):
            matched.add(".cargo/")
        elif path.startswith(("ffi/", "ffi-proc-macros/")):
            matched.add(parts[0] + "/")
        elif path.startswith(".github/"):
            matched.add("/".join(parts[:2]) + ("/" if len(parts) > 2 else ""))
        elif path.startswith("kernel/src/") and len(parts) >= 3:
            module = parts[2].removesuffix(".rs")
            if module in _PROTOCOL_MODULES:
                matched.add("/".join(parts[:3]) + ("/" if len(parts) > 3 else ""))
        elif "tests" in parts and "data" in parts:
            index = parts.index("data")
            matched.add("/".join(parts[:index + 1]) + "/")
    return sorted(matched)


def _valid_sha(value):
    return isinstance(value, str) and _SHA.fullmatch(value) is not None


def _valid_url(value):
    return isinstance(value, str) and re.fullmatch(r"https://github\.com/[^\s<>|()]+", value)


def _valid_path(value):
    return (isinstance(value, str) and bool(value) and not value.startswith("/")
            and ".." not in PurePosixPath(value).parts and "\x00" not in value)


def _nonnegative(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _reason(reasons, code, message, url=None):
    value = {"code": code, "message": message}
    if url:
        value["url"] = url
    for existing in reasons:
        if existing["message"] == message and existing.get("url") == url:
            if existing["code"].endswith("_detail") and not code.endswith("_detail"):
                existing["code"] = code
            return
    reasons.append(value)


def publish_review_status(
    *, github: Any, repository: str, pr_number: int, publication: Any, review: str, run_url: str,
    review_status: dict[str, Any], coverage: Any, mode: str,
) -> dict[str, Any]:
    """Update this run's main AI review Conversation comment with its status and findings.

    Check the recorded commit, run URL, mode and comment ID before updating GitHub. If the
    updated text cannot be confirmed, set review_status to NO VERDICT and mark its status requirements
    unmet. Summary and artifact modes return without posting a PR comment.
    """
    if (not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)
            or isinstance(pr_number, bool) or not isinstance(pr_number, int) or pr_number < 1):
        raise ValueError("invalid repository")
    if mode not in {"inline", "comment", "collapsed", "summary", "artifact"}:
        raise ValueError("invalid publication mode")
    if mode in {"summary", "artifact"}:
        return review_status
    publication = publication if isinstance(publication, dict) else {}
    expected_run_url = f"https://github.com/{repository}/actions/runs/{review_status['run_id']}"
    if (
        publication.get("head_sha") != review_status["head_sha"]
        or publication.get("run_url") != run_url or run_url != expected_run_url
        or publication.get("mode") != mode
    ):
        _status_publication_gap(review_status)
        return review_status
    comment_id = publication.get("main_comment_id")
    if not isinstance(comment_id, int) or isinstance(comment_id, bool) or comment_id < 1:
        _status_publication_gap(review_status)
        return review_status
    endpoint = f"repos/{repository}/issues/comments/{comment_id}"

    def rendered():
        body = format_review_summary(
            overview=review_overview(review), head_sha=review_status["head_sha"],
            findings=review_status["findings"], run_url=run_url,
            counts_available=not publication.get("errors"),
            checks_url=f"https://github.com/{repository}/pull/{pr_number}/checks",
            coverage=coverage if isinstance(coverage, dict) else None, review_status=review_status,
        )
        source = {"head_sha": review_status["head_sha"], "run_url": run_url}
        marker = f"<!-- ai-review-main {json.dumps(source, sort_keys=True)} -->"
        return body.replace(BOT_MARKER, BOT_MARKER + "\n" + marker, 1)

    body = rendered()
    try:
        response = github.write("PATCH", endpoint, {"body": body})
        if not isinstance(response, dict) or response.get("body") != body:
            raise PublicationError("Status response was not confirmed")
    except PublicationError:
        try:
            observed = github.read(endpoint)
        except PublicationError:
            observed = {}
        if not isinstance(observed, dict) or observed.get("body") != body:
            _status_publication_gap(review_status)
            try:
                github.write("PATCH", endpoint, {"body": rendered()})
            except PublicationError:
                pass
    return review_status


def _status_publication_gap(review_status):
    _reason(
        review_status["reasons"], "status_publication_unconfirmed",
        "Python could not confirm the main AI review comment's status update.",
    )
    review_status.update(status="NO VERDICT", status_requirements_met=False)


def _load_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def main() -> None:
    """Read the run's records and write the advisory status JSON and full Check Run report.

    In PR comment modes, update the main AI review comment. Write --output, add status data to
    the execution JSON and rebuild --check-output from the counted findings and their output
    URLs. These findings include earlier final Blockers for the same commit that the current
    orchestrator report omits.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("coverage", "completion", "publication", "history", "metadata", "review",
                 "output", "check-output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("head-sha", "run-id", "repository", "run-url", "mode"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--pr-number", type=int, required=True)
    args = parser.parse_args()
    metadata = _load_json(args.metadata)
    metadata = metadata if isinstance(metadata, dict) else {}
    labels = metadata.get("labels") or []
    breaking = bool(re.match(r"^[a-z]+(?:\([^)]+\))?!:", metadata.get("title", ""))) or any(
        isinstance(label, dict) and label.get("name") == "breaking-change" for label in labels
    )
    coverage, completion, publication = (
        _load_json(args.coverage), _load_json(args.completion), _load_json(args.publication)
    )
    review_status = compute_review_status(
        head_sha=args.head_sha, run_id=args.run_id, coverage=coverage, completion=completion,
        publication=publication, history=_load_json(args.history), breaking_change=breaking,
    )
    review = args.review.read_text() if args.review.is_file() else "Review text is unavailable."
    publish_review_status(
        github=GitHub(), repository=args.repository, pr_number=args.pr_number,
        publication=publication, review=review,
        run_url=args.run_url, review_status=review_status, coverage=coverage, mode=args.mode,
    )
    args.output.write_text(json.dumps(review_status, indent=2) + "\n")
    if isinstance(completion, dict):
        completion["review_status"] = {
            key: value for key, value in review_status.items() if key != "findings"
        }
        completion["review_status"]["findings"] = [
            {key: finding.get(key) for key in (
                "id", "severity", "head_sha", "url", "placement", "delivery",
                "source_finding_id", "source_review_id", "source_comment_id",
            )}
            for finding in review_status["findings"]
        ]
        args.completion.write_text(json.dumps(completion, indent=2) + "\n")
    errors = publication.get("errors") if isinstance(publication, dict) else None
    check_record = {
        "findings": review_status["findings"],
        "mode": publication.get("mode") if isinstance(publication, dict) else None,
        "errors": [error for error in errors if isinstance(error, str)]
        if isinstance(errors, list)
        else ["The final finding output record is unavailable."],
        "coverage": coverage,
    }
    try:
        full_review = publication_check(review, check_record)
    except (KeyError, TypeError, ValueError):
        check_record["coverage"] = None
        full_review = publication_check(review, check_record)
    if isinstance(completion, dict):
        full_review += "\n\n" + execution_summary(completion)
    args.check_output.write_text(format_status_check(review_status) + "\n\n---\n\n" + full_review)


if __name__ == "__main__":
    main()
