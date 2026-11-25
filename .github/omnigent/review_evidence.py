"""Record subagent execution and trace candidate findings to final findings.

The observer appends filtered Omnigent dispatches, accepted session IDs and result-event
metadata to a runner-local JSONL file. The command combines those events with the orchestrator's
candidate-to-final mapping, checker decisions, earlier reviews and confirmed comment URLs.
It writes /tmp/review_execution.json through --output and appends tables to the Markdown
report for the AI Review Summary Check Run. Missing result events leave subagent completion
unknown.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

CHECKER_ROLE = "disprove-reviewer"
MAX_EVENTS_BYTES = 4_000_000
MAX_METADATA_ITEMS = 256
_ID = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}")
_FINAL_ID = re.compile(r"(?:Blocker|ShouldFix|Nit)[1-9][0-9]*")
_SEVERITIES = {"Blocker": 2, "Should Fix": 1, "Nit": 0}
_CHECKER_DECISIONS = {"CONFIRMED", "DISPROVED", "NITPICK", "CONTESTED"}
_TERMINAL = {"failed", "cancelled", "timed_out"}


def observe_review(
    *, path: str = "/tmp/review_observations.jsonl", run_id: str, head_sha: str,
    configured_reviewer_roles: list[str],
) -> Callable[[dict[str, Any]], dict[str, str]]:
    """Return an Omnigent policy that records dispatches, accepted sessions and result events.

    Register it after dispatch guards and before child_task.py appends shared PR task text.
    Append session IDs, event receipt times, candidate-finding IDs/severities and Checker
    decisions to path; discard Reviewer and Checker result prose. Recording failures leave
    missing evidence without rewriting tool arguments or changing whether dispatch is allowed.
    """
    allowed = set(configured_reviewer_roles) | {CHECKER_ROLE}

    def observe(event: dict[str, Any]) -> dict[str, str]:
        try:
            row = _observe(event, allowed)
            if row is not None:
                row.update(run_id=run_id, head_sha=head_sha, observed_at=_now())
                _append_event(Path(path), row)
        except (OSError, ValueError, TypeError, KeyError):
            pass
        return {"result": "ALLOW"}

    return observe


def extract_accounting(review: str, marker: str) -> tuple[str, dict[str, Any]]:
    """Remove the candidate-to-final mapping block and return human text plus its JSON.

    Each mapping relates a Reviewer subagent's candidate finding to a final finding or a reason
    for dropping it. Invalid JSON inside balanced markers returns empty metadata. Missing,
    repeated or unbalanced markers raise ValueError because the removable region is unknown.
    """
    if re.fullmatch(r"[0-9a-f]{32}", marker) is None:
        raise ValueError("invalid accounting marker")
    start = f"<!-- AI_REVIEW_ACCOUNTING_START_{marker} -->"
    end = f"<!-- AI_REVIEW_ACCOUNTING_END_{marker} -->"
    first, last = _marked_bounds(review, start, end)
    clean = (review[:first] + review[last:]).strip()
    try:
        document, _, _ = _marked_document(review, start, end)
    except ValueError:
        return clean, {}
    if (
        not isinstance(document, dict) or set(document) != {"outcomes"}
        or not isinstance(document["outcomes"], list)
    ):
        return clean, {}
    return clean, document


def accounting_prompt_instructions(marker: str) -> str:
    """Return the JSON contract for mapping each candidate finding to its final outcome."""
    if re.fullmatch(r"[0-9a-f]{32}", marker) is None:
        raise ValueError("invalid accounting marker")
    return f"""
Before the closing response marker, include exactly one consolidation accounting block:
<!-- AI_REVIEW_ACCOUNTING_START_{marker} -->
{{"outcomes":[{{"role":"docs-reviewer","id":"C1","outcome":"retained",
"final_id":"Nit1","reason":""}}]}}
<!-- AI_REVIEW_ACCOUNTING_END_{marker} -->
Map every candidate finding from the Reviewer results, including Nits and concerns that repeat
an earlier final finding. Use the Reviewer subagent name and its local candidate-finding ID.
outcome is retained, merged, downgraded or dropped.
retained, merged and downgraded name a final finding ID from the final-finding block. dropped
uses null final_id. Explain every dropped, merged or downgraded candidate finding in a short reason.
Multiple candidate findings may name one final finding.
Map only returned candidate findings and include every final finding in at least one mapping.
Return an explicit empty outcomes list when every Reviewer result has an empty
candidate-finding list.
"""


def build_execution_record(
    observations: Any, accounting: Any, publication: Any, history: Any, *, run_id: str,
    head_sha: str, configured_reviewer_roles: list[str],
) -> dict[str, Any]:
    """Return subagent attempts and consolidation outcomes for one run and reviewed commit.

    Match Reviewer and Checker results to accepted session IDs and configured roles. Every
    candidate finding needs a final-finding mapping or a reason for dropping it. Blocker and
    Should Fix candidate findings also need a decision from the single checker batch. Link final
    findings to confirmed comment URLs or explicit summary/artifact output. Preserve retries; missing
    or conflicting results leave complete false. The configured_reviewer_roles roster names every
    configured Reviewer subagent; a valid Reviewer result from each is required for a complete
    execution record. This requirement is separate from the orchestrator report's publication
    quorum.
    """
    reasons: list[str] = []
    if (not configured_reviewer_roles or len(set(configured_reviewer_roles)) != len(configured_reviewer_roles)
            or CHECKER_ROLE in configured_reviewer_roles):
        reasons.append("The required reviewer roster is missing or malformed.")
    if not run_id or not re.fullmatch(r"[0-9a-f]{40}", head_sha):
        reasons.append("The workflow run or head identity is invalid.")
    attempts = _build_attempts(observations, run_id, head_sha, configured_reviewer_roles, reasons)
    for attempt in attempts:
        if attempt["outcome"] == "unobserved":
            reasons.append(
                f"Subagent {attempt['role']} attempt {attempt['attempt']} "
                "has no result event recorded by the runtime observer."
            )
    candidates: dict[tuple[str, str], dict[str, Any]] = {}
    for role in configured_reviewer_roles:
        returned = [a for a in attempts if a["role"] == role and a["outcome"] == "returned"]
        valid = [a for a in returned if a.get("metadata_valid")]
        if not valid:
            reasons.append(
                f"No valid Reviewer result from {role} was recorded by the runtime observer."
            )
        selected = valid or returned[-1:]
        candidate_sets = {
            frozenset((item["id"], item["severity"]) for item in attempt.get("candidates", []))
            for attempt in valid
        }
        if len(candidate_sets) > 1:
            reasons.append(
                f"Reviewer {role} returned conflicting candidate-finding lists across attempts."
            )
        for attempt in selected:
            for candidate in attempt.get("candidates", []):
                key = role, candidate["id"]
                if previous := candidates.get(key):
                    previous["attempts"].append(attempt["attempt"])
                    if previous["severity"] != candidate["severity"]:
                        reasons.append(
                            f"Candidate finding {role}/{candidate['id']} "
                            "has conflicting severities."
                        )
                        if _SEVERITIES[candidate["severity"]] > _SEVERITIES[previous["severity"]]:
                            previous["severity"] = candidate["severity"]
                    continue
                candidates[key] = {
                    "role": role, **candidate, "attempt": attempt["attempt"],
                    "attempts": [attempt["attempt"]],
                    "checker": None, "outcome": None, "final_id": None, "url": None,
                }
    candidates_to_check = {
        key for key, value in candidates.items() if value["severity"] != "Nit"
    }
    _validate_checker_decisions(attempts, candidates, candidates_to_check, reasons)

    publication = publication if isinstance(publication, dict) else {}
    expected_run_url = f"/actions/runs/{run_id}"
    if (
        publication.get("head_sha") != head_sha
        or publication.get("run_id", run_id) != run_id
        or not str(publication.get("run_url", "")).endswith(expected_run_url)
    ):
        reasons.append("Publication run or head identity is missing or mismatched.")
        publication = {}
    if publication.get("complete") is not True:
        reasons.append("Finding publication is incomplete.")
    finals, aliases = _publication_findings(publication, head_sha, reasons)
    outcomes = accounting.get("outcomes") if isinstance(accounting, dict) else None
    if not isinstance(outcomes, list) or len(outcomes) > MAX_METADATA_ITEMS * len(configured_reviewer_roles):
        reasons.append("The orchestrator's candidate-to-final mapping is missing or malformed.")
        outcomes = []
    accounted: set[tuple[str, str]] = set()
    used_finals: set[str] = set()
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            reasons.append("Consolidation contains a malformed candidate-finding outcome.")
            continue
        key = outcome.get("role"), outcome.get("id")
        if not all(isinstance(part, str) for part in key) or key not in candidates:
            reasons.append("Consolidation names an unknown candidate finding.")
            continue
        if key in accounted:
            reasons.append(f"Candidate finding {key[0]}/{key[1]} has duplicate outcomes.")
            continue
        accounted.add(key)
        candidate = candidates[key]
        state = outcome.get("outcome")
        reason = _text(outcome.get("reason"), 500)
        candidate.update(outcome=state, reason=reason)
        if not isinstance(state, str) or state not in {
            "retained", "merged", "downgraded", "dropped",
        }:
            reasons.append(f"Candidate finding {key[0]}/{key[1]} has an invalid outcome.")
            continue
        if state != "retained" and not reason:
            reasons.append(f"Candidate finding {key[0]}/{key[1]} has no omission or change reason.")
        if state == "dropped":
            if outcome.get("final_id") is not None:
                reasons.append(
                    f"Dropped candidate finding {key[0]}/{key[1]} names a final finding."
                )
            continue
        requested_id = outcome.get("final_id")
        final_id = aliases.get(requested_id) if isinstance(requested_id, str) else None
        if final_id is None:
            reasons.append(
                f"Candidate finding {key[0]}/{key[1]} "
                "does not map to a valid final finding with a confirmed comment "
                "or selected report output."
            )
            continue
        final = finals[final_id]
        candidate.update(
            final_id=final_id, final_severity=final["severity"], url=final["url"],
            placement=final.get("placement"), delivery=final.get("delivery"),
        )
        used_finals.add(final_id)
        checker = candidate.get("checker") or {}
        if checker.get("decision") == "DISPROVED":
            reasons.append(f"Disproved candidate finding {key[0]}/{key[1]} was retained.")
        if checker.get("decision") == "NITPICK" and final["severity"] != "Nit":
            reasons.append(
                f"Candidate finding {key[0]}/{key[1]} contradicts its Checker decision."
            )
        if checker.get("decision") == "CONTESTED" and final["severity"] == "Blocker":
            reasons.append(
                f"Candidate finding {key[0]}/{key[1]} remains a Blocker "
                "despite its CONTESTED Checker decision."
            )
        initial_rank = _SEVERITIES[candidate["severity"]]
        final_rank = _SEVERITIES[final["severity"]]
        if state == "downgraded" and final_rank >= initial_rank:
            reasons.append(f"Candidate finding {key[0]}/{key[1]} was not downgraded.")
        elif state == "retained" and final_rank != initial_rank:
            reasons.append(
                f"Candidate finding {key[0]}/{key[1]} changed severity without a matching outcome."
            )
    for role, candidate_id in candidates.keys() - accounted:
        reasons.append(f"Candidate finding {role}/{candidate_id} has no consolidation outcome.")
    if finals.keys() - used_finals:
        reasons.append("A final finding has no originating candidate finding.")
    filtered_history = _history(history, reasons)
    reasons = list(dict.fromkeys(reasons))
    return {
        "run_id": run_id, "head_sha": head_sha, "configured_reviewer_roles": configured_reviewer_roles,
        "complete": not reasons,
        "reasons": reasons, "attempts": attempts, "candidates": list(candidates.values()),
        "history": filtered_history,
    }


def execution_summary(record: dict[str, Any]) -> str:
    """Build subagent-attempt and candidate-finding tables for the AI Review Summary Check Run."""
    parts = ["## Review execution", "Complete" if record["complete"] else "Incomplete"]
    parts += [f"- {_cell(reason)}" for reason in record["reasons"]]
    parts += ["",
        "| Subagent | Attempt | Result | Dispatch received | Result event received |",
        "| --- | --- | --- | --- | --- |",
    ]
    for attempt in record["attempts"]:
        parts.append("| " + " | ".join(_cell(attempt.get(key)) for key in (
            "role", "attempt", "outcome", "dispatched_at", "returned_at",
        )) + " |")
    parts += [
        "", "| Candidate finding | Consolidation outcome | Final finding or reason |",
        "| --- | --- | --- |",
    ]
    for candidate in record["candidates"]:
        destination = candidate.get("final_id") or candidate.get("reason") or "Unaccounted"
        if candidate.get("url"):
            destination = f"[{destination}]({candidate['url']})"
        parts.append(
            f"| {_cell(candidate['role'])}/{_cell(candidate['id'])} | "
            f"{_cell(candidate.get('outcome'))} | {_cell(destination)} |"
        )
    return "\n\n".join(parts[:2]) + "\n\n" + "\n".join(parts[2:]) + "\n"


def _observe(event: dict[str, Any], allowed: set[str]) -> dict[str, Any] | None:
    data = event.get("data")
    if not isinstance(data, dict):
        return None
    phase = event.get("type")
    request = data if phase == "tool_call" else event.get("request_data")
    if not isinstance(request, dict) or request.get("name") != "sys_session_send":
        return None
    delayed = phase == "tool_result" and "arguments" not in request
    args = request.get("args") if delayed else request.get("arguments")
    if not isinstance(args, dict) or args.get("agent") not in allowed:
        return None
    role = args["agent"]
    row = {"role": role, "title": _text(args.get("title"), 200)}
    if phase == "tool_call":
        row.update(kind="dispatch", dispatch_id=uuid.uuid4().hex)
        if role == CHECKER_ROLE:
            child = args.get("args") or {}
            batch, valid = _metadata(child.get("input"), "CHECK_BATCH", "candidates", allowed)
            row.update(checker_batch=batch, metadata_valid=valid)
        return row
    if phase != "tool_result":
        return None
    if not delayed:
        handle = _json_object(data.get("result"))
        row.update(kind="accepted", session_id=_identifier(handle.get("conversation_id")))
        status = handle.get("status")
        if status in _TERMINAL:
            row["outcome"] = status
        return row
    row.update(kind="returned", session_id=_identifier(args.get("conversation_id")))
    status = data.get("status")
    row["outcome"] = status if status in _TERMINAL else "returned"
    key, marker = ("decisions", "CHECKER_DECISIONS") if role == CHECKER_ROLE else (
        "candidates", "CANDIDATES",
    )
    entries, valid = _metadata(data.get("result"), marker, key, allowed)
    row.update({key: entries, "metadata_valid": valid})
    return row


def _build_attempts(
    observations: Any, run_id: str, head_sha: str, roles: list[str], reasons: list[str],
) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    if not isinstance(observations, list):
        reasons.append("Reviewer execution observations are unavailable.")
        return attempts
    allowed = set(roles) | {CHECKER_ROLE}
    counters: Counter = Counter()
    sessions: dict[str, dict[str, Any]] = {}
    for event in observations:
        if not isinstance(event, dict) or (
            event.get("run_id") != run_id or event.get("head_sha") != head_sha
        ):
            reasons.append("An execution observation has a mismatched run or head identity.")
            continue
        role, kind = event.get("role"), event.get("kind")
        if kind == "truncated":
            reasons.append("Reviewer execution observations were truncated.")
            continue
        if role not in allowed:
            reasons.append("An execution observation names an unknown reviewer.")
            continue
        if kind == "dispatch":
            counters[role] += 1
            attempt = {
                "role": role, "attempt": counters[role], "title": event.get("title"),
                "dispatch_id": event.get("dispatch_id"), "session_id": None,
                "dispatched_at": event.get("observed_at"), "started_at": None,
                "ended_at": None, "returned_at": None, "outcome": "unobserved",
                "metadata_valid": False,
            }
            if role == CHECKER_ROLE:
                attempt.update(
                    checker_batch=event.get("checker_batch", []),
                    batch_valid=event.get("metadata_valid") is True,
                )
            attempts.append(attempt)
            continue
        if kind == "accepted":
            attempt = next((a for a in reversed(attempts) if a["role"] == role
                            and a["title"] == event.get("title") and not a["session_id"]), None)
            if attempt is None:
                reasons.append(
                    "Omnigent accepted a child session without a recorded dispatch request."
                )
                continue
            session_id = event.get("session_id")
            if not session_id:
                attempt["outcome"] = event.get("outcome", "unobserved")
                continue
            attempt["session_id"] = session_id
            if session_id in sessions:
                reasons.append("A reviewer session was reused across distinct attempts.")
            sessions[session_id] = attempt
            if event.get("outcome") in _TERMINAL:
                attempt["outcome"] = event["outcome"]
            continue
        if kind != "returned":
            reasons.append("An execution observation has an unknown event kind.")
            continue
        attempt = sessions.get(event.get("session_id"))
        if attempt is None or attempt["role"] != role:
            reasons.append("A subagent result event has no matching accepted session.")
            continue
        if attempt["returned_at"] is not None:
            reasons.append("Multiple result events were recorded for one subagent attempt.")
            continue
        attempt.update(
            returned_at=event.get("observed_at"), outcome=event.get("outcome", "returned"),
            metadata_valid=event.get("metadata_valid") is True,
        )
        key = "decisions" if role == CHECKER_ROLE else "candidates"
        attempt[key] = event.get(key, [])
    return attempts


def _validate_checker_decisions(attempts, candidates, expected_candidates, reasons):
    checker_attempts = [a for a in attempts if a["role"] == CHECKER_ROLE]
    if not expected_candidates:
        if checker_attempts:
            reasons.append("The checker ran without any proposed Blocker or Should Fix finding.")
        return
    valid_checker_attempts = [
        a for a in checker_attempts if a["outcome"] == "returned"
        and a.get("metadata_valid") and a.get("batch_valid")
    ]
    if len(valid_checker_attempts) != 1:
        reasons.append(
            "The runtime observer must record exactly one valid Checker result."
        )
        return
    checker_attempt = valid_checker_attempts[0]
    batch = checker_attempt["checker_batch"]
    candidates_by_batch_id = {item["batch_id"]: (item["role"], item["id"]) for item in batch}
    if (
        len(candidates_by_batch_id) != len(batch)
        or len(set(candidates_by_batch_id.values())) != len(batch)
        or set(candidates_by_batch_id.values()) != expected_candidates
    ):
        reasons.append("The checker batch does not cover every required candidate finding.")
        return
    checker_decisions = {
        item["batch_id"]: item["decision"] for item in checker_attempt["decisions"]
    }
    if set(checker_decisions) != set(candidates_by_batch_id):
        reasons.append("The checker result omits or adds decisions for candidate findings.")
    for batch_id, key in candidates_by_batch_id.items():
        decision = checker_decisions.get(batch_id)
        candidates[key]["checker"] = {"batch_id": batch_id, "decision": decision}


def _publication_findings(publication, head_sha, reasons):
    finals, aliases = {}, {}
    findings = publication.get("findings")
    if not isinstance(findings, list):
        reasons.append("The final finding publication list is unavailable.")
        return finals, aliases
    output_mode = publication.get("mode")
    for finding in findings:
        if not isinstance(finding, dict):
            reasons.append("A final finding publication record is malformed.")
            continue
        final_id = finding.get("id")
        severity = finding.get("severity")
        if output_mode in ("summary", "artifact"):
            output_valid = (
                finding.get("placement") == output_mode
                and finding.get("url") is None and finding.get("delivery") is None
            )
        else:
            output_valid = (
                _github_url(finding.get("url")) and finding.get("delivery") in ("new", "reused")
            )
        if (not isinstance(final_id, str) or not _FINAL_ID.fullmatch(final_id)
                or not isinstance(severity, str) or severity not in _SEVERITIES
                or finding.get("head_sha") != head_sha or not output_valid):
            reasons.append(
                "A final finding's ID, severity, reviewed commit or publication output is invalid."
            )
            continue
        if final_id in finals:
            reasons.append("A final finding has duplicate publication records.")
            continue
        finals[final_id] = {key: finding.get(key) for key in (
            "id", "severity", "url", "placement", "delivery",
        )}
        finding_aliases = finding.get("aliases", [])
        if not isinstance(finding_aliases, list):
            reasons.append("Final finding aliases are malformed or ambiguous.")
            finding_aliases = []
        for alias in [final_id, *finding_aliases]:
            if not isinstance(alias, str) or not _FINAL_ID.fullmatch(alias) or alias in aliases:
                reasons.append("Final finding aliases are malformed or ambiguous.")
            else:
                aliases[alias] = final_id
    return finals, aliases


def _history(history, reasons):
    if not isinstance(history, dict) or history.get("complete") is not True:
        reasons.append("Trusted review history is incomplete.")
    if not isinstance(history, dict):
        history = {}
    history_reasons = history.get("reasons", [])
    blockers = history.get("blockers", [])
    if not isinstance(history_reasons, list) or not isinstance(blockers, list):
        reasons.append("Trusted review history metadata is malformed.")
        history_reasons, blockers = [], []
    return {
        "complete": history.get("complete") is True,
        "reasons": [_text(reason, 500) for reason in history_reasons
                    if isinstance(reason, str)],
        "blockers": [{key: item.get(key) for key in (
            "id", "head_sha", "url", "fingerprint", "source_review_id", "source_comment_id",
        ) if key in item} for item in blockers if isinstance(item, dict)],
    }


def _metadata(text, marker, key, roles):
    try:
        document, _, _ = _marked_document(
            text, f"<!-- AI_REVIEW_{marker} -->", f"<!-- AI_REVIEW_{marker}_END -->",
        )
        entries = document[key]
        if set(document) != {key} or not isinstance(entries, list):
            return [], False
        if len(entries) > MAX_METADATA_ITEMS:
            return [], False
        parsed, seen, valid = [], set(), True
        for entry in entries:
            if not isinstance(entry, dict):
                valid = False
                continue
            identifier_key = "batch_id" if marker != "CANDIDATES" else "id"
            identifier = entry.get(identifier_key)
            good = isinstance(identifier, str) and _ID.fullmatch(identifier) is not None
            if marker == "CANDIDATES":
                good = good and entry.get("severity") in _SEVERITIES
                fields = ("id", "severity")
            elif marker == "CHECK_BATCH":
                good = good and entry.get("role") in roles and entry.get("role") != CHECKER_ROLE
                good = good and isinstance(entry.get("id"), str) and _ID.fullmatch(entry["id"])
                fields = ("batch_id", "role", "id")
            else:
                good = good and entry.get("decision") in _CHECKER_DECISIONS
                fields = ("batch_id", "decision")
            if not good or identifier in seen:
                valid = False
                continue
            seen.add(identifier)
            parsed.append({field: entry[field] for field in fields})
        return parsed, valid
    except (ValueError, TypeError, KeyError):
        return [], False


def _marked_bounds(text, start, end):
    if not isinstance(text, str) or text.count(start) != 1 or text.count(end) != 1:
        raise ValueError("metadata markers are missing or repeated")
    first = text.index(start)
    last = text.index(end, first + len(start))
    return first, last + len(end)


def _marked_document(text, start, end):
    first, last = _marked_bounds(text, start, end)
    payload = text[first + len(start):last - len(end)]
    if len(payload.encode("utf-8")) > 100_000:
        raise ValueError("metadata exceeds its byte limit")
    return json.loads(payload), first, last


def _append_event(path, row):
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        size = os.fstat(stream.fileno()).st_size
        if size > MAX_EVENTS_BYTES:
            return
        encoded = json.dumps(row, ensure_ascii=True) + "\n"
        if size + len(encoded.encode()) > MAX_EVENTS_BYTES:
            row = {key: row[key] for key in ("run_id", "head_sha", "observed_at")}
            encoded = json.dumps({**row, "kind": "truncated"}) + "\n"
        stream.write(encoded)


def _json_object(value):
    try:
        document = json.loads(value) if isinstance(value, str) else value
        return document if isinstance(document, dict) else {}
    except ValueError:
        return {}


def _identifier(value):
    valid = isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,200}", value)
    return value if valid else None


def _text(value, limit):
    return " ".join(value.split())[:limit] if isinstance(value, str) else ""


def _github_url(value):
    return isinstance(value, str) and re.fullmatch(r"https://github\.com/[^\s<>|()]+", value)


def _cell(value):
    return str(value if value is not None else "Unknown").replace("|", "\\|").replace("\n", " ")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _load(path, *, lines=False):
    try:
        if path.stat().st_size > MAX_EVENTS_BYTES + 1_000:
            return None
        text = path.read_text()
        return [json.loads(line) for line in text.splitlines() if line.strip()] if lines else (
            json.loads(text)
        )
    except (OSError, ValueError):
        return None


def main() -> None:
    """Write the subagent-attempt JSON record and append its tables to the Check Run report file.

    In CI, --output is /tmp/review_execution.json and --check-output is /tmp/check-summary.txt.
    Missing or malformed input produces an incomplete record.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("observations", "accounting", "publication", "history", "configured-reviewer-roles",
                 "output", "check-output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--head-sha", required=True)
    args = parser.parse_args()
    roles = _load(args.configured_reviewer_roles)
    if not isinstance(roles, list) or not roles or not all(isinstance(r, str) for r in roles):
        roles = []
    record = build_execution_record(
        _load(args.observations, lines=True), _load(args.accounting), _load(args.publication),
        _load(args.history), run_id=args.run_id, head_sha=args.head_sha, configured_reviewer_roles=roles,
    )
    if not roles:
        record["complete"] = False
        record["reasons"].append("The configured Reviewer subagent roster is unavailable.")
    args.output.write_text(json.dumps(record, indent=2) + "\n")
    with args.check_output.open("a") as stream:
        stream.write("\n\n" + execution_summary(record))


if __name__ == "__main__":
    main()
