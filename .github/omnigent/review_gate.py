"""Classify and recover SHA-bound AI review gate state."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, NamedTuple


GATE_NAME = "AI Review Gate"
EVIDENCE_NAME = "AI Review Evidence"
TRUSTED_CHECK_APPS = frozenset({"github-actions"})
_BLOCKER_HEADING = re.compile(
    r"^ {0,3}(?:"
    r"###\s+Blocker[1-9][0-9]*(?=\s|:|$)"
    r"|\*\*Blocker[1-9][0-9]*\*\*(?=\s|:|$)"
    r"|[-*+]\s+\*\*Blocker[1-9][0-9]*\*\*(?=\s|:|$)"
    r")",
    re.IGNORECASE | re.MULTILINE,
)
_BLOCKER_SIGNAL = re.compile(
    r"^(?:"
    r"#{2,6}\s*(?:Blockers?\b|Blocker(?=\s|[0-9])|Blocking issues\b)"
    r"|(?:[1-9][0-9]*\.\s+)?\*\*"
    r"(?:Blockers?\b|Blocker(?=\s|[0-9])|Blocking issues\b)"
    r")",
    re.IGNORECASE | re.MULTILINE,
)
_STATE_MARKER = re.compile(
    r"<!-- ai-review-gate:state=(blockers|clean|unavailable);"
    r"override=(active|inactive)(?:;request=(?P<request>[1-9][0-9]*))? -->"
)
_EVIDENCE_MARKER = re.compile(
    r"<!-- ai-review-evidence:state=(blockers|clean)"
    r"(?:;request=(?P<request>[1-9][0-9]*))? -->"
)
_OVERRIDE_MARKER = re.compile(
    r"<!-- ai-review-override:state=(active|inactive);event-id=([1-9][0-9]*) -->"
)
_REVIEW_REQUEST_MARKER = re.compile(
    r"<!-- ai-review-request:state=(?P<state>pending|answered|superseded|failed)"
    r"(?:;fresh=(?P<fresh>blockers|clean|unavailable))? -->"
)


class GateRecord(NamedTuple):
    """Underlying review state and its explicit maintainer override."""

    state: str
    override: str


class GatePublication(NamedTuple):
    """GitHub conclusion and human-readable gate output."""

    conclusion: str
    title: str
    message: str


def review_state(review: str, findings: Any = None) -> str:
    """Classify validated human and inline findings, failing closed on format drift."""
    human_blocker = _BLOCKER_HEADING.search(review) is not None
    machine_blocker = False
    malformed_machine_blocker = False
    if isinstance(findings, list):
        for finding in findings:
            finding_id = finding.get("id") if isinstance(finding, dict) else None
            if not isinstance(finding_id, str):
                continue
            if re.fullmatch(r"Blocker[1-9][0-9]*", finding_id):
                machine_blocker = True
            elif finding_id.lower().startswith("blocker"):
                malformed_machine_blocker = True

    if human_blocker or machine_blocker:
        return "blockers"
    if malformed_machine_blocker or _BLOCKER_SIGNAL.search(review):
        return "unavailable"
    return "clean"


def gate_record(check: Any) -> GateRecord | None:
    """Return the record stored by a trusted AI Review Gate check."""
    if not isinstance(check, dict) or check.get("name") != GATE_NAME:
        return None
    app = check.get("app")
    if not isinstance(app, dict) or app.get("slug") not in TRUSTED_CHECK_APPS:
        return None
    output = check.get("output")
    if not isinstance(output, dict) or not isinstance(output.get("summary"), str):
        return None
    marker = _STATE_MARKER.search(output["summary"])
    return GateRecord(marker.group(1), marker.group(2)) if marker else None


def latest_gate_record(document: Any) -> GateRecord | None:
    """Return the record from the newest trusted gate in a check-runs response."""
    entry = latest_gate_entry(document)
    return entry[1] if entry else None


def latest_gate_entry(document: Any) -> tuple[int, GateRecord] | None:
    """Return the id and record from the newest trusted gate check."""
    if not isinstance(document, dict) or not isinstance(document.get("check_runs"), list):
        return None
    trusted = []
    for check in document["check_runs"]:
        if not isinstance(check, dict) or not isinstance(check.get("id"), int):
            continue
        record = gate_record(check)
        if record is not None:
            trusted.append((check["id"], record))
    return max(trusted, default=None, key=lambda item: item[0])


def latest_override_request(document: Any) -> str | None:
    """Return the newest trusted exact-SHA override request."""
    if not isinstance(document, dict) or not isinstance(document.get("check_runs"), list):
        return None
    trusted = []
    for check in document["check_runs"]:
        if not isinstance(check, dict) or check.get("name") != GATE_NAME:
            continue
        if not isinstance(check.get("id"), int):
            continue
        app = check.get("app")
        output = check.get("output")
        if not isinstance(app, dict) or app.get("slug") not in TRUSTED_CHECK_APPS:
            continue
        if not isinstance(output, dict) or not isinstance(output.get("summary"), str):
            continue
        marker = _OVERRIDE_MARKER.search(output["summary"])
        if marker:
            trusted.append((int(marker.group(2)), check["id"], marker.group(1)))
    return max(trusted, default=(-1, -1, None), key=lambda item: item[:2])[2]


def pending_review_request_ids(document: Any) -> frozenset[int]:
    """Return ids of trusted placeholders that still carry a pending marker."""
    if not isinstance(document, dict) or not isinstance(document.get("check_runs"), list):
        return frozenset()
    trusted = []
    for check in document["check_runs"]:
        if not isinstance(check, dict) or check.get("name") != GATE_NAME:
            continue
        if not isinstance(check.get("id"), int):
            continue
        app = check.get("app")
        output = check.get("output")
        if not isinstance(app, dict) or app.get("slug") not in TRUSTED_CHECK_APPS:
            continue
        if not isinstance(output, dict) or not isinstance(output.get("summary"), str):
            continue
        marker = _REVIEW_REQUEST_MARKER.search(output["summary"])
        if marker and marker.group("state") == "pending":
            trusted.append(check["id"])
    return frozenset(trusted)


def failed_review_request_entry(document: Any) -> tuple[int, str] | None:
    """Return the relevant trusted failed request id and its classified state."""
    if not isinstance(document, dict) or not isinstance(document.get("check_runs"), list):
        return None
    trusted = []
    for check in document["check_runs"]:
        if not isinstance(check, dict) or check.get("name") != GATE_NAME:
            continue
        if not isinstance(check.get("id"), int):
            continue
        app = check.get("app")
        output = check.get("output")
        if not isinstance(app, dict) or app.get("slug") not in TRUSTED_CHECK_APPS:
            continue
        if not isinstance(output, dict) or not isinstance(output.get("summary"), str):
            continue
        marker = _REVIEW_REQUEST_MARKER.search(output["summary"])
        if marker and marker.group("state") == "failed":
            trusted.append((check["id"], marker.group("fresh") or "unavailable"))
    if any(state == "blockers" for _, state in trusted):
        return max(
            (entry for entry in trusted if entry[1] == "blockers"),
            key=lambda item: item[0],
        )
    return max(trusted, default=None, key=lambda item: item[0])


def review_evidence_state(document: Any) -> str | None:
    """Return the sticky state represented by trusted exact-SHA review evidence."""
    entry = review_evidence_entry(document)
    return entry[1] if entry else None


def review_evidence_entry(document: Any) -> tuple[int, str] | None:
    """Return the relevant trusted exact-SHA review evidence id and state."""
    if not isinstance(document, dict) or not isinstance(document.get("check_runs"), list):
        return None
    trusted = []
    for check in document["check_runs"]:
        if not isinstance(check, dict) or check.get("name") != EVIDENCE_NAME:
            continue
        if not isinstance(check.get("id"), int):
            continue
        app = check.get("app")
        output = check.get("output")
        if not isinstance(app, dict) or app.get("slug") not in TRUSTED_CHECK_APPS:
            continue
        if not isinstance(output, dict) or not isinstance(output.get("summary"), str):
            continue
        marker = _EVIDENCE_MARKER.search(output["summary"])
        if marker:
            trusted.append((check["id"], marker.group(1)))
    if any(state == "blockers" for _, state in trusted):
        return max(
            (entry for entry in trusted if entry[1] == "blockers"),
            key=lambda item: item[0],
        )
    return max(trusted, default=None, key=lambda item: item[0])


def reconcile_evidence(
    record: GateRecord,
    evidence_state: str | None,
    *,
    gate_id: int = -1,
    evidence_id: int = -1,
) -> GateRecord:
    """Merge persisted review evidence into a previously published gate record."""
    if record.state == "blockers" or evidence_state == "blockers":
        return GateRecord("blockers", record.override)
    if (
        record.state == "unavailable"
        and evidence_state == "clean"
        and evidence_id > gate_id
    ):
        return GateRecord("clean", record.override)
    return record


def recover_record(gates: Any, evidence: Any = None) -> GateRecord:
    """Recover ordered gate, evidence, override, and pending-review state."""
    gate_entry = latest_gate_entry(gates)
    gate_id, record = gate_entry or (-1, GateRecord("unavailable", "inactive"))
    override = latest_override_request(gates)
    if override:
        record = GateRecord(record.state, override)
    evidence_entry = review_evidence_entry(evidence)
    evidence_id, evidence_state = evidence_entry or (-1, None)
    record = reconcile_evidence(
        record,
        evidence_state,
        gate_id=gate_id,
        evidence_id=evidence_id,
    )
    failed_entry = failed_review_request_entry(gates)
    failed_id, failed_state = failed_entry or (-1, "unavailable")
    if failed_state == "blockers":
        record = GateRecord("blockers", record.override)
    elif failed_id > gate_id and record.state != "blockers":
        record = GateRecord("unavailable", record.override)
    if record.state != "blockers" and unanswered_review_requests(gates, evidence):
        record = GateRecord("unavailable", record.override)
    return record


def answered_review_requests(gates: Any, evidence: Any = None) -> frozenset[int]:
    """Return review placeholder ids named by trusted gate and evidence records."""
    answered = set()
    for document, name, pattern in (
        (gates, GATE_NAME, _STATE_MARKER),
        (evidence, EVIDENCE_NAME, _EVIDENCE_MARKER),
    ):
        if not isinstance(document, dict) or not isinstance(
            document.get("check_runs"), list
        ):
            continue
        for check in document["check_runs"]:
            if not isinstance(check, dict) or check.get("name") != name:
                continue
            app = check.get("app")
            output = check.get("output")
            if not isinstance(app, dict) or app.get("slug") not in TRUSTED_CHECK_APPS:
                continue
            if not isinstance(output, dict) or not isinstance(output.get("summary"), str):
                continue
            marker = pattern.search(output["summary"])
            request_id = marker.groupdict().get("request") if marker else None
            if request_id:
                answered.add(int(request_id))
    return frozenset(answered)


def unanswered_review_requests(
    gates: Any,
    evidence: Any = None,
    *,
    ignored_answer_request_ids: frozenset[int] = frozenset(),
) -> frozenset[int]:
    """Return pending review placeholder ids with no trusted result."""
    answered = answered_review_requests(gates, evidence) - ignored_answer_request_ids
    return pending_review_request_ids(gates) - answered


def latest_label_actor(document: Any, label: str) -> str | None:
    """Return the latest actor that applied a label from a complete paginated response."""
    event = latest_label_event(document, label, "labeled")
    return event[1] if event else None


def latest_label_event(document: Any, label: str, event_name: str) -> tuple[int, str] | None:
    """Return the id and actor for the newest matching event in complete pages."""
    if event_name not in {"labeled", "unlabeled"} or not isinstance(document, list):
        return None
    candidates = []
    for page in document:
        if not isinstance(page, list):
            return None
        for event in page:
            if not isinstance(event, dict) or event.get("event") != event_name:
                continue
            if not isinstance(event.get("id"), int):
                continue
            event_label = event.get("label")
            actor = event.get("actor")
            if not isinstance(event_label, dict) or event_label.get("name") != label:
                continue
            if not isinstance(actor, dict) or not isinstance(actor.get("login"), str):
                continue
            candidates.append((event["id"], actor["login"]))
    return max(candidates, default=None, key=lambda item: item[0])


def resolve_gate_record(
    action: str,
    previous: GateRecord,
    *,
    fresh_state: str = "unavailable",
    review_succeeded: bool = False,
    head_matches: bool = False,
    other_request_pending: bool = False,
) -> GateRecord:
    """Resolve underlying state and requested override for one workflow action."""
    if action in {"bypass", "restore"}:
        # recover_record already folded the newest persisted override into previous.
        return previous
    if action != "review":
        raise ValueError(f"unknown gate action: {action}")

    if previous.state == "blockers" or (fresh_state == "blockers" and head_matches):
        state = "blockers"
    elif other_request_pending:
        state = "unavailable"
    else:
        state = fresh_state if review_succeeded and head_matches else "unavailable"
    return GateRecord(state, previous.override)


def publication(state: str, override: str) -> GatePublication:
    """Return the GitHub conclusion and text for a resolved gate record."""
    if state == "clean":
        return GatePublication(
            "success",
            "No AI blockers found",
            "The AI review found no blocking issues for this commit.",
        )
    if state == "blockers" and override == "active":
        return GatePublication(
            "success",
            "AI blockers bypassed",
            "An authorized maintainer explicitly bypassed the AI blockers for this commit.",
        )
    if state == "blockers":
        return GatePublication(
            "failure",
            "AI blockers found",
            "The AI review found blockers for this commit. Resolve them or add ai-review-bypass.",
        )
    return GatePublication(
        "failure",
        "AI review unavailable",
        "No trusted AI review result exists for this commit. Comment /review inline to run "
        "the review.",
    )


def _main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    classify = subparsers.add_parser("classify")
    classify.add_argument("review", type=Path)
    classify.add_argument("--findings", type=Path)
    recover = subparsers.add_parser("recover")
    recover.add_argument("check_runs", type=Path)
    recover.add_argument("--evidence", type=Path)
    pending = subparsers.add_parser("pending")
    pending.add_argument("check_runs", type=Path)
    pending.add_argument("--evidence", type=Path)
    pending.add_argument("--answering-request", type=int)
    pending.add_argument("--ignore-answer-for", type=int)
    resolve = subparsers.add_parser("resolve")
    resolve.add_argument("--action", choices=("review", "bypass", "restore"), required=True)
    resolve.add_argument("--previous-state", required=True)
    resolve.add_argument("--previous-override", required=True)
    resolve.add_argument("--fresh-state", default="unavailable")
    resolve.add_argument("--review-succeeded", action="store_true")
    resolve.add_argument("--head-matches", action="store_true")
    resolve.add_argument("--other-request-pending", action="store_true")
    publish = subparsers.add_parser("publish")
    publish.add_argument("--state", required=True)
    publish.add_argument("--override", required=True)
    label_actor = subparsers.add_parser("label-actor")
    label_actor.add_argument("events", type=Path)
    label_actor.add_argument("label")
    label_event = subparsers.add_parser("label-event")
    label_event.add_argument("events", type=Path)
    label_event.add_argument("label")
    label_event.add_argument("event", choices=("labeled", "unlabeled"))
    args = parser.parse_args()

    if args.command == "classify":
        findings = json.loads(args.findings.read_text()) if args.findings else None
        print(review_state(args.review.read_text(), findings))
        return
    if args.command == "recover":
        document = json.loads(args.check_runs.read_text())
        evidence = json.loads(args.evidence.read_text()) if args.evidence else None
        record = recover_record(document, evidence)
        print(record.state, record.override)
        return
    if args.command == "pending":
        document = json.loads(args.check_runs.read_text())
        evidence = json.loads(args.evidence.read_text()) if args.evidence else None
        ignored_answers = (
            frozenset({args.ignore_answer_for})
            if args.ignore_answer_for is not None
            else frozenset()
        )
        requests = set(
            unanswered_review_requests(
                document,
                evidence,
                ignored_answer_request_ids=ignored_answers,
            )
        )
        if args.answering_request is not None:
            requests.discard(args.answering_request)
        print("true" if requests else "false")
        return
    if args.command == "resolve":
        record = resolve_gate_record(
            args.action,
            GateRecord(args.previous_state, args.previous_override),
            fresh_state=args.fresh_state,
            review_succeeded=args.review_succeeded,
            head_matches=args.head_matches,
            other_request_pending=args.other_request_pending,
        )
        print(record.state, record.override)
        return
    if args.command == "label-actor":
        document = json.loads(args.events.read_text())
        print(latest_label_actor(document, args.label) or "")
        return
    if args.command == "label-event":
        document = json.loads(args.events.read_text())
        event = latest_label_event(document, args.label, args.event)
        print(*event) if event else print()
        return
    result = publication(args.state, args.override)
    print(json.dumps(result._asdict()))


if __name__ == "__main__":
    _main()
