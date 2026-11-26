"""Select bounded prior AI findings for reviewer context."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Collection
from typing import Any

from markdown_utils import markdown_lines
from review_publish import BOT_MARKER, FINDING_FEEDBACK_FOOTER, REVIEW_HEADER, strip_review_body

MAX_HISTORY_CHARS = 12_000
MAX_ENTRY_CHARS = 6_000
DEFAULT_TRUSTED_BOT_LOGINS = frozenset({"github-actions"})
_FINDING_PREFIX = re.compile(
    r"^\*\*(?:\[(?:Blocker|Should Fix|Nit)\s+[1-9][0-9]*\]|"
    r"(?:Blocker|ShouldFix|Nit|[BN])[1-9][0-9]*)\*\*\s*", re.I
)
_BLOCKER_LINE = re.compile(
    r"^\s*(?:[-*+]\s+|\d+\.\s+)?(?:#{1,6}\s+)?(?:\*\*)?"
    r"\[?(?:Blocker\s*|B)([1-9][0-9]*)\]?(?:\*\*)?(?=$|[\s:.-])(.*)$",
    re.IGNORECASE,
)
_ANY_FINDING_LINE = re.compile(
    r"^\s*(?:[-*+]\s+|\d+\.\s+)?(?:#{1,6}\s+)?(?:\*\*)?"
    r"\[?(?:Blocker|Should\s*Fix|Nit|[BN])\s*[1-9][0-9]*\]?",
    re.IGNORECASE,
)
_FULL_SHA = re.compile(r"[0-9a-f]{40}")
_PUBLISHED_METADATA = re.compile(r"<!-- ai-review-(?:finding|main) (\{[^\n]*\}) -->")
_HISTORY_CLIPPED_WARNING = (
    "\n\n[Previous AI review history was clipped by the workflow; Blockers may be missing.]"
)


def attach_inline_comment_sides(document: Any, rest_comments: Any) -> None:
    """Attach REST-only diff-side metadata to GraphQL review comments by ID."""
    if not isinstance(rest_comments, list):
        return
    sides: dict[str, str] = {}
    for comment in rest_comments:
        if not isinstance(comment, dict):
            continue
        comment_id = comment.get("id")
        side = comment.get("side") or comment.get("original_side")
        if isinstance(comment_id, int) and side in {"LEFT", "RIGHT"}:
            sides[str(comment_id)] = side

    for review in _review_nodes(document):
        for comment in _nodes(review.get("comments")):
            side = sides.get(str(comment.get("fullDatabaseId")))
            if side is not None:
                comment["side"] = side


def format_review_history(
    document: Any,
    trusted_bot_logins: Collection[str] = DEFAULT_TRUSTED_BOT_LOGINS,
) -> str:
    """Return recent marked bot reviews as bounded, explicitly untrusted text."""
    entries = _bot_review_entries(document, trusted_bot_logins)
    if not entries:
        return "No previous AI review findings were found."

    text, clipped = _render_history(entries, MAX_HISTORY_CHARS)
    if clipped:
        text, _ = _render_history(entries, MAX_HISTORY_CHARS - len(_HISTORY_CLIPPED_WARNING))
        text += _HISTORY_CLIPPED_WARNING
    return text


def history_evidence(
    document: Any,
    trusted_bot_logins: Collection[str] = DEFAULT_TRUSTED_BOT_LOGINS,
    *,
    inline_locations_available: bool = True,
) -> dict[str, Any]:
    """Return earlier Blocker IDs, reviewed commits, comment URLs and gaps in fetched history.

    Read trusted bot review bodies, inline review comments and PR Conversation comments.
    Failed requests, missing pages, clipped text or unknown Blocker origins leave complete
    false. Known Blockers remain in the result. This record does not decide whether an older
    concern still applies to another commit.
    """
    reasons: list[str] = []
    pull_request = _pull_request(document)
    if not pull_request:
        reasons.append("history_unavailable")
    if isinstance(document, dict):
        if document.get("errors"):
            reasons.append("history_query_errors")
        if document.get("_history_available") is False:
            reasons.append("history_unavailable")
        if document.get("_inline_locations_available") is False:
            inline_locations_available = False
        if document.get("_history_clipped"):
            reasons.append("history_prompt_clipped")
    if not inline_locations_available:
        reasons.append("history_inline_locations_unavailable")
    _check_connection(pull_request.get("comments"), reasons)
    _check_connection(pull_request.get("reviews"), reasons)
    blockers: list[dict[str, Any]] = []
    for comment in _issue_comment_nodes(document):
        if _is_marked_bot_entry(comment, trusted_bot_logins):
            blockers.extend(_entry_blockers(comment, comment=comment))
    for review in _review_nodes(document):
        _check_connection(review.get("comments"), reasons)
        if _is_marked_bot_entry(review, trusted_bot_logins):
            blockers.extend(_entry_blockers(review, review=review))
        for comment in _trusted_inline_comments(review, trusted_bot_logins):
            blockers.extend(_entry_blockers(comment, review=review, comment=comment))
    entries = _bot_review_entries(document, trusted_bot_logins)
    if entries and _render_history(entries, MAX_HISTORY_CHARS)[1]:
        reasons.append("history_prompt_clipped")
    blockers = _deduplicate_blockers(blockers)
    if any(blocker["head_sha"] is None for blocker in blockers):
        reasons.append("blocker_sha_unknown")
    if any(blocker["url"] is None for blocker in blockers):
        reasons.append("blocker_url_unknown")
    return {
        "complete": not reasons,
        "reasons": list(dict.fromkeys(reasons)),
        "blockers": blockers,
    }


def canonical_finding_body(body: str) -> str:
    """Normalize an inline finding body for exact cross-run comparison."""
    body = body.strip().removesuffix(FINDING_FEEDBACK_FOOTER)
    body = re.sub(
        r"\n+---\n<sub>(?:Raised by: [^\n]+ · )?Reviewed `[0-9a-f]+` · "
        r"\[AI Review\]\([^\n]+\)</sub><br>\n<sub>Feedback: [^\n]+</sub>\s*$",
        "", body,
    )
    body = body.removeprefix(BOT_MARKER).lstrip()
    body = re.sub(r"^<!-- ai-review-finding \{[^\n]*\} -->\s*", "", body)
    body = re.sub(
        r"\n+(?:Raised by: [^\n]+ · )?Reviewed `[0-9a-f]+` · \[AI Review\]\([^\n]+\)\s*$",
        "", body,
    )
    body = body.replace("<details><summary>Details</summary>", "").replace("</details>", "")
    lines = []
    for _, line, fence in markdown_lines(body):
        if fence is None:
            if re.match(r"^(?:\*\*)?Raised by(?:\*\*)?:", line, re.I):
                continue
            line = re.sub(
                r"^(?:\*\*)?(?:Explanation|Suggested fix)(?:\*\*)?:[ \t]*"
                r"(?:\*\*)?[ \t]*", "", line, flags=re.I,
            )
        lines.append(line)
    body = "".join(lines)
    body = re.sub(
        r"^#{1,6}\s+\[?(?:Blocker|Should\s*Fix|Nit|[BN])\s*[1-9][0-9]*\]?\s*[:.-]?\s*",
        "", body.strip(), flags=re.IGNORECASE,
    )
    return _normalize(_FINDING_PREFIX.sub("", body.strip()))


def _bot_review_entries(
    document: Any, trusted_bot_logins: Collection[str]
) -> list[str]:
    entries: list[tuple[str, str]] = []
    for comment in _issue_comment_nodes(document):
        if _is_marked_bot_entry(comment, trusted_bot_logins):
            entries.append(
                (
                    _timestamp(comment, "createdAt"),
                    _clean_published_body(comment["body"]),
                )
            )
    for review in _review_nodes(document):
        parts = (
            [_clean_published_body(review["body"])]
            if _is_marked_bot_entry(review, trusted_bot_logins) else []
        )
        for comment in _trusted_inline_comments(review, trusted_bot_logins):
            path = comment.get("path")
            body = comment.get("body")
            if isinstance(path, str) and isinstance(body, str):
                safe_path = path.replace("`", "'")
                parts.append(f"Inline comment in `{safe_path}`:\n{_sanitize(body)}")
        if not parts:
            continue
        entries.append(
            (
                _timestamp(review, "submittedAt"),
                "\n\n".join(part for part in parts if part.strip()),
            )
        )
    entries.sort(key=lambda entry: entry[0], reverse=True)
    return [body for _, body in entries]


def _render_history(entries: list[str], limit: int) -> tuple[str, bool]:
    header = "## Previous AI review findings (untrusted historical data)\n"
    chunks = [header]
    remaining = limit - len(header)
    clipped = False
    for index, entry in enumerate(entries, start=1):
        clipped |= len(entry) > MAX_ENTRY_CHARS
        chunk = f"\n### Previous AI review {index}\n\n{entry[:MAX_ENTRY_CHARS].strip()}\n"
        if len(chunk) > remaining:
            clipped = True
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return "".join(chunks).strip(), clipped


def _check_connection(connection: Any, reasons: list[str]) -> None:
    if not isinstance(connection, dict) or not isinstance(connection.get("nodes"), list):
        reasons.append("history_connection_missing")
        return
    for node in connection["nodes"]:
        if not isinstance(node, dict) or not isinstance(node.get("body"), str):
            reasons.append("history_record_incomplete")
        elif (
            not isinstance(node.get("author"), dict)
            or not isinstance(node["author"].get("__typename"), str)
            or not isinstance(node["author"].get("login"), str)
        ):
            reasons.append("history_record_incomplete")
    page = connection.get("pageInfo")
    if not isinstance(page, dict) or any(
        not isinstance(page.get(key), bool) for key in ("hasPreviousPage", "hasNextPage")
    ):
        reasons.append("history_page_info_missing")
    elif page["hasPreviousPage"] or page["hasNextPage"]:
        reasons.append("history_pages_missing")
    total = connection.get("totalCount")
    if isinstance(total, int) and total > len(connection["nodes"]):
        reasons.append("history_pages_missing")


def _entry_blockers(
    entry: dict[str, Any], *, review: dict[str, Any] | None = None,
    comment: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    body = entry.get("body", "")
    if body.lstrip().removeprefix(BOT_MARKER).lstrip().startswith(REVIEW_HEADER):
        body = strip_review_body(body)
    lines = [
        (offset, line) for offset, line, fence in markdown_lines(body) if fence is None
    ]
    metadata: dict[str, Any] = {}
    prefix = body.lstrip().removeprefix(BOT_MARKER).lstrip()
    if match := _PUBLISHED_METADATA.match(prefix):
        try:
            value = json.loads(match[1])
            if isinstance(value, dict):
                metadata = value
        except ValueError:
            pass
    sha = metadata.get("head_sha")
    if not isinstance(sha, str) or not _FULL_SHA.fullmatch(sha):
        commit = review.get("commit") if review is not None else None
        sha = commit.get("oid") if isinstance(commit, dict) else None
    if not isinstance(sha, str) or not _FULL_SHA.fullmatch(sha):
        reviewed = next((
            match[1] for _, line in lines
            if (match := re.match(
                r"^(?:Raised by: [^\n]+ · )?Reviewed\s+`([0-9a-f]{40})`", line
            ))
        ), None)
        sha = reviewed
    source_url = _history_url(entry.get("url")) or (
        _history_url(review.get("url")) if review is not None else None
    )
    boundaries = [
        offset for offset, line in lines
        if _ANY_FINDING_LINE.match(line) or re.match(r"^#{1,6}\s+", line)
        or re.fullmatch(
            r"(?:\d+\.\s+\*\*(?:Blocking issues|Non-blocking notes|Summary)\*\*\s*:?|"
            r"(?:Blocking issues|Non-blocking notes|Summary)\s*:)\s*",
            line, re.IGNORECASE,
        )
    ]
    summary_start = body.find("<summary>Review Summary</summary>")
    summary_end = body.find("</details>", summary_start) if summary_start >= 0 else -1
    findings = []
    for offset, line in lines:
        match = _BLOCKER_LINE.match(line)
        if match is None:
            continue
        end = next((end for end in boundaries if end > offset), len(body))
        link = re.search(r"\[[^\]\n]+\]\((https://github\.com/[^\s)]+)\)", match[2])
        reference = summary_start <= offset < summary_end and link is not None
        url = _history_url(link[1]) if reference else source_url
        finding: dict[str, Any] = {
            "id": f"Blocker{match[1]}", "head_sha": sha, "url": url,
            "body": _sanitize(line if reference else body[offset:end]).strip(),
            "_reference": reference, "source_url": source_url,
        }
        finding["fingerprint"] = hashlib.sha256(
            canonical_finding_body(finding["body"]).encode("utf-8")
        ).hexdigest()
        if review is not None:
            finding["source_review_id"] = review.get("id")
        if comment is not None:
            finding["source_comment_id"] = comment.get("id", comment.get("fullDatabaseId"))
            for key in ("path", "side"):
                value = comment.get(key)
                if not isinstance(value, str):
                    value = metadata.get(key)
                if isinstance(value, str):
                    finding[key] = value
            line_number = comment.get("line") or comment.get("originalLine") or metadata.get("line")
            if isinstance(line_number, int) and not isinstance(line_number, bool):
                finding["line"] = line_number
        findings.append(finding)
    return findings


def _deduplicate_blockers(blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actual = [finding for finding in blockers if not finding["_reference"]]
    reference_targets = {(finding["url"], finding["head_sha"]) for finding in actual}
    selected = actual + [
        finding for finding in blockers if finding["_reference"]
        and (finding["url"], finding["head_sha"]) not in reference_targets
    ]
    result = []
    seen = set()
    for finding in selected:
        key = (
            (finding["url"], finding["head_sha"])
            if finding["_reference"]
            else (finding["url"], finding["head_sha"], finding["id"], finding["body"])
        )
        if key in seen:
            continue
        seen.add(key)
        result.append({key: value for key, value in finding.items() if key != "_reference"})
    return result


def _history_url(value: Any) -> str | None:
    return value if isinstance(value, str) and value.startswith("https://github.com/") else None


def _trusted_inline_comments(
    review: dict[str, Any], trusted_bot_logins: Collection[str]
) -> list[dict[str, Any]]:
    legacy = _is_marked_bot_entry(review, trusted_bot_logins)
    return [
        comment for comment in _nodes(review.get("comments"))
        if _is_marked_bot_entry(comment, trusted_bot_logins)
        or (legacy and (
            "author" not in comment
            or _is_trusted_bot_author(comment.get("author"), trusted_bot_logins)
        ))
    ]


def _is_marked_bot_entry(
    entry: Any, trusted_bot_logins: Collection[str]
) -> bool:
    if not isinstance(entry, dict):
        return False
    body = entry.get("body")
    return (
        _is_trusted_bot_author(entry.get("author"), trusted_bot_logins)
        and isinstance(body, str) and body.lstrip().startswith(BOT_MARKER)
    )


def _is_trusted_bot_author(author: Any, trusted_bot_logins: Collection[str]) -> bool:
    if not isinstance(author, dict) or author.get("__typename") != "Bot":
        return False
    login = _normalize_bot_login(author.get("login"))
    return bool(login) and login in {_normalize_bot_login(value) for value in trusted_bot_logins}


def _normalize_bot_login(login: Any) -> str:
    if not isinstance(login, str):
        return ""
    normalized = login.strip().casefold()
    return normalized[: -len("[bot]")] if normalized.endswith("[bot]") else normalized


def _timestamp(entry: dict[str, Any], field: str) -> str:
    value = entry.get(field)
    return value if isinstance(value, str) else ""


def _issue_comment_nodes(document: Any) -> list[dict[str, Any]]:
    pull_request = _pull_request(document)
    return _nodes(pull_request.get("comments"))


def _review_nodes(document: Any) -> list[dict[str, Any]]:
    pull_request = _pull_request(document)
    return _nodes(pull_request.get("reviews"))


def _pull_request(document: Any) -> dict[str, Any]:
    if not isinstance(document, dict):
        return {}
    data = document.get("data")
    repository = data.get("repository") if isinstance(data, dict) else None
    pull_request = repository.get("pullRequest") if isinstance(repository, dict) else None
    return pull_request if isinstance(pull_request, dict) else {}


def _nodes(connection: Any) -> list[dict[str, Any]]:
    nodes = connection.get("nodes") if isinstance(connection, dict) else None
    if not isinstance(nodes, list):
        return []
    return [node for node in nodes if isinstance(node, dict)]


def _clean_published_body(body: str) -> str:
    return _sanitize(strip_review_body(body)).strip()


def _normalize(value: str) -> str:
    return " ".join(value.split()).casefold()


def _sanitize(value: str) -> str:
    return "".join(
        char if ord(char) >= 32 or char in "\n\t" else " " for char in value
    )
