"""Post final AI findings as GitHub PR comments and save confirmed comment URLs.

For PR comment modes, create a main AI review comment and post each finding as an inline
review comment or a separate PR Conversation comment. Reuse matching bot comments instead
of posting another copy. The command writes final finding IDs, severities, comment URLs and
posting errors to the JSON file named by --output, /tmp/publication_record.json in CI.
Explicit summary and artifact modes prepare report records without posting PR comments.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from inline_review import diff_positions, diff_sections
from markdown_utils import markdown_lines
from review_history import canonical_finding_body
from review_publish import BOT_MARKER, finding_label, format_finding, format_review_summary

_ID = re.compile(r"\[?(Blocker|Should\s*Fix|Nit)\s*([1-9][0-9]*)\]?", re.I)
_HEADING = re.compile(r"^#{2,4}\s+(\[?(?:Blocker|Should\s*Fix|Nit)\s*[1-9][0-9]*\]?)(.*)$", re.I)
_FINDING_LIKE_HEADING = re.compile(
    r"^#{2,4}\s+\[?(?:Blocker|Should\s*Fix|Nit)(?=$|[^A-Za-z])", re.I
)
_FINDING_MARKER = re.compile(r"<!-- ai-review-finding (\{[^\n]*\}) -->")
_MAIN_MARKER = re.compile(r"<!-- ai-review-main (\{[^\n]*\}) -->")


class PublicationError(RuntimeError):
    """A GitHub API operation failed; a timed-out comment write may still have succeeded."""

    def __init__(self, message: str, *, ambiguous: bool = True):
        super().__init__(message)
        self.ambiguous = ambiguous


class GitHub:
    """Use the workflow's authenticated gh process without inspecting its credentials."""

    def read(self, path: str) -> Any:
        """Read one GitHub API page, raising if the request fails or its JSON is unreadable."""
        return self._call("GET", path)

    def write(self, method: str, path: str, payload: dict[str, Any]) -> Any:
        """Send a GitHub API write and return its JSON response.

        A transport failure does not establish whether GitHub accepted the write.
        """
        return self._call(method, path, payload)

    def _call(self, method: str, path: str, payload: Any = None) -> Any:
        command = ["gh", "api", "--method", method, path]
        if payload is not None:
            command += ["--input", "-"]
        try:
            result = subprocess.run(
                command, input=json.dumps(payload) if payload is not None else None,
                capture_output=True, text=True, timeout=60, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise PublicationError("GitHub response was not observed") from error
        if result.returncode:
            rejected = re.search(r"HTTP (?:400|401|403|404|409|410|422|429)\b", result.stderr)
            raise PublicationError("GitHub request failed", ambiguous=not bool(rejected))
        try:
            return json.loads(result.stdout)
        except ValueError as error:
            raise PublicationError("GitHub returned an unreadable response") from error


def collect_findings(review: str, structured: Any) -> tuple[list[dict[str, Any]], list[str]]:
    """Read final findings from the orchestrator report's JSON and Markdown, including PR-wide findings.

    The structured finding body takes precedence for a matching ID. Invalid file/line coordinates
    require a Conversation comment; an invalid finding ID or body produces an explicit error.
    """
    errors: list[str] = []
    entries: dict[str, dict[str, Any]] = {}
    boundaries = _review_sections(review)
    for index, (offset, line) in enumerate(boundaries):
        match = _HEADING.match(line)
        if match is None:
            if _FINDING_LIKE_HEADING.match(line):
                errors.append(f"Invalid final finding heading: {line.strip()}.")
            continue
        finding_id, severity = _identity(match[1])
        end = boundaries[index + 1][0] if index + 1 < len(boundaries) else len(review)
        title = match[2].strip().lstrip(":.- ")
        detail = review[offset + len(line):end].strip()
        body = "\n\n".join(part for part in (title, detail) if part)
        if finding_id in entries:
            errors.append(f"Duplicate final finding ID {finding_id}.")
            continue
        entries[finding_id] = {"id": finding_id, "severity": severity, "body": body}
    if not isinstance(structured, list):
        errors.append("Final finding metadata is unavailable.")
        structured = []
    seen: set[str] = set()
    for index, entry in enumerate(structured, start=1):
        try:
            if not isinstance(entry, dict):
                raise ValueError("entry must be an object")
            finding_id, severity = _identity(entry.get("id"))
            if finding_id in seen:
                raise ValueError(f"duplicate ID {finding_id}")
            seen.add(finding_id)
            if entry.get("severity", severity) != severity:
                raise ValueError(f"severity disagrees with {finding_id}")
            body = entry.get("body")
            if not _valid_body(body):
                raise ValueError(f"invalid body for {finding_id}")
            entries[finding_id] = {
                "id": finding_id, "severity": severity, "body": body.strip(),
                "path": entry.get("path"), "line": entry.get("line"),
                "side": entry.get("side"),
            }
        except ValueError as error:
            errors.append(f"Final finding entry {index}: {error}.")
    valid = []
    fingerprints: dict[str, dict[str, Any]] = {}
    for finding in entries.values():
        if not _valid_body(finding["body"]):
            errors.append(f"Invalid body for {finding['id']}.")
            continue
        path, line, side = finding.get("path"), finding.get("line"), finding.get("side")
        if not (
            isinstance(path, str) and path and len(path) <= 500 and not path.startswith("/")
            and ".." not in Path(path).parts and "\x00" not in path
            and isinstance(line, int) and not isinstance(line, bool) and line > 0
            and isinstance(side, str) and side in {"LEFT", "RIGHT"}
        ):
            path, line, side = None, None, None
        finding.update(path=path, line=line, side=side)
        fingerprint_text = json.dumps(
            [finding["severity"], path, line, side, canonical_finding_body(finding["body"])],
            ensure_ascii=True,
        )
        finding["fingerprint"] = hashlib.sha256(fingerprint_text.encode()).hexdigest()
        if existing := fingerprints.get(finding["fingerprint"]):
            existing.setdefault("aliases", []).append(finding["id"])
            continue
        fingerprints[finding["fingerprint"]] = finding
        valid.append(finding)
    return valid, errors


def review_overview(review: str) -> str:
    """Extract the assessment summary, ignoring fenced example headings."""
    boundaries = _review_sections(review)
    for index, (offset, line) in enumerate(boundaries):
        if re.fullmatch(
            r"(?:#{1,4}\s+|\d+\.\s+\*\*)(?:Overall assessment|(?:(?:PR|Assessment) )?Summary)"
            r"(?:\*\*)?\s*:?\s*",
            line,
        ):
            end = boundaries[index + 1][0] if index + 1 < len(boundaries) else len(review)
            return review[offset + len(line):end].strip()
    end = next((offset for offset, line in boundaries if _HEADING.match(line)
                or re.search(r"Blocking issues|Non-blocking notes", line)), len(review))
    return re.sub(r"(?im)^#{1,4}\s+(?:AI Review|Summary)\s*$", "", review[:end]).strip()


def complete_review(review: str, findings: list[dict[str, Any]]) -> str:
    """Build the full Check Run review text from the assessment and final findings."""
    overview = review_overview(review)
    parts = [f"## Overall assessment\n\n{overview}"] if overview else []
    parts.extend(
        f"### {finding_label(finding['id'])[1:-1]}\n\n{finding['body']}" for finding in findings
    )
    return "\n\n".join(parts).strip()


def _review_sections(review: str) -> list[tuple[int, str]]:
    return [
        (offset, line) for offset, line, fence in markdown_lines(review)
        if fence is None and re.match(
            r"^(?:#{1,4}\s+|\d+\.\s+\*\*(?:Blocking issues|Non-blocking notes|"
            r"Overall assessment|(?:(?:PR|Assessment) )?Summary))",
            line,
        )
    ]


def publish_findings(
    *, github: Any, repository: str, pr_number: int, head_sha: str, run_url: str,
    review: str, structured: Any, github_pr_patch: str, pr_change_patch: str | None,
    trusted_bot_logins: list[str], mode: str = "inline", placement: str = "inline-or-conversation",
) -> dict[str, Any]:
    """Post or reuse finding comments and update the main AI review comment for one commit.

    Return final findings with confirmed comment URLs and posting errors. An unobserved write
    remains unconfirmed without an automatic retry. Summary and artifact modes return report
    records without posting PR comments.
    """
    if not re.fullmatch(r"[0-9a-f]{40}", head_sha):
        raise ValueError("invalid head SHA")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or pr_number < 1:
        raise ValueError("invalid PR destination")
    if not run_url.startswith(f"https://github.com/{repository}/actions/runs/"):
        raise ValueError("invalid workflow run URL")
    if mode not in {"inline", "collapsed", "comment", "summary", "artifact"}:
        raise ValueError("invalid publication mode")
    findings, errors = collect_findings(review, structured)
    record: dict[str, Any] = {
        "head_sha": head_sha, "run_url": run_url, "mode": mode, "findings": findings,
        "errors": errors, "complete": False, "main_url": None,
    }
    if mode in {"summary", "artifact"}:
        for finding in findings:
            finding.update(head_sha=head_sha, placement=mode, url=None, delivery=None)
        record["complete"] = not errors
        return record
    prefix = f"repos/{repository}"
    issue_path = f"{prefix}/issues/{pr_number}/comments"
    inline_path = f"{prefix}/pulls/{pr_number}/comments"

    def history() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        reviews = _pages(github, f"{prefix}/pulls/{pr_number}/reviews")
        trusted_reviews = {
            item["id"] for item in reviews
            if _trusted(item, trusted_bot_logins)
            and item.get("body", "").lstrip().startswith(BOT_MARKER)
        }
        issues = [item for item in _pages(github, issue_path) if _trusted(item, trusted_bot_logins)]
        inline = [
            item for item in _pages(github, inline_path)
            if _trusted(item, trusted_bot_logins) and (
                item.get("pull_request_review_id") in trusted_reviews
                or item.get("body", "").lstrip().startswith(BOT_MARKER)
            )
        ]
        return issues, inline

    try:
        issues, inline = history()
    except PublicationError:
        errors.append("Publication history could not be read; no new findings were posted.")
        return record
    source = {"head_sha": head_sha, "run_url": run_url}
    main = next((item for item in issues if _metadata(item, _MAIN_MARKER) == source), None)
    main_marker = f"<!-- ai-review-main {json.dumps(source, sort_keys=True)} -->"
    if main is None:
        provisional = (
            f"{BOT_MARKER}\n{main_marker}\n## AI Review\n\n"
            f"Publishing findings for `{head_sha[:8]}`."
        )
        try:
            main = github.write("POST", issue_path, {"body": provisional})
            if not _url(main) or not isinstance(main.get("id"), int):
                raise PublicationError("Main comment URL was not returned")
        except PublicationError:
            errors.append("The main comment write could not be confirmed.")
            return record
    record["main_url"] = _url(main)
    github_pr_patch_positions = diff_positions(github_pr_patch)
    pr_change_patch_positions = (
        diff_positions(pr_change_patch) if pr_change_patch is not None else github_pr_patch_positions
    )
    github_pr_patch_sections = diff_sections(github_pr_patch)
    pr_change_patch_sections = (
        diff_sections(pr_change_patch) if pr_change_patch is not None else github_pr_patch_sections
    )
    for finding in findings:
        finding.update(head_sha=head_sha, url=None, delivery=None)
        existing = _existing(finding, issues, inline)
        if existing:
            source_metadata = _metadata(existing, _FINDING_MARKER) or {}
            finding.update(
                url=_url(existing), delivery="reused",
                placement="inline" if existing in inline else "conversation",
                source_comment_id=existing.get("id"),
                source_review_id=existing.get("pull_request_review_id"),
                source_head_sha=source_metadata.get("head_sha") or existing.get("commit_id"),
            )
            continue
        position = finding["path"], finding["line"], finding["side"]
        can_inline = (
            mode == "inline" and placement == "inline-or-conversation"
            and position in github_pr_patch_positions and position in pr_change_patch_positions
            and (
                finding["side"] != "LEFT"
                or github_pr_patch_sections.get(finding["path"]) == pr_change_patch_sections.get(finding["path"])
            )
        )
        metadata = {**source, "id": finding["id"], "fingerprint": finding["fingerprint"]}
        body = (
            f"{BOT_MARKER}\n<!-- ai-review-finding {json.dumps(metadata, sort_keys=True)} -->\n"
            + format_finding(finding, head_sha=head_sha, main_url=record["main_url"])
        )
        path = inline_path if can_inline else issue_path
        payload = {"body": body}
        if can_inline:
            payload.update(
                commit_id=head_sha, path=finding["path"],
                line=finding["line"], side=finding["side"],
            )
        try:
            destination = github.write("POST", path, payload)
            if not _url(destination):
                raise PublicationError("Finding URL was not returned")
        except PublicationError as error:
            destination = None
            if can_inline and not error.ambiguous:
                try:
                    destination = github.write("POST", issue_path, {"body": body})
                    can_inline = False
                    if not _url(destination):
                        raise PublicationError("Fallback URL was not returned")
                except PublicationError:
                    destination = None
            if destination is None or not _url(destination):
                finding["error"] = "The finding write could not be confirmed."
                errors.append(f"{finding['id']}: publication could not be confirmed.")
                continue
        finding.update(
            url=_url(destination), delivery="new",
            placement="inline" if can_inline else "conversation",
            source_comment_id=destination.get("id"),
            source_review_id=destination.get("pull_request_review_id"),
            source_head_sha=head_sha,
        )
        (inline if can_inline else issues).append(destination)
    summary = format_review_summary(
        overview=review_overview(review), head_sha=head_sha, findings=findings,
        run_url=run_url, errors=errors, counts_available=not errors,
        checks_url=f"https://github.com/{repository}/pull/{pr_number}/checks",
    )
    final_body = summary.replace(BOT_MARKER, f"{BOT_MARKER}\n{main_marker}", 1)
    try:
        github.write("PATCH", f"{prefix}/issues/comments/{main['id']}", {"body": final_body})
    except PublicationError:
        errors.append("The final main comment could not be confirmed.")
    record["complete"] = not errors and all(finding.get("url") for finding in findings)
    return record


def publication_check(review: str, record: dict[str, Any]) -> str:
    """Build the AI Review Summary Check Run report from all final findings."""
    return complete_review(review, record["findings"]) + "\n"


def _identity(value: Any) -> tuple[str, str]:
    if not isinstance(value, str) or (match := _ID.fullmatch(value.strip())) is None:
        raise ValueError("invalid final finding ID")
    category = re.sub(r"\s+", "", match[1]).casefold()
    severity = {"blocker": "Blocker", "shouldfix": "Should Fix", "nit": "Nit"}[category]
    return severity.replace(" ", "") + match[2], severity


def _valid_body(body: Any) -> bool:
    return (isinstance(body, str) and bool(body.strip()) and len(body) <= 10_000
            and not any(ord(char) < 32 and char not in "\n\t" for char in body))


def _pages(github: Any, path: str) -> list[dict[str, Any]]:
    entries = []
    for page in range(1, 101):
        document = github.read(f"{path}?per_page=100&page={page}")
        if not isinstance(document, list) or not all(isinstance(item, dict) for item in document):
            raise PublicationError("GitHub history page is malformed")
        entries.extend(document)
        if len(document) < 100:
            return entries
    raise PublicationError("GitHub history exceeded the publication bound")


def _trusted(item: dict[str, Any], logins: list[str]) -> bool:
    user = item.get("user") or {}
    def normalize(login: Any) -> str:
        return login.casefold().removesuffix("[bot]") if isinstance(login, str) else ""
    return user.get("type") == "Bot" and normalize(user.get("login")) in {
        normalize(login) for login in logins
    }


def _url(item: Any) -> str | None:
    value = item.get("html_url") if isinstance(item, dict) else None
    return value if isinstance(value, str) and value.startswith("https://github.com/") else None


def _metadata(item: dict[str, Any], pattern: re.Pattern) -> dict[str, Any] | None:
    match = pattern.search(item.get("body") or "")
    if match:
        try:
            value = json.loads(match[1])
            return value if isinstance(value, dict) else None
        except ValueError:
            pass
    return None


def _existing(
    finding: dict[str, Any], issues: list[dict[str, Any]], inline: list[dict[str, Any]]
) -> dict[str, Any] | None:
    for item in [*issues, *inline]:
        metadata = _metadata(item, _FINDING_MARKER)
        if metadata and metadata.get("fingerprint") == finding["fingerprint"] and _url(item):
            return item
        if item in inline and not metadata and (
            item.get("path") == finding["path"]
            and (item.get("line") or item.get("original_line")) == finding["line"]
            and (item.get("side") or item.get("original_side")) == finding["side"]
            and canonical_finding_body(item.get("body", ""))
            == canonical_finding_body(finding["body"])
            and _url(item)
        ):
            return item
    return None


def main() -> None:
    """Publish final findings from the orchestrator report and save publication records."""
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "review", "findings", "github-pr-patch", "trusted-bot-logins", "output",
        "check-output", "full-review-output",
    ):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--pr-change-patch", type=Path)
    for name in ("head-sha", "run-url", "repository", "mode"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument(
        "--placement", choices=["inline-or-conversation", "conversation"],
        default="inline-or-conversation",
    )
    args = parser.parse_args()
    review = args.review.read_text()
    record = publish_findings(
        github=GitHub(), repository=args.repository, pr_number=args.pr_number,
        head_sha=args.head_sha, run_url=args.run_url, review=review,
        structured=json.loads(args.findings.read_text()),
        github_pr_patch=args.github_pr_patch.read_bytes().decode("utf-8", errors="replace"),
        pr_change_patch=(
            args.pr_change_patch.read_bytes().decode("utf-8", errors="replace")
            if args.pr_change_patch else None
        ),
        trusted_bot_logins=json.loads(args.trusted_bot_logins.read_text()),
        mode=args.mode, placement=args.placement,
    )
    args.output.write_text(json.dumps(record, indent=2) + "\n")
    for error in record["errors"]:
        print(f"::warning::{error}")
    args.check_output.write_text(publication_check(review, record))
    args.full_review_output.write_text(complete_review(review, record["findings"]) + "\n")


if __name__ == "__main__":
    main()
