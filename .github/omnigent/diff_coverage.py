"""Prepare independent PR change patch excerpts for subagents and the orchestrator.

Record which patch bytes and file sections enter Reviewer and Checker subagent task messages.
Limit the PR description excerpt separately. Fit the orchestrator's complete initial task message
within the command-argument byte limit, reserving its other text before adding the patch excerpt.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

MAX_SUBAGENT_PATCH_EXCERPT_BYTES = 100_000
MAX_ORCHESTRATOR_PATCH_EXCERPT_BYTES = 80_000
MAX_PR_DESCRIPTION_EXCERPT_BYTES = 8_192
MAX_ORCHESTRATOR_INITIAL_TASK_MESSAGE_BYTES = 120_000
CHANGED_PATHS_FILE = Path("/tmp/ai_review_changed_paths.json")
_SHA = re.compile(r"[0-9a-f]{40}")
_STACK_HEADING = re.compile(
    r"(?:#{1,6}\s+)?(?:\*\*)?(?:[^\w\s]+\s+)?Stacked PRs?(?:\*\*)?\s*#*",
    re.IGNORECASE,
)
_PR_LINK = r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[0-9]+"
_INCREMENTAL_LINK = re.compile(
    rf"Use this \[link\]\({_PR_LINK}/files(?:/[0-9a-f.]+)?\) "
    r"to review incremental changes\."
)
_STACK_ITEM = re.compile(
    rf"[ \t]*[-*] \[[^\]\n]+\]\({_PR_LINK}\)"
    rf"(?: \[\[Files changed\]\({_PR_LINK}/files(?:/[0-9a-f.]+)?\)\])?"
    r"(?: \[(?:MERGED|CLOSED)\])?(?: (?:<-|←) _this PR_| \(this PR\))?"
)


def prepare_review_inputs(
    pr_change_patch_bytes: bytes, raw_pr_description: str, base_sha: str, head_sha: str
) -> dict[str, Any]:
    """Return PR description and subagent patch excerpts with their byte and file coverage."""
    if not _SHA.fullmatch(base_sha) or not _SHA.fullmatch(head_sha):
        raise ValueError("review coverage requires exact base and head SHAs")
    author_description = strip_stack_header(raw_pr_description)
    pr_description_excerpt_text = utf8_prefix(
        author_description.encode("utf-8"), MAX_PR_DESCRIPTION_EXCERPT_BYTES
    )
    subagent_patch_excerpt_text = utf8_prefix(pr_change_patch_bytes, MAX_SUBAGENT_PATCH_EXCERPT_BYTES)
    excerpt_bytes = len(subagent_patch_excerpt_text.encode("utf-8"))
    files = file_sections(pr_change_patch_bytes)
    for section in files:
        section["status"] = (
            "full" if section["end"] <= excerpt_bytes
            else "partial" if section["start"] < excerpt_bytes
            else "omitted"
        )
    coverage = {
        "base_sha": base_sha, "head_sha": head_sha, "total_bytes": len(pr_change_patch_bytes),
        "excerpt_bytes": excerpt_bytes, "complete": excerpt_bytes == len(pr_change_patch_bytes),
        "total_files": len(files),
        "full_files": sum(section["status"] == "full" for section in files),
        "partial_files": sum(section["status"] == "partial" for section in files),
        "omitted_files": sum(section["status"] == "omitted" for section in files),
        "files": files,
    }
    return {
        "pr_description_excerpt_text": pr_description_excerpt_text,
        "pr_description_truncated": (
            len(pr_description_excerpt_text.encode("utf-8")) < len(author_description.encode("utf-8"))
        ),
        "subagent_patch_excerpt_text": subagent_patch_excerpt_text,
        "coverage": coverage,
    }


def build_orchestrator_task_message(
    task_text_without_patch: str,
    pr_change_patch_bytes: bytes,
    coverage: dict[str, Any],
    *,
    limit: int = MAX_ORCHESTRATOR_INITIAL_TASK_MESSAGE_BYTES,
) -> str:
    """Build the initial orchestrator task message with both patch excerpt byte counts.

    Keep run-specific instructions, PR metadata, the description and earlier AI review text in
    task_text_without_patch. If the complete changed-path list cannot fit, name the
    list_changed_paths tool so the orchestrator can read every path in pages. Role instructions,
    tool definitions and later tool results are outside the initial task message limit.
    Reserve the byte-count notice before adding paths and patch text. Raise ValueError if the
    fixed task text leaves no room for that notice within the UTF-8 byte limit.
    """
    prefix_bytes = len(task_text_without_patch.encode("utf-8"))
    if prefix_bytes > limit:
        raise ValueError(
            "orchestrator task text without the patch exceeds the initial-message byte limit"
        )
    max_excerpt_bytes = min(MAX_ORCHESTRATOR_PATCH_EXCERPT_BYTES, len(pr_change_patch_bytes))
    excerpt_notice = (
        "\n\n## Patch excerpts\n"
        f"This orchestrator task message includes {{included}} of {len(pr_change_patch_bytes)} "
        "UTF-8 patch bytes.\n"
        f"Reviewer and Checker task messages include {coverage['excerpt_bytes']} of "
        f"{len(pr_change_patch_bytes)} UTF-8 patch bytes in a separate excerpt.\n"
    )
    reserved_bytes = prefix_bytes + len(
        excerpt_notice.format(included=max_excerpt_bytes).encode("utf-8")
    )
    if reserved_bytes > limit:
        raise ValueError("orchestrator task text leaves no room for patch excerpt byte counts")
    paths = "\n".join(
        json.dumps(section["path"], ensure_ascii=False) for section in coverage["files"]
    )
    manifest = f"\n\n## Complete scoped changed paths\n{paths}\n"
    if reserved_bytes + len(manifest.encode("utf-8")) > limit:
        manifest = (
            "\n\n## Complete scoped changed paths\n"
            "The full path list is available through list_changed_paths. "
            "Read every page using next_offset; this diff excerpt does not list every path.\n"
        )
    if reserved_bytes + len(manifest.encode("utf-8")) > limit:
        manifest = "\nlist_changed_paths"
    if reserved_bytes + len(manifest.encode("utf-8")) > limit:
        return task_text_without_patch + excerpt_notice.format(included=0)
    head, tail = "\n## PR Diff\n\n```diff\n", "\n```\n"
    remaining = limit - reserved_bytes - len((manifest + head + tail).encode("utf-8"))
    if remaining < 0:
        return task_text_without_patch + manifest + excerpt_notice.format(included=0)
    excerpt = utf8_prefix(pr_change_patch_bytes, min(max_excerpt_bytes, remaining))
    notice = excerpt_notice.format(included=len(excerpt.encode("utf-8")))
    return task_text_without_patch + manifest + notice + head + excerpt + tail


def list_changed_paths(offset: int = 0, max_entries: int = 500) -> str:
    """Read changed paths and each file section's inclusion in the subagent patch excerpt."""
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError("offset must be a nonnegative integer")
    if (
        isinstance(max_entries, bool) or not isinstance(max_entries, int)
        or not 1 <= max_entries <= 1000
    ):
        raise ValueError("max_entries must be between 1 and 1000")
    document = json.loads(CHANGED_PATHS_FILE.read_text(encoding="utf-8"))
    if not _SHA.fullmatch(document["base_sha"]) or not _SHA.fullmatch(document["head_sha"]):
        raise ValueError("changed-path manifest requires exact base and head SHAs")
    files = document["files"]
    if not isinstance(files, list):
        raise ValueError("changed-path manifest must contain a file list")
    entries = []
    used_bytes = 512
    for section in files[offset:offset + max_entries]:
        entry = {"path": section["path"], "status": section["status"]}
        entry_bytes = len(json.dumps(entry, ensure_ascii=True).encode("utf-8"))
        if entry_bytes + 512 > 32_000:
            raise ValueError("a scoped path exceeds the path-page byte limit")
        if entries and used_bytes + entry_bytes + 2 > 32_000:
            break
        entries.append(entry)
        used_bytes += entry_bytes + 2
    next_offset = offset + len(entries)
    return json.dumps({
        "base_sha": document["base_sha"], "head_sha": document["head_sha"],
        "total_paths": len(files), "paths": entries,
        "next_offset": next_offset if next_offset < len(files) else None,
    }, ensure_ascii=True)


def strip_stack_header(raw_pr_description: str) -> str:
    """Remove only a leading generated git-stack navigation block terminated by its divider."""
    lines = raw_pr_description.splitlines(keepends=True)
    first = 0
    while first < len(lines) and not lines[first].strip():
        first += 1
    if first == len(lines) or not _STACK_HEADING.fullmatch(lines[first].strip()):
        return raw_pr_description
    items = []
    has_generated_link = False
    has_incremental_link = False
    has_unlinked_branch = False
    for index in range(first + 1, len(lines)):
        line = lines[index].rstrip("\r\n")
        if not line.strip():
            continue
        if re.fullmatch(r"\s*-{3,}\s*", line):
            if items and has_generated_link and (not has_unlinked_branch or has_incremental_link):
                return "".join(lines[index + 1:]).lstrip("\r\n")
            return raw_pr_description
        if _INCREMENTAL_LINK.fullmatch(line):
            has_generated_link = True
            has_incremental_link = True
        elif _STACK_ITEM.fullmatch(line):
            items.append(line)
            has_generated_link |= "[[Files changed]" in line
        elif re.fullmatch(r"[ \t]*[-*] stack/[^\s]+", line):
            has_unlinked_branch = True
        else:
            return raw_pr_description
    return raw_pr_description


def utf8_prefix(data: bytes, limit: int) -> str:
    """Return a UTF-8 prefix without splitting a code point or replacing source bytes."""
    if limit < 0:
        raise ValueError("byte limit must be nonnegative")
    task_text_without_patch = data[:limit]
    try:
        return task_text_without_patch.decode("utf-8")
    except UnicodeDecodeError as error:
        if error.reason == "unexpected end of data" and error.end == len(task_text_without_patch):
            return task_text_without_patch[:error.start].decode("utf-8")
        raise ValueError("review input contains invalid UTF-8") from error


def file_sections(pr_change_patch_bytes: bytes) -> list[dict[str, Any]]:
    """Return each file section's path and raw byte interval in PR change patch order."""
    starts = [match.start() for match in re.finditer(rb"(?m)^diff --git ", pr_change_patch_bytes)]
    if pr_change_patch_bytes and not starts:
        raise ValueError("scoped diff contains no Git file sections")
    files = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(pr_change_patch_bytes)
        section = pr_change_patch_bytes[start:end]
        lines = section.split(b"\n")
        old = new = renamed = None
        for line in lines[1:]:
            if line.startswith(b"@@ ") or line == b"GIT binary patch":
                break
            if line.startswith(b"--- "):
                old = _git_path(line[4:].split(b"\t", 1)[0], prefixed=True)
            elif line.startswith(b"+++ "):
                new = _git_path(line[4:].split(b"\t", 1)[0], prefixed=True)
            elif line.startswith((b"rename to ", b"copy to ")):
                renamed = _git_path(line.split(b" to ", 1)[1])
        path = new or old or renamed
        if path is None:
            path = _header_path(lines[0][len(b"diff --git "):])
        files.append({"path": path, "start": start, "end": end})
    return files


def _git_path(value: bytes, *, prefixed: bool = False) -> str | None:
    if value == b"/dev/null":
        return None
    if value.startswith(b'"'):
        if not value.endswith(b'"'):
            raise ValueError("unterminated quoted Git path")
        value = value[1:-1]
        decoded = bytearray()
        index = 0
        escapes = {ord(key): ord(char) for key, char in {
            "a": "\a", "b": "\b", "t": "\t", "n": "\n", "v": "\v", "f": "\f",
            "r": "\r", '"': '"', "\\": "\\",
        }.items()}
        while index < len(value):
            if value[index] != 92:
                decoded.append(value[index])
                index += 1
                continue
            index += 1
            if index == len(value):
                raise ValueError("unterminated Git path escape")
            if 48 <= value[index] <= 55:
                match = re.match(rb"[0-7]{1,3}", value[index:])
                if match is None:
                    raise ValueError("invalid Git path octal escape")
                decoded.append(int(match[0], 8))
                index += len(match[0])
            elif value[index] in escapes:
                decoded.append(escapes[value[index]])
                index += 1
            else:
                raise ValueError("unsupported Git path escape")
        value = bytes(decoded)
    path = value.decode("utf-8")
    return path[2:] if prefixed and path.startswith(("a/", "b/")) else path


def _header_path(header: bytes) -> str:
    if header.startswith(b'"'):
        first = re.match(rb'"(?:[^"\\]|\\.)*"', header)
        if first is None:
            raise ValueError("invalid quoted Git diff header")
        second = header[first.end():].lstrip(b" ")
        return _git_path(second, prefixed=True)
    quoted_second = header.find(b' "')
    if quoted_second >= 0:
        return _git_path(header[quoted_second + 1:], prefixed=True)
    for separator in re.finditer(rb" b/", header):
        old = _git_path(header[:separator.start()], prefixed=True)
        new = _git_path(header[separator.start() + 1:], prefixed=True)
        if old == new:
            return new
    raise ValueError("cannot determine path from Git diff header")
