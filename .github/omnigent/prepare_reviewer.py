"""Write a copy of the reviewer bundle with its shared instruction blocks expanded.

Each `REVIEW.md` under `reviewer/` keeps its reviewer's own instructions inline and names the
blocks it shares with other reviewers by placeholder. A line that reads `${review_rules}` stands
for `reviewer/shared/review_rules.md`. Omnigent loads `REVIEW.md` as written, so the bundle is
expanded before a reviewer loads it:

    python3 .github/omnigent/prepare_reviewer.py <destination>
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

REVIEWER_DIR = Path(__file__).parent / "reviewer"
_PLACEHOLDER_LINE = re.compile(r"^\$\{([a-z][a-z0-9_]*)\}$", re.MULTILINE)
_MALFORMED_PLACEHOLDER_LINE = re.compile(r"^[ \t]*\$\{[^}\n]*\}[ \t]*$", re.MULTILINE)


def expand_instructions(text: str, shared_dir: Path) -> str:
    """Replace each placeholder line in `text` with the shared block it names.

    Only a line that consists of `${name}` is a placeholder. `ValueError` is raised for a name
    without a matching `<name>.md` in `shared_dir`, for a shared block that contains a placeholder
    line of its own, and for a line that holds nothing but a misspelled or indented placeholder.
    """

    def _shared_block(match: re.Match[str]) -> str:
        path = shared_dir / f"{match.group(1)}.md"
        if not path.is_file():
            raise ValueError(f"{match.group(0)} has no shared block at {path}")
        block = path.read_text(encoding="utf-8").rstrip("\n")
        if _PLACEHOLDER_LINE.search(block):
            raise ValueError(f"{path} must not contain a placeholder line")
        return block

    expanded = _PLACEHOLDER_LINE.sub(_shared_block, text)
    malformed = _MALFORMED_PLACEHOLDER_LINE.search(expanded)
    if malformed:
        raise ValueError(f"{malformed.group(0)!r} is not a valid placeholder line")
    return expanded


def prepare_bundle(destination: Path, source: Path = REVIEWER_DIR) -> None:
    """Copy the bundle at `source` to the new directory `destination` and expand its instructions.

    Every `REVIEW.md` in the copy is complete, so each `config.yaml` beside one loads it unchanged.
    """
    shutil.copytree(source, destination)
    for instructions in destination.rglob("REVIEW.md"):
        text = instructions.read_text(encoding="utf-8")
        instructions.write_text(expand_instructions(text, source / "shared"), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write a copy of the reviewer bundle with shared blocks expanded."
    )
    parser.add_argument("destination", type=Path, help="directory to create for the copy")
    prepare_bundle(parser.parse_args().destination)


if __name__ == "__main__":
    main()
