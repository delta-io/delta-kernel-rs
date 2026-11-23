"""Policies that bind reviewer dispatches to workflow-supplied PR context."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable

_CONTEXT_HEADER = "## Canonical PR context (workflow supplied)"
_CONFIG_OWNED_ARGS = ("model", "reasoning_effort")


def inject_review_context(*, context_path: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Append canonical PR context to every named reviewer dispatch.

    Also removes the dispatch's model and reasoning effort overrides, so each reviewer runs with
    the values in its own config.
    """
    path = Path(context_path)

    def _evaluate(event: dict[str, Any]) -> dict[str, Any]:
        data = event.get("data")
        if (
            event.get("type") != "tool_call"
            or not isinstance(data, dict)
            or data.get("name") != "sys_session_send"
        ):
            return {"result": "ALLOW"}

        arguments = data.get("arguments")
        child_args = arguments.get("args") if isinstance(arguments, dict) else None
        child_input = child_args.get("input") if isinstance(child_args, dict) else None
        if not isinstance(child_input, str) or not child_input.strip():
            return {
                "result": "DENY",
                "reason": "Reviewer dispatch requires non-empty args.input.",
            }

        try:
            context = path.read_bytes().decode("utf-8")
        except (OSError, ValueError):
            return {
                "result": "DENY",
                "reason": "Canonical PR review context is unavailable.",
            }
        if not context.strip():
            return {
                "result": "DENY",
                "reason": "Canonical PR review context is empty.",
            }

        transformed = copy.deepcopy(arguments)
        transformed_args = transformed["args"]
        for name in _CONFIG_OWNED_ARGS:
            transformed_args.pop(name, None)
        transformed_args["input"] = (
            f"{child_input.rstrip()}\n\n{_CONTEXT_HEADER}\n\n{context.rstrip()}"
        )
        return {"result": "ALLOW", "data": transformed}

    return _evaluate
