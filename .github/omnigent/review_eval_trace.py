"""Evaluation-only audit hook for raw dispatches and returned reviewer evidence."""

from __future__ import annotations

import json
import os
from typing import Any, Callable


def record_events(*, path: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Append tool events to a private evaluation artifact without changing their verdict."""

    def evaluate(event: dict[str, Any]) -> dict[str, Any]:
        if event.get("type", "").lower().removeprefix("phase_") in {"tool_call", "tool_result"}:
            # One append per event also permits concurrent reviewer processes.
            # Omnigent attaches a live service client, which is neither audit data nor JSON.
            audited = {key: value for key, value in event.items() if key != "llm_client"}
            data = (json.dumps(audited, ensure_ascii=True) + "\n").encode()
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(fd, data)
            finally:
                os.close(fd)
        return {"result": "ALLOW"}

    return evaluate
