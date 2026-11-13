"""Bound user feedback to a bot finding without mixing review-source lanes."""

from collections import defaultdict

FEEDBACK_WEIGHTS = {
    "thumbs_up": 3,
    "thumbs_down": 3,
    "eyes": 2,
    "positive_reply": 2,
    "corrective_reply": 3,
    "ai_marked_reply_multiplier": 0.25,
}
REPLY_SIGNALS = {
    "endorsed",
    "implemented",
    "corrected",
    "rejected",
    "out-of-scope",
    "acknowledged",
    "ambiguous",
}


def feedback_index(threads, source_lane):
    """Return reactions and same-thread user replies attached to each GitHub Actions finding."""
    result = {}
    for thread in threads:
        by_id = {event["id"]: event for event in thread["comments"]}
        for finding in thread["comments"]:
            if source_lane(finding) != "github-actions":
                continue
            reactions = finding.get("reactions") or {}
            entry = result.setdefault(
                finding["key"],
                {
                    "finding_key": finding["key"],
                    "pr": finding["pr"],
                    "reactions": {
                        name: max(0, int(reactions.get(name, 0))) for name in ("+1", "-1", "eyes")
                    },
                    "replies": [],
                },
            )
            for reply in thread["comments"]:
                if source_lane(reply) != "users" or not reply.get("reply_to"):
                    continue
                parent = by_id.get(reply["reply_to"])
                seen = set()
                while parent and parent["id"] not in seen:
                    seen.add(parent["id"])
                    if parent["key"] == finding["key"]:
                        entry["replies"].append(
                            {
                                "event_key": reply["key"],
                                "author": reply["author"],
                                "role": reply["role"],
                                "body": reply["body"],
                                "url": reply["url"],
                                "automation_marker": reply["automation_marker"],
                                "created_at": reply.get("created_at"),
                                "purpose": "feedback-on-this-finding-only",
                            }
                        )
                        break
                    parent = by_id.get(parent.get("reply_to"))
    return result


def validate_reply_annotations(annotations, finding_keys, index):
    validated = []
    seen = set()
    for annotation in annotations:
        finding_key = annotation["finding_key"]
        if finding_key not in finding_keys:
            raise ValueError(f"feedback must refer to a cited finding: {finding_key}")
        replies = {
            reply["event_key"]: reply for reply in index.get(finding_key, {}).get("replies", [])
        }
        reply_key = annotation["event_key"]
        if reply_key not in replies:
            raise ValueError(f"reply is not attached to finding {finding_key}: {reply_key}")
        reply = replies[reply_key]
        quote, signal = annotation["quote"], annotation["signal"]
        if not quote.strip() or quote not in reply["body"]:
            raise ValueError(f"feedback quote is not verbatim: {reply_key}")
        if signal not in REPLY_SIGNALS or not annotation.get("rationale", "").strip():
            raise ValueError("reply feedback needs a known signal and a rationale")
        if (finding_key, reply_key) in seen:
            raise ValueError(f"classify each reply once per finding: {reply_key}")
        seen.add((finding_key, reply_key))
        validated.append(
            {
                **annotation,
                "author": reply["author"],
                "role": reply["role"],
                "url": reply["url"],
                "automation_marker": reply["automation_marker"],
            }
        )
    return validated


def rank_feedback(finding_keys, index, annotations):
    """Score each direction using its strongest finding per PR."""
    by_finding = defaultdict(list)
    for annotation in annotations:
        by_finding[annotation["finding_key"]].append(annotation)
    findings = []
    for key in sorted(finding_keys):
        entry = index.get(key, {"pr": None, "reactions": {}, "replies": []})
        votes = entry["reactions"]
        units = {name: {} for name in ("positive", "corrective", "scope")}
        for annotation in by_finding[key]:
            signal = annotation["signal"]
            category = (
                "positive"
                if signal in {"endorsed", "implemented"}
                else "corrective"
                if signal in {"corrected", "rejected"}
                else "scope"
                if signal == "out-of-scope"
                else None
            )
            if category:
                weight = (
                    FEEDBACK_WEIGHTS["ai_marked_reply_multiplier"]
                    if annotation["automation_marker"]
                    else 1
                )
                actor = annotation["author"].casefold()
                units[category][actor] = max(units[category].get(actor, 0), weight)
        positive = sum(units["positive"].values())
        corrective = sum(units["corrective"].values())
        scope = sum(units["scope"].values())
        add_score = (
            FEEDBACK_WEIGHTS["thumbs_up"] * votes.get("+1", 0)
            + FEEDBACK_WEIGHTS["positive_reply"] * positive
        )
        correctness_score = (
            FEEDBACK_WEIGHTS["thumbs_down"] * votes.get("-1", 0)
            + FEEDBACK_WEIGHTS["corrective_reply"] * corrective
        )
        scope_score = (
            FEEDBACK_WEIGHTS["eyes"] * votes.get("eyes", 0)
            + FEEDBACK_WEIGHTS["corrective_reply"] * scope
        )
        findings.append(
            {
                "finding_key": key,
                "pr": entry["pr"],
                "reactions": votes,
                "positive_reply_units": positive,
                "corrective_reply_units": corrective,
                "out_of_scope_reply_units": scope,
                "add_score": add_score,
                "correctness_change_score": correctness_score,
                "scope_change_score": scope_score,
                "change_score": correctness_score + scope_score,
                "mixed_feedback": add_score > 0 and correctness_score + scope_score > 0,
                "unclassified_reply_count": len(entry["replies"]) - len(by_finding[key]),
            }
        )
    per_pr = defaultdict(lambda: {"add_score": 0, "change_score": 0})
    for finding in findings:
        for direction in ("add_score", "change_score"):
            per_pr[finding["pr"]][direction] = max(
                per_pr[finding["pr"]][direction], finding[direction]
            )
    add_score = sum(scores["add_score"] for scores in per_pr.values())
    change_score = sum(scores["change_score"] for scores in per_pr.values())
    return {
        "weights": FEEDBACK_WEIGHTS,
        "findings": findings,
        "annotations": annotations,
        "add_score": add_score,
        "change_score": change_score,
        "mixed_feedback": add_score > 0 and change_score > 0,
        "note": (
            "Heuristic ranking, not a correctness probability. Reactions are aggregate current "
            "counts; actor identities are unknown. Same-author replies count once per direction "
            "per finding; pattern scores take the maximum per PR."
        ),
    }
