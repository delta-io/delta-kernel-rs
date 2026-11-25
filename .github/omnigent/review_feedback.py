"""Check whether inline AI review findings have been resolved and rated."""

from __future__ import annotations

import json
import os
import subprocess

from review_history import is_marked_bot_entry


REVIEW_THREADS_QUERY = """
query($owner: String!, $repo: String!, $pr: Int!, $endCursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      headRefOid
      reviewThreads(first: 100, after: $endCursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          isResolved
          comments(first: 1) {
            nodes {
              author { __typename login }
              url
              pullRequestReview {
                author { __typename login }
                body
              }
              reactionGroups {
                content
                reactors(first: 1) { totalCount }
              }
            }
          }
        }
      }
    }
  }
}
"""
FEEDBACK_REACTIONS = {"THUMBS_UP", "THUMBS_DOWN", "EYES"}
CHECK_NAME = "AI Review Comment Resolver"
MAX_LISTED_FINDINGS = 20


def evaluate_feedback(threads: list[dict], minimum_percent: int) -> tuple[bool, str]:
    """Return the threshold result and a summary with links to outstanding findings."""
    total = 0
    outstanding = []
    for thread in threads:
        comments = thread["comments"]["nodes"]
        if not comments:
            continue
        comment = comments[0]
        review = comment["pullRequestReview"]
        if not is_marked_bot_entry(review) or comment["author"] != review["author"]:
            continue

        total += 1
        has_feedback = any(
            group["content"] in FEEDBACK_REACTIONS and group["reactors"]["totalCount"] > 0
            for group in comment["reactionGroups"]
        )
        missing = []
        if not thread["isResolved"]:
            missing.append("resolve the thread")
        if not has_feedback:
            missing.append("react with 👍, 👎, or 👀")
        if missing:
            outstanding.append(f"- [Inline finding]({comment['url']}) — {' and '.join(missing)}.")

    if total == 0:
        return True, "No inline AI review findings from `github-actions[bot]`."

    addressed = total - len(outstanding)
    passed = addressed * 100 >= minimum_percent * total
    summary = (
        f"{addressed} of {total} inline AI review findings are resolved and rated "
        f"({addressed / total:.2%}). The required minimum is {minimum_percent}%."
    )
    if outstanding:
        summary += "\n\n" + "\n".join(outstanding[:MAX_LISTED_FINDINGS])
    if len(outstanding) > MAX_LISTED_FINDINGS:
        summary += f"\n\n{len(outstanding) - MAX_LISTED_FINDINGS} more findings need attention."
    return passed, summary


def main() -> int:
    """Evaluate a PR and publish the result on its head commit."""
    try:
        pr = int(os.environ["PR_NUMBER"])
        if pr < 1:
            raise ValueError
    except ValueError:
        raise SystemExit("The PR number must be a positive integer.") from None

    try:
        minimum_percent = int(os.environ["MIN_ADDRESSED_PERCENT"])
        if not 0 <= minimum_percent <= 100:
            raise ValueError
    except ValueError:
        raise SystemExit(
            "AI_REVIEW_MIN_ADDRESSED_PERCENT must be an integer from 0 to 100."
        ) from None

    repo = os.environ["GITHUB_REPOSITORY"]
    owner, name = repo.split("/", 1)
    query = [
        "gh", "api", "graphql", "--paginate", "--slurp",
        "-f", f"query={REVIEW_THREADS_QUERY}",
        "-f", f"owner={owner}", "-f", f"repo={name}", "-F", f"pr={pr}",
    ]
    pages = json.loads(subprocess.check_output(query))
    pull_requests = [page["data"]["repository"]["pullRequest"] for page in pages]
    head_sha = pull_requests[0]["headRefOid"]
    threads = [thread for pull in pull_requests for thread in pull["reviewThreads"]["nodes"]]
    passed, summary = evaluate_feedback(threads, minimum_percent)

    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
        handle.write(summary + "\n")
    run_url = f"{os.environ['GITHUB_SERVER_URL']}/{repo}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    title = (
        "AI review feedback meets the minimum" if passed else "AI review feedback is incomplete"
    )
    check = {
        "name": CHECK_NAME,
        "head_sha": head_sha,
        "details_url": run_url,
        "status": "completed",
        "conclusion": "success" if passed else "failure",
        "output": {"title": title, "summary": summary},
    }
    subprocess.run(
        ["gh", "api", "--method", "POST", f"repos/{repo}/check-runs", "--input", "-", "--silent"],
        input=json.dumps(check),
        text=True,
        check=True,
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
