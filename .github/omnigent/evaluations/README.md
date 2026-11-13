# Review replay pilot

Run these cases after changing prompts, models, or review tooling. `gate` replays the same frozen
candidates to isolate disprove behavior; `pipeline` runs the checked-in orchestrator and reviewer
roster, then builds the actual inline-publication payload locally. Neither mode posts to GitHub.
The existing `ai-review-config` CI job runs the runner's regression tests on each PR. Paid model
replays are an operator-triggered pilot; they are not yet a scheduled or required CI quality gate.

The initial synthetic cases cover a real panic regression, a guarded false allegation, missing
dependency evidence, and an unaffected existing defect. Labels are provisional and stored separately.
They are not a representative sample of real PRs. Add independently adjudicated historical PR cases
before drawing review-quality conclusions. For a historical case, freeze the original base/head
SHAs in the input provenance, export their relevant source snapshots into `before`/`after`, and
include the exact protocol reference used. Do not include later fixes, review answers, or labels in
the source snapshot. Record omissions; a bounded snapshot must not imply whole-repository coverage.

## Run

Use a supported, authenticated Omnigent runtime. The runner does not read credential files or
configure authentication. Install PyYAML 6.0.3 and use Python 3.12. Create a nonsecret JSON file mapping
`default`, `maintainer-claude`, `maintainer-codex`, and `disprove` to resolved model identifiers valid
for that runtime. Preserve each bundle's configured harness; the Claude and Codex aliases must point
to compatible models. Supply exact runtime/harness versions with `--runtime`.

```sh
python3 .github/omnigent/review_eval.py run .github/omnigent/evaluations/panic.input.json \
  --mode gate --models /tmp/review-eval-models.json --runtime 'runtime and harness versions' \
  --command 'isaac omni' --output /tmp/review-eval/panic-gate
```

Use `--mode pipeline` in a different output directory for the full replay. Use `--command omnigent`
on an ordinary Omnigent installation. Repeat the same cases and model configuration against each
candidate prompt revision. Output directories must be new, so a run cannot overwrite prior evidence.
Use a bounded `--timeout` (default 1800 seconds). Inputs are never compiled or executed.

The temporary source trees contain only current source and the frozen reference, excluding labels,
old snapshots, future commits, and Git history. The prompt contains the original diff. Source tools
are the same bounded readers installed by CI. The copied bundles retain their production prompts,
with resolved model names, an isolated context path, and an evaluation-only event audit policy.

Artifacts include input/prompt/tool/reference digests, explicit runtime/model identifiers, elapsed
seconds, raw stdout/stderr, tool call/result events, and the locally constructed publication payload.
The pipeline requires trace evidence; missing or incompatible runtime hooks fail the replay. Inspect
raw dispatches and child results to recover candidates, gate verdicts/evidence and final mappings.
Do not substitute a model's retrospective account for these events. Token cost remains `null` unless
independently measured; wall time is measured, not inferred. Model/runtime identities are operator
attestations; a gateway alias may drift, so record the resolved provider version before comparison.

## Adjudicate and score

An independent reviewer reads the source and normative reference, validates the provisional labels,
and maps every raw candidate and published finding to an issue or a new discovery. Label runtime
bugs separately from policy findings. Check suggested fixes for harm. A thumbs-up/down reaction,
original reviewer confidence, or agreement between models is not a correctness label. A clean case
is provisional: credit a previously unknown real defect instead of counting it as a false positive.

The adjudication JSON contains `input_digest` from `run.json`, `run_digest` (the runner's `digest(run)`), and a `findings` array. Each row has
`issue_id` (null for a new issue), `discovered`, `published`, `truth` (`true`, `false`, or `uncertain`),
`harmful_fix`, `kind` (`runtime` or `policy`), `gate_verdict` (one gate verdict or null),
`evidence`, `expected_scope` and `reported_scope` (`in`, `out`, or `uncertain`). Keep evidence and
candidate/gate/publication references in each row for human audit. Use one row per deduplicated claim.

```sh
python3 .github/omnigent/review_eval.py score \
  .github/omnigent/evaluations/panic.labels.json /tmp/adjudication.json /tmp/review-eval/panic-gate/run.json
```

Report known-issue discovery and published recall, false published findings per case, new true
findings, uncertainty, harmful published fixes, scope agreement, and runtime/cost availability.
Compare gate verdicts on identical candidates separately from end-to-end discovery. Inspect raw
verdicts for true findings lost at the gate and false findings correctly rejected. A gate-only run
has no discovery or publication stage: leave those booleans false and do not interpret its recall as
pipeline recall. Report small-sample counts; do not claim accuracy gains from this pilot alone.
