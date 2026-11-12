# Review-derived ideas: users

2026-07-12T04:44:29Z through 2026-11-12T04:44:29Z.

Focused semantic synthesis across the four-calendar-month export, extending the prior eight-week ideas with selected older review conversations. All eligible PR review activity was collected, but these annotations are not an exhaustive labeling of every discussion. Human and bot source evidence remain separate. Thread resolutions and counterexamples are retained where inspected, including cancellation semantics, mutable-owner redesign constraints, and public visibility. Each idea has an integration target and code scope; new nested-file targets are proposals only. Recurrence is descriptive metadata, and support counts are cited lower bounds. The five-parameter rstest heuristic remains user input, not a mined threshold.

Source lane: users. Only GitHub Actions receives emoji/reply scores. Other lanes retain one-PR ideas; recurrence and existing-guidance coverage are annotations, not admission gates. Every idea has a concrete integration target and code paths, including proposed nested AGENTS.md files for local patterns. PR-author replies and older comments provide context without inflating recent-reviewer recurrence. Counts are cited lower bounds, not exhaustive frequencies or proof of correctness. Add/change/remove labels are proposals for a later integration agent.

Existing guidance classifications are semantic assessments; source hashes and anchors identify any files assessed. Proposed new files have no existing contents.

## Tests

### Possible additions

#### cross-engine-null-semantics-need-execution-tests (correctness)

When lowering Kernel predicates to another engine, execute null-valued, negated, and nested cases instead of assuming the target engine shares Kernel semantics.

Scope: DataFusion expression/predicate translation, including IN and NOT IN and optional struct behavior.

Exceptions: Treat the reviewed examples as candidates to verify against current Kernel semantics; do not freeze a suspected Kernel bug as a permanent compatibility rule.

Cited evidence: 1 distinct PRs; 2 reviewers; 2 discussions. Recurring: False. Opposing PRs cited: 0. State: idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `datafusion-executor/AGENTS.md` (proposed new file; new).

Translation semantics are crate-local context; a new nested file can combine the behavioral check with inherited repository test conventions.

Applicable code: `datafusion-executor/src`.

Inherited guidance: `AGENTS.md`. Keep this proposal specific to its subtree.

Proposed wording:

```markdown
For expression/predicate lowering, compare executed values and schema with the intended Kernel behavior. Include relevant null, negated, and nested cases; target-engine SQL defaults are not proof of equivalent semantics. Verify suspected Kernel quirks against the current contract before treating them as compatibility requirements.
```

Citations:

- [inline:3694004227](https://github.com/delta-io/delta-kernel-rs/pull/2971#discussion_r3694004227) @nicklan (reviewer; support): This is tricky. DF uses more standard (in SQL) 3-value logic, which _isn't_ what kernel does. For kernel, NULLS are basically just "ignored" in an `IN` expression (see [here](https://github.com/delta-io/delta-kernel-rs/blob/main/kernel/src/engine/arrow_expression/evaluate_expression.rs#L1932)), so: ``` NULL IN (1, 2, 3) = false // kernel ``` but in standard SQL (and in DF, see [here](https://github.com/apache/datafusion/blob/54.0.0/datafusion/physical-expr/src/expressions/in_list.rs#L301)) ``` NULL IN (1, 2, 3) = NULL // standard sql, we say "don't know" by returning NULL ``` So if we were using this for computing data skipping: Now, in a "normal" `IN` this won't matter, DF will drop the row because of the NULL, and kernel will too because `false`, BUT, if we do `NOT NULL in (1, 2 ,3)` kernel will rewrite that to `NOT(NULL in (1, 2, 3)` which will turn into `NOT(false) = true`, so a row will be kept. BUT in DF we'll get `NOT(NULL) = NULL`, so the row will be dropped, and we'll have different results than what kernel wanted. I _think_ we need to `coalesce` with `false` when we eval to fix this, but it's worth thinking about a bit. Would be good to add a test case for this too. Explanation: The review identifies an IN/NOT interaction where standard SQL null semantics can change retained rows.
- [inline:3716357196](https://github.com/delta-io/delta-kernel-rs/pull/2971#discussion_r3716357196) @aaronlinear (author; context): Good catch! Above use case addressed within `in_predicate_matches_membership_and_never_nulls`. However, looking through `IN` a bit more thoroughly it seems that there are more nuances than this. > Kernel Deviation #1 NULL IN (NULL, 2, 3) would return true in kernel. This is directly from [here](https://github.com/delta-io/delta-kernel-rs/blob/5f32a2afc23078d259a0abc8ca71348d7454127f/kernel/src/engine/arrow_expression/evaluate_expression.rs#L698-L704). This is because the `.contains` uses `PartialEq` which will see Long NULL == Long NULL is true. However, DataFusion doesn't support this and since left hand side is NULL the answer becomes NULL as well. This case is implemented as follows: 1. Check if there are any NULL scalar values where we are searching within. 2. If yes, add a check to the end for `OR left == NULL` (`left == NULL` and scalar array contains `NULL`). 3. If no, proceed as normal. Or is this a bug on the Kernel side? > Kernel Deviation #2 Kernel `IN` supports value `IN` `list_column` [here](https://github.com/delta-io/delta-kernel-rs/blob/5f32a2afc23078d259a0abc8ca71348d7454127f/kernel/src/engine/arrow_expression/evaluate_expression.rs#L650-L700). For example, 2 IN [[1, 2], [3], [2, 4]] produces [true, false, true]. This seems like the row-varying version of the literal-array case and could be lowered to something like `array_has(list_column, literal).is_true()`. I haven't implemented this for Kernel implementation, how important / what scenarios does this appear in for Kernel? Explanation: The author uncovers additional null cases and explicitly asks whether one is a Kernel bug.
- [inline:3724471952](https://github.com/delta-io/delta-kernel-rs/pull/2971#discussion_r3724471952) @OussamaSaoudi (reviewer; support): Best way is tests. I think a pr that just adds a bunch of execution tests that codifies these semantics would be super handy. Explanation: The reviewer asks for execution tests to codify the semantics.
- [inline:3642252638](https://github.com/delta-io/delta-kernel-rs/pull/2928#discussion_r3642252638) @kyli87 (reviewer; context): Instead of putting this all into the main CLAUDE.md, can we start a new CLAUDE.md specific to the new datafusion-executor crate and nest it inside that subdir? Explanation: A separate review explicitly requests keeping this crate context in a nested instruction file.

#### validate-backend-request-paths (testing)

Test storage changes against the backend request paths they alter, including pagination continuations and fallback capability paths.

Scope: Default-engine listing and I/O behavior across S3, Azure, and GCS.

Exceptions: Use focused request-level tests where they establish the contract; live-cloud validation can be scoped to the changed behavior and available infrastructure.

Cited evidence: 1 distinct PRs; 2 reviewers; 2 discussions. Recurring: False. Opposing PRs cited: 0. State: idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `default-engine/AGENTS.md` (proposed new file; new).

Storage backend behavior is local engine context; a nested file can inherit root test rules while recording request/pagination checks.

Applicable code: `default-engine/src`.

Inherited guidance: `AGENTS.md`. Keep this proposal specific to its subtree.

Proposed wording:

```markdown
For storage changes, identify the backend-specific request behavior, capability fallback, and pagination continuation affected. Add request-level coverage for those paths and record any live-backend validation performed; do not assume the first page or an in-memory store represents every cloud.
```

Citations:

- [review:5229188083](https://github.com/delta-io/delta-kernel-rs/pull/3003#pullrequestreview-5229188083) @dengsh12 (reviewer; support): The code change LGTM, wonder if you have validated on the real clouds? As every cloud has different behavior and... maybe risky corner cases missing... A draft checklist from codex: ``` Validate S3, GCS, and Azure through both paginated and plain-store paths: Backend Concrete store Variants ━━━━━━━━━ ━━━━━━━━━━━━━━━━━━━━ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ AWS S3 AmazonS3 Standard buckets; S3 Express directory buckets, including access-point URLs ───────── ──────────────────── ────────────────────────────────────────────────────────────────────────────── GCS GoogleCloudStorage Standard buckets ───────── ──────────────────── ────────────────────────────────────────────────────────────────────────────── Azure MicrosoftAzure Blob storage with hierarchical namespace disabled; ADLS Gen2 with it enabled For each cloud backend, exercise: - Paginated: EngineStore::with_paginated(...). - Fallback: EngineStore::plain(...). Passing an existing Arc<ObjectStore> directly to DefaultEngineBuilder::new(...) also takes this path. ``` Explanation: The reviewer explicitly requests validation of the concrete cloud and fallback paths.
- [inline:4010382970](https://github.com/delta-io/delta-kernel-rs/pull/3003#discussion_r4010382970) @DrakeLin (reviewer; support): Yep, this is ready now. You were right that the delimiter-only fallback still regressed late listings. The fallback now keeps `list_with_offset` and filters nested children, while S3/GCS/Azure send both `delimiter=/` and the offset. I also extended the Azure request test through a second continuation page, so we're checking `startFrom` on page 1 and `marker` on page 2. Mind taking another look when you get a chance? Explanation: The follow-up distinguishes initial-page offset from continuation-page markers.

### Possible changes/removals

#### assert-a-distinguishing-oracle (correctness)

Choose fixtures and assertions that would fail for the plausible incorrect implementation being guarded against.

Scope: Expected values computed from the same metadata as production code, inaccurate error-path assertions, and tests with one identity where multiple identities matter.

Exceptions: A shared helper is acceptable when it does not reproduce the decision under test. Prefer focused adversarial cases over another broad matrix.

Cited evidence: 6 distinct PRs; 3 reviewers; 6 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:85` (existing file; strengthen).

The prompt already says not to mirror control flow; strengthen it with an operational falsification check and avoid demanding a new test when an existing assertion already distinguishes the bug.

Applicable code: `kernel/src`, `kernel/tests`.

Existing wording to change:

```markdown
- Don't replicate the code's control flow in the test — that just verifies the code does what it currently does, not what it _should_ do.
```

Proposed wording:

```markdown
For each important regression test, name a plausible wrong implementation and verify that the fixture and assertion distinguish it. Avoid computing expected results with the same algorithm or metadata as production. Check that a negative test fails at the intended operation and stage.
```

Citations:

- [inline:4316834110](https://github.com/delta-io/delta-kernel-rs/pull/3630#discussion_r4316834110) @sanujbasu (reviewer; support): The oracle here folds `String::replace` over `parameter_names()` — the same list `render_template` looks names up in. So the test cannot see a template placeholder the generator failed to list: both sides leave it literal and the `assert_eq!` passes. I confirmed this by mutation: dropping `"mode"` from `DELTA_ADAPTIVE_METADATA_REQUIRES_COLUMN_MAPPING_ID_MODE`'s `parameter_names` leaves all 27 tests green.
- [inline:4264990369](https://github.com/delta-io/delta-kernel-rs/pull/3424#discussion_r4264990369) @sanujbasu (reviewer; support): Could we add a mismatched-identifier case whose bounds would incorrectly exclude a real match if consumed? The selected-file count already detects incorrect fallback, but all identifiers currently carry the same accurate bounds, so the result oracle doesn't independently catch it. Pick adversarial bounds per operation (`"zzz"` for equality, a low maximum for `>`), and use `adds.len()` for the retain-all expectation.
- [inline:4354043037](https://github.com/delta-io/delta-kernel-rs/pull/3099#discussion_r4354043037) @sanujbasu (reviewer; support): Both schema errors and partition-layout errors begin with `schema at version N:`, so this helper cannot distinguish them. In particular, the intermediate partition-layout test would still pass if it failed through the schema check instead.
- [inline:4532979637](https://github.com/delta-io/delta-kernel-rs/pull/3813#discussion_r4532979637) @Jameson-Crate (reviewer; support): [🤖] **S1 · [tests] Test independent transaction and domain identities**
- [inline:3648367288](https://github.com/delta-io/delta-kernel-rs/pull/2983#discussion_r3648367288) @rliao147 (reviewer; support): Can you add a test here which validates that the casted predicates provide a correct verdict on actual values? [Isaac generated one below]: ``` #[rstest] #[case::eq_match(Pred::eq(cast_date(), Scalar::Date(20_641)), "2026-07-07", TRUE)] #[case::eq_nomatch(Pred::eq(cast_date(), Scalar::Date(20_641)), "2026-07-08", FALSE)] #[case::lt_true(Pred::lt(cast_date(), Scalar::Date(20_641)), "2026-07-06", TRUE)] #[case::lt_false(Pred::lt(cast_date(), Scalar::Date(20_641)), "2026-07-07", FALSE)] #[case::gt_true(Pred::gt(cast_date(), Scalar::Date(20_641)), "2026-07-08", TRUE)] #[case::gt_false(Pred::gt(cast_date(), Scalar::Date(20_641)), "2026-07-07", FALSE)] #[case::ge_boundary(Pred::ge(cast_date(), Scalar::Date(20_641)), "2026-07-07", TRUE)] #[case::le_boundary(Pred::le(cast_date(), Scalar::Date(20_641)), "2026-07-07", TRUE)] #[case::ne_match(Pred::ne(cast_date(), Scalar::Date(20_641)), "2026-07-07", FALSE)] #[case::ne_nomatch(Pred::ne(cast_date(), Scalar::Date(20_641)), "2026-07-08", TRUE)] fn test_checkpoint_partition_cast_eval_discriminates_per_operator( #[case] pred: Pred, #[case] partition_value: &str, #[case] expected: Option<bool>, ) { let partition_columns = test_partition_columns(); let skipping_pred = as_checkpoint_skipping_predicate( &pred, &partition_columns, &HashSet::new(), &HashSet::new(), ) .unwrap(); let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([( column_name!("partitionValues_parsed.part_col"), Scalar::from(partition_value), )])); expect_eq!(resolver.eval(&skipping_pred), expected, "{pred:#?} @ {partition_value}"); } ``` Explanation: Execute the casted predicate on values to validate a verdict, rather than only inspecting its structure.
- [inline:3860552984](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860552984) @sanujbasu (reviewer; support): The `storageType`-present / `pathOrInlineDv`-absent branch (the missing-field error) never runs in any test -- `deletion_vector_array` structurally can't build that shape (it keys every field off `path.map(...)`). Since this rejects malformed connector input, worth a direct unit test on the fn (no test module here yet): ```rust #[test] fn dv_unique_id_errors_when_path_missing() { assert!(deletion_vector_unique_id(Some("u"), None, Some(0)).is_err()); } ``` Explanation: The fixture cannot represent a malformed input, leaving the intended error branch untested.

#### reuse-test-infrastructure (testing)

Search existing test helpers and public domain types before adding a custom test engine, enum, or fixture framework.

Scope: Test doubles and setup plumbing.

Exceptions: Use lower-level fixtures for malformed states existing builders cannot express; retain custom doubles for behavior the delegating engine cannot override.

Cited evidence: 6 distinct PRs; 3 reviewers; 6 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:70` (existing file; strengthen).

AGENTS already says to reuse helpers. Add the concrete search obligation to the executable test reviewer instead of repeating a helper inventory.

Applicable code: `ffi/src`, `kernel/src`, `kernel/tests`.

Existing wording to change:

```markdown
Read the existing tests to understand what is already covered.
```

Proposed wording:

```markdown
Before suggesting a new fixture or wrapper, inspect the shared test utilities and nearby tests. Name an existing replacement when available, and preserve its handler defaults and execution behavior.
```

Citations:

- [inline:4532430582](https://github.com/delta-io/delta-kernel-rs/pull/3869#discussion_r4532430582) @scottsand-db (reviewer; support): [🤖 + 🧑] Can we reuse `delta_kernel::engine::DelegatingEngine` here? It already supports overriding the JSON handler while forwarding the other handlers.
- [inline:4446921025](https://github.com/delta-io/delta-kernel-rs/pull/3531#discussion_r4446921025) @chiinlquah (reviewer; support): nit: test helper duplicates shared helpers already available in ffi_test_utils
- [inline:4354042976](https://github.com/delta-io/delta-kernel-rs/pull/3099#discussion_r4354042976) @sanujbasu (reviewer; support): This local enum duplicates the public `delta_kernel::table_features::ColumnMappingMode`, which already derives what these rstests need. The same fixture also hardcodes `maxColumnId` to `"2"`, so adding a field to the schema can silently make the metadata stale.
- [inline:3646789812](https://github.com/delta-io/delta-kernel-rs/pull/2939#discussion_r3646789812) @scottsand-db (reviewer; support): Can't we have the code in kernel crate and mark it as #test-utils feature flag, so that kernel unit tests can use it and integration tests can also use it? We _cannot_ just have tons of duplicate/similar code. Arrow crate / dependencies isn't an excuse -- I believe this is a solvable problem? Explanation: Share feature-gated helpers between unit and integration tests rather than duplicating them.
- [inline:3659584217](https://github.com/delta-io/delta-kernel-rs/pull/2854#discussion_r3659584217) @sanujbasu (reviewer; support): why do we hand craft the delta log instead of using the create table API here? Can we use the API and clean up the hand crafting? Explanation: Use the production create-table path instead of handcrafting a valid log.
- [inline:3860553117](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860553117) @sanujbasu (reviewer; support): `replace_column` moving to `test-utils` is a nice cleanup. `set_field_as_null` still re-rolls the same `index_of` + `columns.to_vec()` + rebuild scaffold though (as do the batch builders in dv.rs/removefile.rs), differing only by calling `replace_array_row`. Could compose the existing helper or add a `replace_column_row(batch, field, value, row)` and route callers through it. Explanation: Compose the existing record-batch helper instead of rebuilding it again.

#### bound-test-matrix-complexity (testing)

Parameterize shared behavior while keeping independent contracts and meaningful combinations visible.

Scope: Tests combining unrelated features, optional expected errors, or large Cartesian products.

Exceptions: Several parameters describing closely related struct fields may remain together. Simple success/error toggles can share a flow; split when setup or assertions become separate programs.

Cited evidence: 4 distinct PRs; 2 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:98` (existing file; strengthen).

The comments support avoiding overloaded matrices. The approximate five-parameter trigger is a user-supplied heuristic, not a measured threshold from the reviews.

Applicable code: `kernel/src`, `kernel/tests`.

Existing wording to change:

```markdown
- Actively look for rstest consolidation opportunities: when writing multiple tests
```

Proposed wording:

```markdown
Check that every parameter changes a meaningful scenario. Around five independently varying logical choices, consider splitting the test by contract; closely related field inputs are not a hard parameter-count limit. Keep named cases for coupled choices and use Cartesian products only when every combination matters.
```

Citations:

- [inline:4342725425](https://github.com/delta-io/delta-kernel-rs/pull/3523#discussion_r4342725425) @nicklan (reviewer; support): I think this could likely lose 200–250 lines without meaningful coverage loss.
- [inline:4265252112](https://github.com/delta-io/delta-kernel-rs/pull/3524#discussion_r4265252112) @nicklan (reviewer; support): I was a little shocked at the size of this file. I think we can shrink it _a lot_. I've left comments below where I think you can remove/simplify stuff. In general try to keep your code tight.
- [inline:4354930607](https://github.com/delta-io/delta-kernel-rs/pull/3671#discussion_r4354930607) @sanujbasu (reviewer; support): Could we split this into separate success and failure rstests instead of passing `expected_error` into one test? The failure test can cover timestamp and integer partition predicates, while the success test covers the case where no partition expression is built. That makes each expected outcome clear from the test name and removes the conditional assertion.
- [inline:4147363573](https://github.com/delta-io/delta-kernel-rs/pull/3367#discussion_r4147363573) @sanujbasu (reviewer; support): [Nit] The `Option<&str>` makes one rstest cover both success and failure, which is harder to scan than the behavior itself. Could we split this into accepted and rejected parameterized tests? The rejected cases can keep `assert_result_error_with_message`, while the accepted cases can simply unwrap.

#### minimize-fixtures-and-redundant-coverage (testing)

Remove setup that cannot affect the asserted behavior, and use existing lower-level coverage to keep integration tests focused.

Scope: Negative tests that fail before file reads, fixtures with irrelevant features, and integration matrices repeating unit coverage.

Exceptions: Keep a representative end-to-end test of the composition; retain setup needed to reach the actual failure path.

Cited evidence: 4 distinct PRs; 3 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:70` (existing file; strengthen).

The reviewer currently emphasizes coverage expansion. This adds a concrete pass for unnecessary setup and duplicated coverage.

Applicable code: `kernel/src`, `kernel/tests`.

Existing wording to change:

```markdown
- Do integration tests use **realistic table fixtures** or synthetic minimal ones?
```

Proposed wording:

```markdown
Trace where the test succeeds or fails. Remove fixture data and helpers unreachable before that point, and identify the existing tests covering lower-level combinations before asking an integration test to repeat them. Preserve at least the distinct end-to-end behavior the change introduces.
```

Citations:

- [inline:4265252112](https://github.com/delta-io/delta-kernel-rs/pull/3524#discussion_r4265252112) @nicklan (reviewer; support): I was a little shocked at the size of this file. I think we can shrink it _a lot_. I've left comments below where I think you can remove/simplify stuff. In general try to keep your code tight.
- [inline:4354042989](https://github.com/delta-io/delta-kernel-rs/pull/3099#discussion_r4354042989) @sanujbasu (reviewer; support): `TableChanges::try_new` fails during boundary validation, before log replay or data-file reads. As a result, the Parquet writes and the `add`/`remove` actions in this test do not contribute to the assertion.
- [inline:4532384669](https://github.com/delta-io/delta-kernel-rs/pull/3869#discussion_r4532384669) @scottsand-db (reviewer; support): [🤖 + 🧑] We generate a valid compaction here and immediately overwrite all its contents. Could this be a focused malformed-compaction test that installs the corrupt bytes directly?
- [inline:4204273507](https://github.com/delta-io/delta-kernel-rs/pull/3424#discussion_r4204273507) @sanujbasu (reviewer; support): Could `create_collation_table` take a fixture rather than `nested: bool`? The two schemas, batches, and bounds differ; the write loop is what is shared. For example:

#### assert-executed-values-and-schema (testing)

For translated expressions and plans, execute representative inputs and compare readable results plus the output schema.

Scope: DataFusion lowering, aggregate execution, and schema-bearing columnar results.

Exceptions: Plan-shape assertions remain useful when plan structure itself is the contract; printed values alone cannot check schema/nullability.

Cited evidence: 3 distinct PRs; 1 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:70` (existing file; unassessed).

Teach the test reviewer to distinguish executable semantic coverage from construction-only or string-only checks.

Applicable code: `datafusion-executor/src`, `kernel/src`, `kernel/src/engine/sync`.

Citations:

- [inline:3737494440](https://github.com/delta-io/delta-kernel-rs/pull/3056#discussion_r3737494440) @OussamaSaoudi (reviewer; support): IIRC there's ways to pretty print the arrow record batch and assert that equals the equpected type. `assort_sorted_batches_eq` or smth like that. I'd like for us to use that as the way that we test execution because it's a lot more readable. Also: assert that the record batch schema == target schema. Explanation: The reviewer asks for readable batch assertions and an explicit schema check.
- [inline:3759514818](https://github.com/delta-io/delta-kernel-rs/pull/3079#discussion_r3759514818) @OussamaSaoudi (reviewer; support): for tests like these, could we just execute the plan and compare against a pretty-print? Then I want you to test all types and especially array, map, and struct. Getting structs right is going to be important. Explanation: Execute the generated plan, including nested types, rather than merely constructing it.
- [inline:3761477146](https://github.com/delta-io/delta-kernel-rs/pull/3083#discussion_r3761477146) @OussamaSaoudi (reviewer; support): Mainly I'd like tests that use assert_eq_sorted_batches for readable tests. Then execute on: * null * empty * some null * all non-null * agg over struct (for count) Explanation: Readable executed aggregate outputs should include null and empty cases.

#### put-coupled-inputs-and-expectations-in-cases (testing)

Each named case should state the logical input and expected result together, removing dispatch that derives both from a case label.

Scope: Tests that use field names, action strings, redundant booleans, or repeated feature-property lists to decide setup and assertions.

Exceptions: Keep legitimate derived values in fixture builders; do not obscure unrelated logical choices inside a case struct just to shorten a signature.

Cited evidence: 3 distinct PRs; 1 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:98` (existing file; strengthen).

Current guidance explains #[case] versus #[values] but does not explain how to keep case rows self-contained and eliminate meaningless parameters.

Applicable code: `kernel/src`, `kernel/tests`.

Existing wording to change:

```markdown
- Use `rstest` to parameterize tests that share the same logic but differ in setup
```

Proposed wording:

```markdown
Use named #[case] rows for coupled inputs and expectations. Avoid label-driven setup or assertion dispatch. Represent an optional operation with Option<T> when a boolean and a value describe the same choice.
```

Citations:

- [inline:4432640855](https://github.com/delta-io/delta-kernel-rs/pull/3647#discussion_r4432640855) @scottsand-db (reviewer; support): [🤖 👨] Suggestion: this new test uses the field name to derive both the replacement value and the expected result. Understanding one case requires following the parameter declaration, this branch, and the final assertion.
- [inline:4432641086](https://github.com/delta-io/delta-kernel-rs/pull/3647#discussion_r4432641086) @scottsand-db (reviewer; support): [🤖 👨] Suggestion: `double_writer` and `replay_batches` describe one choice together. The batch count has no meaning when `double_writer` is false:
- [inline:4532384362](https://github.com/delta-io/delta-kernel-rs/pull/3869#discussion_r4532384362) @scottsand-db (reviewer; support): [🤖 + 🧑] One simplification would make the suppression behavior easier to follow: every case already writes the file removal, domain deletion, and expired transaction marker. The `action` parameter mostly chooses which assertion runs.
- [inline:4285957181](https://github.com/delta-io/delta-kernel-rs/pull/3606#discussion_r4285957181) @scottsand-db (reviewer; support): The table cases would be easier to review if each row stated the feature and whether it is enabled directly. Currently, reviewers must read several string properties and check that they agree with a separate `SchemaChangeSupport` value.

#### cover-semantic-degenerate-inputs (correctness)

Choose boundary cases from the operation semantics: empty, all-null, mixed-null, and nested inputs often differ despite sharing one nominal type.

Scope: Aggregates, collection accessors, encoded scalars, and new branches that existing fixtures cannot exercise.

Exceptions: Select cases that distinguish a plausible failure; do not impose every combination or a large fixture on every change.

Cited evidence: 2 distinct PRs; 3 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:47` (existing file; unassessed).

Add a focused semantic-boundary check to coverage mapping rather than a blanket request for more tests.

Applicable code: `datafusion-executor/src`, `kernel/src`, `kernel/src/engine`.

Citations:

- [inline:3760925318](https://github.com/delta-io/delta-kernel-rs/pull/3083#discussion_r3760925318) @kyli87 (reviewer; support): Maybe I missed it, but might be worth adding a test case for sum/count over rows where all of the values are `null`? Explanation: All-null aggregate inputs are a separate semantic case.
- [inline:3761477146](https://github.com/delta-io/delta-kernel-rs/pull/3083#discussion_r3761477146) @OussamaSaoudi (reviewer; support): Mainly I'd like tests that use assert_eq_sorted_batches for readable tests. Then execute on: * null * empty * some null * all non-null * agg over struct (for count) Explanation: The requested cases distinguish empty, null, mixed, and struct behavior.
- [discussion:5652022243](https://github.com/delta-io/delta-kernel-rs/pull/3250#issuecomment-5652022243) @chiinlquah (reviewer; support): Thanks for the fix!! Sorry for the delay, couple of nits, otherwise lgtm. The added test covers the two fill branches with exact-value assertions but leaves the empty-slice branch (the one genuinely new decision point, bytes.last() == None) and the exactly-16-byte no-op resize untested. Suggested fix, add ``` // Empty slice: no most-significant byte, treated as non-negative, decodes to 0. assert_eq!( decimal_from_bytes(Some(&[]), dtype), Some(Scalar::decimal(0, 38, 3).unwrap()) ); // Full 16-byte negative value: resize is a no-op, sign already present. assert_eq!( decimal_from_bytes(Some(&(-1i128).to_be_bytes()), dtype), Some(Scalar::decimal(-1, 38, 3).unwrap()) ); ``` nit: The PR description lists ByteArray alongside FixedLenByteArray as an affected encoding, but only the FixedLenByteArray arm is wired to decimal_from_bytes; Statistics::ByteArray decimals fall through to return None. Explanation: Empty and exact-width inputs exercise decisions missed by the existing fill-branch tests.

#### test-lazy-composition-and-state-transitions (testing)

Exercise the changed public path across lazy pulls, cache states, and in-flight transitions, asserting the contract the composed API actually promises.

Scope: Cancellation and other stateful iterators with cached, fresh-read, and incremental-update branches.

Exceptions: A terminal component iterator does not make a chained stream terminal. Do not add an exactly-once error assertion unless the public contract requires it.

Cited evidence: 2 distinct PRs; 2 reviewers; 2 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Thread context: The initial exactly-one-Err-then-None proposal was narrowed after discussion of chained sources. The idea preserves the branch/transition coverage request and the later stop-at-first-error contract.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:47` (existing file; unassessed).

Extend coverage mapping to state transitions and composed boundaries, retaining the resolved cancellation contract.

Applicable code: `default-engine/src`, `kernel/src`, `kernel/src/cancellation.rs`, `kernel/src/scan`.

Citations:

- [inline:3641030572](https://github.com/delta-io/delta-kernel-rs/pull/2937#discussion_r3641030572) @dengsh12 (reviewer; support): Seems we are missing tests for `read_json_files_with_cancellation`, and read parquet, parquet footer with in-flight cancellation(` TestCancellationToken::default()` then call cancel later)? Explanation: An already-cancelled token does not exercise cancellation after work starts.
- [inline:3920431432](https://github.com/delta-io/delta-kernel-rs/pull/3239#discussion_r3920431432) @sanujbasu (reviewer; context): This test only covers the same-version cached-data branch, and cancellation happens before the first pull. Could we also add full-scan and incremental-update cases that consume an item, cancel, then assert exactly one Err(Cancelled) followed by None? Those are the paths that chain multiple cancellation-aware sources, so they would catch both a dropped token and duplicate terminal errors. Explanation: The first request identifies missing fresh/incremental branches; its terminal-error proposal was later narrowed.
- [inline:3994534374](https://github.com/delta-io/delta-kernel-rs/pull/3239#discussion_r3994534374) @sanujbasu (reviewer; support): Could we add an `existing_version < snapshot.version()` case that cancels before pulling newly read actions and asserts the first result is `Err(Cancelled)`? This would cover the engine-read branch without requiring a later `next()` to return `None`. Explanation: The revised request tests the engine-read branch without imposing a subsequent None.
- [inline:3993902453](https://github.com/delta-io/delta-kernel-rs/pull/3239#discussion_r3993902453) @sanujbasu (reviewer; context): this has been agreed upon / addressed and we have decided that kernel doesn't need to to wrap the errors Explanation: The discussion ultimately declined an extra wrapper around source errors.
- [inline:3994533600](https://github.com/delta-io/delta-kernel-rs/pull/3239#discussion_r3994533600) @sanujbasu (reviewer; context): Could we document that callers should stop consuming at the first `Err`, since a composed scan stream may report another source-level cancellation if it is polled again? Explanation: The resulting caller contract is to stop consuming at the first error.

#### test-cross-path-equivalence (correctness)

When a change promises equivalence to another public path, compare both paths on the same state and inputs instead of only checking a hand-written expected set.

Scope: Incremental versus full scan, cached versus freshly computed results, and alternate serialization or execution paths claiming the same semantics.

Exceptions: Use an independent semantic oracle as well when both paths share the same implementation; do not impose equivalence where their documented contracts differ.

Cited evidence: 1 distinct PRs; 1 reviewers; 1 discussions. Recurring: False. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:70` (existing file; unassessed).

Add a concrete test-quality check for cross-path correctness promises, while leaving the downstream agent to decide the smallest integration.

Applicable code: `kernel/src`.

Citations:

- [inline:3572284788](https://github.com/delta-io/delta-kernel-rs/pull/2860#discussion_r3572284788) @sanujbasu (reviewer; support): This claim that the surviving Add set matches a full scan at the target version is the key correctness promise of the feature, but as far as I can tell nothing actually tests it. All the predicate tests assert the incremental result against a hand-written expected set, not against what a real `Scan` at the same version with the same predicate produces. If I've missed such a test, ignore this. Explanation: The stated incremental/full-scan equivalence is not established by separate hand-written expected results.

### Other targeted ideas for review

#### extend-existing-rstest-flows (testing)

When new cases share setup, action, and assertions with an existing test, extend that rstest instead of creating a parallel test.

Scope: Nearby tests of the same observable contract.

Exceptions: Separate distinct construction paths or behavior contracts; shared setup alone is insufficient.

Cited evidence: 10 distinct PRs; 7 reviewers; 10 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: unspecified; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:98` (existing file; already-covered).

AGENTS explicitly requires active rstest consolidation, including extending nearby tests. Treat recurrence as an enforcement gap, not a reason to add another copy of the rule.

Applicable code: `datafusion-executor/src`, `ffi/src`, `kernel/src`, `kernel/tests`.

Citations:

- [inline:4395427711](https://github.com/delta-io/delta-kernel-rs/pull/3727#discussion_r4395427711) @OussamaSaoudi (reviewer; support): maybe parameterize `test_iceberg_native_v4_with_invariants_enabled` and `test_iceberg_native_v4_with_invariants_supported_but_not_enabled` together?
- [inline:4341858288](https://github.com/delta-io/delta-kernel-rs/pull/3105#discussion_r4341858288) @kyli87 (reviewer; support): nit: could be parameterized with prior test
- [inline:4355265554](https://github.com/delta-io/delta-kernel-rs/pull/3425#discussion_r4355265554) @scottsand-db (reviewer; support): [🤖 👨 ] Could we simplify check_constraints.rs by consolidating a few repeated test flows?
- [inline:4098397359](https://github.com/delta-io/delta-kernel-rs/pull/3393#discussion_r4098397359) @emkornfield (reviewer; support): are these duplicative with the existing to/from engine data tests?
- [inline:4149672659](https://github.com/delta-io/delta-kernel-rs/pull/1827#discussion_r4149672659) @OussamaSaoudi (reviewer; support): can we parameterize the following three functions using rstest?
- [inline:4320497887](https://github.com/delta-io/delta-kernel-rs/pull/3597#discussion_r4320497887) @Jameson-Crate (reviewer; support): can we just turn this test and `descriptor_and_dv_info_bincode_round_trip` into one large parameterized test with rstest rather than doing this pseudo parameterized test with `for descriptor in [dv_relative(), dv_absolute(), dv_inline()]`
- [inline:3667642269](https://github.com/delta-io/delta-kernel-rs/pull/2971#discussion_r3667642269) @OussamaSaoudi (reviewer; support): similar to last pr, could we just rstest everything into one function so we can easily see all the cases we're covering. Also add nesting Explanation: Consolidate equivalent predicate cases and include nesting.
- [inline:3648427603](https://github.com/delta-io/delta-kernel-rs/pull/2983#discussion_r3648427603) @rliao147 (reviewer; support): can you change this to an rstest which tests both the casted version and the plain version? Explanation: Add the cast/plain dimension to the same test flow.
- [inline:3648432396](https://github.com/delta-io/delta-kernel-rs/pull/2954#discussion_r3648432396) @dengsh12 (reviewer; support): NIT: Could we just change the `expect_fail_parse` to assert `ParseError`? We can reuse the existing test cases then. And can omit this test Explanation: Strengthen the existing error assertion and reuse its cases instead of adding a duplicate test.
- [inline:3567426582](https://github.com/delta-io/delta-kernel-rs/pull/2846#discussion_r3567426582) @dengsh12 (reviewer; support): NIT: seems `v3_column_default_warns_on_nested_non_literal` can be merged with this(both test can use the same nested schema)? Explanation: Two equivalent nested-schema tests can share a parameterized flow.

#### independent-rstest-axes (testing)

Use rstest identities for meaningful independent input axes rather than loops hidden inside a test.

Scope: Existing parameterized flows with independent dimensions such as column-mapping mode.

Exceptions: Avoid products with irrelevant, invalid, or redundant combinations; use named cases when parameters are coupled.

Cited evidence: 4 distinct PRs; 3 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: unspecified; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:98` (existing file; already-covered).

AGENTS already prefers #[values] for independent Cartesian products. Preserve the instruction and audit compliance instead of lengthening it.

Applicable code: `default-engine/src`, `kernel/src`, `kernel/tests`.

Citations:

- [inline:4322378494](https://github.com/delta-io/delta-kernel-rs/pull/3670#discussion_r4322378494) @sanujbasu (reviewer; support): Could we make column-mapping mode an independent `rstest` axis instead of looping inside the test body?
- [inline:4508869287](https://github.com/delta-io/delta-kernel-rs/pull/3820#discussion_r4508869287) @dengsh12 (reviewer; support): NIT: could we rstest over predicates/expected so that the tests can be ran in parallel?
- [inline:4262938369](https://github.com/delta-io/delta-kernel-rs/pull/3520#discussion_r4262938369) @dengsh12 (reviewer; support): As this change is for `list_from_impl`, wonder if we can have a focused unit test for that? sth like ``` #[rstest::rstest] #[case("table", "table")] #[case("table%41", "table%2541")] #[case("table%2F", "table%252F")] #[case("table%252F", "table%25252F")] #[case("table%20", "table%2520")] #[case("table%", "table%25")] #[case("table name", "table%20name")] #[case("table?#", "table%3F%23")] #[case("table\u{e9}", "table%C3%A9")] #[tokio::test] async fn list_from_preserves_table_paths( #[case] raw_table: &str, #[case] encoded_table: &str, #[values(1, 3)] file_count: usize, ) { let store = Arc::new(InMemory::new()); for index in (0..file_count
- [inline:4320497887](https://github.com/delta-io/delta-kernel-rs/pull/3597#discussion_r4320497887) @Jameson-Crate (reviewer; support): can we just turn this test and `descriptor_and_dv_info_bincode_round_trip` into one large parameterized test with rstest rather than doing this pseudo parameterized test with `for descriptor in [dv_relative(), dv_absolute(), dv_inline()]`

## Comments

### Possible additions

#### make-deferred-limitations-actionable (style)

When deferring a real limitation, leave a local explanation of the missing behavior and a tracking issue or clear trigger for revisiting it.

Scope: Known protocol gaps, deliberately unsupported operations, and follow-up work explicitly agreed during review.

Exceptions: Avoid vague cleanup TODOs, duplicating issue descriptions, or treating speculative risks as confirmed defects.

Cited evidence: 4 distinct PRs; 3 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:368` (existing file; new).

Add a concrete convention for durable TODOs while preserving the existing ban on temporal/conversational comments.

Applicable code: `ffi/src`, `kernel/src`.

Citations:

- [inline:3919189099](https://github.com/delta-io/delta-kernel-rs/pull/3225#discussion_r3919189099) @dengsh12 (reviewer; support): NIT: wonder if we want to add TODOs like v3. AI can know the limitations through reading the code ``` /// TODO(#2492): Implement the schema-evolution requirements for IcebergCompatV3. /// TODO: Support ALTER TABLE on tables with IcebergCompatV3 enabled. /// /// Requirements to enforce when the corresponding write paths are supported: /// - Geo types: when supported, they must not be usable as partition columns on IcebergCompatV3 /// tables. /// - REPLACE TABLE: when supported, partition columns must not change across the replace. /// - Timestamp parquet encoding: when kernel can write INT96 or INT64, IcebergCompatV3 tables must /// always use INT64; INT96 is forbidden. /// - ALTER TABLE SET/UNSET TBLPROPERTIES: when supported, reject any property change that would /// disable IcebergCompatV3 on an existing table. /// - Void type: when delta-spark supports VOID type on icebergCompatV3 tables, add it to V3's type /// allowlist (`is_v3_supported_type` in `iceberg_compat::v3`). /// /// Tracking issue: <https://github.com/delta-io/delta-kernel-rs/issues/2492> ``` Explanation: A feature-support declaration should expose its known unimplemented requirements.
- [inline:3928747215](https://github.com/delta-io/delta-kernel-rs/pull/3099#discussion_r3928747215) @dengsh12 (reviewer; support): NIT: Would be nice we have an issue for the TODO Explanation: The reviewer asks for an issue to track the TODO.
- [inline:4009628701](https://github.com/delta-io/delta-kernel-rs/pull/3325#discussion_r4009628701) @kyli87 (reviewer; support): This feels a bit too much like an AI comment referencing current context, maybe replace this with a TODO to unify these all eventually? Explanation: Replace iteration-specific commentary with an actionable unification task if warranted.
- [inline:3647149581](https://github.com/delta-io/delta-kernel-rs/pull/2856#discussion_r3647149581) @sanujbasu (reviewer; support): Yes. Lets file a tracking issue to make sure we don't lose it. Please assign it to yourself for now with the issue tagged to the Area: write validation. Explanation: A deferred validation task needs a tracking issue rather than being lost in the review.

### Possible changes/removals

#### qualify-contracts-and-test-descriptions (correctness)

State precisely which inputs, branches, and implementations a documented guarantee covers; describe tests at the boundary they actually exercise.

Scope: Conditional fast paths, default Engine implementations, test descriptions, and temporary Kernel restrictions.

Exceptions: Do not weaken an actual public guarantee merely because one implementation currently falls short; surface the mismatch for integration review.

Cited evidence: 5 distinct PRs; 3 reviewers; 6 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:330` (existing file; unassessed).

Add a documentation-review check for overbroad guarantees and mismatches between examples, tests, and implementation branches.

Applicable code: `default-engine/src`, `kernel/src`, `kernel/tests`.

Citations:

- [inline:3909824000](https://github.com/delta-io/delta-kernel-rs/pull/3215#discussion_r3909824000) @DrakeLin (reviewer; support): The comment claims callers derive their field type from the returned array, but only the struct-rebuild path does; the scalar evaluator builds its field from the requested kernel type and would fail `RecordBatch::try_new` on an uncast array. Explanation: A claim about all callers is only true for one reconstruction path.
- [inline:3909824001](https://github.com/delta-io/delta-kernel-rs/pull/3215#discussion_r3909824001) @DrakeLin (reviewer; support): The doc comment describes exercising a `stats_parsed` struct and the no-file-skipping symptom, but the test builds a flat `{minTs, maxTs}` batch and never runs pruning. Describe the evaluator boundary it actually tests; the nested shape is covered by the separate `apply_schema` test. Explanation: The test description claims a nested/pruning path that the flat evaluator test does not run.
- [inline:3920462374](https://github.com/delta-io/delta-kernel-rs/pull/3239#discussion_r3920462374) @sanujbasu (reviewer; support): This opening sounds like a guarantee for every implementation, but the default below only checks cancellation before calling read_parquet_footer and cannot interrupt the read once it starts. Could we say that an engine may race the read, and call out that the default only checks up front? The current wording made me expect mid-read cancellation from the default. Explanation: The default checks cancellation before I/O and cannot promise to interrupt an in-flight read.
- [inline:4009501159](https://github.com/delta-io/delta-kernel-rs/pull/3225#discussion_r4009501159) @dengsh12 (reviewer; support): NIT: Would we want to have some small comment stating it's a kernel restriction not protocol restriction(like before)? Feel like generally when we add some kernel specific restriction we add comments Explanation: A Kernel limitation should not be presented as a protocol restriction.
- [inline:3568609887](https://github.com/delta-io/delta-kernel-rs/pull/2860#discussion_r3568609887) @sanujbasu (reviewer; support): Two small things on this doc, both benign but worth tightening. The second `None` condition — "or when neither data stats nor referenced partition columns are available" — can't actually happen through this constructor. That `(None, None)` bail lives in `build_unified_schema_and_expr` and only fires when `physical_stats_schema` is `None`, but here we always compute it and pass `Some(&physical_stats_schema)`, so the stats slot is never `None`. The clause reads true because it's inherited from the underlying `new` contract, but through *this* entry point it sends the reader hunting for a path that doesn't exist. The real `None` return is the `.ok()?` on `build_expected_stats_schemas` (and `new`'s own `predicate?`), which the first clause already covers. I'd just drop the second clause. Separately, since supplying `predicate_partition_schema` here is what newly turns on partition-value pruning for the change-data-feed path, it'd be worth stating the guarantee the callers rely on explicitly: pruning only ever applies to Add rows — Remove/cdc rows (null `add.path`) are never pruned, via the `is_add` guard wired in just below. Right now that invariant is only implicit in the `add.path IS NOT NULL` expression. Explanation: A wrapper documents a None branch reachable in an underlying helper but impossible through this constructor.
- [inline:3567401345](https://github.com/delta-io/delta-kernel-rs/pull/2846#discussion_r3567401345) @dengsh12 (reviewer; support): Seems ` Returns a warning for every column default that violates an IcebergCompatV3 rule` indicates we have no false positive. Wonder if we want to soft the tone here? Explanation: A warning heuristic should not promise zero false positives.

#### document-observable-api-not-internal-plumbing (style)

Keep API documentation centered on observable behavior and caller decisions; place algorithm and implementation rationale beside the implementation.

Scope: Rustdoc summaries and type/function documentation containing internal helper names or step-by-step plumbing.

Exceptions: Implementation details belong in public docs when they materially constrain ownership, complexity, side effects, or usage.

Cited evidence: 3 distinct PRs; 2 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:332` (existing file; unassessed).

The current doc-style section is the authoritative place to strengthen the caller-facing versus maintainer-facing distinction.

Applicable code: `default-engine/src`, `kernel/src`.

Citations:

- [inline:3738562442](https://github.com/delta-io/delta-kernel-rs/pull/3003#discussion_r3738562442) @dengsh12 (reviewer; support): NIT: Feel like we don't need to mention `list_with_delimiter`, generally we describe the behavior not the detail implement at top of struct function. Maybe say `Without it, listing falls back to full directory listing(include direct/indirect children) and bounds the result client-side.`? Explanation: Describe listing behavior rather than the specific helper implementing it.
- [inline:3848263515](https://github.com/delta-io/delta-kernel-rs/pull/3181#discussion_r3848263515) @kyli87 (reviewer; support): This seems like an implementation detail and imo shouldn't be in the doc string. I'd recommend getting rid of these lines and then merging the input/output illustration into the existing `# Examples` section below (which I do think is useful) Explanation: Move implementation detail out of the doc string while preserving useful input/output examples.
- [inline:3640228057](https://github.com/delta-io/delta-kernel-rs/pull/2937#discussion_r3640228057) @dengsh12 (reviewer; support): NIT: Feel like we can omit this caller detail("plain and cancellation-aware reads must emit an identical `JsonReadCompleted`"), just describe the behavior of the fn? Explanation: Caller wiring is not the contract of the metered function.

#### explain-nonlocal-invariants-and-policies (design)

Document non-obvious invariants and asymmetric policies at their owning implementation so later cleanup does not erase intentional behavior.

Scope: Schema-derived sentinel values, shared validation identities, and rules whose rationale is not visible at the call site.

Exceptions: Do not repeat field inventories or obvious syntax; one authoritative explanation plus a link is preferable to copies.

Cited evidence: 3 distinct PRs; 3 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:368` (existing file; unassessed).

Give the existing why-comments rule concrete examples of the explanations a reviewer should preserve or request.

Applicable code: `datafusion-executor/src`, `kernel/src`.

Citations:

- [inline:3860481231](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860481231) @sanujbasu (reviewer; support): `record_add(path, None)` is only correct because the mandatory add-file schema has no `deletionVector` column, so a plain staged add can't carry one. That's a non-local invariant — if `add_files` ever gains DV support, this silently keys the add on `None` and the (path, dvId) check goes half-blind. A one-liner tying the `None` to the schema would protect it: ```rust // Plain staged adds carry no deletion vector (the mandatory add-file schema has no // deletionVector column), so the dv-id is always None here. self.existing_file_actions.record_add(path, None) ``` Explanation: A None value depends on a schema invariant outside the local call.
- [inline:3860552506](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860552506) @sanujbasu (reviewer; support): This is the core of the PR and it's undocumented. The contract is genuinely non-obvious and asymmetric: two adds (or two removes) for a path -> error; add+remove same path *same* dv -> error; but add+remove same path *different* dv -> allowed (that's the DV-update shape). Easy for someone later to "fix" that last case as a bug and break DV updates. A doc block with the rules would help a lot. Aside: the `(path, dv_unique_id)` identity here is the same concept as `FileActionKey` in log_replay -- the dedup *policies* differ so I wouldn't merge them, but it's worth a note that they're the same key idea. Explanation: The asymmetric add/remove policy is easy to accidentally simplify incorrectly.
- [inline:3648732451](https://github.com/delta-io/delta-kernel-rs/pull/2887#discussion_r3648732451) @kyli87 (reviewer; support): I felt like Ryan's prior comment thread helped me understand what was happening here - can we leave a similar code comment to capture why we nest add and then unpack it again here. Explanation: A review explanation of nesting/unpacking should be preserved for future maintainers.
- [inline:3693912544](https://github.com/delta-io/delta-kernel-rs/pull/2972#discussion_r3693912544) @OussamaSaoudi (reviewer; support): huh `named_struct` is so funky. add a comment to explain that it's [name_expr1, val_expr1, name_expr2, val_expr2, ...] Explanation: An external expression encoding needs a concise explanation of its alternating layout.

#### show-small-input-output-examples (style)

Use a small concrete input/output example when terminology or a transformation is difficult to infer from the signature.

Scope: Path suffix semantics, delimiter behavior, and schema/struct transformations.

Exceptions: Skip examples that merely repeat obvious syntax; prefer one canonical runnable example over duplicated setup blocks.

Cited evidence: 2 distinct PRs; 2 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:332` (existing file; unassessed).

Strengthen the example guidance around situations where a small mapping resolves ambiguity.

Applicable code: `default-engine/src`, `kernel/src`.

Citations:

- [inline:3667999261](https://github.com/delta-io/delta-kernel-rs/pull/3003#discussion_r3667999261) @scottsand-db (reviewer; support): Give examples of some inputs and outputs? i.e. just give an example of one input with a trailing /, and another input without a trailing / Explanation: Trailing-slash behavior becomes concrete with two input/output examples.
- [inline:3668223524](https://github.com/delta-io/delta-kernel-rs/pull/3003#discussion_r3668223524) @dengsh12 (reviewer; support): NIT: could we explain what is `delimiter pushdown`? A small example would help Explanation: An unfamiliar optimization term needs a definition and a small example.
- [inline:3974575686](https://github.com/delta-io/delta-kernel-rs/pull/2995#discussion_r3974575686) @dengsh12 (reviewer; support): NIT: This seems not straghtforward, could we have an example of input/output in doc comments? Explanation: A non-obvious struct transform needs an input/output example.

### Other targeted ideas for review

#### comments-explain-intent-once (style)

Use names and one authoritative explanation instead of repeated inventories, temporal notes, or comments restating the test.

Scope: Source comments, test documentation, and duplicated setup examples.

Exceptions: Keep comments that explain a hidden invariant, a non-obvious fixture choice, or an implementation constraint.

Cited evidence: 9 distinct PRs; 5 reviewers; 9 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: unspecified; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:330` (existing file; already-covered).

AGENTS already covers intent, duplication, temporal references, and descriptive test names. Avoid adding overlapping style rules.

Applicable code: `default-engine/src`, `docs/user-guide`, `ffi/src`, `kernel/src`, `kernel/tests`.

Citations:

- [inline:4067058500](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r4067058500) @sanujbasu (reviewer; support): This comment only needs to explain why the registry is required for `event_span`; the test name already states the compact-success/full-error behavior.
- [inline:4432575586](https://github.com/delta-io/delta-kernel-rs/pull/3647#discussion_r4432575586) @scottsand-db (reviewer; support): Please don't repeat `checking only numFiles and tableSizeBytes` 6+ times. Duplicated comments like this should raise alarm bells -- it means when you add more validation you have to update 6+ callsites.
- [inline:4155974942](https://github.com/delta-io/delta-kernel-rs/pull/3416#discussion_r4155974942) @scovich (reviewer; support): This basically duplicates the "getting started" doc, which in turn duplicates the quick-start guide. Can we not just link to one canonical version so we have fewer example Cargo.toml snippets that can drift?
- [inline:4209528052](https://github.com/delta-io/delta-kernel-rs/pull/3503#discussion_r4209528052) @scovich (reviewer; support): This last clause after the comma is a weird "clarification" that smells like an LLM temporal reference. Why would anyone think that disabling metadata filtering would have any semantic impact on the subsequent row-level predicate? Kernel defines data skipping as best-effort and subject to false positives. So even the best data skipping cannot replace the row level predicate to mop up any stragglers.
- [inline:4355266481](https://github.com/delta-io/delta-kernel-rs/pull/3425#discussion_r4355266481) @scottsand-db (reviewer; support): [🤖 👨 ] For test naming, I’d use the scenario or operation followed by the expected result:
- [inline:3693432532](https://github.com/delta-io/delta-kernel-rs/pull/3010#discussion_r3693432532) @aaronlinear (reviewer; support): Maybe unnecessary comment? Overall, is it possible to keep the comments more concise? Explanation: Remove a comment that adds no explanation.
- [inline:3860480649](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860480649) @sanujbasu (reviewer; support): Can we drop the specific `39%` / `10,000`-action numbers from the doc? They'll rot as the code changes, and they belong in the PR description (which already has the perf tables). Minor, but the `39%` here doesn't quite match the ~30% at N=10,000 in the local-FS table either. A qualitative line ("cost grows with the file-action count") is enough here. Explanation: Benchmark-specific counts/percentages are fragile source documentation.
- [inline:4009628701](https://github.com/delta-io/delta-kernel-rs/pull/3325#discussion_r4009628701) @kyli87 (reviewer; support): This feels a bit too much like an AI comment referencing current context, maybe replace this with a TODO to unify these all eventually? Explanation: Replace conversational iteration history with a concrete future task when one exists.
- [inline:3572251158](https://github.com/delta-io/delta-kernel-rs/pull/2846#discussion_r3572251158) @scottsand-db (reviewer; support): `two kinds` --- easy for this number to get out of date btw Explanation: A count of kinds in durable documentation can become stale.

## Design/structure

### Possible additions

#### feature-support-is-operation-specific (correctness)

When declaring a table feature supported, enumerate CREATE, ALTER, and write behavior and verify schema/feature requirements in both directions.

Scope: Feature capability declarations and their validation functions, especially staged implementation of Iceberg compatibility and new schema types.

Exceptions: A feature may be readable before every write operation is implemented; explicitly reject unsupported operations instead of inferring blanket support.

Cited evidence: 4 distinct PRs; 4 reviewers; 5 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `kernel/src/table_features/AGENTS.md` (proposed new file; new).

These operation/capability rules are local to feature maintenance and can inherit repository-wide protocol guidance through a new nested file.

Applicable code: `kernel/src/table_features`.

Inherited guidance: `AGENTS.md`. Keep this proposal specific to its subtree.

Proposed wording:

```markdown
For a feature-support change, map the supported read, CREATE, ALTER, and write operations separately. Check both feature-to-schema and schema-to-feature requirements. Trace callers of the declaration; explicitly reject operations whose required metadata assignment or validation is not implemented.
```

Citations:

- [inline:3919334729](https://github.com/delta-io/delta-kernel-rs/pull/3225#discussion_r3919334729) @dengsh12 (reviewer; support): Aside: Another evidence we want Operation.alter, Operation.create... Every new feature may want to consider if it's supported for create/alter Explanation: Each feature addition raises separate CREATE versus ALTER support decisions.
- [inline:3929778127](https://github.com/delta-io/delta-kernel-rs/pull/3225#discussion_r3929778127) @DrakeLin (reviewer; support): Marking V2 supported exposes ALTER, but ALTER assigns only flat IDs. Adding an Array or Map can commit without required element/key/value IDs; block active V2 ALTER until nested-ID assignment is implemented. Explanation: Declaring support exposes ALTER even though nested-ID assignment is not implemented.
- [inline:3647481565](https://github.com/delta-io/delta-kernel-rs/pull/2931#discussion_r3647481565) @sanujbasu (reviewer; support): so !table feature => no geo type in schema geo type in schema => has table feature. Where is the latter? Explanation: A schema type also needs the corresponding feature declaration, not just validation in the opposite direction.
- [inline:3567254978](https://github.com/delta-io/delta-kernel-rs/pull/2845#discussion_r3567254978) @dengsh12 (reviewer; support): Just want to confirm: Are we sure we want to write to those legacy table, or just allow reading those legacy table? Explanation: Reading legacy featureless tables and creating/writing them are separate compatibility decisions.
- [inline:3572204250](https://github.com/delta-io/delta-kernel-rs/pull/2846#discussion_r3572204250) @scottsand-db (reviewer; support): So we only do this on appends? not on CREATE or ALTER table? is that intentional? Explanation: The earliest part of the window already asks whether validation covers CREATE and ALTER as well as append.

#### make-fallback-and-enum-coverage-explicit (correctness)

Require explicit decisions where falling through an enum or type classification can silently skip validation or discard work.

Scope: Operator-effect classification and data representations controlling validation.

Exceptions: An intentional conservative default for unknown external values may be appropriate; document and test that fallback rather than imposing exhaustiveness on open-ended input.

Cited evidence: 3 distinct PRs; 3 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:267` (existing file; new).

The current equality-versus-matches guidance is about spelling; these comments expose a separate correctness concern about silent defaults.

Applicable code: `kernel/src`.

Proposed wording:

```markdown
When a closed enum controls validation or whether work can be eliminated, prefer an exhaustive match without a wildcard so new variants require an explicit decision. For open-ended inputs, verify that unsupported representations take an intentional conservative fallback rather than silently bypassing required checks.
```

Citations:

- [inline:4410013850](https://github.com/delta-io/delta-kernel-rs/pull/3735#discussion_r4410013850) @sanujbasu (reviewer; support): This is fail-open for future operators: ```rust matches!(self, Self::Assert(_) | Self::WriteJson(_)) ``` A newly added side-effecting operator compiles as `false` if its author forgets this list, which lets the absent-input join simplifications drop it. Could this be an exhaustive `match` with `Assert | WriteJson => true` and every current non-side-effecting variant listed under `false`, without a wildcard? Then adding any operator forces an explicit side-effect decision at compile time.
- [inline:4516792895](https://github.com/delta-io/delta-kernel-rs/pull/3779#discussion_r4516792895) @kyli87 (reviewer; support): You could just do an exhaustive match on MetricEvent instead of using matches! macro
- [inline:4428728777](https://github.com/delta-io/delta-kernel-rs/pull/3717#discussion_r4428728777) @dengsh12 (reviewer; support): Wonder if we want to support dictionary-encoded strings here as well? Currently, they fall through to `None` and skip checksum validation.

#### checkpoint-format-is-not-filename (correctness)

Check protocol-defined checkpoint contents independently from filename shape.

Scope: Classic-named checkpoints can use V1 or V2 contents.

Exceptions: Filename parsing still identifies checkpoint parts and naming shape; it does not by itself determine the checkpoint content version.

Cited evidence: 1 distinct PRs; 1 reviewers; 1 discussions. Recurring: False. Opposing PRs cited: 0. State: idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `kernel/src/checkpoint/AGENTS.md` (proposed new file; new).

This is a checkpoint-specific protocol pattern. Propose local guidance that inherits root AGENTS.md rather than adding a subsystem detail to repository-wide rules. The downstream agent should verify the protocol interpretation before integration.

Applicable code: `kernel/src/checkpoint`.

Inherited guidance: `AGENTS.md`. Keep this proposal specific to its subtree.

Proposed wording:

```markdown
When classifying checkpoint versions, check protocol-defined contents independently from filename shape. Classic checkpoint filenames can contain either V1 or V2 checkpoint content; keep filename-shape and content-version decisions separate.
```

Citations:

- [inline:4532290093](https://github.com/delta-io/delta-kernel-rs/pull/3831#discussion_r4532290093) @OussamaSaoudi (reviewer; support): unfortunately incorrect. v2 checkpoints can be named the same way as v1 checkpoints. https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-checkpoint-table-feature

#### snapshot-diagnostics-without-success-overhead (design)

Keep useful failure diagnostics while avoiding large captures and clones on successful snapshot construction.

Scope: Snapshot tracing spans and fallible resolution of log segments and metadata.

Exceptions: Some failure paths require richer context; bound it deliberately and verify what parent spans/exporters already provide.

Cited evidence: 1 distinct PRs; 1 reviewers; 3 discussions. Recurring: False. Opposing PRs cited: 0. State: idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `kernel/src/snapshot/AGENTS.md` (proposed new file; new).

Snapshot-specific tracing and ownership constraints fit a nested file instead of a repository-wide logging rule.

Applicable code: `kernel/src/snapshot`.

Inherited guidance: `AGENTS.md`. Keep this proposal specific to its subtree.

Proposed wording:

```markdown
For snapshot tracing changes, inspect successful and failed loads separately. Avoid capturing or cloning full snapshots/log segments solely for success-path instrumentation. Preserve sufficient failure context and check parent/exporter coverage. Prefer a guard against heavy fields over an exact whitelist of all allowed compact fields.
```

Citations:

- [inline:3965459034](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r3965459034) @sanujbasu (reviewer; support): Could we validate that error diagnostics remain sufficient after removing these fields? `#[instrument(err)]` emits the returned error but does not add skipped arguments. For early fresh-load failures, `version` is still empty because it is recorded only after protocol/metadata loading succeeds; the incremental span records a version but no path. Errors such as `MissingMetadata` or JSON parse failures may therefore lack the segment version or offending file. Could we keep `skip_all` and emit bounded failure-only context—phase, path, relevant versions, and the offending file when known? A tracing test could verify that this context appears on failure without capturing the large arguments on success. Do existing exporters already recover enough context from the parent `snap.build` span and correlation ID? Explanation: Removing captured arguments must not silently remove needed failure context.
- [inline:3997318004](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r3997318004) @sanujbasu (reviewer; support): Could we avoid this O(files) success-path clone by making the fallible resolution step borrow the heavy inputs and moving `log_segment` only after it succeeds? ```rust let (table_configuration, crc) = Self::resolve_pm( &location, &log_segment, engine, &metric_context, incremental_replay, ) .inspect_err(|error| { error!( %error, ?location, ?log_segment, ?metric_context, ?incremental_replay, built_as_latest, "failed to construct snapshot from log segment" ); })?; Self::new_with_crc(log_segment, table_configuration, crc, built_as_latest) ``` This preserves the full failure dump for protocol/metadata and configuration failures without retaining a second `LogSegment` on every successful load. `new_with_crc` remains a narrow consuming failure boundary; if its dump is required too, its CRC validation could happen during preparation before the final move. The incremental `log_tail` case may need separate treatment because the merge consumes it. Explanation: Borrow heavy inputs across fallible work rather than cloning them on every successful load.
- [inline:3997325675](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r3997325675) @sanujbasu (reviewer; support): Could we broaden this regression test into a guardrail for the other snapshot spans that rely on `skip_all`, particularly `snap.build` and one `&self` `Snapshot` method? If either annotation is changed to partial `skip(...)`, tracing will auto-capture `self`; its `Debug` reaches `existing_snapshot`/`log_tail` or the full snapshot `log_segment`. The current name filter silently ignores those spans even though the test executes `build()`. A denylist check for heavy fields such as `self`, `existing_snapshot`, `log_segment`, `log_tail`, `snapshot`, and `engine` across the relevant snapshot spans would catch that regression without requiring an exact-set update whenever a legitimate compact field is added. Explanation: A regression guard can forbid heavy success-path fields without freezing the exact set of legitimate compact fields.

### Possible changes/removals

#### remove-unhelpful-helper-layers (design)

Remove wrappers and optional machinery that add navigation or configuration without a distinct responsibility.

Scope: Chains of near-identical helpers, single-use wrappers, and macro options every caller supplies identically.

Exceptions: Retain helpers that name a real invariant, own unsafe/lifetime obligations, or remove repeated nontrivial logic.

Cited evidence: 5 distinct PRs; 4 reviewers; 5 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md:26` (existing file; strengthen).

The existing abstraction guidance does not explicitly ask the reviewer to price the navigation cost of helper chains.

Applicable code: `datafusion-executor/src`, `delta-kernel-unity-catalog/src`, `kernel/src`, `kernel/tests`, `workloads/src`.

Existing wording to change:

```markdown
- **Speculative generality.** Is new generality justified by a second caller that exists? Name the second caller or call it bloat. Type parameters, trait objects, and config knobs with one instantiation are guilty until proven innocent.
```

Proposed wording:

```markdown
Inspect helper chains and optional forms with one actual use. Suggest collapsing layers when it removes indirection or duplicated logic; do not replace a clear local operation with another wrapper merely to reduce line count.
```

Citations:

- [inline:4478666149](https://github.com/delta-io/delta-kernel-rs/pull/3749#discussion_r4478666149) @Fokko (reviewer; support): We have quite a few of these methods now: `find_commit_cover`, `find_commit_cover_after`, `find_commit_cover_paths`, `find_commit_cover_paths_in_range` and `commit_cover_paths_using`.
- [review:5634891975](https://github.com/delta-io/delta-kernel-rs/pull/3534#pullrequestreview-5634891975) @kyli87 (reviewer; support): Still have some more questions about the code.
- [inline:4149763795](https://github.com/delta-io/delta-kernel-rs/pull/3450#discussion_r4149763795) @OussamaSaoudi (reviewer; support): single use function, let's just inline
- [inline:4329931757](https://github.com/delta-io/delta-kernel-rs/pull/3596#discussion_r4329931757) @scottsand-db (reviewer; support): [🤖 🧑] Can we remove the optional => $handler form? All three forward_methods! invocations forward directly through self.inner. We can drop $(=> $handler:ident)? from the matcher and simplify the generated call to self.inner.$method($($arg),*).
- [inline:3716624101](https://github.com/delta-io/delta-kernel-rs/pull/2972#discussion_r3716624101) @OussamaSaoudi (reviewer; support): Im not convinced we need this PatchValue abstraction. I'd rather we have a closure or just inline the logic we need for computing expressions/colmuns. Explanation: The reviewer questions a PatchValue abstraction with no necessary independent responsibility.
- [inline:3658989026](https://github.com/delta-io/delta-kernel-rs/pull/2855#discussion_r3658989026) @scottsand-db (reviewer; context): Can this be a helper function? In general -- keep the key, main production functions clean and simple. delegate multi-line trivial logic (extracting a file name) to helper functions Explanation: A helper is useful when it keeps the main operation readable; single-use is not by itself a reason to inline.
- [inline:4009213676](https://github.com/delta-io/delta-kernel-rs/pull/3318#discussion_r4009213676) @emkornfield (reviewer; context): nit: this method is 90 lines long, I wonder if there are sensible places that we could add helpers for to make the core test more compact? Explanation: A long test can benefit from helpers that expose the substantive flow.

#### reuse-existing-state (design)

Extend the existing representation before introducing overlapping types or recomputing already-owned state.

Scope: New structs, enums, cache representations, or parameters carrying facts another argument already owns.

Exceptions: Keep distinct types when they enforce different invariants, visibility, ownership, or FFI lifetimes; compare semantics before deduplicating.

Cited evidence: 5 distinct PRs; 4 reviewers; 5 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md:26` (existing file; strengthen).

The architecture prompt already rejects duplicated concepts; make the check concrete enough to catch overlapping state before proposing another abstraction.

Applicable code: `default-engine/src`, `kernel/src`.

Existing wording to change:

```markdown
- **Duplicated concepts.** Does this introduce a concept that already has a home in the crate under a different name? Two names for one concept costs more than either name being imperfect.
```

Proposed wording:

```markdown
For each new type or stored field, find the closest existing representation. Flag immediately packed/unpacked wrappers and duplicated derived state when reusing the existing owner would preserve the required invariants. Cite both representations and sketch the smaller change.
```

Citations:

- [inline:4532251372](https://github.com/delta-io/delta-kernel-rs/pull/3831#discussion_r4532251372) @OussamaSaoudi (reviewer; support): This seems very off. Why isn't this info part of CheckpointShape instead? CheckpointShape should be derived from whatever Snapshot already has.
- [review:5397615978](https://github.com/delta-io/delta-kernel-rs/pull/3426#pullrequestreview-5397615978) @OussamaSaoudi (reviewer; support): My main feedback is to reduce the number of new types and structs. PvResolution, CheckedPmResolution, PmCandidate, ResolvedtableConfiguration, all have very similar types, and don't contribute much in terms of functionality, nor do they do much to hide fields since they're immediately packed/unpacked.
- [inline:4201455731](https://github.com/delta-io/delta-kernel-rs/pull/3372#discussion_r4201455731) @nicklan (reviewer; support): Okay, maybe, but why does that matter? We _already_ have a `HashMap` for `metadata_columns` so schemas don't have stable serialization today and we haven't found that to be an issue. Mostly this feels like adding a new type we use for no clear benefit. I'd suggest a HashMap and a follow-up issue to tackle deterministic schema serialization if needed.
- [inline:4354043021](https://github.com/delta-io/delta-kernel-rs/pull/3099#discussion_r4354043021) @sanujbasu (reviewer; support): `table_schema` is cloned out of `read_configuration`, and then both values are passed into `LogReplayScanner::try_new`. They must remain synchronized, but the signature allows a future caller to provide a mismatched pair.
- [inline:3668545717](https://github.com/delta-io/delta-kernel-rs/pull/3003#discussion_r3668545717) @dengsh12 (reviewer; support): Wonder if we really want this, seems we always have `object_store` + `Option<paginated>` in real callers, what about just ``` pub struct EngineStore { object_store: Arc<DynObjectStore>, paginated: Option<Arc<dyn PaginatedListStore>>, } ``` Then remove the trait `ListingStore`? Explanation: The existing object store plus optional paginated capability can replace an extra trait/wrapper.

#### errors-identify-the-failed-condition (correctness)

Use the established error category and diagnostic context for the condition that actually failed, rather than a generic or misleading explanation.

Scope: Schema errors, file-action validation, overflow, and incompatible partition layouts.

Exceptions: Preserve intentional public error contracts; do not expose unstable internal details merely to make a message longer.

Cited evidence: 4 distinct PRs; 4 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md:52` (existing file; unassessed).

Teach reviewers to compare the predicate that fails with the returned error and the context callers need to diagnose it.

Applicable code: `ffi/src`, `kernel/src`.

Citations:

- [inline:3648662373](https://github.com/delta-io/delta-kernel-rs/pull/2999#discussion_r3648662373) @chiinlquah (reviewer; support): Instead of Unsupported, can we instead use Error::Schema for "type that kernel doesnt support". > /// Schema mismatch has occurred or invalid schema used somewhere unknown types should be a subset of invalid schema Explanation: Unsupported schema types should use the schema error category.
- [inline:3860552622](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860552622) @sanujbasu (reviewer; support): Two things. The message says "same deletion vector ID" even when neither side has a DV (a plain add + remove of the same path, both `None`) -- confusing, since there's no deletion vector anywhere. And the `require!` block is byte-identical between `record_add` and `record_remove`, so a wording fix has to be made twice. I'd factor the mirror bodies into one helper (pass both maps in to sidestep the borrow) and reword. Also `Error::invalid_transaction_state` reads better than `generic` here and matches the blind-append / append-only checks. Explanation: A no-DV collision should not claim a deletion-vector ID; reuse the typed invalid-state error.
- [inline:3973238939](https://github.com/delta-io/delta-kernel-rs/pull/3222#discussion_r3973238939) @nicklan (reviewer; support): I think the error is wrong here? In fact a checkpoint MUST be followed by a commit. :) this will happen if `checked_add` fails, which means it overflowed. So I guess the error should be "checkpoint version was MAX_INT" or something like that Explanation: checked_add overflow does not mean the protocol forbids a commit after a checkpoint.
- [inline:3928812499](https://github.com/delta-io/delta-kernel-rs/pull/3099#discussion_r3928812499) @dengsh12 (reviewer; support): Aha, I guess error message will not cause compatibility issue with spark? If we include partition cols into the error, future debug may be easier. But lmk if there are some specific concerns on spark compatibility Explanation: Partition information makes an incompatibility error actionable.

#### place-configuration-with-its-owning-lifecycle (design)

Attach configuration and derived views to the object or lifecycle stage that owns the decision, avoiding independent knobs that can drift apart.

Scope: Behavior booleans passed identically at every call, scan-derived schemas, and duplicated constructors.

Exceptions: A separate argument is appropriate for a truly per-call decision; do not force a consuming builder where real mutable-owner call sites cannot use it.

Cited evidence: 3 distinct PRs; 2 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md:26` (existing file; unassessed).

Make the architecture reviewer trace the owner of each configuration decision before proposing another flag or wrapper.

Applicable code: `default-engine/src`, `kernel/src`, `kernel/src/scan`, `kernel/src/transaction`.

Citations:

- [inline:3860480321](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860480321) @sanujbasu (reviewer; support): +1 to this. Every caller in the diff ends up passing `false`, and each one needs a `/* skip_duplicate_validation */` comment to stay readable — that's the tell it's in the wrong place. Whether to validate is a property of how the transaction was assembled, not of `commit` itself. A `without_duplicate_file_action_validation()` builder method (default on) keeps `commit(engine)` unbroken and mirrors `without_row_transforms`. It's nearly free too, since `FileActionTracker::without_duplicate_validation()` already exists — store one bool on the transaction and read it in `commit`. That also lets FFI opt out later without touching the C ABI. Explanation: An all-false commit flag with inline labels belongs to transaction assembly, not each commit invocation.
- [inline:3992171706](https://github.com/delta-io/delta-kernel-rs/pull/3308#discussion_r3992171706) @dengsh12 (reviewer; support): Codex suggested providing a `scan::stats_output_schemas` instead of this. Seems making sense? It avoid the case that someone pass X to `expected_stats_schemas` then pass Y to `ScanBuilder`. But I'm unsure if someone wants to have the stats schema before scan -- defer the choice to you Explanation: Deriving output schema from Scan avoids passing inconsistent configurations to separate APIs.
- [inline:3738539980](https://github.com/delta-io/delta-kernel-rs/pull/3003#discussion_r3738539980) @dengsh12 (reviewer; support): > An alternative could be new(store: impl Into<EngineStore>) with impl From<Arc<DynObjectStore>> for EngineStore for smaller blast radius but less explicit. Good idea! Seems it makes sense? As what the `DefaultEngineBuilder` really needs is `EngineStore`, I would recommend one `new(store: impl Into<EngineStore>)` over two similar constructor ``` pub fn new(object_store: Arc<DynObjectStore>) pub fn from_engine_store(store: EngineStore) -> Self ``` Explanation: One constructor accepting the actual owned abstraction can replace overlapping constructors.
- [inline:3692727176](https://github.com/delta-io/delta-kernel-rs/pull/3010#discussion_r3692727176) @arnav-databricks (author; context): I tested this out ``` struct Writer { acc: Acc } impl Writer { fn write_row_group(&mut self, b: u8) -> Result<(), String> { self.acc = self.acc.merge(b)?; } } error[E0507]: cannot move out of `self.acc` which is behind a mutable reference | 12 | self.acc = self.acc.merge(b)?; | ^^^^^^^^ -------- `self.acc` moved due to this method call | note: `Acc::merge` takes ownership of the receiver `self`, which moves `self.acc` ``` then, in an attempt to resolve that, we would hypothetically use the option + take, resulting in ``` self.acc = Some(self.acc.take().unwrap().merge(b)?); ? returns before the reassignment. On a failed merge the field is left None. I ran it: thread 'main' panicked at d.rs:15: called `Option::unwrap()` on a `None` value ``` I've taken your previous comments, so we won't be testing for poisoned every time, so that motivation would be resolved. just couldn't get this to compile though Explanation: A consuming accumulator redesign was rejected because it cannot be moved from its mutable owner safely.

#### preserve-lazy-dataflow-resource-bounds (design)

Before simplifying an iterator or batch pipeline, trace which work becomes eager, duplicated, or discarded and preserve its intended memory and I/O bounds.

Scope: Log replay, listing offsets, schema transforms, and conversions over complete action sets.

Exceptions: Materialization can be justified by bounded inputs or measured reuse; record the tradeoff and avoid unrelated optimizations.

Cited evidence: 3 distinct PRs; 2 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md:26` (existing file; unassessed).

Add a resource-cost check to structural simplification so shorter code does not silently change streaming behavior.

Applicable code: `default-engine/src`, `kernel/src`, `kernel/src/scan`, `kernel/src/table_changes`.

Citations:

- [inline:3779279633](https://github.com/delta-io/delta-kernel-rs/pull/3030#discussion_r3779279633) @dengsh12 (reviewer; support): I'm a little concerned about the memory... Seems in the past we did two json reads(prepare, scan) to save memory, here we read all jsons into memory which defeats the purpose? If we are really allowed to read all jsons into memory, seems here we don't need two json reads? Just read once and reuse will save half of the memory and I/O Explanation: Reading all JSON into memory defeats the purpose of the prior two lazy reads.
- [inline:3668425642](https://github.com/delta-io/delta-kernel-rs/pull/3003#discussion_r3668425642) @dengsh12 (reviewer; support): Wonder if we can just use `store.list_with_offset`? `list_with_delimiter` will start from the beginning of the subdir and I'm a little concern about the perf. E.g. the subdir has 0 - 1m.json, to visit 1m.json, we need to list from 0.json now. If we just use `store.list_with_offset`, and early break when we exit the subdir, seems the perf will be better? Explanation: Starting a listing at the beginning instead of the requested offset changes work from late-position to full-directory scanning.
- [inline:4010491422](https://github.com/delta-io/delta-kernel-rs/pull/2995#discussion_r4010491422) @OussamaSaoudi (reviewer; support): this is adding an O(|actions|) copy/evaluation. Is there any way to merge this transform into `get_add_transform_expr`? Explanation: A new transform adds work over every action and may belong in an existing pass.
- [inline:4030717175](https://github.com/delta-io/delta-kernel-rs/pull/3030#discussion_r4030717175) @dengsh12 (reviewer; support): In the past we read the json commits twice so that we don't need to read the whole jsons into memory. Here we read the whole json commit into memory... can we impl in a lazy way? I think we can - lazily read the json twice(let call the two reads `r1`, `r2`) - parallelize the read by passing batched json logs into `r1` and `r2` Happy to further chat! Explanation: The later reply reiterates the need to consume batched reads lazily.

#### validate-invariants-at-entrypoints (correctness)

Make required validation part of the entry point that establishes the invariant, and share the same checks across alternate construction paths.

Scope: Validator constructors, hinted versus regular snapshot construction, and CREATE versus existing-table write paths.

Exceptions: Keep validation optional only where the API explicitly accepts untrusted/unvalidated state and names the obligation.

Cited evidence: 3 distinct PRs; 3 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md:26` (existing file; unassessed).

Strengthen architectural checks for invariants that rely on callers remembering a second operation.

Applicable code: `kernel/src`, `kernel/src/snapshot`, `kernel/src/transaction`.

Citations:

- [inline:3860474655](https://github.com/delta-io/delta-kernel-rs/pull/3150#discussion_r3860474655) @sanujbasu (reviewer; support): Then construction and validation should be one operation. Don’t expose a validator that callers can construct without running. ``` impl StagedDataValidator { pub(crate) fn validate_remove_files( removes: &[FilteredEngineData], file_actions: &mut FileActionTracker, ) -> DeltaResult<()> { let validator = Self::staged_remove_file(file_actions)?; validator.validate_filtered(removes) } } ``` Usage: ``` StagedDataValidator::validate_remove_files( &self.remove_files_metadata, &mut file_actions, )?; ``` An even cleaner design is a rule-specific free function: ``` pub(crate) fn validate_remove_files( removes: &[FilteredEngineData], file_actions: &mut FileActionTracker, ) -> DeltaResult<()> { StagedDataValidator::staged_remove_file(file_actions)? .validate_filtered(removes) } ``` Then make staged_remove_file() and validate_filtered() private. This enforces the invariant through the API: callers can request validation, but cannot obtain an unused validator. This applies to all validators? Explanation: Construction of a validator without executing it should not be the public operation.
- [inline:3971967137](https://github.com/delta-io/delta-kernel-rs/pull/3222#discussion_r3971967137) @chiinlquah (reviewer; support): these checks should be exactly identical to the regular construction path right? Can we extract and share the validation? Explanation: Hinted construction should share validation with regular construction.
- [inline:3929778120](https://github.com/delta-io/delta-kernel-rs/pull/3225#discussion_r3929778120) @DrakeLin (reviewer; support): V2 write validation covers existing-table transactions, but CREATE still uses TableConfiguration context and can commit forbidden type changes such as integer → double. Run completed V2 write validation for CREATE as well. Explanation: CREATE must not bypass the feature validation applied to other writes.

#### engine-interfaces-hide-storage-layout (design)

Keep logical Engine interfaces independent of Arrow-specific storage layout and terminology; localize physical details to the implementation.

Scope: EngineData collection accessors and engine-agnostic kernel code.

Exceptions: An explicitly Arrow-only adapter may expose Arrow layout; the abstraction boundary must make that restriction clear.

Cited evidence: 2 distinct PRs; 2 reviewers; 2 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md:26` (existing file; unassessed).

Add an abstraction check for implementation details that would prevent another engine representation from conforming naturally.

Applicable code: `datafusion-executor/src`, `kernel/src`, `kernel/src/engine`, `kernel/src/engine_data.rs`.

Citations:

- [inline:3754425686](https://github.com/delta-io/delta-kernel-rs/pull/3072#discussion_r3754425686) @nicklan (reviewer; support): This feels.... wrong. I don't think offset based indexing is fundamental to how an array of structs is stored. It's more of an artifact of how arrow happens to do it. Wouldn't it make more sense for a `StructArrayAccessor` to have a method like `visit_struct(&self, data: &EngineData, column_names: &[ColumnName], visitor: &mut dyn RowVisitor)`? So we again abstract over _how_ the struct data is stored, and just say we'll let you visit it. Then the arrow based impl can use offsets, but a different system that doesn't work with offsets can do things however it likes. Explanation: Offset-based indexing is an Arrow artifact, not a fundamental array-of-structs API.
- [inline:3762400578](https://github.com/delta-io/delta-kernel-rs/pull/3072#discussion_r3762400578) @emkornfield (author; context): Yeah, agreed updated to something that I think is more sane. Sorry I missed this at first, I though I localized the range change to the arrow missed that it was part of EngineData. Explanation: The author confirms the storage detail leaked into EngineData and describes changing it.
- [inline:3640167040](https://github.com/delta-io/delta-kernel-rs/pull/2937#discussion_r3640167040) @dengsh12 (reviewer; support): NIT: The code is in kernel not defaultengine, could we keep the comment engine-agonostic? Explanation: Comments in kernel code should remain engine-agnostic.

#### visibility-follows-real-consumer-needs (design)

Choose API visibility and accessors from actual consumer needs and exposed invariants rather than applying a blanket private-or-public rule.

Scope: Information structs, internal-api gates, and new getters introduced solely to route around an existing method.

Exceptions: Plain data may legitimately be public; exposing it should still account for return-type visibility and compatibility.

Cited evidence: 2 distinct PRs; 2 reviewers; 2 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: change; final integration belongs to the downstream agent.

Thread context: The early request for internal-api reuse was superseded by agreement to expose the information publicly. This is a qualification to simplistic encapsulation rules, not evidence that every getter should be hidden.

Integration target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md:120` (existing file; unassessed).

Qualify visibility guidance with the observed case where additional internal getters created churn and public information was the simpler agreed result.

Applicable code: `kernel/src`, `kernel/src/committer`, `kernel/src/scan`.

Citations:

- [inline:3883240685](https://github.com/delta-io/delta-kernel-rs/pull/3201#discussion_r3883240685) @rliao147 (reviewer; context): @arnav-databricks you should make this fn [internal-api] instead of introducing the new getters. It already computes the metadata for you. Explanation: The first suggestion prefers reusing an existing effective-metadata method over new getters.
- [inline:3884198727](https://github.com/delta-io/delta-kernel-rs/pull/3201#discussion_r3884198727) @scottsand-db (reviewer; support): Sorry for the disagreement with @rliao147 and the resultant churn for you. Let's just make this public. It's just a struct of information, this isn't some fancy internal feature. I don't want to have to bother with another PR in the future that has to make this public 😅 Explanation: The later resolution explicitly favors exposing the information struct publicly.
- [inline:3884270411](https://github.com/delta-io/delta-kernel-rs/pull/3201#discussion_r3884270411) @rliao147 (reviewer; context): Ack, let's make it public. Sorry about the churn @arnav-databricks. Explanation: The other reviewer acknowledges the public resolution.
- [inline:3648704015](https://github.com/delta-io/delta-kernel-rs/pull/2887#discussion_r3648704015) @kyli87 (reviewer; support): Make this public so it can be consumed? I assume this will be the main entry point for getting a scan plan? Explanation: A consumer-facing scan entry point needs public visibility.

### Other targeted ideas for review

#### keep-pr-concerns-focused (design)

Separate independent preparatory cleanup or FFI plumbing when it makes the substantive change easier to review.

Scope: Large changes mixing renames, API reorganization, core logic, and independently reviewable FFI work.

Exceptions: Do not defer a required correctness fix or create nonfunctional intermediate changes just to reduce diff size.

Cited evidence: 5 distinct PRs; 5 reviewers; 5 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: unspecified; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md:110` (existing file; already-covered).

The maintainer prompt already states the relevant preference. Preserve this as existing guidance rather than adding a duplicate to AGENTS.md.

Applicable code: `ffi/src`, `ffi/tests`, `kernel/src`.

Citations:

- [review:5805271107](https://github.com/delta-io/delta-kernel-rs/pull/3832#pullrequestreview-5805271107) @emkornfield (reviewer; support): A few high level comments: 1. Lets try to keep reviews small and focused as possible. I think for this PR it would be easy to at least separate out FFI from the core logic changes. 2. I think it might be worthwhile to do a prefactor to see if we can share code between Create and update paths, it seems like there are already some inconsistencies here CC @scottsand-db to check if this makes sense.
- [review:5616740587](https://github.com/delta-io/delta-kernel-rs/pull/3150#pullrequestreview-5616740587) @scottsand-db (reviewer; support): Thanks for this PR. Overall, I think that structs + trait + fn name + variable names have gotten confusing, and I suggest we do a prefactor PR that simplifies and improves naming to make things clearer and more readable. Here is my suggestion below
- [review:5375997219](https://github.com/delta-io/delta-kernel-rs/pull/3470#pullrequestreview-5375997219) @Fokko (reviewer; support): To clarify, I figured breaking out the `MODIFIED` in its own PR would allow us to get it in right away, since it non-contraversial. This way we can simplify the big PRs
- [inline:3970270505](https://github.com/delta-io/delta-kernel-rs/pull/3222#discussion_r3970270505) @chiinlquah (reviewer; support): these new validations seem like scope creep, can we separate them out. I think they'll require different set of eyes as they modify validation for existing tables/workloads as well. Explanation: Separate changed validation semantics needing a different review from the primary feature.
- [inline:4029157812](https://github.com/delta-io/delta-kernel-rs/pull/3332#discussion_r4029157812) @kyli87 (reviewer; support): Are these new .expected files intentional? they seem unrelated to this change? Explanation: Unrelated expected-data churn makes the change harder to assess.

#### simplify-local-control-flow (style)

Use direct local control flow or construct shared values once when branches merely repeat the same operation.

Scope: Duplicate-check validation, avoidable intermediate bindings, and repeated calls with one differing option.

Exceptions: Keep named intermediates when they expose an invariant, clarify ownership, or make a complex computation easier to read.

Cited evidence: 3 distinct PRs; 3 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: unspecified; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md:52` (existing file; already-covered).

The maintainer prompt already states the relevant preference. Preserve this as existing guidance rather than adding a duplicate to AGENTS.md.

Applicable code: `ffi/src`, `kernel/src`.

Citations:

- [inline:4197484927](https://github.com/delta-io/delta-kernel-rs/pull/3174#discussion_r4197484927) @scottsand-db (reviewer; support): ```rust for metadata in &self.state.domain_metadata_additions { let domain = metadata.domain();
- [inline:4197449743](https://github.com/delta-io/delta-kernel-rs/pull/3174#discussion_r4197449743) @scottsand-db (reviewer; support): ```rust let mut removals_seen = HashSet::new();
- [inline:4322345301](https://github.com/delta-io/delta-kernel-rs/pull/3670#discussion_r4322345301) @sanujbasu (reviewer; support): Could we make this helper private and construct the options once using the existing `FoldWithOption` import? Both branches duplicate the same `Expression::map_to_struct` call, and the helper has only one production caller:
- [inline:4462083333](https://github.com/delta-io/delta-kernel-rs/pull/3758#discussion_r4462083333) @nicklan (reviewer; support): I think all the places you have this pattern of `let error_allocators = unsafe { *error_allocators };` can be simplified to:

#### ffi-representation-follows-ownership (design)

Choose borrowed C-compatible views or owned handles from the actual retention lifetime, rather than adding ownership and release steps to call-scoped data.

Scope: FFI visitor payloads, optional input pointers, and values sent from engine code back into Rust.

Exceptions: Use an owned handle when data must outlive the call; raw pointers still require explicit validity and ABI-compatibility contracts.

Cited evidence: 2 distinct PRs; 1 reviewers; 3 discussions. Recurring: True. Opposing PRs cited: 0. State: idea.

Suggested action: unspecified; final integration belongs to the downstream agent.

Integration target: `ffi/AGENTS.md:367` (existing file; already-covered).

The existing nested FFI guidance owns this pattern and already contains the relevant ownership rules; retain the mined examples for emulation and possible clarification.

Applicable code: `ffi/src`, `ffi/examples`.

Citations:

- [inline:4031814128](https://github.com/delta-io/delta-kernel-rs/pull/3296#discussion_r4031814128) @kyli87 (reviewer; support): I think using a Handle and then requiring the callback to free it explicitly is overkill and error prone. Since the lifetime of the options is always scoped to the single invocation of a visitor function, we can just use a borrowed object (similar to how KernelStringSlice is handled). So concretely, I think that boils down to: - Defining a new repr(c) equivalent of MapToStructOptions such as `FfiMapToStructOptions` - That struct contains a KernelStringSlice representing the configured timezone - The visitor callbacks can take a `*const FfiMapToStructOptions` pointer and then directly extract the timezone from it as needed Then that struct can be re-used by the kernel_visitor stuff as well. Explanation: Visitor-scoped options can be borrowed instead of requiring the callback to free a handle.
- [inline:4031838506](https://github.com/delta-io/delta-kernel-rs/pull/3296#discussion_r4031838506) @kyli87 (reviewer; support): So this is for engine to pass a map-to-struct to kernel. I think the `Handle` approach doesnt work here because Engine would need some way of constructing the Handle. Handle is usually Rust allocated. Explanation: An engine cannot construct an opaque Rust-owned handle without an allocation API.
- [inline:4029713351](https://github.com/delta-io/delta-kernel-rs/pull/3332#discussion_r4029713351) @kyli87 (reviewer; support): Options are mildly annoying over FFI, and we usually don't use references but rather pointers. Could we just have these all take in a `*const EngineMetadata` pointer instead? And Rust provides an `as_ref` method to convert the pointer into an `Option<&T>`: https://doc.rust-lang.org/std/primitive.pointer.html#method.as_ref Explanation: A pointer can carry the documented optional-input contract and be converted after validation.
