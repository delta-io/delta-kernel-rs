# Delta Kernel User Guide: Writing Standards

This file governs the mdBook user guide. Every page is a Markdown file in
`src/`. The audience is Rust developers building Delta Lake connectors. Assume
Rust competence (ownership, traits, async, error handling) but NOT Delta Lake
expertise. Never explain Rust concepts. Always define Delta concepts
(transaction log, checkpoint, action types, etc.) on first use.

## Quick reference

```
Voice:      second person, present tense, active, contractions OK
Headings:   sentence case (exception: type names like "The Engine trait")
Dashes:     never use "--" in prose; restructure into two sentences
Code:       MUST compile via mdbook test
Output:     MUST show expected output after code that produces it
Errors:     prefer ? over .unwrap(); hidden boilerplate returns DeltaResult
Terms:      Snapshot, Scan, Transaction, Engine, connector, predicate
Dataset:    Alice/30/Seattle, Bob/25/Portland, Carol/35/Denver
Links:      relative paths, link first occurrence per section only
Scope:      teach Kernel API usage, not Delta protocol internals
```

## Build and test commands

Run from `docs/user-guide/`:

```sh
mdbook serve         # local hot-reload preview at localhost:3000
make test            # compile every Rust code block in the guide
mdbook build         # render to HTML; also runs linkcheck if mdbook-linkcheck2 is installed
```

First-time setup (matches CI):

```sh
cargo install --locked mdbook mdbook-mermaid mdbook-linkcheck2
```

CI runs `make test` and `mdbook build` (with linkcheck) on every push, PR,
and merge group event. See `.github/workflows/user-guide.yml`.

## Purpose and scope

This guide teaches developers **how to use Kernel to build a connector**. It is
NOT a Delta protocol reference. The whole point of Kernel is that connector
authors do not need to understand protocol internals.

- Explain Kernel's APIs, types, and patterns. Show how to call them correctly.
- Mention protocol concepts (log replay, data skipping, deletion vectors) only
  to the degree needed to understand why a Kernel API exists or behaves a
  certain way.
- Never deep-dive into protocol mechanics (action reconciliation rules, commit
  JSON schema, checkpoint format internals, timestamp ordering semantics). If
  the reader needs that level of detail, link to the
  [Delta protocol spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

### Protocol depth framework

Use this three-tier system to decide how much protocol context to include:

| Depth | When to use | Example |
|-------|-------------|---------|
| **Zero mention** | The protocol detail has no API-surface consequence | Checkpoint Parquet format internals, action reconciliation ordering rules |
| **One sentence** | The reader needs to know WHY an API exists or behaves a certain way | "Delta tables use deletion vectors to mark rows as deleted without rewriting files, which is why `DvInfo` exists on `ScanFile`." |
| **Short paragraph** (3-5 sentences) | The reader must make a decision or write code differently because of protocol behavior | The best-effort nature of data skipping, where the reader needs to decide whether to apply row-level filtering after the scan |

**Concrete example of too deep vs. right amount:**

Too deep:
> "During log replay, Kernel reads all JSON commit files and checkpoint Parquet
> files, then reconciles Add and Remove actions using the tombstone-retention
> rule: a Remove action cancels a matching Add if the Remove's timestamp is
> greater than or equal to the Add's timestamp within the same checkpoint
> interval."

Right amount:
> "Kernel replays the Delta log to determine which files are currently active.
> You don't need to understand the replay rules. The `Scan` gives you the
> final file list."

**The decision test:** does the connector author need to write code differently
because of this detail? If yes, explain enough for them to write correct code.
If no, one sentence of context at most.

**Exception zones:** Catalog integration pages and custom Engine implementation
pages necessarily carry more protocol context than the reading/writing how-to
pages. The commit type taxonomy (staged, ratified, published) is required
vocabulary for catalog committer authors. Apply the decision test per-paragraph
rather than per-page for these sections.

## Page structure

Every page, regardless of type, MUST follow this skeleton:

1. **H1 title** that names the concept or task.
2. **Opening sentence** appropriate to the page type (see opener patterns in
   the Page types table below). Never start with "This page covers X." For
   how-to pages, lead with the reader's goal. For explanation pages, lead with
   a definition and why it matters.
3. **Prerequisite/context link** if the page assumes knowledge from another
   page: "Before reading this page, make sure you understand
   [Building a Scan](../reading/building_a_scan.md)."
4. **Body** using progressive disclosure: simplest case first, then advanced
   options. Each section should motivate WHY before explaining HOW.
5. **"What's next"** section at the bottom with 2-3 links to logical next
   pages. For sequential navigation (the natural reading order).
6. **"See also"** section (optional) for lateral cross-references to related
   but non-sequential pages. Useful on reference and explanation pages.

## Page types

This guide uses the Diataxis model. There are four types of documentation. Each
demands a different voice, structure, and opener. MUST NOT mix types in one page.

| Type | Purpose | Voice | Opener pattern | Our pages |
|------|---------|-------|----------------|-----------|
| **Tutorial** | Learning by doing | "Let's build..." | "In this tutorial, you will [outcome]." | Quick starts |
| **How-to** | Accomplish a task | "To do X, do Y" | "To [goal], you [action]." | Building a scan, Appending data, Filter pushdown |
| **Reference** | Look up exact details | Neutral, structured | "[Type/Trait] [does what]." One-sentence definition. | Engine trait, Schema and types, Implementing engine |
| **Explanation** | Understand why | Discursive, second person OK | "[Concept] is [definition]. This matters because [why]." | Architecture, Catalog overview |

### How to decide which type a page is

- Is the reader doing something for the first time with hand-holding? -> Tutorial
- Is the reader trying to accomplish a specific task and already knows the basics? -> How-to
- Is the reader looking up a specific API, type, trait, or method signature? -> Reference
- Is the reader trying to understand WHY something works the way it does,
  or how pieces fit together at a high level? -> Explanation
- If none of the above fit, default to How-to (task-oriented).

If a page feels like it needs to be two types (e.g., `implementing_engine.md`
is both "how to implement the Engine trait" and "reference for all handler
signatures"), split it into two pages or pick the dominant type and link to
the other. For example, a how-to page can link to a reference page for the
full trait signatures rather than inlining them all.

**Page classifications:**

| Page | Type | Rationale |
|------|------|-----------|
| `quick_start_read.md`, `quick_start_write.md` | Tutorial | Step-by-step, one path, builds a working thing |
| `installation.md`, `feature_flags.md` | Reference | Look up dependencies, flags, and their effects |
| `building_a_scan.md`, `column_selection.md`, `filter_pushdown.md` | How-to | "To read data, configure a scan like this" |
| `append.md`, `create_table.md`, `idempotent_writes.md` | How-to | "To write data, follow these steps" |
| `scan_metadata.md`, `parallel_scan_metadata.md` | How-to | "To distribute reads, use scan_metadata like this" |
| `architecture.md` | Explanation | Why Kernel is layered, how the pieces fit together |
| `engine_trait.md` (concepts) | Explanation | Why the Engine trait exists, what role it plays |
| `implementing_engine.md` | Reference | Trait signatures, key contracts, default implementations |
| `engine_data.md` | Reference | EngineData trait, visitor pattern, method contracts |
| `schema_and_types.md` | Reference | Type system listing, schema construction |
| `catalog_managed/overview.md` | Explanation | Why catalog-managed tables exist, how Kernel fits in |
| `catalog_managed/committer.md` | How-to | "To implement a catalog committer, do this" |
| `catalog_managed/reading.md`, `writing.md` | How-to | "To read/write catalog-managed tables, do this" |
| `connector/overview.md` | Explanation | What a connector is, Kernel vs. connector responsibilities |
| `configuring_storage.md` | How-to | "To configure S3/GCS/Azure storage, pass these options" |
| `checkpointing.md` | How-to | "To checkpoint a table, call these APIs" |
| `ffi/overview.md` | Explanation + Reference | Consider splitting if it grows |

### Type-specific guidance

**Tutorials:** Take the reader by the hand. One path, no choices. Every step
MUST produce a visible, verifiable result. Minimize explanation. Link to it
instead. It is OK to USE concepts (traits, `Result`, `Arc`) before they are
formally explained. This is the spiral curriculum pattern: hook the reader with
a working result, then circle back to explain the mechanics in later chapters.
Use explicit forward references: "We're using the default engine here.
[The Engine Trait](../concepts/engine_trait.md) explains how to customize it."

**How-to pages:** Assume competence. Lead with the goal, not the page
description. Follow the recipe model: specific steps, specific outcome.

**Reference pages:** Mirror the structure of the API, not the user's workflow.
Neutral voice is acceptable here (third person, impersonal constructions). An
exhaustive listing is expected. A brief "when you would use this" sentence
before trait signatures is still required to avoid the "wall of API" problem.

**Explanation pages:** The opener rule relaxes here. "This section explains how
Kernel supports catalog-managed tables" is acceptable for explanation pages
because the reader's goal is understanding, not doing. Still lead with why it
matters, not what the page contains. Consider a "When NOT to use this" section
where applicable (e.g., "If you just need a DataFrame, use delta-rs directly.
Kernel is for building connectors that need protocol-level control.").

## Voice and tone

- MUST: second person ("you"), present tense, active voice.
- MUST: use contractions (don't, you'll, it's). Do not use slang.
- MUST NOT: use "we" to mean the project or the authors. "We" is acceptable
  only in tutorial context ("Let's walk through this").
- MUST NOT: use emojis, exclamation marks, or marketing superlatives
  ("blazingly fast", "world-class", "cutting-edge").
- SHOULD: sound like a knowledgeable colleague, not a textbook.
- For **reference pages**, neutral/impersonal voice is acceptable where it
  reads more naturally (e.g., "The handler resolves columns by field ID" rather
  than forcing "You resolve columns by field ID" when the handler does it).

## Sentence-level rules

MUST:
- One idea per sentence.
- Place conditions before instructions: "To compile the project, run
  `cargo build`" not "Run `cargo build` to compile the project."
- When warning the reader, always explain the consequence, not only the rule.

SHOULD:
- Eliminate "There is/are" constructions. Rewrite with a concrete subject.
- Replace weak verbs: "occurs" -> specific verb; "is able to" -> "can";
  "has the ability to" -> "can"; "in order to" -> "to".

MUST NOT:
- Never say "simply", "just", "easy", or "obvious".
- Never use `--` (double dash) as an em dash in prose. Restructure into
  two sentences, or use a period and start a new sentence.

### Rewrites cheat sheet

| Instead of...                             | Write...                               |
|-------------------------------------------|----------------------------------------|
| This page covers X                        | To accomplish X, you do Y *(how-to)* / X is Y. This matters because Z *(explanation)* |
| There is a method called `foo` that...    | The `foo` method...                    |
| The table will be created by the engine   | The engine creates the table           |
| In order to read the table                | To read the table                      |
| It is important to note that              | *(just state the fact directly)*       |
| Simply call `build()`                     | Call `build()`                         |
| The system is able to handle              | The system handles                     |
| Run `cargo build` to compile              | To compile, run `cargo build`          |
| causes the triggering of                  | triggers                               |
| provides a detailed description of        | describes                              |
| at this point in time                     | now                                    |
| due to the fact that                      | because                                |
| in the event that                         | if                                     |

## Code examples

### Code fence annotations

- `` ```rust `` : compiles AND runs during `mdbook test`. Use for self-contained
  examples that need no external resources.
- `` ```rust,no_run `` : compiles but does not run. Use for examples needing a
  real table, network, or async runtime.
- `` ```rust,ignore `` : not compiled. Use for fragments shown in context of a
  larger block above, or for trait/struct definitions from the Kernel API.
- `` ```rust,compile_fail `` : MUST fail to compile. Use for "break-then-fix"
  teaching where you show a common mistake, display the compiler error, then
  show the corrected version.
- `` ```rust,should_panic `` : compiles and runs, but MUST panic. Use for
  examples demonstrating error conditions.
- `` ```toml ``, `` ```sh ``, `` ```text ``, `` ```json `` : non-Rust code.

### Code example rules

- MUST: every code example that can be made compilable MUST be compilable.
  Wrap boilerplate (imports, engine setup, async runtime) in hidden `# ` lines
  so the reader sees only the relevant code, but `mdbook test` still compiles.
- MUST: show expected output after every code block that produces output. Use a
  `` ```text `` block. If the output is a table, show the ASCII table.
- SHOULD: use inline comments to narrate what each step does. Comments explain
  intent ("why"), not syntax ("what").
- SHOULD: number the steps in comments (`// 1. Parse the table`,
  `// 2. Build engine`) for longer examples, matching the prose breakdown.
- For **error handling**, MUST use `?` over `.unwrap()`. Hidden boilerplate
  should include a `main()` returning `DeltaResult<()>` (or
  `Result<(), Box<dyn std::error::Error>>` when mixing error types). Readers
  copy examples verbatim. `.unwrap()` in examples teaches destructive habits.
- For **async examples**, include `tokio` in the hidden boilerplate
  (`# #[tokio::main]` / `# async fn main()`). The reader should not need to
  know about the async runtime to follow the example.
- For **external crate examples** (e.g., Unity Catalog crates in catalog
  pages), use `rust,ignore` since the reader cannot compile them without the
  external crate. Note the dependency explicitly in prose. See the root
  `CLAUDE.md` for the canonical list of crates in this workspace.
- SHOULD: include experimentation prompts where natural. After an example, a
  brief "Try changing `with_predicate(age < 30)` to `age > 30` and observe
  which files are skipped" invites active learning.

## Consistent example dataset

Use the same Delta table throughout the guide wherever possible:

```text
Schema: name (STRING), age (INTEGER), city (STRING)
Rows:   ("Alice", 30, "Seattle"), ("Bob", 25, "Portland"), ("Carol", 35, "Denver")
```

**When to deviate:**
- Partitioned tables: add a partition column (e.g., `city`)
- Nested types: use a purpose-built schema showing the nesting
- Schema evolution: use a modified version of the canonical schema
- Reference pages listing types/traits: no dataset needed

When deviating, always show the schema and a few sample rows.

## Terminology

Use these terms consistently. Do not alternate with synonyms.

| Term | Not this | Notes |
|------|----------|-------|
| Snapshot | table state, table view | "version" is fine when referring to the numeric version (e.g., "table version 5"). Use "Snapshot" when referring to the object/type. |
| Scan | query, read operation | |
| Transaction | write operation | "commit" is fine when referring to the act of finalizing (e.g., "commit the transaction"). Do not use "commit" as a noun synonym for Transaction. |
| Engine | backend, provider, runtime | Refers to the Kernel `Engine` trait. |
| connector | integration, plugin, adapter | The code that glues a compute engine to Kernel. |
| predicate | | The Kernel API concept. "filter" is fine in user-facing context (e.g., "filter your data by age") but do not use "filter expression" as a synonym for `predicate` in API-level discussion. |
| data skipping | file pruning, file skipping | |
| EngineData | batch, record batch | "RecordBatch" is fine when specifically referring to the Arrow type. |

Note: "compute engine" and "query engine" both refer to the external caller
(Spark, DataFusion, Flink, etc.), not to Kernel's `Engine` trait. Use
"compute engine" by default. "Query engine" is acceptable when discussing
query-specific behavior.

Define Delta-specific terms (transaction log, checkpoint, action, partition,
deletion vector, etc.) on first use in each page. Rust-specific terms (trait,
`Arc`, lifetime, `?` operator) need no definition.

## Formatting conventions

- MUST: sentence case for headings ("Building a scan" not "Building a Scan").
  Exception: proper nouns and type names ("The Engine trait").
- MUST: Oxford comma ("S3, GCS, and Azure").
- MUST: backtick code font for identifiers: `Snapshot`, `scan_builder()`,
  `cargo build`, file paths.
- MUST: `**bold**` for introducing a term for the first time in a definition.
- SHOULD: `_italic_` for emphasis within a sentence.
- Tables SHOULD have a header row and use consistent alignment.

## Admonitions

MUST use `> [!NOTE]`, `> [!WARNING]`, `> [!TIP]` for all callouts. Do not use
`> **Note:**` or other ad-hoc formats.

Use sparingly. There is evidence readers skip elements outside their focus of
interest (Google developer style guide). Reserve for genuinely important info:

- `> [!NOTE]`: supplementary information. Feature flag requirements go here.
- `> [!WARNING]`: ONLY for situations with real consequences (data loss,
  silent incorrectness, hard-to-debug errors, irreversible actions).
- `> [!TIP]`: a non-obvious shortcut or best practice.

## Cross-references

- Link on first occurrence of a concept per section. Do not link every mention.
- Use relative paths: `[Building a Scan](../reading/building_a_scan.md)`.
- When deferring a concept, name the destination explicitly: "Deletion vectors
  are covered in [Scan Metadata](./scan_metadata.md)."
- Use explicit forward references in tutorials: "We're using the default engine
  here. [The Engine Trait](../concepts/engine_trait.md) explains how to
  customize it."

## Diagrams

- Use ASCII art (`` ```text `` blocks) for architecture and flow diagrams.
  ASCII art renders in all environments (terminal, browser, PDF) without
  requiring image tooling.
- Use images in `src/images/` only when ASCII art cannot convey the concept
  (e.g., screenshots, complex visual flows). All images MUST have alt text.
- Keep diagrams focused. If a diagram needs more than ~30 lines, consider
  splitting it or simplifying.

## Page length and scope

- SHOULD: aim for 100-250 lines per page. Under 50 lines suggests the page is
  too thin and should be merged into a parent page. Over 400 lines suggests
  the page covers too many topics and should be split.
- Each page should cover one concept or one task. If you find yourself writing
  multiple H2 sections that could each stand alone, consider splitting.
- Do not bleed per-operation details into pages that do not own them. If a
  page needs to reference a concept (for example, which maintenance operations
  Kernel supports, or whether log compaction is enabled), link to the page
  that owns the topic rather than restating it. Ownership rules of thumb:
  maintenance operations belong on the Maintenance Operations pages; protocol
  details belong on the [Delta protocol spec](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)
  or the page explicitly about that feature; connector-specific concerns
  belong on the connector pages.

## SUMMARY.md structure

- Follow the reader's learning path: getting started first, then core concepts,
  then reading, writing, advanced topics. Within each section, order by
  increasing complexity.
- MUST NOT nest deeper than 2 levels (3 max in rare cases). Flat navigation is
  easier to scan.
- Use `# Part Title` headings to separate major sections (e.g.,
  `# Getting Started`, `# Building Connectors`, `# Reference`).
- Use draft chapters (`[Future Topic]()`) for planned but unwritten content.
  This is preferred over creating stub files.
- Use separators (`---`) before suffix content like Glossary and Contributing.

## File hygiene

- No legacy or stub files. If a page has fewer than 10 lines of content, either
  finish it or remove its entry from SUMMARY.md and use a draft chapter instead.
- Root-level `src/*.md` files (outside of subdirectories) should only be
  `SUMMARY.md` and the introduction. All other content belongs in a subdirectory.
- Images go in `src/images/`.
- Keep `SUMMARY.md` in sync with the actual file tree.

## CI and tooling

- MUST: run `mdbook test` in CI on every PR. This compiles all Rust code
  examples and catches stale code. Stale examples are the #1 documentation
  quality killer.
- MUST: run `mdbook-linkcheck2` in CI to catch broken internal links. It's a
  maintained fork; the original `mdbook-linkcheck` is incompatible with
  mdbook 0.5+.
- MUST: set `create-missing = false` in `book.toml`. The default `true` setting
  silently creates empty files when SUMMARY.md paths are wrong, masking errors.

## Anti-patterns

These failures are NOT already covered by the rules above and deserve explicit
callouts.

1. **"Wall of API"**: trait signatures or method lists without a motivating
   example first. Before showing `pub trait ParquetHandler { ... }`, write one
   sentence explaining when a connector author would implement it and show a
   minimal use case.

2. **The "obvious to the author" gap**: skipping steps that seem self-evident.
   Bad: "Configure the engine, then run the scan." Good: "Pass an
   `Arc<dyn Engine>` to `scan.execute()`. The `Arc` is required because the
   iterator must keep the engine alive."

3. **Undifferentiated FAQ**: if a question is asked frequently, the answer
   belongs on the relevant page, not in a separate FAQ. FAQs become
   unsearchable graveyards.

4. **Stale stubs**: a page with just `# Observability` and nothing else wastes
   the reader's time and erodes trust. Delete it or use a draft chapter.

5. **Examples that skip imports**: never force the reader to guess what to
   import. Use hidden `# ` lines so imports are compiled but visually
   hidden. The eyeball icon in mdBook lets curious readers reveal them.
