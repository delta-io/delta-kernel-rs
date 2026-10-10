# Delta Kernel Roadmap

This outlines the roadmap for the Delta Kernel open source project. This roadmap is directional, and features may move in/out of milestones pending available resources, priorities, and community discussions.

## Near term (next 1–2 quarters)

- **Production-ready reads and writes:** Close any remaining correctness, feature, and performance
  gaps. Priorities include (but are not limited to):
  - Distributed log-replay via Declarative plans
  - Support for all data-types and table-features specified in the protocol
  - Write metadata validation
  - Removing kernel dependence on synchronous execution (likely via co-routines)
  - Enabling connectors to do optimistic concurrency control

- **Declarative plans:** Kernel will describe operations such as log replay and scans as
  engine-independent plans instead of calling imperative engine APIs as today. Engines can then
  translate, optimize, and distribute these plans using their own execution run-times, while Delta
  protocol logic remains centralized in Kernel. Currently we are working on maturing the plan
  model, executor APIs, and exploring how to integrate these executors into production engines.

- **A unified cross-language Kernel:** Develop JVM bindings over the FFI interface exposed by the
  ffi crate. This includes all kernel features, errors, metrics, and debugging support. We will also
  package this as simple to use JAR file that Java based connectors can use to migrate off the
  existing Java Kernel.

- **Improved Testing Infrastructure:** Expand Delta Acceptance Testing (DAT) to cover write
  workloads, add representative read/write benchmarks to CI, and improve observability and error
  classification. This work should make regressions easier to catch before release and make
  connectors less likely to encounter errors in production.

## Following quarters

- **Broader transaction and DML support:** Continue toward Delta feature parity with simpler create
  and alter flows, unified transaction APIs, conflict detection and retries, post-commit operations,
  and the foundations for operations such as `UPDATE`, `DELETE`, and `MERGE`.

- **More engines on one protocol implementation:** Support JVM and Rust/native connectors as they
  migrate to Kernel.

## Ongoing

- Keep public APIs small, stable, and extensible for custom engines.
- Improve documentation, examples, release quality, and responsiveness to community issues and contributions.

