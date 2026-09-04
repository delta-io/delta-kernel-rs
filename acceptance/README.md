# Delta Kernel Acceptance Testing

This directory contains acceptance tests for the Delta Kernel (Rust) project.
These tests have been implemented as a separate sub-project in the workspace to
ensure that they are acting as a "connector" for the kernel APIs, thereby
allowing us to test the exact same engine interfaces that any connector
implemented on top of delta-kernel-rs would be using.

## Authoring a DAT case locally

CI downloads a published DAT corpus, pinned to a version in `build.rs`. To create
a *new* DAT case without bumping that version, generate a corpus locally and point
the build at it with `DELTA_ACCEPTANCE_WORKLOADS_PATH`. See
https://github.com/delta-incubator/dat/blob/main/workload-generator/README.md for
the full authoring guide.

Add a case to a `*Suite.scala` in the DAT `workload-generator`. Each `test(...)`
block is one case: build a table with `sql`, register it, then declare the specs to
generate (`readSpec` for scans, `snapshotSpec` for protocol/metadata).

```scala
// in src/test/scala/io/delta/workload/tables/ReadsSuite.scala
test("my_case") {
  sql("CREATE TABLE tbl (id INT, price DECIMAL(10,2)) USING delta")
  sql("INSERT INTO tbl VALUES (1, 9.99), (2, 100.00)")
  val t = registerTable("tbl")
  readSpec(t)                        // full scan
  readSpec(t, predicate = "id > 1")  // predicate pushdown
  snapshotSpec(t)                    // protocol + metadata
}
```

Then generate just that case and run it. The `-z` filter generates one case instead
of the whole suite (each case is a few seconds of Spark).

```bash
# 1. Generate the corpus locally (Spark needs JDK 17).
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # any JDK 17
export PATH="$JAVA_HOME/bin:$PATH"
cd <dat-clone>/workload-generator
WORKLOAD_OUTPUT_DIR=/tmp/wl WORKLOAD_FORCE=true sbt 'testOnly *ReadsSuite -- -z "my_case"'

# 2. Point kernel-rs at the local corpus and iterate.
cd <delta-kernel-rs>
DELTA_ACCEPTANCE_WORKLOADS_PATH=/tmp/wl cargo nextest run -p acceptance
```

Do **not** commit a generated corpus. Bump `ACCEPTANCE_WORKLOADS_VERSION`
in `build.rs` once the case lands in a DAT release.
