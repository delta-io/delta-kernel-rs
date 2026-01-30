---
name: Extract ICT Helper
overview: Extract the in-commit timestamp (ICT) logic (lines 457-477) into a `get_in_commit_timestamp()` helper method on `Transaction`, then invoke it from the commit preparation code.
todos:
  - id: add-helper
    content: Add get_in_commit_timestamp() helper method to Transaction impl
    status: completed
  - id: update-callsite
    content: Replace inline ICT logic with call to new helper
    status: completed
    dependencies:
      - add-helper
---

# Extract In-Commit Timestamp Helper Method

## Overview

Refactor the in-commit timestamp (ICT) calculation logic into a dedicated helper method on `Transaction` for better code organization and reusability.

## Changes to [kernel/src/transaction/mod.rs](kernel/src/transaction/mod.rs)

### 1. Add new helper method `get_in_commit_timestamp`

Add a private helper method to the `Transaction` impl block (near `is_create_table()` around line 653):

```rust
/// Computes the in-commit timestamp for this transaction if ICT is enabled.
/// Returns `None` if ICT is not enabled on the table.
fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> DeltaResult<Option<i64>> {
    let has_ict = self
        .read_snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::InCommitTimestamp);
    
    if has_ict && !self.is_create_table() {
        Ok(self.read_snapshot
            .get_in_commit_timestamp(engine)?
            .map(|prev_ict| {
                // The Delta protocol requires the timestamp to be "the larger of two values":
                // - The time at which the writer attempted the commit (current_time)
                // - One millisecond later than the previous commit's inCommitTimestamp (last_commit_timestamp + 1)
                self.commit_timestamp.max(prev_ict + 1)
            }))
    } else if has_ict && self.is_create_table() {
        // ICT is enabled but this is a create-table transaction - not yet supported
        Err(Error::unsupported(
            "InCommitTimestamp is not yet supported for create table",
        ))
    } else {
        Ok(None)
    }
}
```

### 2. Simplify the commit preparation code

Replace lines 456-478 with a direct inline call to the helper:

```rust
// Step 2: Construct commit info with ICT if enabled
let commit_info = CommitInfo::new(
    self.commit_timestamp,
    self.get_in_commit_timestamp(engine)?,
    self.operation.clone(),
    self.engine_info.clone(),
);
```

This eliminates the intermediate `in_commit_timestamp` variable and reduces 22 lines of inline logic to a single method call, improving readability and making the ICT logic reusable if needed elsewhere.
