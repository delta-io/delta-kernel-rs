# Data manipulation (DML)

**Data manipulation language (DML)** describes operations that insert, update, or delete table
rows, such as `INSERT`, `UPDATE`, `DELETE`, and `MERGE`. File-rewrite operations such as `OPTIMIZE`
use the same write APIs, but reorganize existing rows without changing the table's logical contents.

Before reading this page, make sure you understand
[Quick start: writing a table](../getting_started/quick_start_write.md).

Kernel provides APIs to read rows, write replacement files, and commit file additions and removals.
It doesn't execute SQL commands or implement all DML semantics. [OPTIMIZE](./DML/optimize.md) is the
rewrite operation covered in this guide. Other DML operations and their interactions with table
features aren't fully covered by Kernel's write validation. If you implement them, follow the
[Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) requirements for every
feature enabled on the table. A successful commit doesn't prove that your operation preserves those
requirements.

For example, **Change Data Feed (CDF)** exposes row-level changes between table versions. Kernel
doesn't support writing change data files. On a CDF-enabled table, it rejects data-changing
transactions that combine file additions with removals or deletion-vector updates. Setting
`with_operation("MERGE".to_string())` records an operation name; it doesn't implement `MERGE` or
enable otherwise unsupported writes.

## What's next

- [Appending data](./DML/append.md) explains how to write and commit new files.
- [Removing data](./DML/remove.md) explains how to select and remove existing files.
- [OPTIMIZE](./DML/optimize.md) explains how to rewrite files with and without row tracking.
