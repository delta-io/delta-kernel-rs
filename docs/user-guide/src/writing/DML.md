# Data manipulation (DML)

**Data Manipulation Language (DML)** operations are a subset of SQL statements used to retrieve,
add, modify, and delete row-level data inside database tables, such as `SELECT`, `INSERT`, `UPDATE`,
`DELETE`, and `MERGE`.

Before reading this page, make sure you understand
[Quick start: writing a table](../getting_started/quick_start_write.md).

Kernel hasn't been fully designed for all DML operations. The pages below cover the operations
considered so far. For other operations, follow the
[Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) requirements for the
features enabled on your table.

## What's next

- [Appending data](./DML/append.md) explains how to write and commit new files.
- [Removing data](./DML/remove.md) explains how to select and remove existing files.
- [OPTIMIZE](./DML/optimize.md) explains how to rearrange data across files.
