Architecture
============

`delta-core` crate architecture. currently only supports reads.

10,000-foot view
----------------

Two major APIs:
1. Library/dev API
2. User API

Consider the usage pattern by example: if `delta-rs` wants to leverage
delta-core to read tables, it first must take a dependency on delta-core and
provide any of the traits it wishes (otherwise rely on defaults already
provided in delta-core) - this is API (1) above. Then the library code can
leverage the user API (2) in order to perform actual interaction with delta
tables.

main traits:
- StorageClient
- JsonReader
- ParquetReader
- ExpressionEvaluator

goals
-----

in order of priority (this is placeholder and we need to redo them):
1. simplicity and ease of use (probably too vague)
2. query engine agnostic
3. performance is explicitly secondary goal with the exception of operating in bounded memory

code map
--------

todo large pieces of code and maybe diagram?
