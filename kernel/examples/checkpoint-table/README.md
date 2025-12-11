Checkpoint Table
===========

# About

This example shows how to checkpoint a Delta table.

You can run this example from anywhere in this repository by running `cargo run -p checkpoint-table -- [args]` or by navigating to this directory and running `cargo run -- [args]`.

!!!WARNING!!!: This doesn't use put-if-absent, or a catalog based commit, so it is UNSAFE.  As such
 you need to pass --unsafe-i-know-what-im-doing as an argument to get this to actually write the
 checkpoint, otherwise it will just do all the work it _would_ have done, but not actually write the
 final checkpoint.

# Examples

Assuming you're running in the directory of this example:

- checkpoint the table at "/tmp/my_table"

```bash
cargo run -- --unsafe-i-know-what-im-doing /tmp/my_table
```

- Just see that the checkpoint would work and how large it would be:

```bash
cargo run -- /tmp/my_table
```

- Get usage info:

```bash
cargo run -- --help
```
