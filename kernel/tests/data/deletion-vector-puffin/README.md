# Puffin deletion-vector fixture

`deletion-vector-61d16c75.puffin` is an Apache Iceberg Puffin file holding a single
`deletion-vector-v1` blob that deletes row positions 0 and 9. It was produced by iceberg-rust's
`PuffinWriter`, wrapping the DV blob taken from
`../table-with-dv-small/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin` (the bytes after
that file's 1-byte Delta version header). The DV blob is byte-identical across the Delta and
Iceberg formats; only the surrounding framing differs.

The blob sits at offset 4 (right after the 4-byte `PFA1` magic) and the Iceberg footer records its
length as 44. A Delta `DeletionVectorDescriptor` therefore reads it with `offset = 4`,
`sizeInBytes = 36` (44 minus the 8-byte length-prefix + CRC framing), and `cardinality = 2`.

The blob carries the `referenced-data-file` (`data_file_1.parquet`) and `cardinality` (`2`)
properties the Puffin spec requires for `deletion-vector-v1`.
