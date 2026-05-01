# Summary

[Introduction](./introduction.md)

# Getting Started

- [Installation](./getting_started/installation.md)
- [Quick Start: Reading a Table](./getting_started/quick_start_read.md)
- [Quick Start: Writing a Table](./getting_started/quick_start_write.md)

# Core Concepts

- [Architecture Overview](./concepts/architecture.md)
- [The Engine Trait](./concepts/engine_trait.md)
- [Schemas and Data Types](./concepts/schema_and_types.md)
- [Feature Flags](./concepts/feature_flags.md)

# Reading Tables

- [Building a Scan](./reading/building_a_scan.md)
- [Column Selection](./reading/column_selection.md)
- [Filter Pushdown and File Skipping](./reading/filter_pushdown.md)
- [Advanced Reads with scan_metadata()](./reading/scan_metadata.md)
- [Distributed Log Replay](./reading/parallel_scan_metadata.md)
- [Time Travel and Snapshot Management](./reading/time_travel.md)
- [Reading Change Data Feed](./reading/change_data_feed.md)

# Writing Tables

- [Creating a Table](./writing/create_table.md)
- [Appending Data](./writing/append.md)
- [Writing to Partitioned Tables](./writing/partitioned_writes.md)
- [Removing Data](./writing/removing_files.md)
- [Domain Metadata](./writing/domain_metadata.md)
- [Idempotent Writes](./writing/idempotent_writes.md)
- [Altering a Table](./writing/alter_table.md)

# Maintenance Operations

- [Checkpointing](./maintenance/checkpointing.md)
- [Version Checksums](./maintenance/version_checksums.md)

# Building a Connector

- [Overview](./connector/overview.md)
- [Implementing the Engine Trait](./connector/implementing_engine.md)
- [The EngineData Trait](./connector/engine_data.md)

# Catalog-Managed Tables

- [Overview](./catalog_managed/overview.md)
- [Implementing a Catalog Committer](./catalog_managed/committer.md)
- [Reads](./catalog_managed/reading.md)
- [Writes](./catalog_managed/writing.md)

# Unity Catalog Integration

- [Overview](./unity_catalog/overview.md)
- [Creating UC Tables](./unity_catalog/creating_tables.md)
- [Reading UC Tables](./unity_catalog/reading.md)
- [Writing to UC Tables](./unity_catalog/writing.md)

# Storage Configuration

- [Configuring Storage](./storage/configuring_storage.md)

# Observability

- [Metrics and Monitoring](./observability/observability.md)

# FFI (C/C++ Integration)

- [Overview](./ffi/overview.md)
