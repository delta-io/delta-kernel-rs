# Reference Files

Cross-check implementations against the Delta protocol spec (the source of truth) and
the existing Java kernel / Spark implementations. The Java kernel and Spark code are
references for design guidance only -- they may contain bugs, legacy decisions, or
Spark-specific assumptions that don't apply to delta-kernel-rs. Do not blindly copy
behavior; always reason from the protocol spec first.

NOTE: The Java kernel (`~/delta/kernel`) and Spark (`~/delta/spark`) directories may not
be checked out locally. If you need to reference them and they don't exist, inform the
user so they can clone the repositories.

## Snapshot

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/SnapshotImpl.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/snapshot/SnapshotManager.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/table/SnapshotFactory.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/Snapshot.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/SnapshotManagement.scala`

## Log Segment

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/snapshot/LogSegment.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaLogFileIndex.scala`

## Checkpoint

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/checkpoints/Checkpointer.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/checkpoints/CheckpointInstance.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/files/ParsedV2CheckpointData.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/Checkpoints.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/CheckpointProvider.scala`

## Transaction

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/TransactionImpl.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/TransactionBuilderImpl.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/files/TransactionalWrite.scala`

## Conflict Resolution

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/replay/ConflictChecker.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/ConflictChecker.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/ConflictCheckerPredicateElimination.scala`

## Log Replay

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/replay/LogReplay.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/replay/ActionsIterator.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/replay/ActiveAddFilesIterator.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/actions/InMemoryLogReplay.scala`

## Table Features

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/tablefeatures/TableFeatures.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/tablefeatures/TableFeature.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/TableFeature.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/actions/TableFeatureSupport.scala`

## Table Properties

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/TableConfig.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala`

## Domain Metadata

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/metadatadomain/JsonMetadataDomain.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/actions/DomainMetadata.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/util/DomainMetadataUtils.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/JsonMetadataDomain.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/DomainMetadataUtils.scala`

## Row Tracking

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/rowtracking/RowTracking.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/rowtracking/RowTrackingMetadataDomain.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/RowTracking.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/MaterializedRowTrackingColumn.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/commands/backfill/RowTrackingBackfillExecutor.scala`

## Clustering

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/clustering/ClusteringMetadataDomain.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/clustering/ClusteringUtils.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/skipping/clustering/ClusteredTableUtils.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/skipping/clustering/ZCube.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/skipping/MultiDimClustering.scala`

## Statistics / Data Skipping

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/stats/FileSizeHistogram.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/StatsSchemaHelper.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/DataSkippingUtils.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/DataSkippingPredicate.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/StatisticsCollection.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/DataSkippingReader.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/DataSkippingPredicateBuilder.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/DeltaScan.scala`

## Scan / ScanBuilder

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/Scan.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/ScanBuilder.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/ScanImpl.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/ScanBuilderImpl.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/DeltaScanGenerator.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/PrepareDeltaScan.scala`

## Expressions / Predicates

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/expressions/Expression.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/expressions/Predicate.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/expressions/Column.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/expressions/Literal.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/engine/ExpressionHandler.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/util/ExpressionUtils.java`

## Schema

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/types/StructType.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/types/StructField.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/types/DataType.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/util/SchemaUtils.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/schema/SchemaUtils.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/schema/SchemaMergingUtils.scala`

## Column Mapping

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/util/ColumnMapping.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaColumnMapping.scala`

## Deletion Vectors

- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/deletionvectors/DeletionVectorUtils.java`
- `~/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/actions/DeletionVectorDescriptor.java`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/actions/DeletionVectorDescriptor.scala`
- `~/delta/spark/src/main/scala/org/apache/spark/sql/delta/commands/DeletionVectorUtils.scala`
