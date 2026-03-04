//! Cross-engine contract tests: verifies that both the default (Arrow/Tokio) and sync engines
//! satisfy the [`JsonHandler`] and [`ParquetHandler`] trait contracts.
//!
//! Each `#[rstest]` here runs the same contract body from [`super::tests`] against both concrete
//! handler implementations. This is the canonical place for cross-engine contract coverage;
//! individual engine modules contain only engine-specific tests.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rstest::rstest;

use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::json::DefaultJsonHandler;
use crate::engine::default::parquet::DefaultParquetHandler;
use crate::engine::sync::json::SyncJsonHandler;
use crate::engine::sync::SyncParquetHandler;
use crate::{JsonHandler, ParquetHandler};

fn default_parquet_handler() -> Box<dyn ParquetHandler> {
    Box::new(DefaultParquetHandler::new(
        Arc::new(LocalFileSystem::new()),
        Arc::new(TokioBackgroundExecutor::new()),
    ))
}

fn sync_parquet_handler() -> Box<dyn ParquetHandler> {
    Box::new(SyncParquetHandler)
}

fn default_json_handler() -> Box<dyn JsonHandler> {
    Box::new(DefaultJsonHandler::new(
        Arc::new(LocalFileSystem::new()),
        Arc::new(TokioBackgroundExecutor::new()),
    ))
}

fn sync_json_handler() -> Box<dyn JsonHandler> {
    Box::new(SyncJsonHandler)
}

#[rstest]
#[case::default_engine(default_parquet_handler())]
#[case::sync_engine(sync_parquet_handler())]
fn test_reads_footer(#[case] handler: Box<dyn ParquetHandler>) {
    super::tests::test_parquet_handler_reads_footer(handler.as_ref());
}

#[rstest]
#[case::default_engine(default_parquet_handler())]
#[case::sync_engine(sync_parquet_handler())]
fn test_footer_errors_on_missing_file(#[case] handler: Box<dyn ParquetHandler>) {
    super::tests::test_parquet_handler_footer_errors_on_missing_file(handler.as_ref());
}

#[rstest]
#[case::default_engine(default_parquet_handler())]
#[case::sync_engine(sync_parquet_handler())]
fn test_footer_preserves_field_ids(#[case] handler: Box<dyn ParquetHandler>) {
    super::tests::test_parquet_handler_footer_preserves_field_ids(handler.as_ref());
}

#[rstest]
#[case::default_engine(default_parquet_handler())]
#[case::sync_engine(sync_parquet_handler())]
fn test_write_always_overwrites(#[case] handler: Box<dyn ParquetHandler>) {
    super::tests::test_parquet_handler_write_always_overwrites(handler.as_ref());
}

#[rstest]
#[case::default_engine(default_json_handler())]
#[case::sync_engine(sync_json_handler())]
fn test_json_file_path_contract(#[case] handler: Box<dyn JsonHandler>) {
    super::tests::test_json_handler_file_path_contract(handler.as_ref());
}
