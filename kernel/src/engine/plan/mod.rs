//! Plan-based engine implementation.
//!
//! This module contains an implementation of the [`Engine`](crate::Engine) trait that is
//! backed by a [`PlanExecutor`](crate::plan::PlanExecutor). The engine delegates handler
//! operations (storage, JSON, parquet, evaluation) to declarative plan execution, rather than
//! implementing each handler independently.
//!
//! [`PlanBasedEngine`] routes storage operations through a
//! [`PlanExecutor`](crate::plan::PlanExecutor) via [`PlanBasedStorageHandler`], while using the
//! default Arrow-based handlers for JSON, Parquet, and expression evaluation. [`NaivePlanExecutor`]
//! provides a concrete executor
//! backed by [`ObjectStoreStorageHandler`](crate::engine::default::filesystem::ObjectStoreStorageHandler).

pub mod engine;
pub mod naive;
pub mod storage;

pub use engine::PlanBasedEngine;
pub use naive::NaivePlanExecutor;
pub use storage::PlanBasedStorageHandler;
