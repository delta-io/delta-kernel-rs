//! Plan-based engine implementation.
//!
//! This module will contain an implementation of the [`Engine`](crate::Engine) trait that is
//! backed by a [`PlanExecutor`](crate::plan::PlanExecutor). The engine delegates handler
//! operations (storage, JSON, parquet, evaluation) to declarative plan execution, rather than
//! implementing each handler independently.

pub mod naive;

pub use naive::NaivePlanExecutor;
