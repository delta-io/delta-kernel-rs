//! Defines code for post commit hooks that can be executed after a [`super::Transaction`] completes.

use crate::{DeltaResult, Engine};

/// Different types of post commit hooks kernel supports
pub enum PostCommitHookType {}

pub trait PostCommitHook: std::fmt::Debug {
    /// Invoke this hook to perform it's action. Returns `()` on success, or an error on failure.
    fn invoke(&self, engine: &dyn Engine) -> DeltaResult<()>;

    /// Get the type of hook this is
    fn get_type(&self) -> PostCommitHookType;
}
