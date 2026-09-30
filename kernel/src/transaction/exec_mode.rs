#[cfg(doc)]
use super::Transaction;
use crate::engine_data::FilteredEngineData;
use crate::EngineData;

/// Execution mode for a [`Transaction`].
///
/// The mode stores staged data-file changes and determines which staging and commit methods are
/// available. Its default value contains no staged changes.
pub trait ExecutionMode: StagedDataChanges + Default + std::fmt::Debug {}

/// Checks staged work without inspecting rows or executing relations.
pub trait StagedDataChanges {
    /// Whether any add-file inputs are staged, regardless of their row counts.
    fn has_adds(&self) -> bool;
    /// Whether any remove-file inputs are staged, regardless of selection vectors.
    fn has_removes(&self) -> bool;
    /// Whether any DV-update inputs are staged, including batches with no matched rows.
    fn has_dv_inputs(&self) -> bool;
    /// Whether any files have matched DV updates that require validation.
    fn has_dv_updates(&self) -> bool;
}

/// Transaction mode that stages [`EngineData`] and commits through the imperative engine APIs.
///
/// This is the default mode of [`Transaction`].
#[derive(Default)]
pub struct Imperative {
    pub(super) add_files_metadata: Vec<Box<dyn EngineData>>,
    pub(super) remove_files_metadata: Vec<FilteredEngineData>,
    // Matched scan files with new DV descriptors and rewritten statistics appended.
    pub(super) dv_matched_files: Vec<FilteredEngineData>,
    pub(super) num_dv_updates: usize,
}

impl ExecutionMode for Imperative {}

impl StagedDataChanges for Imperative {
    fn has_adds(&self) -> bool {
        !self.add_files_metadata.is_empty()
    }

    fn has_removes(&self) -> bool {
        !self.remove_files_metadata.is_empty()
    }

    fn has_dv_inputs(&self) -> bool {
        !self.dv_matched_files.is_empty()
    }

    fn has_dv_updates(&self) -> bool {
        self.num_dv_updates > 0
    }
}

impl std::fmt::Debug for Imperative {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Imperative")
            .field("add_batches", &self.add_files_metadata.len())
            .field("remove_batches", &self.remove_files_metadata.len())
            .field("dv_batches", &self.dv_matched_files.len())
            .field("num_dv_updates", &self.num_dv_updates)
            .finish()
    }
}
