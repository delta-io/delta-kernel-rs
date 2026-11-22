use serde::Serialize;

use crate::{LimitedBytes, LimitedString, LimitedVec, MemoryBudget, MemoryError, MemoryResult};

/// An owned log-tail descriptor whose variable-length path is charged to its tail's budget.
#[derive(Debug)]
pub struct LogFile {
    /// Delta commit version.
    pub version: u64,
    /// File size reported by the engine or catalog.
    pub size: u64,
    /// Owned path or URL, copied without URL parsing or normalization.
    pub location: LimitedString,
}

/// Fallible storage for a selected, oldest-first consecutive commit tail.
///
/// This stores descriptors, not file contents. Checkpoint selection, URL validation, and catalog
/// provenance remain the responsibility of Kernel's log-segment builder.
#[derive(Debug)]
pub struct LogTail {
    files: LimitedVec<LogFile>,
}

impl LogTail {
    /// Create an empty tail sharing `budget` with other lifecycle state.
    pub fn new(budget: &MemoryBudget) -> Self {
        Self {
            files: LimitedVec::new(budget),
        }
    }

    /// Copy a descriptor, rejecting a gap or repeated version before changing the tail.
    pub fn try_push(&mut self, version: u64, size: u64, location: &str) -> MemoryResult<()> {
        if self
            .files
            .last()
            .is_some_and(|last| last.version.checked_add(1) != Some(version))
        {
            return Err(MemoryError::InvalidLogOrder);
        }
        let location = LimitedString::try_copy(self.files.budget(), location)?;
        self.files.try_push(LogFile {
            version,
            size,
            location,
        })
    }

    /// Borrow the selected tail in increasing version order.
    pub fn files(&self) -> &[LogFile] {
        &self.files
    }
}

/// Transaction action serialization and staging under a shared allocation budget.
///
/// Only serialized actions and their outer staging vector are owned here. EngineData and
/// serializer-internal allocations are outside this cap. No external writes occur while staging;
/// callers can discard the buffer after an error before attempting an atomic commit.
#[derive(Debug)]
pub struct ActionBuffer {
    actions: LimitedVec<LimitedBytes>,
}

impl ActionBuffer {
    /// Create an empty action buffer sharing `budget` with snapshot and scan state.
    pub fn new(budget: &MemoryBudget) -> Self {
        Self {
            actions: LimitedVec::new(budget),
        }
    }

    /// Serialize and stage one complete log-action envelope.
    ///
    /// On failure, prior actions remain intact and the partial action is freed. `action` should
    /// include its Delta action key, for example `{"txn": SetTransaction}`.
    pub fn try_stage<T: Serialize + ?Sized>(&mut self, action: &T) -> MemoryResult<()> {
        let bytes = crate::try_to_json(self.actions.budget(), action)?;
        self.actions.try_push(bytes)
    }

    /// Borrow serialized actions individually for a streaming commit writer.
    pub fn actions(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.actions.iter().map(|action| &action[..])
    }

    /// Return the number of successfully staged actions.
    pub fn len(&self) -> usize {
        self.actions.len()
    }

    /// Return whether no actions have been staged.
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    /// Copy actions into a single newline-delimited commit buffer under the same budget.
    ///
    /// The output and staged actions coexist and are both charged until their owners are dropped.
    pub fn try_encode_commit(&self) -> MemoryResult<LimitedBytes> {
        let size = self.actions.iter().try_fold(0usize, |size, action| {
            size.checked_add(action.len())
                .and_then(|size| size.checked_add(1))
                .ok_or(MemoryError::CapacityOverflow)
        })?;
        let mut bytes = LimitedBytes::new(self.actions.budget());
        bytes.try_reserve_exact(size)?;
        for action in self.actions.iter() {
            bytes.try_extend_from_slice(action)?;
            bytes.try_push(b'\n')?;
        }
        Ok(bytes)
    }
}
