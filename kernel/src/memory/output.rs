use std::io::{self, Write};

use bytes::Bytes;

use super::{LimitedBytes, MemoryBudget, MemoryError};

/// A writer that preserves allocation errors across serializers' I/O error wrappers.
pub(crate) struct JsonOutput {
    storage: LimitedBytes,
    failure: Option<MemoryError>,
}

impl JsonOutput {
    pub(crate) fn new(budget: &MemoryBudget) -> Self {
        Self {
            storage: LimitedBytes::new(budget),
            failure: None,
        }
    }

    pub(crate) fn failure(&self) -> Option<MemoryError> {
        self.failure
    }

    pub(crate) fn finish(self) -> Bytes {
        Bytes::from_owner(self.storage)
    }
}

impl Write for JsonOutput {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.storage.try_extend_from_slice(input).map_err(|error| {
            self.failure = Some(error);
            io::Error::from(io::ErrorKind::OutOfMemory)
        })?;
        Ok(input.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
