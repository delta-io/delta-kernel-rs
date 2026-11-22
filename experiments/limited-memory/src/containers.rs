use std::io::{self, Write};

use serde::Serialize;

use crate::{LimitedBytes, MemoryError, MemoryResult};

pub(crate) fn write_json<T: Serialize + ?Sized>(
    bytes: &mut LimitedBytes,
    value: &T,
) -> MemoryResult<()> {
    let mut writer = JsonWriter {
        bytes,
        failure: None,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(()),
        Err(_) => Err(writer.failure.unwrap_or(MemoryError::Serialization)),
    }
}

struct JsonWriter<'a> {
    bytes: &'a mut LimitedBytes,
    failure: Option<MemoryError>,
}

impl Write for JsonWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.try_extend_from_slice(bytes).map_err(|error| {
            self.failure = Some(error);
            io::Error::from(io::ErrorKind::OutOfMemory)
        })?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
