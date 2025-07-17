use std::error::Error;

pub trait VariantBufferManager {
    /// Returns the slice where value needs to be written to. This method may be called several
    /// times during the construction of a new `value` field in a variant. The implementation must
    /// make sure that on every call, all the data written to the value buffer so far are preserved.
    fn borrow_value_buffer(&mut self) -> &mut [u8];

    /// Returns the slice where the variant metadata needs to be written to. This method may be
    /// called several times during the construction of a new `metadata` field in a variant. The
    /// implementation must make sure that on every call, all the data written to the metadata
    /// buffer so far are preserved.
    fn borrow_metadata_buffer(&mut self) -> &mut [u8];

    /// Ensures that the next call to `borrow_value_buffer` returns a slice having at least `size`
    /// bytes. Also ensures that the value bytes written so far are persisted - this means that
    /// if `borrow_value_buffer` is to return a new buffer from the next call onwards, the new
    /// buffer must have the contents of the old value buffer.
    fn ensure_value_buffer_size(&mut self, size: usize) -> Result<(), Box<dyn Error>>;

    /// Ensures that the next call to `borrow_metadata_buffer` returns a slice having at least
    /// `size` bytes. Also ensures that the value metadata written so far are persisted - this means
    /// that if `borrow_metadata_buffer` is to return a new buffer from the next call onwards, the
    /// new buffer must have the contents of the old metadata buffer.
    fn ensure_metadata_buffer_size(&mut self, size: usize) -> Result<(), Box<dyn Error>>;
}

pub struct SampleVariantBufferManager {
    pub value_buffer: Box<[u8]>,
    pub metadata_buffer: Box<[u8]>,
}

impl VariantBufferManager for SampleVariantBufferManager {
    fn borrow_value_buffer(&mut self) -> &mut [u8] {
        &mut self.value_buffer
    }

    fn ensure_value_buffer_size(&mut self, size: usize) -> Result<(), Box<dyn Error>> {
        let cur_len = self.value_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            let mut new_buffer = vec![0u8; size].into_boxed_slice();
            new_buffer[..cur_len].copy_from_slice(&self.value_buffer);
            self.value_buffer = new_buffer;
        }
        Ok(())
    }

    fn borrow_metadata_buffer(&mut self) -> &mut [u8] {
        &mut self.metadata_buffer
    }

    fn ensure_metadata_buffer_size(&mut self, size: usize) -> Result<(), Box<dyn Error>> {
        let cur_len = self.metadata_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            let mut new_buffer = vec![0u8; size].into_boxed_slice();
            new_buffer[..cur_len].copy_from_slice(&self.metadata_buffer);
            self.metadata_buffer = new_buffer;
        }
        Ok(())
    }
}
