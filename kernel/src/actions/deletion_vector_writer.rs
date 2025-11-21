//! Code for writing deletion vectors to object storage.
//!
//! This module provides APIs for engines to write deletion vectors as part of a Delta transaction.

use std::borrow::Borrow;
use std::io::Write;

use bytes::Bytes;
use roaring::RoaringTreemap;

use crate::actions::deletion_vector::{
    create_dv_crc32, DeletionVectorDescriptor, DeletionVectorPath, DeletionVectorStorageType,
};
use crate::{DeltaResult, Error};

/// A trait that allows engines to provide deletion vectors in various formats.
///
/// Engines can implement this trait to provide their own deletion vector implementations,
/// or use the provided [`KernelDeletionVector`] implementation backed by RoaringTreemap.
///
/// # Examples
///
/// ```rust
/// use delta_kernel::actions::deletion_vector_writer::DeletionVector;
///
/// struct MyDeletionVector {
///     deleted_indexes: Vec<u64>,
/// }
///
/// impl DeletionVector for MyDeletionVector {
///     type IndexIterator = std::vec::IntoIter<u64>;
///
///     fn into_iter(self) -> Self::IndexIterator {
///         self.deleted_indexes.into_iter()
///     }
///
///     fn cardinality(&self) -> u64 {
///         self.deleted_indexes.len() as u64
///     }
/// }
/// ```
pub trait DeletionVector: Sized {
    /// Iterator type that yields deleted row indexes.
    type IndexIterator: Iterator<Item = u64>;

    /// Return an iterator over deleted row indexes.
    fn into_iter(self) -> Self::IndexIterator;

    /// Return the number of deleted rows in the deletion vector.
    fn cardinality(&self) -> u64;

    /// Serialize the deletion vector into bytes.
    ///
    /// This serializes the deletion vector in the format expected by the Delta Lake protocol.
    /// it may be overridden for more efficient serialization if the implementation already has the data in a suitable format.
    /// But generally, only do this if you fully understand the the format requirements.
    fn serialize(self) -> DeltaResult<Bytes> {
        let treemap: RoaringTreemap = self.into_iter().collect();
        let mut serialized = Vec::new();
        treemap
            .serialize_into(&mut serialized)
            .map_err(|e| Error::generic(format!("Failed to serialize deletion vector: {}", e)))?;
        Ok(Bytes::from(serialized))
    }
}

/// Metadata about a written deletion vector, excluding the storage path.
///
/// This structure contains the information needed to construct a full
/// [`DeletionVectorDescriptor`]
/// after writing the DV to storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionVectorWriteResult {
    /// Start of the data for this DV in number of bytes from the beginning of the file.
    /// Does not include CRC length or size in bytes prefix.
    pub offset: i32,

    /// Size of the serialized DV in bytes (raw data size).
    pub size_in_bytes: i32,

    /// Number of rows the deletion vector logically removes from the file.
    pub cardinality: i64,
}

impl DeletionVectorWriteResult {
    /// Convert the write result to a deletion vector descriptor.
    ///
    /// As an implementation detail, this method will always use the persisted relative storage type.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the deletion vector file.
    pub fn to_descriptor(self, path: &DeletionVectorPath) -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: path.encoded_relative_path(),
            offset: Some(self.offset),
            size_in_bytes: self.size_in_bytes,
            cardinality: self.cardinality,
        }
    }
}

/// A Kernel-provided deletion vector implementation backed by [`RoaringTreemap`].
///
/// This is the default implementation that engines can use. It provides memory-efficient
/// storage of deleted row indexes using compressed bitmaps.
///
/// # Examples
///
/// ```rust
/// use delta_kernel::actions::deletion_vector_writer::KernelDeletionVector;
///
/// let mut dv = KernelDeletionVector::new();
/// dv.add_deleted_row_indexes([0, 5, 10]);
/// ```
#[derive(Debug, Clone)]
pub struct KernelDeletionVector {
    dv: RoaringTreemap,
}

impl Default for KernelDeletionVector {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelDeletionVector {
    /// Create a new empty deletion vector.
    pub fn new() -> Self {
        Self {
            dv: RoaringTreemap::new(),
        }
    }

    /// Adds indexes to be deleted to this deletion vector.
    pub fn add_deleted_row_indexes<I, T>(&mut self, iter: I)
    where
        I: IntoIterator<Item = T>,
        T: Borrow<u64>,
    {
        for index in iter {
            self.dv.insert(*index.borrow());
        }
    }

    /// Get the number of deleted rows in this deletion vector.
    pub fn cardinality(&self) -> u64 {
        self.dv.len()
    }
}

impl DeletionVector for KernelDeletionVector {
    type IndexIterator = roaring::treemap::IntoIter;

    fn into_iter(self) -> Self::IndexIterator {
        self.dv.into_iter()
    }

    /// Optimized serialization that directly serializes the internal RoaringTreemap.
    fn serialize(self) -> DeltaResult<Bytes> {
        let mut serialized = Vec::new();
        self.dv
            .serialize_into(&mut serialized)
            .map_err(|e| Error::generic(format!("Failed to serialize deletion vector: {}", e)))?;
        Ok(Bytes::from(serialized))
    }

    fn cardinality(&self) -> u64 {
        self.dv.len()
    }
}

/// A streaming writer for deletion vectors.
///
/// This writer allows for writing multiple deletion vectors to a single file in a streaming
/// fashion, which is memory-efficient for distributed workloads where deletion vectors are
/// generated on executors.
///
/// # Format
///
/// The writer produces deletion vector files in the Delta Lake format:
/// - The first byte of the file is a version byte (currently 1)
/// - Each DV is prefixed with a 4-byte size (big-endian) of the serialized data
/// - Followed by a 4-byte magic number (0x64485871, little-endian)
/// - Followed by the serialized 64-bit Roaring Bitmap
/// - Followed by a 4-byte CRC32 checksum (big-endian) of the serialized data
///
/// # Examples
///
/// ```rust
/// use delta_kernel::actions::deletion_vector_writer::{StreamingDeletionVectorWriter, KernelDeletionVector};
///
/// let mut buffer = Vec::new();
/// let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
///
/// let mut dv = KernelDeletionVector::new();
/// dv.add_deleted_row_indexes([1, 5, 10]);
///
/// let descriptor = writer.write_deletion_vector(dv)?;
/// writer.finalize()?;
/// # Ok::<(), delta_kernel::Error>(())
/// ```
pub struct StreamingDeletionVectorWriter<'a, W: Write> {
    writer: &'a mut W,
    current_offset: usize,
}

impl<'a, W: Write> StreamingDeletionVectorWriter<'a, W> {
    /// Create a new streaming deletion vector writer.
    ///
    /// # Arguments
    ///
    /// * `writer` - A mutable reference to any type implementing [`std::io::Write`].
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut buffer = Vec::new();
    /// let writer = StreamingDeletionVectorWriter::new(&mut buffer);
    /// ```
    pub fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            current_offset: 0,
        }
    }

    /// Write a deletion vector to the underlying writer.
    ///
    /// This method can be called multiple times to write multiple deletion vectors to the same
    /// writer. The caller is responsible for keeping track of which deletion vector corresponds to
    /// which data file.
    ///
    /// # Arguments
    ///
    /// * `deletion_vector` - The deletion vector to write
    ///
    /// # Returns
    ///
    /// A [`DeletionVectorWriteResult`] containing the offset, size, and cardinality
    /// of the written deletion vector.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The writer fails to write data
    /// - The deletion vector cannot be serialized
    /// - The offset or size would overflow an i32
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut dv = KernelDeletionVector::new();
    /// dv.add_deleted_row_indexes([1, 5, 10]);
    ///
    /// let descriptor = writer.write_deletion_vector(dv)?;
    /// println!("Written DV at offset {} with size {}", descriptor.offset, descriptor.size_in_bytes);
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn write_deletion_vector(
        &mut self,
        deletion_vector: impl DeletionVector,
    ) -> DeltaResult<DeletionVectorWriteResult> {
        // Write version byte on first write
        if self.current_offset == 0 {
            // Write header.
            self.writer
                .write_all(&[1u8])
                .map_err(|e| Error::generic(format!("Failed to write version byte: {}", e)))?;
            self.current_offset = 1;
        }

        let cardinality = deletion_vector.cardinality();
        // Serialize the deletion vector to bytes
        let serialized = deletion_vector.serialize()?;

        // Calculate sizes

        // The size field contains the size of data + magic(4) (doesn't include CRC)
        let dv_size = serialized.len() + 4;
        // Use i32::MAX as the limit since Java implementations don't have unsigned integers.
        // This ensures compatibility with the Scala/Java implementation [1].
        //
        // [1] https://github.com/delta-io/delta/blob/b388f280d083d4cf92c6434e4f7a549fc26cd1fa/spark/src/main/scala/org/apache/spark/sql/delta/deletionvectors/RoaringBitmapArray.scala#L311
        if dv_size > i32::MAX as usize {
            return Err(Error::generic(
                "Deletion vector size exceeds maximum allowed size",
            ));
        }

        // Record the offset where this DV size starts.
        let dv_offset: i32 = self
            .current_offset
            .try_into()
            .map_err(|_| Error::generic("Deletion vector offset doesn't fit in i32"))?;

        // Write size (big-endian, as per Delta spec)
        let size_bytes = (dv_size as u32).to_be_bytes();
        self.writer
            .write_all(&size_bytes)
            .map_err(|e| Error::generic(format!("Failed to write size: {}", e)))?;

        // Write magic number (little-endian)
        // This is the RoaringBitmapArray format magic
        let magic: u32 = 1681511377;
        self.writer
            .write_all(&magic.to_le_bytes())
            .map_err(|e| Error::generic(format!("Failed to write magic: {}", e)))?;

        // Write the serialized treemap
        self.writer
            .write_all(&serialized)
            .map_err(|e| Error::generic(format!("Failed to write deletion vector data: {}", e)))?;

        // Calculate and write CRC32 checksum (big-endian)
        // The CRC must include both the magic and the serialized data
        let crc_instance = create_dv_crc32();
        let mut digest = crc_instance.digest();
        digest.update(&magic.to_le_bytes());
        digest.update(&serialized);
        let checksum = digest.finalize();
        self.writer
            .write_all(&checksum.to_be_bytes())
            .map_err(|e| Error::generic(format!("Failed to write CRC32 checksum: {}", e)))?;

        // Update offset for next write (size_prefix + magic + data + crc)
        let bytes_written = 4 + dv_size + 4; // size + (magic + data) + crc
        self.current_offset += bytes_written;

        Ok(DeletionVectorWriteResult {
            offset: dv_offset,
            size_in_bytes: dv_size as i32,
            cardinality: cardinality as i64,
        })
    }

    /// Finalize all writes and flush the underlying writer.
    ///
    /// This method should be called after all deletion vectors have been written.
    /// After calling this method, the writer should not be used anymore.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing the writer fails.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// writer.write_deletion_vector(dv1)?;
    /// writer.write_deletion_vector(dv2)?;
    /// writer.finalize()?;
    /// # Ok::<(), delta_kernel::Error>(())
    /// ```
    pub fn finalize(self) -> DeltaResult<()> {
        // Note: Currently this method only flushes the writer, but is kept as an explicit API
        // for future-proofing. If we need to support formats that require footers (e.g., Puffin
        // files or new DV file formats), this provides a consistent place to add that logic
        // without breaking downstream code.
        //

        self.writer
            .flush()
            .map_err(|e| Error::generic(format!("Failed to flush writer: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_kernel_deletion_vector_new() {
        let dv = KernelDeletionVector::new();
        assert_eq!(dv.cardinality(), 0);
    }

    #[test]
    fn test_kernel_deletion_vector_add_indexes() {
        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes([1u64, 5, 10]);

        assert_eq!(dv.cardinality(), 3);
        assert_eq!(
            dv.into_iter().collect::<RoaringTreemap>(),
            RoaringTreemap::from_iter([1, 5, 10])
        );
    }

    #[test]
    fn test_streaming_writer_single_dv() {
        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);

        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes([0u64, 9]);

        let descriptor = writer.write_deletion_vector(dv).unwrap();
        writer.finalize().unwrap();

        // Check descriptor values
        assert_eq!(descriptor.offset, 1); // After version byte
        assert_eq!(descriptor.cardinality, 2);
        assert!(descriptor.size_in_bytes > 0);

        // Check buffer contents
        assert!(!buffer.is_empty());
        assert_eq!(buffer[0], 1); // Version byte
    }

    #[test]
    fn test_streaming_writer_multiple_dvs() {
        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);

        let mut dv1 = KernelDeletionVector::new();
        dv1.add_deleted_row_indexes([0u64, 9]);

        let mut dv2 = KernelDeletionVector::new();
        dv2.add_deleted_row_indexes([5u64, 15, 25]);

        let desc1 = writer.write_deletion_vector(dv1).unwrap();
        let desc2 = writer.write_deletion_vector(dv2).unwrap();
        writer.finalize().unwrap();

        // Check that offsets are different and sequential
        assert_eq!(desc1.offset, 1);
        assert!(desc2.offset > desc1.offset);
        assert_eq!(desc1.cardinality, 2);
        assert_eq!(desc2.cardinality, 3);
    }

    #[test]
    fn test_streaming_writer_empty_dv() {
        use crate::Engine;
        use std::fs::File;
        use tempfile::tempdir;
        use url::Url;

        // Create a temporary directory and file
        let temp_dir = tempdir().unwrap();
        let table_url = Url::from_directory_path(temp_dir.path()).unwrap();

        let dv_path = DeletionVectorPath::new(table_url.clone(), String::from("test"));
        let file_path = dv_path.absolute_path().unwrap().to_file_path().unwrap();

        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        let mut file = File::create(&file_path).unwrap();

        // Create an empty deletion vector
        let dv = KernelDeletionVector::new();

        let mut writer = StreamingDeletionVectorWriter::new(&mut file);
        let write_result = writer.write_deletion_vector(dv).unwrap();
        writer.finalize().unwrap();
        drop(file); // Ensure file is closed

        // Check descriptor values for empty DV
        assert_eq!(write_result.offset, 1); // After version byte
        assert_eq!(write_result.cardinality, 0);
        assert!(write_result.size_in_bytes > 0); // Still has magic number

        // Read back using the descriptor to verify empty bitmap can be read
        use crate::engine::sync::SyncEngine;
        let engine = SyncEngine::new();
        let storage = engine.storage_handler();

        let descriptor = write_result.to_descriptor(&dv_path);
        let treemap = descriptor.read(storage, &table_url).unwrap();

        // Verify the treemap is empty
        assert_eq!(treemap.len(), 0);
        assert!(treemap.is_empty());
    }

    #[test]
    fn test_streaming_writer_roundtrip() {
        // Write a deletion vector
        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);

        let mut dv = KernelDeletionVector::new();
        let test_indexes = vec![3, 4, 7, 11, 18, 29];
        dv.add_deleted_row_indexes(&test_indexes);

        let descriptor = writer.write_deletion_vector(dv).unwrap();
        writer.finalize().unwrap();

        // Now try to read it back
        let mut cursor = Cursor::new(buffer);
        cursor.set_position(descriptor.offset as u64);

        // Read size
        let mut size_buf = [0u8; 4];
        std::io::Read::read_exact(&mut cursor, &mut size_buf).unwrap();
        let size = u32::from_be_bytes(size_buf);
        assert_eq!(size, descriptor.size_in_bytes as u32);

        // Read magic
        let mut magic_buf = [0u8; 4];
        std::io::Read::read_exact(&mut cursor, &mut magic_buf).unwrap();
        let magic = u32::from_le_bytes(magic_buf);
        assert_eq!(magic, 1681511377);

        // Read the serialized data (size includes magic, so actual data is size - 4)
        let serialized_data_len = (size - 4) as usize;
        let mut serialized_data = vec![0u8; serialized_data_len];
        std::io::Read::read_exact(&mut cursor, &mut serialized_data).unwrap();

        // Read and verify CRC32 checksum
        let mut crc_buf = [0u8; 4];
        std::io::Read::read_exact(&mut cursor, &mut crc_buf).unwrap();
        let stored_checksum = u32::from_be_bytes(crc_buf);

        // Calculate expected checksum (must include magic + serialized data)
        let crc_instance = create_dv_crc32();
        let mut digest = crc_instance.digest();
        digest.update(&magic_buf);
        digest.update(&serialized_data);
        let expected_checksum = digest.finalize();
        assert_eq!(
            stored_checksum, expected_checksum,
            "CRC32 checksum mismatch"
        );

        // Deserialize the treemap
        let treemap = RoaringTreemap::deserialize_from(&serialized_data[..]).unwrap();
        assert_eq!(treemap.len(), test_indexes.len() as u64);
        for idx in test_indexes {
            assert!(treemap.contains(idx));
        }
    }

    #[test]
    fn test_deletion_vector_trait() {
        struct TestDV {
            indexes: Vec<u64>,
        }

        impl DeletionVector for TestDV {
            type IndexIterator = std::vec::IntoIter<u64>;

            fn into_iter(self) -> Self::IndexIterator {
                self.indexes.into_iter()
            }

            fn cardinality(&self) -> u64 {
                self.indexes.len() as u64
            }
        }

        let test_dv = TestDV {
            indexes: vec![1, 2, 3],
        };

        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
        let descriptor = writer.write_deletion_vector(test_dv).unwrap();

        assert_eq!(descriptor.cardinality, 3);
    }

    #[test]
    fn test_array_based_deletion_vector() {
        use crate::Engine;
        use std::fs::File;
        use tempfile::tempdir;
        use url::Url;

        // Custom DeletionVector implementation that wraps an array of u64
        struct ArrayDeletionVector {
            deleted_rows: Vec<u64>,
        }

        impl ArrayDeletionVector {
            fn new(deleted_rows: Vec<u64>) -> Self {
                Self { deleted_rows }
            }
        }

        impl DeletionVector for ArrayDeletionVector {
            type IndexIterator = std::vec::IntoIter<u64>;

            fn into_iter(self) -> Self::IndexIterator {
                self.deleted_rows.into_iter()
            }

            fn cardinality(&self) -> u64 {
                self.deleted_rows.len() as u64
            }
        }

        // Create a temporary directory and file
        let temp_dir = tempdir().unwrap();
        let table_url = Url::from_directory_path(temp_dir.path()).unwrap();

        let dv_path = DeletionVectorPath::new(table_url.clone(), String::from("test"));
        let file_path = dv_path.absolute_path().unwrap().to_file_path().unwrap();

        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        let mut file = File::create(&file_path).unwrap();

        // Create an array-based deletion vector with specific deleted row indexes
        let deleted_indexes = vec![5u64, 12, 23, 45, 67, 89, 100];
        let array_dv = ArrayDeletionVector::new(deleted_indexes.clone());

        // Write using StreamingDeletionVectorWriter
        let mut writer = StreamingDeletionVectorWriter::new(&mut file);
        let write_result = writer.write_deletion_vector(array_dv).unwrap();
        writer.finalize().unwrap();
        drop(file); // Ensure file is closed

        // Verify the write result metadata
        assert_eq!(write_result.cardinality, deleted_indexes.len() as i64);
        assert_eq!(write_result.offset, 1); // After version byte
        assert!(write_result.size_in_bytes > 0);

        // Read back using the descriptor to verify the data was written correctly
        use crate::engine::sync::SyncEngine;
        let engine = SyncEngine::new();
        let storage = engine.storage_handler();

        let descriptor = write_result.to_descriptor(&dv_path);
        let treemap = descriptor.read(storage, &table_url).unwrap();

        // Verify the exact set of indexes matches
        let read_indexes: Vec<u64> = treemap.into_iter().collect();
        assert_eq!(read_indexes, deleted_indexes);
    }

    #[test]
    fn test_to_descriptor_preserves_absolute_path() {
        use url::Url;

        let table_path = Url::parse("file:///tmp/test_table/").unwrap();
        let prefix = String::from("deletion_vectors");

        let dv_path = DeletionVectorPath::new(table_path.clone(), prefix);

        // Get the absolute path from DeletionVectorPath
        let expected_absolute_path = dv_path.absolute_path().unwrap();

        // Create a write result and convert to descriptor
        let write_result = DeletionVectorWriteResult {
            offset: 1,
            size_in_bytes: 100,
            cardinality: 42,
        };

        let descriptor = write_result.to_descriptor(&dv_path);

        // Get the absolute path from the descriptor
        let actual_absolute_path = descriptor.absolute_path(&table_path).unwrap();

        // Verify they match
        assert_eq!(Some(expected_absolute_path), actual_absolute_path);
    }

    #[test]
    fn test_to_descriptor_preserves_absolute_path_empty_prefix() {
        use url::Url;

        let table_path = Url::parse("file:///tmp/test_table/").unwrap();
        let prefix = String::from("");

        let dv_path = DeletionVectorPath::new(table_path.clone(), prefix);

        // Get the absolute path from DeletionVectorPath
        let expected_absolute_path = dv_path.absolute_path().unwrap();

        // Create a write result and convert to descriptor
        let write_result = DeletionVectorWriteResult {
            offset: 10,
            size_in_bytes: 50,
            cardinality: 5,
        };

        let descriptor = write_result.to_descriptor(&dv_path);

        // Get the absolute path from the descriptor
        let actual_absolute_path = descriptor.absolute_path(&table_path).unwrap();

        // Verify they match
        assert_eq!(Some(expected_absolute_path), actual_absolute_path);
    }

    #[test]
    fn test_to_descriptor_fields() {
        use url::Url;

        let table_path = Url::parse("s3://my-bucket/delta_table/").unwrap();
        let prefix = String::from("dv");

        let dv_path = DeletionVectorPath::new(table_path.clone(), prefix);

        let write_result = DeletionVectorWriteResult {
            offset: 42,
            size_in_bytes: 256,
            cardinality: 100,
        };

        let descriptor = write_result.to_descriptor(&dv_path);

        // Verify descriptor fields match write result
        assert_eq!(descriptor.offset, Some(42));
        assert_eq!(descriptor.size_in_bytes, 256);
        assert_eq!(descriptor.cardinality, 100);
        assert_eq!(
            descriptor.storage_type,
            DeletionVectorStorageType::PersistedRelative
        );
    }

    #[test]
    fn test_multiple_deletion_vectors_roundtrip_with_descriptor() {
        use crate::Engine;
        use std::fs::File;
        use tempfile::tempdir;
        use url::Url;

        // Create a temporary directory and file
        let temp_dir = tempdir().unwrap();
        let table_url = Url::from_directory_path(temp_dir.path()).unwrap();

        let dv_path = DeletionVectorPath::new(table_url.clone(), String::from("abc"));
        let file_path = dv_path.absolute_path().unwrap().to_file_path().unwrap();

        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        let mut file = File::create(&file_path).unwrap();

        // Create multiple deletion vectors with different data
        let test_data = vec![
            vec![0u64, 5, 10, 15],
            vec![1u64, 2, 3, 100, 200],
            vec![50u64, 51, 52, 53, 54, 55],
        ];

        // Write all deletion vectors and collect their descriptors
        let mut descriptors = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut file);

        for indexes in &test_data {
            let mut dv = KernelDeletionVector::new();
            dv.add_deleted_row_indexes(indexes);

            let write_result = writer.write_deletion_vector(dv).unwrap();
            descriptors.push(write_result);
        }

        writer.finalize().unwrap();
        drop(file); // Ensure file is closed

        // Create a storage handler using sync engine
        use crate::engine::sync::SyncEngine;
        let engine = SyncEngine::new();
        let storage = engine.storage_handler();

        // Now read back each deletion vector using the descriptors
        for (write_result, expected_indexes) in descriptors.iter().zip(&test_data) {
            // Create a new DeletionVectorPath for each DV (they would have different UUIDs normally,
            // but for this test we're writing multiple to the same file)
            let descriptor = write_result.clone().to_descriptor(&dv_path);

            // Read the deletion vector back using the descriptor
            let treemap = descriptor.read(storage.clone(), &table_url).unwrap();

            // Verify the content matches
            assert_eq!(
                treemap,
                expected_indexes.iter().collect::<RoaringTreemap>(),
                "read {:?} != expected {:?}",
                treemap,
                expected_indexes
            );
        }
    }
}
