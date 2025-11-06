//! Code relating to parsing and using deletion vectors

use std::io::{Cursor, Read};
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::schema::derive_macro_utils::ToDataType;
use delta_kernel_derive::ToSchema;
use roaring::RoaringTreemap;
use url::Url;

use crc::{Crc, CRC_32_ISO_HDLC};

use crate::schema::DataType;
use crate::utils::require;
use crate::{DeltaResult, Error, StorageHandler};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub enum DeletionVectorStorageType {
    #[cfg_attr(test, serde(rename = "u"))]
    PersistedRelative,
    #[cfg_attr(test, serde(rename = "i"))]
    Inline,
    #[cfg_attr(test, serde(rename = "p"))]
    PersistedAbsolute,
}

impl FromStr for DeletionVectorStorageType {
    type Err = Error;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s {
            "u" => Ok(Self::PersistedRelative),
            "i" => Ok(Self::Inline),
            "p" => Ok(Self::PersistedAbsolute),
            _ => Err(Error::internal_error(format!(
                "Unsupported deletion vector format option: {}",
                s
            ))),
        }
    }
}

impl std::fmt::Display for DeletionVectorStorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeletionVectorStorageType::PersistedRelative => write!(f, "u"),
            DeletionVectorStorageType::Inline => write!(f, "i"),
            DeletionVectorStorageType::PersistedAbsolute => write!(f, "p"),
        }
    }
}

impl ToDataType for DeletionVectorStorageType {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

/// Represents an abstract path to a deletion vector file.
///
/// This is used in the public API to construct the path to a deletion vector file and
/// has logic to convert [`crate::actions::deletion_vector_writer::DeletionVectorWriteResult`]
/// to a [`DeletionVectorDescriptor`] with appropriate storage type and path.
pub struct DeletionVectorPath {
    /// The base URL path to the Delta table
    table_path: Url,
    /// Unique identifier for this deletion vector file
    uuid: uuid::Uuid,
    /// Optional directory prefix within the table path where the DV file will be located,
    /// this is to allow for randomizing reads/writes to avoid object store throttling.
    prefix: String,
}

impl DeletionVectorPath {
    pub(crate) fn new(table_path: Url, prefix: String) -> Self {
        Self {
            table_path,
            uuid: uuid::Uuid::new_v4(),
            prefix,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_uuid(table_path: Url, prefix: String, uuid: uuid::Uuid) -> Self {
        Self {
            table_path,
            uuid,
            prefix,
        }
    }

    /// Helper method to construct the relative path to a deletion vector file
    /// from the prefix and UUID suffix.
    fn relative_path(prefix: &str, uuid: &uuid::Uuid) -> String {
        if !prefix.is_empty() {
            format!("{prefix}/deletion_vector_{uuid}.bin")
        } else {
            format!("deletion_vector_{uuid}.bin")
        }
    }

    /// Returns the absolute path to the deletion vector file.
    pub fn absolute_path(&self) -> DeltaResult<Url> {
        let dv_suffix = Self::relative_path(&self.prefix, &self.uuid);
        self.table_path
            .join(&dv_suffix)
            .map_err(|_| Error::DeletionVector(format!("invalid path: {dv_suffix}")))
    }

    /// Returns the compressed encoded path for use in descriptor (prefix + z85 encoded UUID).
    pub(crate) fn encoded_relative_path(&self) -> String {
        format!("{}{}", self.prefix, z85::encode(self.uuid.as_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[cfg_attr(
    test,
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: DeletionVectorStorageType,

    /// Three format options are currently proposed:
    /// - If `storageType = 'u'` then `<random prefix - optional><base85 encoded uuid>`:
    ///   The deletion vector is stored in a file with a path relative to the data
    ///   directory of this Delta table, and the file name can be reconstructed from
    ///   the UUID. See Derived Fields for how to reconstruct the file name. The random
    ///   prefix is recovered as the extra characters before the (20 characters fixed length) uuid.
    /// - If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector
    ///   is stored inline in the log. The format used is the `RoaringBitmapArray`
    ///   format also used when the DV is stored on disk and described in [Deletion Vector Format].
    /// - If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an
    ///   absolute path given by this path, which has the same format as the `path` field
    ///   in the `add`/`remove` actions.
    ///
    /// [Deletion Vector Format]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
    pub path_or_inline_dv: String,

    /// Start of the data for this DV in number of bytes from the beginning of the file it is stored in.
    /// Always None (absent in JSON) when `storageType = 'i'`.
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

impl DeletionVectorDescriptor {
    pub fn unique_id(&self) -> String {
        Self::unique_id_from_parts(
            &self.storage_type.to_string(),
            &self.path_or_inline_dv,
            self.offset,
        )
    }
    pub(crate) fn unique_id_from_parts(
        storage_type: &str,
        path_or_inline_dv: &str,
        offset: Option<i32>,
    ) -> String {
        match offset {
            Some(offset) => format!("{storage_type}{path_or_inline_dv}@{offset}"),
            None => format!("{storage_type}{path_or_inline_dv}"),
        }
    }

    pub fn absolute_path(&self, parent: &Url) -> DeltaResult<Option<Url>> {
        match self.storage_type {
            DeletionVectorStorageType::PersistedRelative => {
                let path_len = self.path_or_inline_dv.len();
                require!(
                    path_len >= 20,
                    Error::DeletionVector(format!("Invalid length {path_len}, must be >= 20"))
                );
                let prefix_len = path_len - 20;
                let decoded = z85::decode(&self.path_or_inline_dv[prefix_len..])
                    .map_err(|_| Error::deletion_vector("Failed to decode DV uuid"))?;
                let uuid = uuid::Uuid::from_slice(&decoded)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let dv_suffix =
                    DeletionVectorPath::relative_path(&self.path_or_inline_dv[..prefix_len], &uuid);
                let dv_path = parent
                    .join(&dv_suffix)
                    .map_err(|_| Error::DeletionVector(format!("invalid path: {dv_suffix}")))?;
                Ok(Some(dv_path))
            }
            DeletionVectorStorageType::PersistedAbsolute => {
                Ok(Some(Url::parse(&self.path_or_inline_dv).map_err(|_| {
                    Error::DeletionVector(format!("invalid path: {}", self.path_or_inline_dv))
                })?))
            }
            DeletionVectorStorageType::Inline => Ok(None),
        }
    }

    /// Read a dv in stored form into a [`RoaringTreemap`]
    // A few notes:
    //  - dvs write integers in BOTH big and little endian format. The magic and dv itself are
    //  little, while the version, size, and checksum are big
    //  - dvs can potentially indicate the size in the delta log, and _also_ in the file. If both
    //  are present, we assert they are the same
    pub fn read(
        &self,
        storage: Arc<dyn StorageHandler>,
        parent: &Url,
    ) -> DeltaResult<RoaringTreemap> {
        match self.absolute_path(parent)? {
            None => {
                let byte_slice = z85::decode(&self.path_or_inline_dv)
                    .map_err(|_| Error::deletion_vector("Failed to decode DV"))?;
                let magic = slice_to_u32(&byte_slice[0..4], Endian::Little)?;
                match magic {
                    1681511377 => RoaringTreemap::deserialize_from(&byte_slice[4..])
                        .map_err(|err| Error::DeletionVector(err.to_string())),
                    1681511376 => {
                        todo!("Don't support native serialization in inline bitmaps yet");
                    }
                    _ => Err(Error::DeletionVector(format!("Invalid magic {magic}"))),
                }
            }
            Some(path) => {
                let size_in_bytes: u32 =
                    self.size_in_bytes
                        .try_into()
                        .or(Err(Error::DeletionVector(format!(
                            "size_in_bytes doesn't fit in usize for {path}"
                        ))))?;

                let dv_data = storage
                    .read_files(vec![(path.clone(), None)])?
                    .next()
                    .ok_or(Error::missing_data(format!(
                        "No deletion vector data for {path}"
                    )))??;
                let dv_data_len = dv_data.len();

                let mut cursor = Cursor::new(dv_data);
                let mut version_buf = [0; 1];
                cursor.read(&mut version_buf).map_err(|err| {
                    Error::DeletionVector(format!("Failed to read version from {path}: {err}"))
                })?;
                let version = u8::from_be_bytes(version_buf);
                require!(
                    version == 1,
                    Error::DeletionVector(format!("Invalid version {version} for {path}"))
                );

                // Deletion vector file format:
                // +---------------+-----------------+
                // |  num bytes    |  value          |
                // +===============+=================+
                // | 1 byte        |  version        |
                // +---------------+-----------------+
                // | offset-1      |  other dvs...   |
                // +---------------+-----------------+ <- this_dv_start
                // | 4 bytes       |  dv_size        |
                // +---------------+-----------------+
                // | 4 bytes       |  magic value    |
                // +---------------+-----------------+ <- bitmap_start
                // | dv_size - 4   |  bitmap         |
                // +---------------+-----------------+ <- crc_start
                // | 4 bytes       |  CRC            |
                // +---------------+-----------------+

                let this_dv_start: usize =
                    self.offset
                        .unwrap_or(1)
                        .try_into()
                        .or(Err(Error::DeletionVector(format!(
                            "Offset {:?} doesn't fit in usize for {path}",
                            self.offset
                        ))))?;
                let magic_start = this_dv_start + 4;
                // bitmap_start = this_dv_start + 4 (dv_size field) + 4 (magic field)
                let bitmap_start = this_dv_start + 8;
                // crc_start = this_dv_start + 4 (dv_size field) + dv_size (magic field + bitmap)
                // Safety: size_in_bytes is checked to fit in u32 which for all known platforms should
                // fix in usize range.
                let crc_start = this_dv_start + 4 + (size_in_bytes as usize);
                require!(
                    this_dv_start < dv_data_len,
                    Error::DeletionVector(format!(
                        "This DV start is out of bounds for {path} (Offset: {this_dv_start} >= Size: {dv_data_len})"
                    ))
                );

                cursor.set_position(this_dv_start as u64);
                let dv_size = read_u32(&mut cursor, Endian::Big)?;
                require!(
                    dv_size == size_in_bytes,
                    Error::DeletionVector(format!(
                        "DV size mismatch for {path}. Log indicates {size_in_bytes}, file says: {dv_size}"
                    ))
                );
                let magic = read_u32(&mut cursor, Endian::Little)?;
                require!(
                    magic == 1681511377,
                    Error::DeletionVector(format!("Invalid magic {magic} for {path}"))
                );

                let bytes = cursor.into_inner();

                // +4 to account for CRC value
                require!(
                    bytes.len() >= crc_start + 4,
                    Error::DeletionVector(format!(
                        "Can't read deletion vector for {path} as there are not enough bytes. Expected {}, but got {}",
                        crc_start + 4,
                        bytes.len()
                    ))
                );

                let mut crc_cursor: Cursor<Bytes> =
                    Cursor::new(bytes.slice(crc_start..crc_start + 4));
                let crc = read_u32(&mut crc_cursor, Endian::Big)?;
                let crc32 = create_dv_crc32();
                // CRC is calculated from magic field through end of bitmap
                // Safety: verified bytes is larger than crc_start + 4, above.
                let expected_crc = crc32.checksum(&bytes.slice(magic_start..crc_start));
                require!(
                    crc == expected_crc,
                    Error::DeletionVector(format!(
                        "CRC32 checksum mismatch for {path}. Got: {crc}, expected: {expected_crc}"
                    ))
                );
                // Safety: verified bytes is larger than crc_start + 4, above.
                let dv_bytes = bytes.slice(bitmap_start..crc_start);
                let cursor = Cursor::new(dv_bytes);
                RoaringTreemap::deserialize_from(cursor).map_err(|err| {
                    Error::DeletionVector(format!(
                        "Failed to deserialize deletion vector for {path}: {err}"
                    ))
                })
            }
        }
    }

    /// Materialize the row indexes of the deletion vector as a `Vec<u64>` in which each element
    /// represents a row index that is deleted from the table.
    pub fn row_indexes(
        &self,
        storage: Arc<dyn StorageHandler>,
        parent: &Url,
    ) -> DeltaResult<Vec<u64>> {
        Ok(self.read(storage, parent)?.into_iter().collect())
    }
}

enum Endian {
    Big,
    Little,
}

/// Factory function to create a CRC-32 instance using the ISO HDLC algorithm.
/// This ensures consistent CRC algorithm usage for deletion vectors.
pub(crate) fn create_dv_crc32() -> Crc<u32> {
    Crc::<u32>::new(&CRC_32_ISO_HDLC)
}

/// small helper to read a big or little endian u32 from a cursor
fn read_u32(cursor: &mut Cursor<Bytes>, endian: Endian) -> DeltaResult<u32> {
    let mut buf = [0; 4];
    cursor
        .read(&mut buf)
        .map_err(|err| Error::DeletionVector(err.to_string()))?;
    match endian {
        Endian::Big => Ok(u32::from_be_bytes(buf)),
        Endian::Little => Ok(u32::from_le_bytes(buf)),
    }
}

/// decode a slice into a u32
fn slice_to_u32(buf: &[u8], endian: Endian) -> DeltaResult<u32> {
    let array = buf
        .try_into()
        .map_err(|_| Error::generic("Must have a 4 byte slice to decode to u32"))?;
    match endian {
        Endian::Big => Ok(u32::from_be_bytes(array)),
        Endian::Little => Ok(u32::from_le_bytes(array)),
    }
}

/// helper function to convert a treemap into a boolean vector where, for index i, if the bit is
/// set, the vector will be false, and otherwise at index i the vector will be true
pub(crate) fn deletion_treemap_to_bools(treemap: RoaringTreemap) -> Vec<bool> {
    treemap_to_bools_with(treemap, false)
}

/// helper function to convert a treemap into a boolean vector where, for index i, if the bit is
/// set, the vector will be true, and otherwise at index i the vector will be false
pub(crate) fn selection_treemap_to_bools(treemap: RoaringTreemap) -> Vec<bool> {
    treemap_to_bools_with(treemap, true)
}

/// helper function to generate vectors of bools from treemap. If `set_bit` is `true`, this is
/// [`selection_treemap_to_bools`]. If `set_bit` is false, this is [`deletion_treemap_to_bools`]
fn treemap_to_bools_with(treemap: RoaringTreemap, set_bit: bool) -> Vec<bool> {
    fn combine(high_bits: u32, low_bits: u32) -> usize {
        ((u64::from(high_bits) << 32) | u64::from(low_bits)) as usize
    }

    match treemap.max() {
        Some(max) => {
            // there are values in the map
            //TODO(nick) panic if max is > MAX_USIZE
            let mut result = vec![!set_bit; max as usize + 1];
            let bitmaps = treemap.bitmaps();
            for (index, bitmap) in bitmaps {
                for bit in bitmap.iter() {
                    let vec_index = combine(index, bit);
                    result[vec_index] = set_bit;
                }
            }
            result
        }
        None => {
            // empty set, return empty vec
            vec![]
        }
    }
}

/// helper function to split an `Option<Vec<bool>>`. Because deletion vectors apply to a whole file,
/// but parquet readers can chunk the file, there is a need to split the vector up.
/// If the passed vector is Some(vector):
///   - If `split_index < vector.len()`, split `vector` at `split_index`. The passed vector is
///     modified in place, and the split off component is returned.
///   - If `split_index` >= vector.len()` will return None. If `extend` is Some(b), the passed
///     vector will be extended with `b` to have a length of `split_index`. If `extend` is `None`,
///     do nothing and return `None`
/// If the passed `vector` is `None`, do nothing and return None
pub fn split_vector(
    vector: Option<&mut Vec<bool>>,
    split_index: usize,
    extend: Option<bool>,
) -> Option<Vec<bool>> {
    match vector {
        Some(vector) if split_index < vector.len() => Some(vector.split_off(split_index)),
        Some(vector) if extend.is_some() => {
            vector.extend(std::iter::repeat_n(
                // safety: we just checked `is_some` above
                #[allow(clippy::unwrap_used)]
                extend.unwrap(),
                split_index - vector.len(),
            ));
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use roaring::RoaringTreemap;

    use crate::{engine::sync::SyncEngine, Engine};

    use super::DeletionVectorDescriptor;
    use super::*;

    fn dv_relative() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_absolute() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedAbsolute,
            path_or_inline_dv:
                "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_inline() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::Inline,
            path_or_inline_dv: "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L"
                .to_string(),
            offset: None,
            size_in_bytes: 44,
            cardinality: 6,
        }
    }

    fn dv_example() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }
    }

    #[test]
    fn test_deletion_vector_absolute_path() {
        let parent = Url::parse("s3://mytable/").unwrap();

        let relative = dv_relative();
        let expected =
            Url::parse("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, relative.absolute_path(&parent).unwrap().unwrap());

        let absolute = dv_absolute();
        let expected =
            Url::parse("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, absolute.absolute_path(&parent).unwrap().unwrap());

        let inline = dv_inline();
        assert_eq!(None, inline.absolute_path(&parent).unwrap());

        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let dv_url = parent
            .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin")
            .unwrap();
        let example = dv_example();
        assert_eq!(dv_url, example.absolute_path(&parent).unwrap().unwrap());
    }

    #[test]
    fn test_inline_read() {
        let inline = dv_inline();
        let sync_engine = SyncEngine::new();
        let storage = sync_engine.storage_handler();
        let parent = Url::parse("http://not.used").unwrap();
        let tree_map = inline.read(storage, &parent).unwrap();
        assert_eq!(tree_map.len(), 6);
        for i in [3, 4, 7, 11, 18, 29] {
            assert!(tree_map.contains(i));
        }
        for i in [1, 2, 8, 17, 55, 200] {
            assert!(!tree_map.contains(i));
        }
    }

    #[test]
    fn test_deletion_vector_read() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let sync_engine = SyncEngine::new();
        let storage = sync_engine.storage_handler();

        let example = dv_example();
        let tree_map = example.read(storage.clone(), &parent).unwrap();

        let expected: Vec<u64> = vec![0, 9];
        let found = tree_map.iter().collect::<Vec<_>>();
        assert_eq!(found, expected)
    }

    // this test is ignored by default as it's expensive to allocate such big vecs full of `true`. you can run it via:
    // cargo test actions::deletion_vector::tests::test_dv_to_bools -- --ignored
    #[test]
    #[ignore]
    fn test_dv_to_bools() {
        let mut rb = RoaringTreemap::new();
        rb.insert(0);
        rb.insert(2);
        rb.insert(7);
        rb.insert(30854);
        rb.insert(4294967297);
        rb.insert(4294967300);
        let bools = super::deletion_treemap_to_bools(rb);
        let mut expected = vec![true; 4294967301];
        expected[0] = false;
        expected[2] = false;
        expected[7] = false;
        expected[30854] = false;
        expected[4294967297] = false;
        expected[4294967300] = false;
        assert_eq!(bools, expected);
    }

    // Unlike [`test_dv_to_bools`], this test is not ignored because the large zero-initialized selection vector is fast to allocate.
    // It just gets a bunch of empty pages from the OS. [`tet_dv_to_bools`] is slow because we must
    // set every element to `true`.
    #[test]
    fn test_sv_to_bools() {
        let mut rb = RoaringTreemap::new();
        rb.insert(0);
        rb.insert(2);
        rb.insert(7);
        rb.insert(30854);
        rb.insert(4294967297);
        rb.insert(4294967300);
        let bools = super::selection_treemap_to_bools(rb);
        let mut expected = vec![false; 4294967301];
        expected[0] = true;
        expected[2] = true;
        expected[7] = true;
        expected[30854] = true;
        expected[4294967297] = true;
        expected[4294967300] = true;
        assert_eq!(bools, expected);
    }

    #[test]
    fn test_dv_row_indexes() {
        let example = dv_inline();
        let sync_engine = SyncEngine::new();
        let storage = sync_engine.storage_handler();
        let parent = Url::parse("http://not.used").unwrap();
        let row_idx = example.row_indexes(storage, &parent).unwrap();

        assert_eq!(row_idx.len(), 6);
        assert_eq!(&row_idx, &[3, 4, 7, 11, 18, 29]);
    }

    #[test]
    fn test_deletion_vector_storage_type_from_str_valid() {
        // Test valid single character codes
        assert_eq!(
            "u".parse::<DeletionVectorStorageType>().unwrap(),
            DeletionVectorStorageType::PersistedRelative
        );
        assert_eq!(
            "i".parse::<DeletionVectorStorageType>().unwrap(),
            DeletionVectorStorageType::Inline
        );
        assert_eq!(
            "p".parse::<DeletionVectorStorageType>().unwrap(),
            DeletionVectorStorageType::PersistedAbsolute
        );
    }

    #[test]
    fn test_deletion_vector_storage_type_from_str_invalid() {
        // Test invalid codes return errors
        assert!("x".parse::<DeletionVectorStorageType>().is_err());
        assert!("U".parse::<DeletionVectorStorageType>().is_err());
        assert!("I".parse::<DeletionVectorStorageType>().is_err());
        assert!("P".parse::<DeletionVectorStorageType>().is_err());
        assert!("".parse::<DeletionVectorStorageType>().is_err());
        assert!("invalid".parse::<DeletionVectorStorageType>().is_err());
        assert!("PersistedRelative"
            .parse::<DeletionVectorStorageType>()
            .is_err());
        assert!("Inline".parse::<DeletionVectorStorageType>().is_err());
        assert!("PersistedAbsolute"
            .parse::<DeletionVectorStorageType>()
            .is_err());
    }

    #[test]
    fn test_deletion_vector_storage_type_from_str_error_message() {
        // Test that error messages contain the invalid input
        let result = "invalid".parse::<DeletionVectorStorageType>();
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("invalid"));
        assert!(error_msg.contains("Unsupported deletion vector format option"));
    }

    #[test]
    fn test_deletion_vector_storage_type_roundtrip() {
        // Test that Display -> FromStr roundtrip works
        let variants = [
            DeletionVectorStorageType::PersistedRelative,
            DeletionVectorStorageType::Inline,
            DeletionVectorStorageType::PersistedAbsolute,
        ];

        for variant in variants {
            let string_repr = variant.to_string();
            let parsed = string_repr.parse::<DeletionVectorStorageType>().unwrap();
            assert_eq!(variant, parsed);
        }
    }

    #[test]
    fn test_deletion_vector_path_uniqueness() {
        // Verify that two DeletionVectorPath instances created with the same arguments
        // produce different absolute paths due to unique UUIDs
        let table_path = Url::parse("file:///tmp/test_table/").unwrap();
        let prefix = String::from("deletion_vectors");

        let dv_path1 = DeletionVectorPath::new(table_path.clone(), prefix.clone());
        let dv_path2 = DeletionVectorPath::new(table_path.clone(), prefix.clone());

        let abs_path1 = dv_path1.absolute_path().unwrap();
        let abs_path2 = dv_path2.absolute_path().unwrap();

        // The absolute paths should be different because each DeletionVectorPath
        // gets a unique UUID
        assert_ne!(abs_path1, abs_path2);
        assert_ne!(
            dv_path1.encoded_relative_path(),
            dv_path2.encoded_relative_path()
        );
    }

    #[test]
    fn test_deletion_vector_path_absolute_path_with_prefix() {
        let table_path = Url::parse("file:///tmp/test_table/").unwrap();
        let prefix = String::from("dv");
        let known_uuid = uuid::Uuid::parse_str("abcdef01-2345-6789-abcd-ef0123456789").unwrap();

        let dv_path = DeletionVectorPath::new_with_uuid(table_path.clone(), prefix, known_uuid);
        let abs_path = dv_path.absolute_path().unwrap();

        // Verify the exact path with known UUID
        let expected =
            "file:///tmp/test_table/dv/deletion_vector_abcdef01-2345-6789-abcd-ef0123456789.bin";
        assert_eq!(abs_path.as_str(), expected);
    }

    #[test]
    fn test_deletion_vector_path_absolute_path_with_known_uuid() {
        // Test with a known UUID to verify exact path construction
        let table_path = Url::parse("file:///tmp/test_table/").unwrap();
        let prefix = String::from("dv");
        let known_uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        let dv_path = DeletionVectorPath::new_with_uuid(table_path, prefix, known_uuid);
        let abs_path = dv_path.absolute_path().unwrap();

        // Verify the exact path is constructed correctly
        let expected_path =
            "file:///tmp/test_table/dv/deletion_vector_550e8400-e29b-41d4-a716-446655440000.bin";
        assert_eq!(abs_path.as_str(), expected_path);

        // Verify the encoded_relative_path is exactly as expected (prefix + z85 encoded UUID: 20 chars)
        let encoded = dv_path.encoded_relative_path();
        assert_eq!(encoded, "dvrsTVZ&*Sl-RXRWjryu/!");
    }

    #[test]
    fn test_deletion_vector_path_absolute_path_with_known_uuid_empty_prefix() {
        // Test with a known UUID and empty prefix
        let table_path = Url::parse("file:///tmp/test_table/").unwrap();
        let prefix = String::from("");
        let known_uuid = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();

        let dv_path = DeletionVectorPath::new_with_uuid(table_path, prefix, known_uuid);
        let abs_path = dv_path.absolute_path().unwrap();

        // Verify the exact path is constructed correctly without prefix directory
        let expected_path =
            "file:///tmp/test_table/deletion_vector_123e4567-e89b-12d3-a456-426614174000.bin";
        assert_eq!(abs_path.as_str(), expected_path);

        // Verify the encoded_relative_path is exactly as expected (z85 encoded UUID: 20 chars)
        let encoded = dv_path.encoded_relative_path();
        assert_eq!(encoded, "5<w-%>:JjlQ/G/]6C<1m");
    }
}
