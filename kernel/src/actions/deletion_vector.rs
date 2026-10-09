//! Code relating to parsing and using deletion vectors

use std::io::{Cursor, Read};
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use crc::{Crc, CRC_32_ISO_HDLC};
use delta_kernel::schema::derive_macro_utils::ToDataType;
use delta_kernel_derive::{internal_api, IntoStructData, ToSchema};
use percent_encoding::percent_decode_str;
use roaring::RoaringTreemap;
use serde::Deserialize;
use url::Url;

#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::amt_path_util::{resolve_table_relative, validate_table_relative};
use crate::schema::DataType;
use crate::utils::require;
use crate::{KernelError, KernelResult, Result, Scalar, StorageHandler};

/// Magic number for portable RoaringBitmap serialization format.
/// This is the standard format defined in the RoaringBitmap Specification
/// and is used by Delta for deletion vector storage.
/// See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format
const ROARING_BITMAP_PORTABLE_MAGIC: u32 = 1681511377;

/// Magic number for native RoaringBitmap serialization format.
/// This format is reserved for future use and not currently supported.
const ROARING_BITMAP_NATIVE_MAGIC: u32 = 1681511376;

const INLINE_DELETION_VECTOR_MAGIC_SIZE: usize = 4;

/// Percent-decodes `s` into an owned UTF-8 string, erroring on invalid UTF-8. Used to decode a
/// relativized absolute DV path into the same form as the (unencoded) `'u'`/`'r'` paths that name
/// the same file.
fn percent_decode(s: &str) -> KernelResult<String> {
    percent_decode_str(s)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|e| KernelError::deletion_vector(format!("DV path is not valid UTF-8: {e}")))
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub enum DeletionVectorStorageType {
    #[cfg_attr(test, serde(rename = "u"))]
    PersistedRelative,
    #[cfg_attr(test, serde(rename = "i"))]
    Inline,
    #[cfg_attr(test, serde(rename = "p"))]
    PersistedAbsolute,
    /// Unlike [`PersistedRelative`](Self::PersistedRelative) (`'u'`), `path_or_inline_dv` stores
    /// the raw path with no base85/UUID encoding; it is percent-encoded only when resolved against
    /// the table root.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[cfg_attr(test, serde(rename = "r"))]
    PersistedUnencodedRelative,
}

impl FromStr for DeletionVectorStorageType {
    type Err = KernelError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "u" => Ok(Self::PersistedRelative),
            "i" => Ok(Self::Inline),
            "p" => Ok(Self::PersistedAbsolute),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            "r" => Ok(Self::PersistedUnencodedRelative),
            _ => Err(KernelError::internal_error(format!(
                "Unsupported deletion vector format option: {s}"
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
            #[cfg(feature = "adaptive-metadata-in-dev")]
            DeletionVectorStorageType::PersistedUnencodedRelative => write!(f, "r"),
        }
    }
}

impl ToDataType for DeletionVectorStorageType {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<DeletionVectorStorageType> for Scalar {
    fn from(value: DeletionVectorStorageType) -> Self {
        value.to_string().into()
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
    pub fn absolute_path(&self) -> Result<Url> {
        let dv_suffix = Self::relative_path(&self.prefix, &self.uuid);
        self.table_path
            .join(&dv_suffix)
            .map_err(|_| KernelError::DeletionVector(format!("invalid path: {dv_suffix}")))
    }

    /// Returns the compressed encoded path for use in descriptor (prefix + z85 encoded UUID).
    pub(crate) fn encoded_relative_path(&self) -> String {
        format!("{}{}", self.prefix, z85::encode(self.uuid.as_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, IntoStructData, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase", try_from = "DeletionVectorRaw")]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'],
    /// plus 'r' when built with the `adaptive-metadata-in-dev` cargo feature.
    pub storage_type: DeletionVectorStorageType,

    /// The format depends on `storage_type`:
    /// - If `storageType = 'u'` then `<random prefix - optional><base85 encoded uuid>`: The
    ///   deletion vector is stored in a file with a path relative to the data directory of this
    ///   Delta table, and the file name can be reconstructed from the UUID. See Derived Fields for
    ///   how to reconstruct the file name. The random prefix is recovered as the extra characters
    ///   before the (20 characters fixed length) uuid.
    /// - If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector is stored
    ///   inline in the log. The format used is the `RoaringBitmapArray` format also used when the
    ///   DV is stored on disk and described in [Deletion Vector Format].
    /// - If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an
    ///   absolute path given by this path, which has the same format as the `path` field in the
    ///   `add`/`remove` actions.
    /// - If `storageType = 'r'` then `<relative path>`: the raw, unencoded path to the DV file,
    ///   relative to the table root (per the Iceberg V4 path spec). Only valid when built with the
    ///   `adaptive-metadata-in-dev` cargo feature.
    ///
    /// [Deletion Vector Format]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
    pub path_or_inline_dv: String,

    /// Start of the data for this DV in number of bytes from the beginning of the file it is
    /// stored in. Always None (absent in JSON) when `storageType = 'i'`.
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeletionVectorRaw {
    storage_type: String,
    path_or_inline_dv: String,
    offset: Option<i32>,
    size_in_bytes: i32,
    cardinality: i64,
}

impl TryFrom<DeletionVectorRaw> for DeletionVectorDescriptor {
    type Error = KernelError;

    fn try_from(raw: DeletionVectorRaw) -> Result<Self> {
        Self::try_new(
            raw.storage_type.parse()?,
            raw.path_or_inline_dv,
            raw.offset,
            raw.size_in_bytes,
            raw.cardinality,
        )
    }
}

impl DeletionVectorDescriptor {
    /// Construct a validated [`DeletionVectorDescriptor`] from its raw fields.
    ///
    /// Validates the protocol-level invariants from the "Deletion Vector Descriptor Schema"
    /// section of the Delta protocol:
    /// - `size_in_bytes` and `cardinality` must be non-negative.
    /// - If `offset` is present, it must be non-negative.
    /// - `Inline` descriptors must not carry an offset.
    /// - `PersistedRelative` paths carry an optional random prefix followed by a 20-character
    ///   z85-encoded UUID, so they must be at least 20 characters long.
    /// - `PersistedAbsolute` paths must parse as a URL.
    /// - `PersistedUnencodedRelative` paths must be a non-empty, table-root-relative path: no URI
    ///   scheme (i.e. not an absolute URL) and no leading `/`.
    ///
    /// `Inline` payload bytes are accepted verbatim; the framing of the embedded RoaringBitmap
    /// is only checked when the DV is later read via [`Self::read`].
    pub fn try_new(
        storage_type: DeletionVectorStorageType,
        path_or_inline_dv: impl Into<String>,
        offset: Option<i32>,
        size_in_bytes: i32,
        cardinality: i64,
    ) -> Result<Self> {
        require!(
            size_in_bytes >= 0,
            KernelError::deletion_vector("size_in_bytes must be non-negative")
        );
        require!(
            cardinality >= 0,
            KernelError::deletion_vector("cardinality must be non-negative")
        );
        require!(
            offset.is_none_or(|o| o >= 0),
            KernelError::deletion_vector("offset must be non-negative")
        );
        let path_or_inline_dv = path_or_inline_dv.into();
        match storage_type {
            DeletionVectorStorageType::Inline => require!(
                offset.is_none(),
                KernelError::deletion_vector("inline deletion vectors must not carry an offset")
            ),
            DeletionVectorStorageType::PersistedRelative => {
                // Byte-slice rather than char-slice: z85 is ASCII-only, and string slicing
                // would panic if a non-ASCII byte boundary fell inside the trailing 20-byte
                // window. `z85::decode` accepts `&[u8]` and rejects non-z85 bytes.
                let bytes = path_or_inline_dv.as_bytes();
                require!(
                    bytes.len() >= 20,
                    KernelError::deletion_vector(format!(
                        "persisted-relative DV path must be at least 20 bytes, got {}",
                        bytes.len()
                    ))
                );
                let suffix = &bytes[bytes.len() - 20..];
                z85::decode(suffix).map_err(|_| {
                    KernelError::deletion_vector(
                        "persisted-relative DV path must end with a z85-encoded UUID",
                    )
                })?;
            }
            DeletionVectorStorageType::PersistedAbsolute => {
                Url::parse(&path_or_inline_dv).map_err(|e| {
                    KernelError::deletion_vector(format!(
                        "persisted-absolute DV path must parse as a URL: {e}"
                    ))
                })?;
            }
            #[cfg(feature = "adaptive-metadata-in-dev")]
            DeletionVectorStorageType::PersistedUnencodedRelative => {
                validate_table_relative(&path_or_inline_dv)?;
            }
        }
        Ok(Self {
            storage_type,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
        })
    }

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

    /// Computes the normalized `adaptiveMetadata` DV object identity for a descriptor's raw parts:
    /// `<marker><path>[@<offset>]`.
    ///
    /// Unlike [`Self::unique_id_from_parts`] (which keys on the literal `storageType`), this names
    /// the underlying physical DV blob independent of its storage-type encoding, so an `add` and a
    /// `remove` that encode the same blob under different storage types still match. The storage
    /// type is normalized to a marker and the path to a table-relative (or, when it cannot be made
    /// relative, absolute) form, per the AMT v4 RFC (delta-io/delta#7515):
    ///
    /// - `'u'` -> marker `r`, path = the z85-decoded `<prefix>/deletion_vector_<uuid>.bin`.
    /// - `'r'` -> marker `r`, path = the raw path, unchanged.
    /// - `'p'` -> relativized against `table_root`: marker `r` + relative path if it resolves under
    ///   the root, else marker `p` + the original absolute path.
    /// - `'i'` -> marker `i`, path = `path_or_inline_dv`, unchanged.
    ///
    /// `offset` is kept in the identity (a single file may pack multiple DVs at different offsets).
    pub(crate) fn normalized_unique_id_from_parts(
        storage_type: DeletionVectorStorageType,
        path_or_inline_dv: &str,
        offset: Option<i32>,
        table_root: &Url,
    ) -> KernelResult<String> {
        let (marker, path) =
            Self::normalized_marker_and_path(storage_type, path_or_inline_dv, table_root)?;
        Ok(Self::unique_id_from_parts(marker, &path, offset))
    }

    /// Normalizes a descriptor's `(storage_type, path_or_inline_dv)` to the `(marker, path)` pair
    /// used by [`Self::normalized_unique_id_from_parts`]. See that method for the per-storage-type
    /// rules.
    fn normalized_marker_and_path(
        storage_type: DeletionVectorStorageType,
        path_or_inline_dv: &str,
        table_root: &Url,
    ) -> KernelResult<(&'static str, String)> {
        Ok(match storage_type {
            DeletionVectorStorageType::Inline => ("i", path_or_inline_dv.to_string()),
            DeletionVectorStorageType::PersistedRelative => {
                ("r", Self::decode_uuid_relative_path(path_or_inline_dv)?)
            }
            #[cfg(feature = "adaptive-metadata-in-dev")]
            DeletionVectorStorageType::PersistedUnencodedRelative => {
                ("r", path_or_inline_dv.to_string())
            }
            DeletionVectorStorageType::PersistedAbsolute => {
                let absolute = Url::parse(path_or_inline_dv).map_err(|e| {
                    KernelError::deletion_vector(format!(
                        "persisted-absolute DV path must parse as a URL: {e}"
                    ))
                })?;
                // `make_relative` returns `None` for a different scheme/host and a `..`-prefixed
                // path for a URL outside the root; both mean "not under the table root", so the
                // DV stays absolute (marker `p`). A path under the root relativizes to marker `r`,
                // percent-decoded so it matches the decoded `'u'`/`'r'` form naming the same file.
                match table_root.make_relative(&absolute) {
                    Some(relative) if !relative.is_empty() && !relative.starts_with("../") => {
                        ("r", percent_decode(&relative)?)
                    }
                    _ => ("p", path_or_inline_dv.to_string()),
                }
            }
        })
    }

    /// Decodes a `PersistedRelative` path to its relative-path form
    /// (`<prefix>/deletion_vector_<uuid>.bin`).
    ///
    /// The encoded path is an optional random prefix followed by a fixed-length 20-character z85
    /// UUID; the prefix becomes a directory component, and is omitted entirely when absent:
    ///
    /// ```text
    /// "ab^-aqEH.-t@S}K{vb[*k^" -> "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
    /// "vBn[lx{q8@P<9BNH/isA"   -> "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
    /// ```
    ///
    /// Errors if called on a non-`PersistedRelative` descriptor, if the encoded path is shorter
    /// than the 20-character z85 UUID suffix, or if that suffix fails to decode into a UUID.
    pub(crate) fn relative_path(&self) -> KernelResult<String> {
        require!(
            self.storage_type == DeletionVectorStorageType::PersistedRelative,
            KernelError::DeletionVector(format!(
                "relative_path is only valid for PersistedRelative, got {:?}",
                self.storage_type
            ))
        );
        Self::decode_uuid_relative_path(&self.path_or_inline_dv)
    }

    /// Decodes a `'u'`-encoded `pathOrInlineDv` (optional random prefix + 20-character z85 UUID)
    /// into its table-relative `<prefix>/deletion_vector_<uuid>.bin` form. Shared by
    /// [`Self::relative_path`] and the normalized DV identity.
    fn decode_uuid_relative_path(path_or_inline_dv: &str) -> KernelResult<String> {
        // Byte-slice rather than char-slice: z85 is ASCII-only, and string slicing would panic if
        // a non-ASCII byte boundary fell inside the trailing 20-byte window. Mirrors `try_new`.
        let bytes = path_or_inline_dv.as_bytes();
        require!(
            bytes.len() >= 20,
            KernelError::DeletionVector(format!("Invalid length {}, must be >= 20", bytes.len()))
        );
        let prefix_len = bytes.len() - 20;
        let decoded = z85::decode(&bytes[prefix_len..])
            .map_err(|_| KernelError::deletion_vector("Failed to decode DV uuid"))?;
        let uuid = uuid::Uuid::from_slice(&decoded)
            .map_err(|err| KernelError::DeletionVector(err.to_string()))?;
        let prefix = std::str::from_utf8(&bytes[..prefix_len])
            .map_err(|_| KernelError::deletion_vector("DV path prefix is not valid UTF-8"))?;
        Ok(DeletionVectorPath::relative_path(prefix, &uuid))
    }

    pub fn absolute_path(&self, parent: &Url) -> Result<Option<Url>> {
        match self.storage_type {
            DeletionVectorStorageType::PersistedRelative => {
                let dv_suffix = self.relative_path()?;
                let dv_path = parent.join(&dv_suffix).map_err(|_| {
                    KernelError::DeletionVector(format!("invalid path: {dv_suffix}"))
                })?;
                Ok(Some(dv_path))
            }
            DeletionVectorStorageType::PersistedAbsolute => {
                Ok(Some(Url::parse(&self.path_or_inline_dv).map_err(|_| {
                    KernelError::DeletionVector(format!("invalid path: {}", self.path_or_inline_dv))
                })?))
            }
            #[cfg(feature = "adaptive-metadata-in-dev")]
            DeletionVectorStorageType::PersistedUnencodedRelative => {
                // Validate (reject absolute) and resolve here, not just in `try_new`: descriptors
                // reach the read path via struct literals (e.g. the log-replay visitor) that
                // bypass `try_new`.
                resolve_table_relative(&self.path_or_inline_dv, parent).map(Some)
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
    pub fn read(&self, storage: Arc<dyn StorageHandler>, parent: &Url) -> Result<RoaringTreemap> {
        match self.absolute_path(parent)? {
            None => {
                let byte_slice = z85::decode(&self.path_or_inline_dv)
                    .map_err(|_| KernelError::deletion_vector("Failed to decode DV"))?;
                require!(
                    byte_slice.len() >= INLINE_DELETION_VECTOR_MAGIC_SIZE,
                    KernelError::deletion_vector(
                        "Inline deletion vector payload must contain at least 4 bytes"
                    )
                );
                let magic = slice_to_u32(
                    &byte_slice[..INLINE_DELETION_VECTOR_MAGIC_SIZE],
                    Endian::Little,
                )?;
                match magic {
                    ROARING_BITMAP_PORTABLE_MAGIC => {
                        RoaringTreemap::deserialize_from(&byte_slice[4..])
                            .map_err(|err| KernelError::DeletionVector(err.to_string()))
                    }
                    ROARING_BITMAP_NATIVE_MAGIC => Err(KernelError::deletion_vector(
                        "Native serialization in inline bitmaps is not yet supported",
                    )),
                    _ => Err(KernelError::DeletionVector(format!(
                        "Invalid magic {magic}"
                    ))),
                }
            }
            Some(path) => {
                let size_in_bytes: u32 =
                    self.size_in_bytes
                        .try_into()
                        .or(Err(KernelError::DeletionVector(format!(
                            "size_in_bytes doesn't fit in usize for {path}"
                        ))))?;

                let dv_data = storage
                    .read_files(vec![(path.clone(), None)])?
                    .next()
                    .ok_or(KernelError::missing_data(format!(
                        "No deletion vector data for {path}"
                    )))??;
                let dv_data_len = dv_data.len();

                let mut cursor = Cursor::new(dv_data);
                let mut version_buf = [0; 1];
                cursor.read(&mut version_buf).map_err(|err| {
                    KernelError::DeletionVector(format!(
                        "Failed to read version from {path}: {err}"
                    ))
                })?;
                let version = u8::from_be_bytes(version_buf);
                require!(
                    version == 1,
                    KernelError::DeletionVector(format!("Invalid version {version} for {path}"))
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
                        .or(Err(KernelError::DeletionVector(format!(
                            "Offset {:?} doesn't fit in usize for {path}",
                            self.offset
                        ))))?;
                let magic_start = this_dv_start + 4;
                // bitmap_start = this_dv_start + 4 (dv_size field) + 4 (magic field)
                let bitmap_start = this_dv_start + 8;
                // crc_start = this_dv_start + 4 (dv_size field) + dv_size (magic field + bitmap)
                // Safety: size_in_bytes is checked to fit in u32 which for all known platforms
                // should fix in usize range.
                let crc_start = this_dv_start + 4 + (size_in_bytes as usize);
                require!(
                    this_dv_start < dv_data_len,
                    KernelError::DeletionVector(format!(
                        "This DV start is out of bounds for {path} (Offset: {this_dv_start} >= Size: {dv_data_len})"
                    ))
                );

                cursor.set_position(this_dv_start as u64);
                let dv_size = read_u32(&mut cursor, Endian::Big)?;
                require!(
                    dv_size == size_in_bytes,
                    KernelError::DeletionVector(format!(
                        "DV size mismatch for {path}. Log indicates {size_in_bytes}, file says: {dv_size}"
                    ))
                );
                let magic = read_u32(&mut cursor, Endian::Little)?;
                require!(
                    magic == ROARING_BITMAP_PORTABLE_MAGIC,
                    KernelError::DeletionVector(format!("Invalid magic {magic} for {path}"))
                );

                let bytes = cursor.into_inner();

                // +4 to account for CRC value
                require!(
                    bytes.len() >= crc_start + 4,
                    KernelError::DeletionVector(format!(
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
                    KernelError::DeletionVector(format!(
                        "CRC32 checksum mismatch for {path}. Got: {crc}, expected: {expected_crc}"
                    ))
                );
                // Safety: verified bytes is larger than crc_start + 4, above.
                let dv_bytes = bytes.slice(bitmap_start..crc_start);
                let cursor = Cursor::new(dv_bytes);
                RoaringTreemap::deserialize_from(cursor).map_err(|err| {
                    KernelError::DeletionVector(format!(
                        "Failed to deserialize deletion vector for {path}: {err}"
                    ))
                })
            }
        }
    }

    /// Materialize the row indexes of the deletion vector as a `Vec<u64>` in which each element
    /// represents a row index that is deleted from the table.
    pub fn row_indexes(&self, storage: Arc<dyn StorageHandler>, parent: &Url) -> Result<Vec<u64>> {
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
fn read_u32(cursor: &mut Cursor<Bytes>, endian: Endian) -> KernelResult<u32> {
    let mut buf = [0; 4];
    cursor
        .read(&mut buf)
        .map_err(|err| KernelError::DeletionVector(err.to_string()))?;
    match endian {
        Endian::Big => Ok(u32::from_be_bytes(buf)),
        Endian::Little => Ok(u32::from_le_bytes(buf)),
    }
}

/// decode a slice into a u32
fn slice_to_u32(buf: &[u8], endian: Endian) -> KernelResult<u32> {
    let array = buf
        .try_into()
        .map_err(|_| KernelError::generic("Must have a 4 byte slice to decode to u32"))?;
    match endian {
        Endian::Big => Ok(u32::from_be_bytes(array)),
        Endian::Little => Ok(u32::from_le_bytes(array)),
    }
}

/// helper function to convert a treemap into a boolean vector where, for index i, if the bit is
/// set, the vector will be false, and otherwise at index i the vector will be true
#[internal_api]
pub(crate) fn deletion_treemap_to_bools(treemap: RoaringTreemap) -> Vec<bool> {
    treemap_to_bools_with(treemap, false)
}

/// helper function to convert a treemap into a boolean vector where, for index i, if the bit is
/// set, the vector will be true, and otherwise at index i the vector will be false
#[internal_api]
pub(crate) fn selection_treemap_to_bools(treemap: RoaringTreemap) -> Vec<bool> {
    treemap_to_bools_with(treemap, true)
}

/// helper function to generate vectors of bools from treemap. If `set_bit` is `true`, this is
/// [`selection_treemap_to_bools`]. If `set_bit` is false, this is [`deletion_treemap_to_bools`]
#[internal_api]
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

    use super::{DeletionVectorDescriptor, *};
    use crate::engine::sync::SyncEngine;
    use crate::Engine;

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
    fn descriptor_deserialization_uses_validating_constructor() {
        let valid = r#"{
            "storageType":"i",
            "pathOrInlineDv":"",
            "sizeInBytes":0,
            "cardinality":0
        }"#;
        let descriptor: DeletionVectorDescriptor = serde_json::from_str(valid).unwrap();
        assert_eq!(descriptor.storage_type, DeletionVectorStorageType::Inline);

        let invalid = r#"{
            "storageType":"i",
            "pathOrInlineDv":"",
            "offset":1,
            "sizeInBytes":0,
            "cardinality":0
        }"#;
        let error = serde_json::from_str::<DeletionVectorDescriptor>(invalid).unwrap_err();
        assert!(error.to_string().contains("must not carry an offset"));
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

        #[cfg(feature = "adaptive-metadata-in-dev")]
        {
            let unencoded = DeletionVectorDescriptor::try_new(
                DeletionVectorStorageType::PersistedUnencodedRelative,
                "data/deletion_vector_x.bin",
                Some(1),
                4,
                2,
            )
            .unwrap();
            let expected = Url::parse("s3://mytable/data/deletion_vector_x.bin").unwrap();
            assert_eq!(expected, unencoded.absolute_path(&parent).unwrap().unwrap());
        }

        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let dv_url = parent
            .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin")
            .unwrap();
        let example = dv_example();
        assert_eq!(dv_url, example.absolute_path(&parent).unwrap().unwrap());
    }

    // An unencoded-relative path is percent-encoded (preserving `/`) when resolved, so reserved
    // characters survive a round trip through the object store.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[rstest::rstest]
    #[case::space("data/leaf a.bin", "s3://mytable/data/leaf%20a.bin")]
    #[case::percent("data/a%b.bin", "s3://mytable/data/a%25b.bin")]
    #[case::hash("data/a#b.bin", "s3://mytable/data/a%23b.bin")]
    #[case::question("data/a?b.bin", "s3://mytable/data/a%3Fb.bin")]
    #[case::non_ascii("data/M\u{fc}nchen.bin", "s3://mytable/data/M%C3%BCnchen.bin")]
    fn unencoded_relative_absolute_path_percent_encodes(
        #[case] path: &str,
        #[case] expected: &str,
    ) {
        let parent = Url::parse("s3://mytable/").unwrap();
        // The struct literal bypasses `try_new`, mirroring the log-replay visitor.
        let dv = dv_with_path(DeletionVectorStorageType::PersistedUnencodedRelative, path);
        assert_eq!(
            Url::parse(expected).unwrap(),
            dv.absolute_path(&parent).unwrap().unwrap()
        );
    }

    // `absolute_path` enforces the table-relative invariant even for descriptors built via a
    // struct literal (as the log-replay visitor does), so a scheme-bearing or rooted path cannot
    // resolve outside the table.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[rstest::rstest]
    #[case::scheme("s3://other/dv.bin", "absolute URL")]
    #[case::leading_slash("/abs/dv.bin", "leading '/'")]
    #[case::empty("", "must not be empty")]
    #[case::dot_dot("data/../b.bin", "'..' segment")]
    fn unencoded_relative_absolute_path_rejects_non_relative(
        #[case] path: &str,
        #[case] expected: &str,
    ) {
        let parent = Url::parse("s3://mytable/").unwrap();
        let dv = dv_with_path(DeletionVectorStorageType::PersistedUnencodedRelative, path);
        let err = dv.absolute_path(&parent).unwrap_err().to_string();
        assert!(
            err.contains(expected),
            "error {err:?} did not contain {expected:?}"
        );
    }

    fn dv_with_path(
        storage_type: DeletionVectorStorageType,
        path_or_inline_dv: &str,
    ) -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv: path_or_inline_dv.to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }
    }

    #[rstest::rstest]
    // z85 UUID with a random prefix -> `<prefix>/deletion_vector_<uuid>.bin`.
    #[case(
        "ab^-aqEH.-t@S}K{vb[*k^",
        "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
    )]
    // Bare 20-char z85 UUID, no prefix.
    #[case(
        "vBn[lx{q8@P<9BNH/isA",
        "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
    )]
    fn test_relative_path_decodes(#[case] encoded: &str, #[case] expected: &str) {
        let dv = dv_with_path(DeletionVectorStorageType::PersistedRelative, encoded);
        assert_eq!(dv.relative_path().unwrap(), expected);
    }

    #[rstest::rstest]
    // Non-relative storage types are rejected by the guard.
    #[case(
        DeletionVectorStorageType::PersistedAbsolute,
        "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin",
        "only valid for PersistedRelative"
    )]
    #[case(
        DeletionVectorStorageType::Inline,
        "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
        "only valid for PersistedRelative"
    )]
    #[cfg_attr(
        feature = "adaptive-metadata-in-dev",
        case(
            DeletionVectorStorageType::PersistedUnencodedRelative,
            "data/deletion_vector_x.bin",
            "only valid for PersistedRelative"
        )
    )]
    // Path shorter than the 20-byte z85 UUID suffix.
    #[case(
        DeletionVectorStorageType::PersistedRelative,
        "short",
        "Invalid length"
    )]
    // A non-ASCII byte straddling the trailing-20-byte window must error, not panic. `é` is two
    // bytes, so this 21-byte path leaves `prefix_len == 1` inside the multi-byte `é`; byte-slicing
    // (vs the pre-fix char-slicing) errors cleanly instead of panicking.
    #[case(
        DeletionVectorStorageType::PersistedRelative,
        "éaaaaaaaaaaaaaaaaaaa",
        "Failed to decode DV uuid"
    )]
    fn test_relative_path_errors(
        #[case] storage_type: DeletionVectorStorageType,
        #[case] path_or_inline_dv: &str,
        #[case] expected_error: &str,
    ) {
        let dv = dv_with_path(storage_type, path_or_inline_dv);
        let err = dv.relative_path().unwrap_err().to_string();
        assert!(
            err.contains(expected_error),
            "error {err:?} did not contain {expected_error:?}"
        );
    }

    #[test]
    fn test_magic_number_constants() {
        assert_eq!(ROARING_BITMAP_PORTABLE_MAGIC, 1681511377);
        assert_eq!(ROARING_BITMAP_NATIVE_MAGIC, 1681511376);
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

    #[rstest::rstest]
    #[case::empty(vec![], "at least 4 bytes")]
    #[case::one_byte(vec![0], "at least 4 bytes")]
    #[case::two_bytes(vec![0, 1], "at least 4 bytes")]
    #[case::three_bytes(vec![0, 1, 2], "at least 4 bytes")]
    #[case::invalid_magic(vec![0, 0, 0, 0], "Invalid magic")]
    fn test_inline_read_rejects_malformed_payload(
        #[case] bytes: Vec<u8>,
        #[case] expected_error: &str,
    ) {
        let encoded = z85::encode(&bytes);
        let inline = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            encoded,
            None,
            bytes.len() as i32,
            0,
        )
        .unwrap();
        let sync_engine = SyncEngine::new();
        let storage = sync_engine.storage_handler();
        let parent = Url::parse("http://not.used").unwrap();

        let error = inline.read(storage, &parent).unwrap_err();
        assert!(matches!(&error, KernelError::DeletionVector(_)));
        assert!(
            error.to_string().contains(expected_error),
            "expected error containing {expected_error:?}, got {error}"
        );
    }

    #[test]
    fn test_inline_native_serialization_error() {
        // Construct an inline DV payload whose first 4 bytes (little-endian) are the
        // native serialization magic (1681511376). The `read` method should return
        // a DeletionVector error indicating native serialization isn't supported.
        let sync_engine = SyncEngine::new();
        let storage = sync_engine.storage_handler();
        let parent = Url::parse("http://not.used").unwrap();

        let mut bytes = Vec::new();
        // native serialization magic (little-endian)
        bytes.extend_from_slice(&1681511376u32.to_le_bytes());
        // some trailing bytes (content not important for this test)
        bytes.extend_from_slice(&[1u8, 2, 3, 4]);

        let encoded = z85::encode(&bytes);

        let inline = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::Inline,
            path_or_inline_dv: encoded,
            offset: None,
            size_in_bytes: bytes.len() as i32,
            cardinality: 0,
        };

        let err = inline.read(storage, &parent).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Native serialization in inline bitmaps is not yet supported"));
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

    // Reads through a `StorageHandler` to show the percent-encoded URL finds the raw-named object.
    // Only a space is used: object_store's local filesystem stores `#`, `%` and non-ASCII bytes
    // percent-encoded on disk, so a raw-named file with them is unreachable through it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn unencoded_relative_read_resolves_encoded_characters() {
        let table_dir = tempfile::tempdir().unwrap();
        let raw_path = "data/dv a b.bin";
        std::fs::create_dir(table_dir.path().join("data")).unwrap();
        std::fs::copy(
            "./tests/data/table-with-dv-small/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin",
            table_dir.path().join(raw_path),
        )
        .unwrap();
        let parent = Url::from_directory_path(table_dir.path()).unwrap();
        let storage = SyncEngine::new().storage_handler();
        let dv = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedUnencodedRelative,
            path_or_inline_dv: raw_path.to_string(),
            ..dv_example()
        };

        let tree_map = dv.read(storage, &parent).unwrap();
        assert_eq!(tree_map.iter().collect::<Vec<_>>(), vec![0, 9]);
    }

    // this test is ignored by default as it's expensive to allocate such big vecs full of `true`.
    // you can run it via: cargo test actions::deletion_vector::tests::test_dv_to_bools --
    // --ignored
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

    // Unlike [`test_dv_to_bools`], this test is not ignored because the large zero-initialized
    // selection vector is fast to allocate. It just gets a bunch of empty pages from the OS.
    // [`tet_dv_to_bools`] is slow because we must set every element to `true`.
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
        #[cfg(feature = "adaptive-metadata-in-dev")]
        assert_eq!(
            "r".parse::<DeletionVectorStorageType>().unwrap(),
            DeletionVectorStorageType::PersistedUnencodedRelative
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
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        assert!("r".parse::<DeletionVectorStorageType>().is_err());
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

        #[cfg(feature = "adaptive-metadata-in-dev")]
        {
            let variant = DeletionVectorStorageType::PersistedUnencodedRelative;
            assert_eq!(variant.to_string(), "r");
            assert_eq!(
                variant
                    .to_string()
                    .parse::<DeletionVectorStorageType>()
                    .unwrap(),
                variant
            );
        }
    }

    // `expected` is the normalized `<marker><path>[@<offset>]` identity against table root
    // `s3://mytable/`.
    #[rstest::rstest]
    // `'u'`: z85 UUID (with "ab" prefix) decodes to the `.bin` relative path, marker `r`.
    #[case(
        DeletionVectorStorageType::PersistedRelative,
        "ab^-aqEH.-t@S}K{vb[*k^",
        Some(4),
        "rab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin@4"
    )]
    // `'r'`: raw relative path used as-is, marker `r`.
    #[cfg_attr(
        feature = "adaptive-metadata-in-dev",
        case(
            DeletionVectorStorageType::PersistedUnencodedRelative,
            "data/deletion_vector_x.bin",
            Some(2),
            "rdata/deletion_vector_x.bin@2"
        )
    )]
    // `'p'` under the table root -> relativized, marker `r`.
    #[case(
        DeletionVectorStorageType::PersistedAbsolute,
        "s3://mytable/data/dv.bin",
        Some(1),
        "rdata/dv.bin@1"
    )]
    // `'p'` under the root with a percent-encoded segment -> decoded on relativize.
    #[case(
        DeletionVectorStorageType::PersistedAbsolute,
        "s3://mytable/a%20b/dv.bin",
        None,
        "ra b/dv.bin"
    )]
    // `'p'` outside the table root (different host) -> stays absolute, marker `p`.
    #[case(
        DeletionVectorStorageType::PersistedAbsolute,
        "s3://other-bucket/dv.bin",
        Some(1),
        "ps3://other-bucket/dv.bin@1"
    )]
    // `'i'`: inline payload unchanged, marker `i`, no offset.
    #[case(DeletionVectorStorageType::Inline, "ABC", None, "iABC")]
    fn test_normalized_unique_id_from_parts(
        #[case] storage_type: DeletionVectorStorageType,
        #[case] path_or_inline_dv: &str,
        #[case] offset: Option<i32>,
        #[case] expected: &str,
    ) {
        let table_root = Url::parse("s3://mytable/").unwrap();
        let id = DeletionVectorDescriptor::normalized_unique_id_from_parts(
            storage_type,
            path_or_inline_dv,
            offset,
            &table_root,
        )
        .unwrap();
        assert_eq!(id, expected);
    }

    /// The core property: a `'u'`, `'r'`, and under-root `'p'` descriptor naming the same physical
    /// DV blob (same offset) all normalize to the same identity, so an `add`/`remove` match
    /// regardless of how each encoded the blob.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn test_normalized_unique_id_matches_across_storage_types() {
        let table_root = Url::parse("s3://mytable/").unwrap();
        let relative = "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";
        let normalize = |storage_type, path: &str| {
            DeletionVectorDescriptor::normalized_unique_id_from_parts(
                storage_type,
                path,
                Some(4),
                &table_root,
            )
            .unwrap()
        };
        let from_u = normalize(
            DeletionVectorStorageType::PersistedRelative,
            "ab^-aqEH.-t@S}K{vb[*k^",
        );
        let from_r = normalize(
            DeletionVectorStorageType::PersistedUnencodedRelative,
            relative,
        );
        let from_p = normalize(
            DeletionVectorStorageType::PersistedAbsolute,
            &format!("s3://mytable/{relative}"),
        );
        assert_eq!(from_u, from_r);
        assert_eq!(from_u, from_p);
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

        // Verify the encoded_relative_path is exactly as expected (prefix + z85 encoded UUID: 20
        // chars)
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

    #[rstest::rstest]
    #[case::inline_with_offset(DeletionVectorStorageType::Inline, "ABC", Some(0), 4, 1, "inline")]
    #[case::persisted_relative_short_path(
        DeletionVectorStorageType::PersistedRelative,
        "short",
        Some(1),
        4,
        1,
        "20 bytes"
    )]
    #[case::persisted_relative_invalid_z85(
        DeletionVectorStorageType::PersistedRelative,
        // 20 bytes but `_` is outside the z85 alphabet, so z85::decode fails.
        "____________________",
        Some(1),
        4,
        1,
        "z85"
    )]
    #[case::persisted_relative_non_ascii(
        DeletionVectorStorageType::PersistedRelative,
        // 22 bytes (`euro` U+20AC is 3 bytes) with `len - 20 = 2` landing inside the
        // multi-byte codepoint; byte-slicing keeps this from panicking and z85 rejects
        // the non-ASCII payload.
        "a\u{20ac}aaaaaaaaaaaaaaaaaa",
        Some(1),
        4,
        1,
        "z85"
    )]
    #[case::persisted_absolute_non_url(
        DeletionVectorStorageType::PersistedAbsolute,
        "not a url",
        Some(1),
        4,
        1,
        "URL"
    )]
    #[cfg_attr(
        feature = "adaptive-metadata-in-dev",
        case::unencoded_relative_empty(
            DeletionVectorStorageType::PersistedUnencodedRelative,
            "",
            Some(1),
            4,
            1,
            "must not be empty"
        )
    )]
    #[cfg_attr(
        feature = "adaptive-metadata-in-dev",
        case::unencoded_relative_leading_slash(
            DeletionVectorStorageType::PersistedUnencodedRelative,
            "/abs/dv.bin",
            Some(1),
            4,
            1,
            "leading '/'"
        )
    )]
    #[cfg_attr(
        feature = "adaptive-metadata-in-dev",
        case::unencoded_relative_absolute_url(
            DeletionVectorStorageType::PersistedUnencodedRelative,
            "s3://bucket/dv.bin",
            Some(1),
            4,
            1,
            "absolute URL"
        )
    )]
    #[case::negative_size(
        DeletionVectorStorageType::Inline, "ABC", None, -1, 0, "size_in_bytes"
    )]
    #[case::negative_cardinality(
        DeletionVectorStorageType::Inline, "ABC", None, 4, -1, "cardinality"
    )]
    #[case::negative_offset(
        DeletionVectorStorageType::PersistedAbsolute, "file:///tmp/dv.bin", Some(-1), 4, 1, "offset"
    )]
    fn dv_descriptor_try_new_rejects_invalid(
        #[case] storage_type: DeletionVectorStorageType,
        #[case] path: &str,
        #[case] offset: Option<i32>,
        #[case] size_in_bytes: i32,
        #[case] cardinality: i64,
        #[case] expected_substr: &str,
    ) {
        let err = DeletionVectorDescriptor::try_new(
            storage_type,
            path,
            offset,
            size_in_bytes,
            cardinality,
        )
        .expect_err("expected validation error");
        assert!(
            err.to_string().contains(expected_substr),
            "expected error containing {expected_substr:?}, got: {err}"
        );
    }

    #[test]
    fn dv_descriptor_try_new_round_trips_fields() {
        let descriptor = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::PersistedAbsolute,
            "file:///tmp/dv.bin",
            Some(7),
            42,
            9,
        )
        .expect("valid descriptor");

        assert_eq!(
            descriptor.storage_type,
            DeletionVectorStorageType::PersistedAbsolute
        );
        assert_eq!(descriptor.path_or_inline_dv, "file:///tmp/dv.bin");
        assert_eq!(descriptor.offset, Some(7));
        assert_eq!(descriptor.size_in_bytes, 42);
        assert_eq!(descriptor.cardinality, 9);
    }
}
