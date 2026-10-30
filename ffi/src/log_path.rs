//! FFI interface for LogPath.

use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::{FileMeta, KernelResult, LogPath, Version};
use url::Url;

use crate::{kernel_string_slice, FfiSlice, KernelStringSlice, TryFromStringSlice};

/// Borrowed array of FFI-safe log paths.
pub type LogPathArray = FfiSlice<FfiLogPath>;

impl LogPathArray {
    /// Convert this array into a Vec of kernel LogPaths
    ///
    /// # Safety
    /// The ptr must point to `len` valid FfiLogPath elements, and those elements
    /// must remain valid for the duration of this call
    pub(crate) unsafe fn log_paths(&self) -> KernelResult<Vec<LogPath>> {
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|ffi_path| unsafe { ffi_path.log_path() })
            .collect::<KernelResult<Vec<_>>>()
    }
}

/// FFI-safe LogPath representation that can be passed from the engine
#[repr(C)]
pub struct FfiLogPath {
    /// URL location of the log file
    location: KernelStringSlice,
    /// Last modified time as milliseconds since unix epoch
    last_modified: i64,
    /// Size in bytes of the log file
    size: u64,
}

impl FfiLogPath {
    /// Create a new FFI LogPath. The location string slice must be valid UTF-8.
    pub fn new(location: KernelStringSlice, last_modified: i64, size: u64) -> Self {
        Self {
            location,
            last_modified,
            size,
        }
    }

    /// URL location of the log file as a string slice
    pub fn location(&self) -> &KernelStringSlice {
        &self.location
    }

    /// Last modified time as milliseconds since unix epoch
    pub fn last_modified(&self) -> i64 {
        self.last_modified
    }

    /// Size in bytes of the log file
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Convert this FFI log path into a kernel LogPath
    ///
    /// # Safety
    ///
    /// The `self.location` string slice must be valid UTF-8 and represent a valid URL.
    unsafe fn log_path(&self) -> KernelResult<LogPath> {
        let location_str = unsafe { TryFromStringSlice::try_from_slice(&self.location) }?;
        let url = Url::parse(location_str)?;
        let file_meta = FileMeta {
            location: url,
            last_modified: self.last_modified,
            size: self.size,
        };
        LogPath::try_new(file_meta)
    }
}

/// Borrowed array of parsed log paths. Nested strings share the array's borrow lifetime.
pub type ParsedLogPathArray = FfiSlice<FfiParsedLogPath>;

/// File naming scheme and any type-specific version or part information.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum FfiLogPathFileType {
    /// Published commit with a version-only filename.
    Commit,
    /// UUID-named commit in `_delta_log/_staged_commits/`.
    StagedCommit,
    /// Classic-named checkpoint; its contents may follow the V1 or V2 checkpoint specification.
    ClassicCheckpoint,
    /// UUID-named V2 checkpoint.
    UuidCheckpoint,
    /// One part of a multipart V1 checkpoint.
    MultiPartCheckpoint {
        /// One-based part number.
        part_num: u32,
        /// Total number of parts.
        num_parts: u32,
    },
    /// Compaction whose inclusive start version is stored in [`FfiParsedLogPath::version`].
    CompactedCommit {
        /// Inclusive end version.
        hi: Version,
    },
    /// Version checksum.
    Crc,
    /// Versioned path with an unrecognized naming scheme.
    Unknown,
}

/// Output-only parsed log path with borrowed file metadata.
///
/// The location string is valid only for the enclosing callback. Copy it before retaining this
/// record. The file type describes the retained path, not the commit's current publication state.
#[repr(C)]
pub struct FfiParsedLogPath {
    /// URL, modification time, and size of the file.
    pub file: FfiLogPath,
    /// File version, or inclusive start version for a compaction.
    pub version: Version,
    /// Parsed file naming scheme, including compaction or multipart payloads where applicable.
    pub file_type: FfiLogPathFileType,
}

impl FfiParsedLogPath {
    pub(crate) fn from_parsed(path: &ParsedLogPath) -> Self {
        let location = path.location.location.as_str();
        Self {
            file: FfiLogPath::new(
                kernel_string_slice!(location),
                path.location.last_modified,
                path.location.size,
            ),
            version: path.version,
            file_type: match path.file_type {
                LogPathFileType::Commit => FfiLogPathFileType::Commit,
                LogPathFileType::StagedCommit => FfiLogPathFileType::StagedCommit,
                LogPathFileType::ClassicCheckpoint => FfiLogPathFileType::ClassicCheckpoint,
                LogPathFileType::UuidCheckpoint => FfiLogPathFileType::UuidCheckpoint,
                LogPathFileType::MultiPartCheckpoint {
                    part_num,
                    num_parts,
                } => FfiLogPathFileType::MultiPartCheckpoint {
                    part_num,
                    num_parts,
                },
                LogPathFileType::CompactedCommit { hi } => {
                    FfiLogPathFileType::CompactedCommit { hi }
                }
                LogPathFileType::Crc => FfiLogPathFileType::Crc,
                LogPathFileType::Unknown => FfiLogPathFileType::Unknown,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case("00000000000000000012.json", FfiLogPathFileType::Commit)]
    #[case(
        "_staged_commits/00000000000000000012.550e8400-e29b-41d4-a716-446655440000.json",
        FfiLogPathFileType::StagedCommit
    )]
    #[case(
        "00000000000000000012.checkpoint.parquet",
        FfiLogPathFileType::ClassicCheckpoint
    )]
    #[case(
        "00000000000000000012.checkpoint.550e8400-e29b-41d4-a716-446655440000.json",
        FfiLogPathFileType::UuidCheckpoint
    )]
    #[case("00000000000000000012.checkpoint.0000000002.0000000003.parquet", FfiLogPathFileType::MultiPartCheckpoint { part_num: 2, num_parts: 3 })]
    #[case("00000000000000000012.00000000000000000019.compacted.json", FfiLogPathFileType::CompactedCommit { hi: 19 })]
    #[case("00000000000000000012.crc", FfiLogPathFileType::Crc)]
    #[case("00000000000000000012.other", FfiLogPathFileType::Unknown)]
    fn parsed_path_preserves_metadata_after_copy(
        #[case] name: &str,
        #[case] expected_type: FfiLogPathFileType,
    ) {
        let location = format!("memory:///table/_delta_log/{name}");
        let source = ParsedLogPath::new(FileMeta {
            location: Url::parse(&location).unwrap(),
            last_modified: 1234,
            size: 5678,
        })
        .unwrap();
        let (copied_location, modified, size, version, file_type) = {
            let borrowed = FfiParsedLogPath::from_parsed(&source);
            (
                unsafe { String::try_from_slice(borrowed.file.location()) }.unwrap(),
                borrowed.file.last_modified(),
                borrowed.file.size(),
                borrowed.version,
                borrowed.file_type,
            )
        };
        drop(source);
        assert_eq!(copied_location, location);
        assert_eq!((modified, size, version), (1234, 5678, 12));
        assert_eq!(file_type, expected_type);
    }
}
