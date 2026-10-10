//! FFI interface for LogPath.

use delta_kernel::{FileMeta, KernelResult, LogPath};
use url::Url;

use crate::{FfiSlice, KernelStringSlice, TryFromStringSlice};

/// Borrowed array of FFI-safe log paths.
pub type LogPathArray = FfiSlice<FfiLogPath>;

impl LogPathArray {
    /// Decodes unchanged source-snapshot paths, joining recognized URLs against parsed roots.
    ///
    /// # Safety
    ///
    /// The array and all nested strings must be readable for this call, and must contain the
    /// unchanged paths exported by the source snapshot.
    pub(crate) unsafe fn trusted_snapshot_log_paths(
        &self,
        table_root: &Url,
    ) -> KernelResult<Vec<LogPath>> {
        let log_root = table_root.join("_delta_log/")?;
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|path| {
                let location: &str = unsafe { TryFromStringSlice::try_from_slice(&path.location) }?;
                let location = if let Some(suffix) = location.strip_prefix(log_root.as_str()) {
                    log_root.join(suffix)?
                } else if let Some(suffix) = location.strip_prefix(table_root.as_str()) {
                    table_root.join(suffix)?
                } else {
                    Url::parse(location)?
                };
                LogPath::try_new_from_trusted_snapshot(FileMeta {
                    location,
                    last_modified: path.last_modified,
                    size: path.size,
                })
            })
            .collect()
    }

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
