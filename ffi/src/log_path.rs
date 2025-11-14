//! FFI interface for LogPath.

use delta_kernel::{DeltaResult, FileMeta, LogPath};
use url::Url;

use crate::{KernelStringSlice, TryFromStringSlice};

/// FFI-safe array of LogPaths. Note that we _explicitly_ do not implement `Copy` on this struct
/// despite all types being `Copy`, to avoid accidental misuse of the pointer.
///
/// This struct is essentially a borrowed view into an array. The owner must ensure the underlying
/// array remains valid for the duration of its use.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct LogPathArray {
    /// Pointer to the first element of the FfiLogPath array. If len is 0, this pointer may be null,
    /// otherwise it must be non-null.
    pub ptr: *const FfiLogPath,
    /// Number of elements in the array
    pub len: usize,
}

impl LogPathArray {
    /// Create an empty LogPathArray
    pub fn empty() -> Self {
        Self {
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    /// Convert this array into a Vec of kernel LogPaths
    ///
    /// # Safety
    /// The ptr must point to `len` valid FfiLogPath elements, and those elements
    /// must remain valid for the duration of this call
    pub(crate) unsafe fn log_paths(&self) -> DeltaResult<Vec<LogPath>> {
        if self.ptr.is_null() || self.len == 0 {
            return Ok(Vec::new());
        }

        let slice = unsafe { std::slice::from_raw_parts(self.ptr, self.len) };
        slice
            .iter()
            .map(|ffi_path| unsafe { ffi_path.log_path() })
            .collect::<Result<Vec<_>, _>>()
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
    unsafe fn log_path(&self) -> DeltaResult<LogPath> {
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
