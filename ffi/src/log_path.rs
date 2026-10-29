//! FFI interface for LogPath.

use delta_kernel::{FileMeta, KernelResult, LogPath};
use url::Url;

use crate::{FfiSlice, KernelStringSlice, TryFromStringSlice};

/// Borrowed array of FFI-safe log paths.
pub type LogPathArray = FfiSlice<FfiLogPath>;

impl LogPathArray {
    /// Decodes paths using the parsed table root for absolute URLs beneath its log directory.
    ///
    /// Filename classification and snapshot-hint validation are unchanged. Other locations use
    /// general URL parsing; this method does not establish that a path belongs to the table.
    ///
    /// # Safety
    ///
    /// The array and all nested location strings must remain readable for the call.
    pub(crate) unsafe fn log_paths_with_root(
        &self,
        table_root: &Url,
    ) -> KernelResult<Vec<LogPath>> {
        let log_root = table_root.join("_delta_log/")?;
        unsafe { self.try_as_slice() }?
            .iter()
            .map(|path| {
                let location = unsafe { TryFromStringSlice::try_from_slice(&path.location) }?;
                path.with_location(log_url(location, &log_root)?)
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
        self.with_location(Url::parse(location_str)?)
    }

    fn with_location(&self, location: Url) -> KernelResult<LogPath> {
        let file_meta = FileMeta {
            location,
            last_modified: self.last_modified,
            size: self.size,
        };
        LogPath::try_new(file_meta)
    }
}

fn log_url(location: &str, log_root: &Url) -> Result<Url, url::ParseError> {
    if let Some(suffix) = location.strip_prefix(log_root.as_str()) {
        // Relative parsing must preserve the absolute input's path, including leading whitespace.
        let leading_control_or_space = suffix.as_bytes().first().is_some_and(|byte| *byte <= b' ');
        if !leading_control_or_space
            && !suffix.starts_with(['/', '\\'])
            && !suffix
                .split('/')
                .next()
                .is_some_and(|part| part.contains(':'))
        {
            return log_root.join(suffix);
        }
    }
    Url::parse(location)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::commit("00000000000000000123.json")]
    #[case::checkpoint("00000000000000000122.checkpoint.parquet")]
    #[case::staged("_staged_commits/00000000000000000123.uuid.json")]
    #[case::crc("00000000000000000123.crc")]
    #[case::escaped("dir%20name/00000000000000000123.json")]
    #[case::leading_space(" 00000000000000000123.json")]
    #[case::leading_control("\u{0001}00000000000000000123.json")]
    #[case::tab_before_slash("\t/00000000000000000123.json")]
    #[case::dot_segments("a/../00000000000000000123.json")]
    #[case::leading_slash("/00000000000000000123.json")]
    #[case::leading_backslash("\\00000000000000000123.json")]
    #[case::scheme_like("https:00000000000000000123.json")]
    #[case::query("00000000000000000123.json?value=123")]
    #[case::fragment("00000000000000000123.json#fragment")]
    fn joined_log_url_matches_general_parser(
        #[case] suffix: &str,
        #[values("memory:///table/", "https://example.com/table/", "file:///table/")] root: &str,
    ) {
        let log_root = Url::parse(root).unwrap().join("_delta_log/").unwrap();
        let location = format!("{log_root}{suffix}");
        assert_eq!(log_url(&location, &log_root), Url::parse(&location));
    }

    #[rstest]
    #[case::outside("memory:///other/_delta_log/00000000000000000123.json")]
    #[case::different_scheme("s3://bucket/table/_delta_log/00000000000000000123.json")]
    #[case::invalid("not a URL")]
    fn other_log_urls_retain_general_parsing(#[case] location: &str) {
        let log_root = Url::parse("memory:///table/_delta_log/").unwrap();
        assert_eq!(log_url(location, &log_root), Url::parse(location));
    }

    #[rstest]
    #[case::space(" 00000000000000000123.json")]
    #[case::control("\u{0001}00000000000000000123.json")]
    fn rooted_descriptor_conversion_preserves_invalid_filename(#[case] suffix: &str) {
        let root = Url::parse("memory:///table/").unwrap();
        let location = format!("{root}_delta_log/{suffix}");
        let paths = [FfiLogPath::new(
            unsafe { KernelStringSlice::new_unsafe(&location) },
            1234,
            5678,
        )];
        let array = unsafe { LogPathArray::new_unsafe(&paths) };
        assert!(unsafe { array.log_paths() }.is_err());
        assert!(unsafe { array.log_paths_with_root(&root) }.is_err());
    }

    #[test]
    fn rooted_descriptor_conversion_preserves_file_metadata_and_classification() {
        let locations = [
            "memory:///table/_delta_log/00000000000000000123.json",
            "memory:///table/_delta_log/00000000000000000122.checkpoint.parquet",
            "memory:///other/_delta_log/00000000000000000123.json",
        ];
        let paths: Vec<_> = locations
            .iter()
            .map(|location| {
                FfiLogPath::new(
                    unsafe { KernelStringSlice::new_unsafe(location) },
                    1234,
                    5678,
                )
            })
            .collect();
        let array = unsafe { LogPathArray::new_unsafe(&paths) };
        let root = Url::parse("memory:///table/").unwrap();
        assert_eq!(
            unsafe { array.log_paths_with_root(&root) }.unwrap(),
            unsafe { array.log_paths() }.unwrap()
        );
        assert!(unsafe { LogPathArray::empty().log_paths_with_root(&root) }
            .unwrap()
            .is_empty());
    }
}
