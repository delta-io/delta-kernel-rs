// Placeholder for stacked pr
#![allow(dead_code)]
// Methods are consumed in follow-up steps (FileStatsVisitor, CrcDelta, Crc::apply).
//! [`FileSizeHistogram`] tracks the distribution of file sizes across predefined bins.
//!
//! Used in CRC (version checksum) files to record the size distribution of active files in a
//! table version. Supports incremental updates via [`insert`](FileSizeHistogram::insert),
//! [`remove`](FileSizeHistogram::remove), [`try_add`](FileSizeHistogram::try_add), and
//! [`try_sub`](FileSizeHistogram::try_sub).
//!
//! [FileSizeHistogram]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#file-size-histogram-schema

use serde::{Deserialize, Serialize};

use crate::utils::require;
use crate::{DeltaResult, Error};

const KB: i64 = 1024;
const MB: i64 = KB * 1024;
const GB: i64 = MB * 1024;

/// Default bin boundaries for file size categorization, matching Delta Kernel Java.
///
/// 95 boundaries covering:
/// - 0 and powers of 2 from 8KB to 4MB
/// - 4MB steps from 8MB to 40MB
/// - 8MB steps from 48MB to 120MB
/// - 4MB steps from 124MB to 144MB (fine granularity around 128MB target file size)
/// - 16MB steps from 160MB to 576MB
/// - 64MB steps from 640MB to 1408MB
/// - 128MB steps from 1536MB to 2GB
/// - 256MB steps from 2304MB to 4GB
/// - Powers of 2 from 8GB to 256GB
#[rustfmt::skip]
const DEFAULT_BIN_BOUNDARIES: [i64; 95] = [
    0,
    // Powers of 2 from 8KB to 4MB
    8 * KB, 16 * KB, 32 * KB, 64 * KB, 128 * KB, 256 * KB, 512 * KB,
    MB, 2 * MB, 4 * MB,
    // 4MB steps from 8MB to 40MB
    8 * MB, 12 * MB, 16 * MB, 20 * MB, 24 * MB, 28 * MB, 32 * MB, 36 * MB, 40 * MB,
    // 8MB steps from 48MB to 120MB
    48 * MB, 56 * MB, 64 * MB, 72 * MB, 80 * MB, 88 * MB, 96 * MB, 104 * MB, 112 * MB, 120 * MB,
    // 4MB steps from 124MB to 144MB
    124 * MB, 128 * MB, 132 * MB, 136 * MB, 140 * MB, 144 * MB,
    // 16MB steps from 160MB to 576MB
    160 * MB, 176 * MB, 192 * MB, 208 * MB, 224 * MB, 240 * MB, 256 * MB, 272 * MB,
    288 * MB, 304 * MB, 320 * MB, 336 * MB, 352 * MB, 368 * MB, 384 * MB, 400 * MB,
    416 * MB, 432 * MB, 448 * MB, 464 * MB, 480 * MB, 496 * MB, 512 * MB, 528 * MB,
    544 * MB, 560 * MB, 576 * MB,
    // 64MB steps from 640MB to 1408MB
    640 * MB, 704 * MB, 768 * MB, 832 * MB, 896 * MB, 960 * MB, 1024 * MB, 1088 * MB,
    1152 * MB, 1216 * MB, 1280 * MB, 1344 * MB, 1408 * MB,
    // 128MB steps from 1536MB to 2GB
    1536 * MB, 1664 * MB, 1792 * MB, 1920 * MB, 2048 * MB,
    // 256MB steps from 2304MB to 4GB
    2304 * MB, 2560 * MB, 2816 * MB, 3072 * MB, 3328 * MB, 3584 * MB, 3840 * MB, 4096 * MB,
    // Powers of 2 from 8GB to 256GB
    8 * GB, 16 * GB, 32 * GB, 64 * GB, 128 * GB, 256 * GB,
];

/// Tracks the distribution of file sizes across predefined bins.
///
/// Each bin `i` covers the range `[sorted_bin_boundaries[i], sorted_bin_boundaries[i+1])`,
/// with the last bin extending to infinity. The histogram records both the count of files
/// and the total bytes in each bin.
///
/// See the [Delta protocol spec] for the full schema definition.
///
/// [Delta protocol spec]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#file-size-histogram-schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSizeHistogram {
    /// A sorted array of bin boundaries where each element represents the start of a bin
    /// (inclusive) and the next element represents the end of the bin (exclusive). The first
    /// element must be 0.
    pub(crate) sorted_bin_boundaries: Vec<i64>,
    /// Count of files in each bin. Length must match `sorted_bin_boundaries`.
    pub(crate) file_counts: Vec<i64>,
    /// Total bytes of files in each bin. Length must match `sorted_bin_boundaries`.
    pub(crate) total_bytes: Vec<i64>,
}

impl FileSizeHistogram {
    /// Creates a new histogram with the given arrays, after validation.
    ///
    /// Validates that:
    /// - All arrays have the same length (>= 2)
    /// - The first boundary is 0
    /// - Boundaries are sorted in ascending order
    #[allow(dead_code)]
    pub(crate) fn try_new(
        sorted_bin_boundaries: Vec<i64>,
        file_counts: Vec<i64>,
        total_bytes: Vec<i64>,
    ) -> DeltaResult<Self> {
        require!(
            sorted_bin_boundaries.len() >= 2,
            Error::internal_error(format!(
                "sorted_bin_boundaries must have at least 2 elements, got {}",
                sorted_bin_boundaries.len()
            ))
        );
        require!(
            sorted_bin_boundaries[0] == 0,
            Error::internal_error(format!(
                "First boundary must be 0, got {}",
                sorted_bin_boundaries[0]
            ))
        );
        require!(
            sorted_bin_boundaries.len() == file_counts.len()
                && sorted_bin_boundaries.len() == total_bytes.len(),
            Error::internal_error(format!(
                "All arrays must have the same length: boundaries={}, file_counts={}, total_bytes={}",
                sorted_bin_boundaries.len(),
                file_counts.len(),
                total_bytes.len()
            ))
        );
        require!(
            sorted_bin_boundaries.windows(2).all(|w| w[0] < w[1]),
            Error::internal_error(
                "sorted_bin_boundaries must be sorted in strictly ascending order"
            )
        );
        Ok(Self {
            sorted_bin_boundaries,
            file_counts,
            total_bytes,
        })
    }

    /// Creates a default histogram with the standard 95 bin boundaries and zero counts.
    ///
    /// Uses the same bin boundaries as Delta Kernel Java for cross-implementation compatibility.
    pub(crate) fn create_default() -> Self {
        let len = DEFAULT_BIN_BOUNDARIES.len();
        Self {
            sorted_bin_boundaries: DEFAULT_BIN_BOUNDARIES.to_vec(),
            file_counts: vec![0; len],
            total_bytes: vec![0; len],
        }
    }

    /// Returns the bin index for the given file size via binary search.
    ///
    /// The bin index is the largest `i` such that `sorted_bin_boundaries[i] <= file_size`.
    /// Files larger than the maximum boundary are placed in the last bin.
    fn get_bin_index(&self, file_size: i64) -> usize {
        match self.sorted_bin_boundaries.binary_search(&file_size) {
            Ok(idx) => idx,
            // binary_search returns Err(insertion_point) where insertion_point is where the
            // value would be inserted. We want the bin before that. Since boundaries[0] = 0
            // and file_size >= 0, insertion_point >= 1, so subtraction is safe.
            Err(insertion_point) => insertion_point - 1,
        }
    }

    /// Adds a file of the given size to the histogram, incrementing the appropriate bin's
    /// file count and total bytes.
    pub(crate) fn insert(&mut self, file_size: i64) -> DeltaResult<()> {
        require!(
            file_size >= 0,
            Error::internal_error(format!("File size must be non-negative, got {}", file_size))
        );
        let idx = self.get_bin_index(file_size);
        self.file_counts[idx] += 1;
        self.total_bytes[idx] += file_size;
        Ok(())
    }

    /// Removes a file of the given size from the histogram, decrementing the appropriate bin's
    /// file count and total bytes.
    ///
    /// Returns an error if the bin does not have sufficient counts or bytes.
    #[allow(dead_code)]
    pub(crate) fn remove(&mut self, file_size: i64) -> DeltaResult<()> {
        require!(
            file_size >= 0,
            Error::internal_error(format!("File size must be non-negative, got {}", file_size))
        );
        let idx = self.get_bin_index(file_size);
        require!(
            self.file_counts[idx] > 0 && self.total_bytes[idx] >= file_size,
            Error::internal_error(format!(
                "Cannot remove {} bytes from bin {} which has {} bytes and {} files",
                file_size, idx, self.total_bytes[idx], self.file_counts[idx]
            ))
        );
        self.file_counts[idx] -= 1;
        self.total_bytes[idx] -= file_size;
        Ok(())
    }

    /// Adds two histograms element-wise. Both must have the same bin boundaries.
    ///
    /// Returns a new histogram whose file counts and total bytes are the sum of the two inputs.
    pub(crate) fn try_add(&self, other: &FileSizeHistogram) -> DeltaResult<FileSizeHistogram> {
        require!(
            self.sorted_bin_boundaries == other.sorted_bin_boundaries,
            Error::internal_error("Cannot add histograms with different bin boundaries")
        );
        let file_counts = self
            .file_counts
            .iter()
            .zip(&other.file_counts)
            .map(|(a, b)| a + b)
            .collect();
        let total_bytes = self
            .total_bytes
            .iter()
            .zip(&other.total_bytes)
            .map(|(a, b)| a + b)
            .collect();
        Ok(FileSizeHistogram {
            sorted_bin_boundaries: self.sorted_bin_boundaries.clone(),
            file_counts,
            total_bytes,
        })
    }

    /// Subtracts another histogram element-wise. Both must have the same bin boundaries.
    ///
    /// Returns an error if any bin would have negative file counts or total bytes.
    pub(crate) fn try_sub(&self, other: &FileSizeHistogram) -> DeltaResult<FileSizeHistogram> {
        require!(
            self.sorted_bin_boundaries == other.sorted_bin_boundaries,
            Error::internal_error("Cannot subtract histograms with different bin boundaries")
        );
        let len = self.sorted_bin_boundaries.len();
        let mut file_counts = Vec::with_capacity(len);
        let mut total_bytes = Vec::with_capacity(len);
        for i in 0..len {
            let count = self.file_counts[i] - other.file_counts[i];
            let bytes = self.total_bytes[i] - other.total_bytes[i];
            require!(
                count >= 0 && bytes >= 0,
                Error::internal_error(format!(
                    "Subtraction would result in negative counts or bytes at bin {}",
                    i
                ))
            );
            file_counts.push(count);
            total_bytes.push(bytes);
        }
        Ok(FileSizeHistogram {
            sorted_bin_boundaries: self.sorted_bin_boundaries.clone(),
            file_counts,
            total_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    // ===== Construction =====

    #[test]
    fn create_default_has_95_bins_starting_at_zero() {
        let hist = FileSizeHistogram::create_default();
        assert_eq!(hist.sorted_bin_boundaries.len(), 95);
        assert_eq!(hist.file_counts.len(), 95);
        assert_eq!(hist.total_bytes.len(), 95);
        assert_eq!(hist.sorted_bin_boundaries[0], 0);
        assert!(hist.file_counts.iter().all(|&c| c == 0));
        assert!(hist.total_bytes.iter().all(|&b| b == 0));
    }

    #[test]
    fn try_new_valid_histogram() {
        let hist = FileSizeHistogram::try_new(vec![0, 100], vec![5, 3], vec![200, 900]).unwrap();
        assert_eq!(hist.sorted_bin_boundaries, vec![0, 100]);
        assert_eq!(hist.file_counts, vec![5, 3]);
        assert_eq!(hist.total_bytes, vec![200, 900]);
    }

    #[rstest]
    #[case::empty_boundaries(vec![], vec![], vec![], "at least 2 elements")]
    #[case::single_boundary(vec![0], vec![0], vec![0], "at least 2 elements")]
    #[case::nonzero_first_boundary(vec![1, 100], vec![0, 0], vec![0, 0], "First boundary must be 0")]
    #[case::mismatched_array_lengths(vec![0, 100], vec![0], vec![0, 0], "same length")]
    #[case::unsorted_boundaries(vec![0, 200, 100], vec![0, 0, 0], vec![0, 0, 0], "strictly ascending")]
    #[case::duplicate_boundaries(vec![0, 100, 100], vec![0, 0, 0], vec![0, 0, 0], "strictly ascending")]
    fn try_new_rejects_invalid_inputs(
        #[case] boundaries: Vec<i64>,
        #[case] file_counts: Vec<i64>,
        #[case] total_bytes: Vec<i64>,
        #[case] expected_msg: &str,
    ) {
        assert_result_error_with_message(
            FileSizeHistogram::try_new(boundaries, file_counts, total_bytes),
            expected_msg,
        );
    }

    // ===== Binary search =====

    #[test]
    fn get_bin_index_exact_boundary_match() {
        let hist = FileSizeHistogram::try_new(vec![0, 100, 200], vec![0; 3], vec![0; 3]).unwrap();
        assert_eq!(hist.get_bin_index(0), 0);
        assert_eq!(hist.get_bin_index(100), 1);
        assert_eq!(hist.get_bin_index(200), 2);
    }

    #[test]
    fn get_bin_index_between_boundaries() {
        let hist = FileSizeHistogram::try_new(vec![0, 100, 200], vec![0; 3], vec![0; 3]).unwrap();
        assert_eq!(hist.get_bin_index(50), 0);
        assert_eq!(hist.get_bin_index(99), 0);
        assert_eq!(hist.get_bin_index(150), 1);
        assert_eq!(hist.get_bin_index(199), 1);
    }

    #[test]
    fn get_bin_index_beyond_max_boundary_returns_last_bin() {
        let hist = FileSizeHistogram::try_new(vec![0, 100, 200], vec![0; 3], vec![0; 3]).unwrap();
        assert_eq!(hist.get_bin_index(999), 2);
        assert_eq!(hist.get_bin_index(i64::MAX), 2);
    }

    // ===== Insert / Remove =====

    #[test]
    fn insert_increments_count_and_bytes() {
        let mut hist =
            FileSizeHistogram::try_new(vec![0, 100, 200], vec![0; 3], vec![0; 3]).unwrap();
        hist.insert(50).unwrap();
        hist.insert(75).unwrap();
        hist.insert(150).unwrap();
        assert_eq!(hist.file_counts, vec![2, 1, 0]);
        assert_eq!(hist.total_bytes, vec![125, 150, 0]);
    }

    #[test]
    fn insert_negative_size_returns_error() {
        let mut hist = FileSizeHistogram::create_default();
        assert_result_error_with_message(hist.insert(-1), "non-negative");
    }

    #[test]
    fn remove_decrements_count_and_bytes() {
        let mut hist =
            FileSizeHistogram::try_new(vec![0, 100, 200], vec![2, 1, 0], vec![125, 150, 0])
                .unwrap();
        hist.remove(50).unwrap();
        assert_eq!(hist.file_counts, vec![1, 1, 0]);
        assert_eq!(hist.total_bytes, vec![75, 150, 0]);
    }

    #[test]
    fn remove_from_empty_bin_returns_error() {
        let mut hist = FileSizeHistogram::try_new(vec![0, 100], vec![0, 0], vec![0, 0]).unwrap();
        assert_result_error_with_message(hist.remove(50), "Cannot remove");
    }

    #[test]
    fn remove_more_bytes_than_available_returns_error() {
        let mut hist = FileSizeHistogram::try_new(vec![0, 100], vec![1, 0], vec![10, 0]).unwrap();
        assert_result_error_with_message(hist.remove(50), "Cannot remove");
    }

    #[test]
    fn remove_negative_size_returns_error() {
        let mut hist = FileSizeHistogram::create_default();
        assert_result_error_with_message(hist.remove(-1), "non-negative");
    }

    // ===== try_add / try_sub =====

    #[test]
    fn try_add_combines_counts_and_bytes() {
        let a = FileSizeHistogram::try_new(vec![0, 100], vec![2, 3], vec![50, 400]).unwrap();
        let b = FileSizeHistogram::try_new(vec![0, 100], vec![1, 4], vec![30, 600]).unwrap();
        let result = a.try_add(&b).unwrap();
        assert_eq!(result.file_counts, vec![3, 7]);
        assert_eq!(result.total_bytes, vec![80, 1000]);
        assert_eq!(result.sorted_bin_boundaries, vec![0, 100]);
    }

    #[test]
    fn try_add_mismatched_boundaries_returns_error() {
        let a = FileSizeHistogram::try_new(vec![0, 100], vec![0; 2], vec![0; 2]).unwrap();
        let b = FileSizeHistogram::try_new(vec![0, 200], vec![0; 2], vec![0; 2]).unwrap();
        assert_result_error_with_message(a.try_add(&b), "different bin boundaries");
    }

    #[test]
    fn try_sub_produces_difference() {
        let a = FileSizeHistogram::try_new(vec![0, 100], vec![5, 8], vec![200, 900]).unwrap();
        let b = FileSizeHistogram::try_new(vec![0, 100], vec![2, 3], vec![50, 400]).unwrap();
        let result = a.try_sub(&b).unwrap();
        assert_eq!(result.file_counts, vec![3, 5]);
        assert_eq!(result.total_bytes, vec![150, 500]);
    }

    #[test]
    fn try_sub_mismatched_boundaries_returns_error() {
        let a = FileSizeHistogram::try_new(vec![0, 100], vec![0; 2], vec![0; 2]).unwrap();
        let b = FileSizeHistogram::try_new(vec![0, 200], vec![0; 2], vec![0; 2]).unwrap();
        assert_result_error_with_message(a.try_sub(&b), "different bin boundaries");
    }

    #[test]
    fn try_sub_negative_counts_returns_error() {
        let a = FileSizeHistogram::try_new(vec![0, 100], vec![1, 0], vec![50, 0]).unwrap();
        let b = FileSizeHistogram::try_new(vec![0, 100], vec![2, 0], vec![50, 0]).unwrap();
        assert_result_error_with_message(a.try_sub(&b), "negative counts or bytes");
    }

    #[test]
    fn try_sub_negative_bytes_returns_error() {
        let a = FileSizeHistogram::try_new(vec![0, 100], vec![2, 0], vec![50, 0]).unwrap();
        let b = FileSizeHistogram::try_new(vec![0, 100], vec![1, 0], vec![100, 0]).unwrap();
        assert_result_error_with_message(a.try_sub(&b), "negative counts or bytes");
    }

    // ===== Serde =====

    #[test]
    fn serde_round_trip_default_histogram() {
        let hist = FileSizeHistogram::create_default();
        let json = serde_json::to_string(&hist).unwrap();
        let deserialized: FileSizeHistogram = serde_json::from_str(&json).unwrap();
        assert_eq!(hist, deserialized);
    }

    #[test]
    fn serde_round_trip_populated_histogram() {
        let mut hist = FileSizeHistogram::create_default();
        hist.insert(500).unwrap();
        hist.insert(10_000_000).unwrap();
        let json = serde_json::to_string(&hist).unwrap();
        let deserialized: FileSizeHistogram = serde_json::from_str(&json).unwrap();
        assert_eq!(hist, deserialized);
    }

    #[test]
    fn serde_uses_camel_case_field_names() {
        let hist = FileSizeHistogram::try_new(vec![0, 100], vec![1, 2], vec![10, 200]).unwrap();
        let json_value = serde_json::to_value(&hist).unwrap();
        assert!(json_value.get("sortedBinBoundaries").is_some());
        assert!(json_value.get("fileCounts").is_some());
        assert!(json_value.get("totalBytes").is_some());
        // Snake case should NOT be present
        assert!(json_value.get("sorted_bin_boundaries").is_none());
        assert!(json_value.get("file_counts").is_none());
        assert!(json_value.get("total_bytes").is_none());
    }

    #[test]
    fn serde_deserialize_from_crc_json_format() {
        let json = r#"{
            "sortedBinBoundaries": [0, 8192, 16384],
            "fileCounts": [10, 0, 0],
            "totalBytes": [5259, 0, 0]
        }"#;
        let hist: FileSizeHistogram = serde_json::from_str(json).unwrap();
        assert_eq!(hist.sorted_bin_boundaries, vec![0, 8192, 16384]);
        assert_eq!(hist.file_counts, vec![10, 0, 0]);
        assert_eq!(hist.total_bytes, vec![5259, 0, 0]);
    }
}
