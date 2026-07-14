//! Stats field ID calculation utilities for Adaptive ContentTreeNode Tree (AMT).
//!
//! This module provides functions to compute stats field IDs for parent struct fields,
//! which are used in the AMT format for storing per-column statistics.

/// Number of supported stats per column (each column gets a range of 200 field IDs).
const NUM_SUPPORTED_STATS_PER_COLUMN: i32 = 200;

/// Name of the variant inner field for which we track stats. Other variant fields
/// (e.g. `metadata`) are excluded from stats collection.
// TODO: remove allow(dead_code) once variant stats collection lands.
#[allow(dead_code)]
const VARIANT_VALUE_FIELD_NAME: &str = "value";

/// Starting field ID of the stats space for data field IDs (regular column stats).
const STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS: i32 = 10_000;

/// Starting field ID of the stats space for metadata (reserved) field IDs.
/// Metadata stats occupy `[9_000, 10_000)`, just below the data stats space.
const STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS: i32 = 9_000;

/// Exclusive upper bound of the stats field ID range reserved for content_stats.
/// Valid stats field IDs are in `[STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS,
/// STATS_SPACE_FIELD_ID_END)`.
const STATS_SPACE_FIELD_ID_END: i32 = 200_000_000;

/// The maximum stats field ID for data columns (the base-id for the last data field that fits).
const MAX_DATA_STATS_FIELD_ID: i32 = STATS_SPACE_FIELD_ID_END - NUM_SUPPORTED_STATS_PER_COLUMN;

/// The maximum data field ID whose stats struct fits within the reserved range.
const MAX_DATA_FIELD_ID: i32 = (MAX_DATA_STATS_FIELD_ID
    - STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS)
    / NUM_SUPPORTED_STATS_PER_COLUMN;

/// The set of reserved metadata field IDs that have stats tracked in `content_stats`.
/// Per the spec, only `_last_updated_sequence_number` (2147483539) and `_row_id` (2147483540)
/// are supported.
const SUPPORTED_METADATA_FIELD_IDS: [i32; 2] = [2_147_483_539, 2_147_483_540];

/// The smallest field ID in [`SUPPORTED_METADATA_FIELD_IDS`]. Metadata stats offsets are
/// computed relative to this value.
const FIRST_SUPPORTED_METADATA_FIELD_ID: i32 = SUPPORTED_METADATA_FIELD_IDS[0];

/// Computes the base field ID for a column's stats struct, given a parent struct field ID.
///
/// Stats field IDs occupy the range `[9_000, 200_000_000)`:
/// - Metadata fields in [`SUPPORTED_METADATA_FIELD_IDS`] use `[9_000, 10_000)`, computed as `9_000
///   + 200 * (field_id - FIRST_SUPPORTED_METADATA_FIELD_ID)`
/// - Data fields `[0, MAX_DATA_FIELD_ID]` use `[10_000, 200_000_000)`, computed as `10_000 + 200 *
///   field_id`
///
/// Returns `None` for negative field IDs, unsupported metadata field IDs, or data field IDs
/// whose stats would fall outside the reserved range.
///
/// # Examples
///
/// These examples use `ignore` because the function is `pub(crate)`.
///
/// ```ignore
/// assert_eq!(field_id_to_statistics_base(0), Some(10_000));
/// assert_eq!(field_id_to_statistics_base(1), Some(10_200));
/// // _last_updated_sequence_number (2147483539) -> 9_000
/// assert_eq!(field_id_to_statistics_base(2_147_483_539), Some(9_000));
/// // _row_id (2147483540) -> 9_200
/// assert_eq!(field_id_to_statistics_base(2_147_483_540), Some(9_200));
/// ```
// TODO: remove allow(dead_code) once AMT stats collection uses this.
#[allow(dead_code)]
pub(crate) fn field_id_to_statistics_base(field_id: i32) -> Option<i32> {
    if SUPPORTED_METADATA_FIELD_IDS.contains(&field_id) {
        let offset = field_id - FIRST_SUPPORTED_METADATA_FIELD_ID;
        Some(
            STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS
                + (NUM_SUPPORTED_STATS_PER_COLUMN * offset),
        )
    } else if (0..=MAX_DATA_FIELD_ID).contains(&field_id) {
        Some(
            STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS
                + (NUM_SUPPORTED_STATS_PER_COLUMN * field_id),
        )
    } else {
        None
    }
}

/// Computes the original field ID from a stats field ID (inverse of
/// [`field_id_to_statistics_base`]).
///
/// Only stats field IDs in `[9_000, 200_000_000)` that are multiples of 200 are valid.
/// For the metadata range `[9_000, 10_000)`, the recovered field ID must be in
/// [`SUPPORTED_METADATA_FIELD_IDS`]; otherwise `None` is returned.
///
/// # Examples
///
/// These examples use `ignore` because the function is `#[cfg(test)] pub(crate)`.
///
/// ```ignore
/// assert_eq!(statistics_base_to_field_id(10_000), Some(0));
/// assert_eq!(statistics_base_to_field_id(10_200), Some(1));
/// assert_eq!(statistics_base_to_field_id(9_000), Some(2_147_483_539));
/// assert_eq!(statistics_base_to_field_id(9_200), Some(2_147_483_540));
/// ```
#[cfg(test)]
pub(crate) fn statistics_base_to_field_id(stats_field_id: i32) -> Option<i32> {
    if !(STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS..STATS_SPACE_FIELD_ID_END)
        .contains(&stats_field_id)
        || stats_field_id % NUM_SUPPORTED_STATS_PER_COLUMN != 0
    {
        return None;
    }

    if stats_field_id < STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS {
        // Metadata space: reverse 9_000 + 200 * (field_id - FIRST_SUPPORTED)
        let field_id = (stats_field_id - STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS)
            / NUM_SUPPORTED_STATS_PER_COLUMN
            + FIRST_SUPPORTED_METADATA_FIELD_ID;
        if SUPPORTED_METADATA_FIELD_IDS.contains(&field_id) {
            Some(field_id)
        } else {
            None
        }
    } else {
        // Data space: reverse 10_000 + 200 * field_id
        Some(
            (stats_field_id - STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS)
                / NUM_SUPPORTED_STATS_PER_COLUMN,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_id_to_statistics_base() {
        // field_id -> expected stats_field_id
        let cases = [
            // Data fields: 10_000 + 200 * field_id
            (0, Some(10_000)),
            (1, Some(10_200)),
            (2, Some(10_400)),
            (5, Some(11_000)),
            (100, Some(30_000)),
            (MAX_DATA_FIELD_ID, Some(MAX_DATA_STATS_FIELD_ID)),
            // Negative field IDs are invalid
            (-1, None),
            // Data field IDs above MAX_DATA_FIELD_ID are invalid
            (MAX_DATA_FIELD_ID + 1, None),
            // Supported metadata fields: 9_000 + 200 * (field_id - FIRST_SUPPORTED)
            (2_147_483_539, Some(9_000)), // _last_updated_sequence_number
            (2_147_483_540, Some(9_200)), // _row_id
            // Unsupported reserved metadata fields
            (2_147_483_541, None), // _commit_snapshot_id
            (2_147_483_645, None), // _pos
            (2_147_483_646, None), // _file
        ];
        for (field_id, expected) in cases {
            assert_eq!(
                field_id_to_statistics_base(field_id),
                expected,
                "Failed for field_id {}",
                field_id
            );
        }
    }

    #[test]
    fn test_statistics_base_to_field_id() {
        // stats_field_id -> expected field_id
        let cases = [
            // Data space
            (10_000, Some(0)),
            (10_200, Some(1)),
            (10_400, Some(2)),
            (11_000, Some(5)),
            (30_000, Some(100)),
            (MAX_DATA_STATS_FIELD_ID, Some(MAX_DATA_FIELD_ID)),
            // Metadata space (supported)
            (9_000, Some(2_147_483_539)), // _last_updated_sequence_number
            (9_200, Some(2_147_483_540)), // _row_id
            // Metadata space (unsupported -- outside SUPPORTED_METADATA_FIELD_IDS)
            (9_400, None),
            (9_600, None),
            (9_800, None),
            // Invalid: below the metadata space start
            (-1, None),
            (0, None),
            (200, None),
            (5_000, None),
            (8_600, None),
            (8_800, None),
            // Invalid: not a multiple of 200
            (10_001, None),
            (10_201, None),
            (10_500, None),
            (10_900, None),
            // Invalid: at or above the exclusive upper bound
            (STATS_SPACE_FIELD_ID_END, None),
            (STATS_SPACE_FIELD_ID_END + 200, None),
            (i32::MAX, None),
        ];
        for (stats_field_id, expected) in cases {
            assert_eq!(
                statistics_base_to_field_id(stats_field_id),
                expected,
                "Failed for stats_field_id {}",
                stats_field_id
            );
        }
    }

    #[test]
    fn test_roundtrip() {
        for field_id in [0, 1, 2, 5, 100, 1000] {
            let stats_id = field_id_to_statistics_base(field_id).unwrap();
            let recovered = statistics_base_to_field_id(stats_id).unwrap();
            assert_eq!(
                field_id, recovered,
                "Roundtrip failed for field_id {}",
                field_id
            );
        }
    }
}
