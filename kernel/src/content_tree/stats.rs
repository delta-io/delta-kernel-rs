//! Stats field ID calculation utilities for Adaptive Metadata Tree (AMT).
//!
//! This module provides functions to compute stats field IDs for parent struct fields,
//! which are used in the AMT format for storing per-column statistics.

/// Number of supported stats per column (each column gets a range of 200 field IDs).
const NUM_SUPPORTED_STATS_PER_COLUMN: i32 = 200;

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

/// Iceberg reserved field ID for `_last_updated_sequence_number` (`Integer.MAX_VALUE - 108`).
const LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID: i32 = 2_147_483_539;

/// Iceberg reserved field ID for `_row_id` (`Integer.MAX_VALUE - 107`).
const ROW_ID_FIELD_ID: i32 = 2_147_483_540;

/// The set of reserved metadata field IDs that have stats tracked in `content_stats`.
/// Per the spec, only `_last_updated_sequence_number` and `_row_id` are supported.
const SUPPORTED_METADATA_FIELD_IDS: [i32; 2] =
    [LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID, ROW_ID_FIELD_ID];

/// The smallest field ID in [`SUPPORTED_METADATA_FIELD_IDS`]. Metadata stats offsets are
/// computed relative to this value.
const FIRST_SUPPORTED_METADATA_FIELD_ID: i32 = SUPPORTED_METADATA_FIELD_IDS[0];

/// A contiguous region of the stats field ID space with a fixed [`NUM_SUPPORTED_STATS_PER_COLUMN`]
/// stride, mapping field IDs to their stats base and back.
///
/// A field ID `f` maps to base `start + 200 * (f - field_base)`; the data space uses
/// `field_base == 0` so its base is simply `start + 200 * f`.
struct StatsSpace {
    start: i32,
    field_base: i32,
}

impl StatsSpace {
    /// The base stats field ID for `field_id` within this space.
    const fn base(&self, field_id: i32) -> i32 {
        self.start + NUM_SUPPORTED_STATS_PER_COLUMN * (field_id - self.field_base)
    }

    /// The field ID for a `base` stats field ID within this space (inverse of [`Self::base`]).
    #[allow(dead_code)] // only used by the test-only inverse `statistics_base_to_field_id`
    const fn field_id(&self, base: i32) -> i32 {
        (base - self.start) / NUM_SUPPORTED_STATS_PER_COLUMN + self.field_base
    }
}

const METADATA_SPACE: StatsSpace = StatsSpace {
    start: STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS,
    field_base: FIRST_SUPPORTED_METADATA_FIELD_ID,
};

const DATA_SPACE: StatsSpace = StatsSpace {
    start: STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS,
    field_base: 0,
};

/// Computes the base field ID for a column's stats struct, given a parent struct field ID.
///
/// Stats field IDs occupy the range `[9_000, 200_000_000)`:
/// - Metadata fields in [`SUPPORTED_METADATA_FIELD_IDS`] map into `[9_000, 10_000)`.
/// - Data fields `[0, MAX_DATA_FIELD_ID]` map into `[10_000, 200_000_000)`.
///
/// Returns `None` for negative field IDs, unsupported metadata field IDs, or data field IDs
/// whose stats would fall outside the reserved range.
// TODO: remove allow(dead_code) once AMT stats collection consumes this.
#[allow(dead_code)]
pub(crate) fn field_id_to_statistics_base(field_id: i32) -> Option<i32> {
    if SUPPORTED_METADATA_FIELD_IDS.contains(&field_id) {
        Some(METADATA_SPACE.base(field_id))
    } else if (0..=MAX_DATA_FIELD_ID).contains(&field_id) {
        Some(DATA_SPACE.base(field_id))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Inverse of [`field_id_to_statistics_base`], used to cross-check the forward mapping.
    ///
    /// Only stats field IDs in `[9_000, 200_000_000)` that are multiples of 200 are valid.
    /// For the metadata range `[9_000, 10_000)`, the recovered field ID must be in
    /// [`SUPPORTED_METADATA_FIELD_IDS`]; otherwise `None` is returned.
    fn statistics_base_to_field_id(stats_field_id: i32) -> Option<i32> {
        if !(STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS..STATS_SPACE_FIELD_ID_END)
            .contains(&stats_field_id)
            || stats_field_id % NUM_SUPPORTED_STATS_PER_COLUMN != 0
        {
            return None;
        }

        if stats_field_id < STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS {
            let field_id = METADATA_SPACE.field_id(stats_field_id);
            SUPPORTED_METADATA_FIELD_IDS
                .contains(&field_id)
                .then_some(field_id)
        } else {
            Some(DATA_SPACE.field_id(stats_field_id))
        }
    }

    /// Valid `(field_id, stats_base)` pairs, asserted in both directions
    #[rstest]
    #[case(0, 10_000)]
    #[case(1, 10_200)]
    #[case(2, 10_400)]
    #[case(5, 11_000)]
    #[case(100, 30_000)]
    #[case(MAX_DATA_FIELD_ID, MAX_DATA_STATS_FIELD_ID)]
    #[case(LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID, 9_000)]
    #[case(ROW_ID_FIELD_ID, 9_200)]
    fn valid_mapping_roundtrips(#[case] field_id: i32, #[case] stats_base: i32) {
        assert_eq!(field_id_to_statistics_base(field_id), Some(stats_base));
        assert_eq!(statistics_base_to_field_id(stats_base), Some(field_id));
    }

    /// Field IDs that `field_id_to_statistics_base` must reject.
    #[rstest]
    #[case(-1)] // negative
    #[case(MAX_DATA_FIELD_ID + 1)] // data field ID above the reserved range
    #[case(2_147_483_541)] // _commit_snapshot_id (unsupported reserved metadata)
    #[case(2_147_483_645)] // _pos (unsupported reserved metadata)
    #[case(2_147_483_646)] // _file (unsupported reserved metadata)
    fn field_id_to_statistics_base_rejects_invalid(#[case] field_id: i32) {
        assert_eq!(field_id_to_statistics_base(field_id), None);
    }

    /// Stats field IDs that `statistics_base_to_field_id` must reject.
    #[rstest]
    #[case(9_400)] // multiple of 200 in metadata range, but unsupported field ID
    #[case(9_600)]
    #[case(9_800)]
    #[case(-1)] // below the metadata space start
    #[case(0)]
    #[case(200)]
    #[case(5_000)]
    #[case(8_600)]
    #[case(8_800)]
    #[case(9_001)] // non-multiple of 200 in the metadata range
    #[case(10_001)] // non-multiple of 200 in the data range
    #[case(10_201)]
    #[case(10_500)]
    #[case(10_900)]
    #[case(STATS_SPACE_FIELD_ID_END)] // at the exclusive upper bound
    #[case(STATS_SPACE_FIELD_ID_END + 200)] // above the upper bound
    #[case(i32::MAX)]
    fn statistics_base_to_field_id_rejects_invalid(#[case] stats_field_id: i32) {
        assert_eq!(statistics_base_to_field_id(stats_field_id), None);
    }
}
