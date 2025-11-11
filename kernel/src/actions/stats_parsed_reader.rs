//! Helper module for reading stats_parsed from checkpoint Parquet files

use std::collections::HashMap;

use crate::engine_data::GetData;
use crate::schema::StructType;
use crate::statistics::StatsParsed;
use crate::DeltaResult;

/// Parse stats_parsed struct from checkpoint Parquet data
///
/// This is a simplified implementation that extracts the nested stats_parsed column
/// from checkpoint files when present. Currently only supports basic types.
///
/// Full implementation would need to handle all data types and nested structs.
pub(crate) fn parse_stats_parsed_from_getters<'a>(
    row_index: usize,
    stats_parsed_getter: &'a dyn GetData<'a>,
    _table_schema: &StructType,
) -> DeltaResult<Option<StatsParsed>> {
    // For now, return None - full implementation would extract stats_parsed
    // This requires extending GetData trait to handle nested structs and more types

    // Try to get numRecords - if it fails, stats_parsed doesn't exist
    let num_records = match stats_parsed_getter.get_long(row_index, "add.statsParsed.numRecords")? {
        Some(n) => n,
        None => return Ok(None),
    };

    // For this simplified version, just return basic stats
    // Full implementation would parse minValues, maxValues, and nullCount
    Ok(Some(StatsParsed {
        num_records,
        min_values: HashMap::new(),
        max_values: HashMap::new(),
        null_count: HashMap::new(),
        tight_bounds: None,
    }))
}
