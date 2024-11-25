//! Code to handle column mapping, including modes and schema transforms
use super::ReaderFeatures;
use crate::actions::Protocol;
use crate::table_properties::TableProperties;

use serde::{Deserialize, Serialize};
use strum::EnumString;

/// Modes of column mapping a table can be in
#[derive(Debug, Default, EnumString, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingMode {
    /// No column mapping is applied
    None,
    /// Columns are mapped by their field_id in parquet
    Id,
    /// Columns are mapped to a physical name
    #[default]
    Name,
}

/// Determine the column mapping mode for a table based on the [`Protocol`] and [`TableProperties`]
pub(crate) fn column_mapping_mode(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> ColumnMappingMode {
    match table_properties.column_mapping_mode {
        Some(mode) if protocol.min_reader_version() == 2 => mode,
        Some(mode)
            if protocol.min_reader_version() == 3
                && protocol.has_reader_feature(&ReaderFeatures::ColumnMapping) =>
        {
            mode
        }
        _ => ColumnMappingMode::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_column_mapping_mode() {
        let table_properties: HashMap<_, _> =
            [("delta.columnMapping.mode".to_string(), "id".to_string())]
                .into_iter()
                .collect();
        let table_properties = TableProperties::from(table_properties.iter());

        let protocol = Protocol::try_new(2, 5, None::<Vec<String>>, None::<Vec<String>>).unwrap();
        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        let empty_features = Some::<[String; 0]>([]);
        let protocol =
            Protocol::try_new(3, 7, empty_features.clone(), empty_features.clone()).unwrap();
        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            empty_features,
        )
        .unwrap();
        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );
    }
}
