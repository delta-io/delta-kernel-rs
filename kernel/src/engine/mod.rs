//! Provides an engine implementation that implements the required traits. The engine can optionally
//! be built into the kernel by setting the `default-engine` feature flag. See the related module
//! for more information.

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(feature = "arrow-expression")]
pub(crate) mod arrow_utils;
#[cfg(feature = "internal-api")]
pub use self::arrow_utils::{parse_json, to_json_bytes};

#[cfg(feature = "default-engine-base")]
pub mod default;

#[cfg(test)]
pub(crate) mod sync;

#[cfg(feature = "default-engine-base")]
pub mod arrow_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod arrow_get_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod ensure_data_types;
#[cfg(feature = "default-engine-base")]
pub mod parquet_row_group_skipping;

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use object_store::path::Path;
    use std::sync::Arc;
    use url::Url;

    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine_data::FilteredEngineData;
    use crate::{Engine, EngineData};

    use test_utils::delta_path_for_version;

    fn test_list_from_should_sort_and_filter(
        engine: &dyn Engine,
        base_url: &Url,
        engine_data: impl Fn() -> Box<dyn EngineData>,
    ) {
        let json = engine.json_handler();
        let get_data = || {
            let data = engine_data();
            let filtered_data = FilteredEngineData::with_all_rows_selected(data);
            Box::new(std::iter::once(Ok(filtered_data)))
        };

        let expected_names: Vec<Path> = (1..4)
            .map(|i| delta_path_for_version(i, "json"))
            .collect_vec();

        for i in expected_names.iter().rev() {
            let path = base_url.join(i.as_ref()).unwrap();
            json.write_json_file(&path, get_data(), false).unwrap();
        }
        let path = base_url.join("other").unwrap();
        json.write_json_file(&path, get_data(), false).unwrap();

        let storage = engine.storage_handler();

        // list files after an offset
        let test_url = base_url.join(expected_names[0].as_ref()).unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len() - 1);
        for (file, expected) in files.iter().zip(expected_names.iter().skip(1)) {
            assert_eq!(file.location, base_url.join(expected.as_ref()).unwrap());
        }

        let test_url = base_url
            .join(delta_path_for_version(0, "json").as_ref())
            .unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len());

        // list files inside a directory / key prefix
        let test_url = base_url.join("_delta_log/").unwrap();
        let files: Vec<_> = storage.list_from(&test_url).unwrap().try_collect().unwrap();
        assert_eq!(files.len(), expected_names.len());
        for (file, expected) in files.iter().zip(expected_names.iter()) {
            assert_eq!(file.location, base_url.join(expected.as_ref()).unwrap());
        }
    }

    fn get_arrow_data() -> Box<dyn EngineData> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "dog",
            ArrowDataType::Utf8,
            true,
        )]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["remi", "wilson"]))],
        )
        .unwrap();
        Box::new(ArrowEngineData::new(data))
    }

    pub(crate) fn test_arrow_engine(engine: &dyn Engine, base_url: &Url) {
        test_list_from_should_sort_and_filter(engine, base_url, get_arrow_data);
    }
}
