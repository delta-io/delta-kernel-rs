use std::io::BufReader;
use std::sync::Arc;

use arrow_arith::boolean::{is_not_null, not};
use arrow_array::{new_null_array, RecordBatch, StringArray, StructArray};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::nullif::nullif;
use tracing::debug;

use crate::error::{DeltaResult, Error};
use crate::scan::Expression;

pub(crate) fn data_skipping_filter(
    actions: RecordBatch,
    predicate: &Expression,
) -> DeltaResult<RecordBatch> {
    let adds = actions
        .column_by_name("add")
        .ok_or(Error::MissingColumn("Column 'add' not found.".into()))?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(Error::UnexpectedColumnType(
            "Expected type 'StructArray'.".into(),
        ))?;
    let stats = adds
        .column_by_name("stats")
        .ok_or(Error::MissingColumn("Column 'stats' not found.".into()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(Error::UnexpectedColumnType(
            "Expected type 'StringArray'.".into(),
        ))?;
    // parse each row as json using the stats schema from data skipping filter
    // HACK see https://github.com/apache/arrow/issues/33662
    let data_fields: Vec<_> = predicate
        .columns()
        .iter()
        .map(|name| Field::new(name, DataType::Int32, true))
        .collect();
    let stats_schema = Schema::new(vec![
        Field::new(
            "minValues",
            DataType::Struct(data_fields.clone().into()),
            true,
        ),
        Field::new("maxValues", DataType::Struct(data_fields.into()), true),
    ]);
    let parsed = concat_batches(
        &stats_schema.into(),
        stats
            .iter()
            .map(hack_parse)
            .collect::<Result<Vec<_>, _>>()?
            .iter(),
    )?;

    let skipping_vector = predicate.construct_metadata_filters(parsed)?;
    let skipping_vector = &is_not_null(&nullif(&skipping_vector, &not(&skipping_vector)?)?)?;

    let before_count = actions.num_rows();
    let after = filter_record_batch(&actions, skipping_vector)?;
    debug!(
        "number of actions before/after data skipping: {before_count} / {}",
        after.num_rows()
    );
    Ok(after)
}

fn hack_parse(json_string: Option<&str>) -> DeltaResult<RecordBatch> {
    let data_fields = vec![Field::new("ids", DataType::Int32, true)];
    let stats_schema = Schema::new(vec![
        Field::new(
            "minValues",
            DataType::Struct(data_fields.clone().into()),
            true,
        ),
        Field::new(
            "maxValues",
            DataType::Struct(data_fields.clone().into()),
            true,
        ),
    ]);
    match json_string {
        Some(s) => Ok(ReaderBuilder::new(stats_schema.into())
            .build(BufReader::new(s.as_bytes()))?
            .collect::<Vec<_>>()
            .into_iter()
            .next()
            .transpose()?
            .ok_or(Error::MissingData("Expected data".into()))?),
        None => Ok(RecordBatch::try_new(
            stats_schema.into(),
            vec![
                Arc::new(new_null_array(
                    &DataType::Struct(data_fields.clone().into()),
                    1,
                )),
                Arc::new(new_null_array(&DataType::Struct(data_fields.into()), 1)),
            ],
        )?),
    }
}
