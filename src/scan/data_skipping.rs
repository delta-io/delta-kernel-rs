use crate::scan::Expression;
use arrow::array::{StringArray, StructArray};
use arrow::record_batch::RecordBatch;
use tracing::debug;

use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::json::ReaderBuilder;
use std::io::BufReader;
use std::sync::Arc;

pub(crate) fn data_skipping_filter(
    actions: RecordBatch,
    predicate: &Option<Expression>,
) -> RecordBatch {
    let predicate = match predicate {
        Some(p) => p,
        None => return actions,
    };
    debug!(
        "actions before data skipping:\n{}\n{}\n{}",
        arrow::util::pretty::pretty_format_columns("add", &[actions.column(0).clone()]).unwrap(),
        arrow::util::pretty::pretty_format_columns("remove", &[actions.column(1).clone()]).unwrap(),
        arrow::util::pretty::pretty_format_columns("metadata", &[actions.column(2).clone()])
            .unwrap()
    ); // FIXME
    let adds = actions
        .column_by_name("add")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let stats = adds
        .column_by_name("stats")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // parse each row as json using the stats schema from data skipping filter
    // HACK see https://github.com/apache/arrow/issues/33662
    let data_fields: Vec<_> = predicate
        .columns()
        .iter()
        .map(|name| Field::new(name, arrow::datatypes::DataType::Int32, true))
        .collect();
    let stats_schema = Schema::new(vec![
        Field::new(
            "minValues",
            arrow::datatypes::DataType::Struct(data_fields.clone().into()),
            true,
        ),
        Field::new(
            "maxValues",
            arrow::datatypes::DataType::Struct(data_fields.into()),
            true,
        ),
    ]);
    let parsed = arrow::compute::concat_batches(
        &stats_schema.into(),
        stats.iter().map(hack_parse).collect::<Vec<_>>().iter(),
    )
    .unwrap();

    let skipping_vector = predicate.construct_metadata_filters(parsed).unwrap();
    let skipping_vector = &arrow::compute::is_not_null(
        &arrow::compute::nullif(
            &skipping_vector,
            &arrow::compute::not(&skipping_vector).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();

    let before_count = actions.num_rows();
    let after = arrow::compute::filter_record_batch(&actions, &skipping_vector).unwrap();
    debug!(
        "number of actions before/after data skipping: {before_count} / {}",
        after.num_rows()
    );
    after
}

fn hack_parse(json_string: Option<&str>) -> RecordBatch {
    let data_fields = vec![Field::new("ids", arrow::datatypes::DataType::Int32, true)];
    let stats_schema = Schema::new(vec![
        Field::new(
            "minValues",
            arrow::datatypes::DataType::Struct(data_fields.clone().into()),
            true,
        ),
        Field::new(
            "maxValues",
            arrow::datatypes::DataType::Struct(data_fields.clone().into()),
            true,
        ),
    ]);
    match json_string {
        Some(s) => ReaderBuilder::new(stats_schema.into())
            .build(BufReader::new(s.as_bytes()))
            .unwrap()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|i| i.unwrap())
            .next()
            .unwrap(),
        None => RecordBatch::try_new(
            stats_schema.into(),
            vec![
                Arc::new(arrow::array::new_null_array(
                    &arrow::datatypes::DataType::Struct(data_fields.clone().into()),
                    1,
                )),
                Arc::new(arrow::array::new_null_array(
                    &arrow::datatypes::DataType::Struct(data_fields.into()),
                    1,
                )),
            ],
        )
        .unwrap(),
    }
}
