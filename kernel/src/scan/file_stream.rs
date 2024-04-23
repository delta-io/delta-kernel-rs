use std::collections::HashSet;
use std::sync::Arc;

use either::Either;
use lazy_static::lazy_static;
use tracing::debug;

use super::data_skipping::DataSkippingFilter;
use crate::actions::{get_log_schema, ADD_NAME, REMOVE_NAME};
use crate::actions::{visitors::AddVisitor, visitors::RemoveVisitor, Add, Remove};
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::Expression;
use crate::schema::{DataType, MapType, SchemaRef, StructField, StructType};
use crate::{DataVisitor, DeltaResult, EngineData, EngineInterface, ExpressionHandler};

struct LogReplayScanner {
    filter: Option<DataSkippingFilter>,

    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<(String, Option<String>)>,
}

#[derive(Default)]
struct AddRemoveVisitor {
    adds: Vec<(Add, usize)>,
    removes: Vec<Remove>,
    selection_vector: Option<Vec<bool>>,
    // whether or not we are visiting commit json (=true) or checkpoint (=false)
    is_log_batch: bool,
}

const ADD_FIELD_COUNT: usize = 15;

impl AddRemoveVisitor {
    fn new(selection_vector: Option<Vec<bool>>, is_log_batch: bool) -> Self {
        AddRemoveVisitor {
            selection_vector,
            is_log_batch,
            ..Default::default()
        }
    }
}

impl DataVisitor for AddRemoveVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Add will have a path at index 0 if it is valid
            if let Some(path) = getters[0].get_opt(i, "add.path")? {
                // Keep the file unless the selection vector is present and is false for this row
                if !self
                    .selection_vector
                    .as_ref()
                    .is_some_and(|selection| !selection[i])
                {
                    self.adds.push((
                        AddVisitor::visit_add(i, path, &getters[..ADD_FIELD_COUNT])?,
                        i,
                    ))
                }
            }
            // Remove will have a path at index 15 if it is valid
            // TODO(nick): Should count the fields in Add to ensure we don't get this wrong if more
            // are added
            // TODO(zach): add a check for selection vector that we never skip a remove
            else if self.is_log_batch {
                if let Some(path) = getters[ADD_FIELD_COUNT].get_opt(i, "remove.path")? {
                    let remove_getters = &getters[ADD_FIELD_COUNT..];
                    self.removes
                        .push(RemoveVisitor::visit_remove(i, path, remove_getters)?);
                }
            }
        }
        Ok(())
    }
}

lazy_static! {
    // NB: If you update this schema, ensure you update the comment describing it in the doc comment
    // for `scan_row_schema` in scan/mod.rs!
    pub(crate) static ref SCAN_ROW_SCHEMA: Arc<StructType> = Arc::new(StructType::new(vec!(
        StructField::new("path", DataType::STRING, true),
        StructField::new("size", DataType::LONG, true),
        StructField::new("modificationTime", DataType::LONG, true),
        StructField::new(
            "deletionVector",
            StructType::new(vec![
                StructField::new("storageType", DataType::STRING, false),
                StructField::new("pathOrInlineDv", DataType::STRING, false),
                StructField::new("offset", DataType::INTEGER, true),
                StructField::new("sizeInBytes", DataType::INTEGER, false),
                StructField::new("cardinality", DataType::LONG, false),
            ]),
            true
        ),
        StructField::new(
            "fileConstantValues",
            StructType::new(vec![StructField::new(
                "partitionValues",
                MapType::new(DataType::STRING, DataType::STRING, false),
                true,
            )]),
            true
        ),
    )));
    static ref SCAN_ROW_DATATYPE: DataType = SCAN_ROW_SCHEMA.as_ref().clone().into();
}

impl LogReplayScanner {
    /// Create a new [`LogReplayScanner`] instance
    fn new(
        table_client: &dyn EngineInterface,
        table_schema: &SchemaRef,
        predicate: &Option<Expression>,
    ) -> Self {
        Self {
            filter: DataSkippingFilter::new(table_client, table_schema, predicate),
            seen: Default::default(),
        }
    }

    /// Extract Add actions from a single batch. This will filter out rows that
    /// don't match the predicate and Add actions that have corresponding Remove
    /// actions in the log.
    fn process_batch(
        &mut self,
        actions: &dyn EngineData,
        is_log_batch: bool,
    ) -> DeltaResult<Vec<Add>> {
        // apply data skipping to get back a selection vector for actions that passed skipping
        // note: None implies all files passed data skipping.
        let selection_vector = self
            .filter
            .as_ref()
            .map(|filter| filter.apply(actions))
            .transpose()?;

        let visitor = self.setup_batch_process(selection_vector, actions, is_log_batch)?;

        visitor
            .adds
            .into_iter()
            .filter_map(|(add, _)| {
                // Note: each (add.path + add.dv_unique_id()) pair has a
                // unique Add + Remove pair in the log. For example:
                // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
                if !self.seen.contains(&(add.path.clone(), add.dv_unique_id())) {
                    debug!("Found file: {}, is log {}", &add.path, is_log_batch);
                    if is_log_batch {
                        // Remember file actions from this batch so we can ignore duplicates
                        // as we process batches from older commit and/or checkpoint files. We
                        // don't need to track checkpoint batches because they are already the
                        // oldest actions and can never replace anything.
                        self.seen.insert((add.path.clone(), add.dv_unique_id()));
                    }
                    Some(Ok(add))
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_add_transform_expr(&self) -> Expression {
        Expression::Struct(vec![
            Expression::column("add.path"),
            Expression::column("add.size"),
            Expression::column("add.modificationTime"),
            Expression::column("add.deletionVector"),
            Expression::Struct(vec![Expression::column("add.partitionValues")]),
        ])
    }

    fn process_scan_batch(
        &mut self,
        expression_handler: &dyn ExpressionHandler,
        actions: &dyn EngineData,
        is_log_batch: bool,
    ) -> DeltaResult<(Box<dyn EngineData>, Vec<bool>)> {
        // apply data skipping to get back a selection vector for actions that passed skipping
        // note: None implies all files passed data skipping.
        let filter_vector = self
            .filter
            .as_ref()
            .map(|filter| filter.apply(actions))
            .transpose()?;

        // we start our selection vector based on what was filtered. we will add to this vector
        // below if a file has been removed
        let mut selection_vector = match filter_vector {
            Some(ref filter_vector) => filter_vector.clone(),
            None => vec![false; actions.length()],
        };

        let visitor = self.setup_batch_process(filter_vector, actions, is_log_batch)?;

        for (add, index) in visitor.adds.into_iter() {
            // Note: each (add.path + add.dv_unique_id()) pair has a
            // unique Add + Remove pair in the log. For example:
            // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
            if !self.seen.contains(&(add.path.clone(), add.dv_unique_id())) {
                debug!(
                    "Including file in scan: {}, is log {is_log_batch}",
                    add.path
                );
                if is_log_batch {
                    // Remember file actions from this batch so we can ignore duplicates
                    // as we process batches from older commit and/or checkpoint files. We
                    // don't need to track checkpoint batches because they are already the
                    // oldest actions and can never replace anything.
                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                }
                selection_vector[index] = true;
            } else {
                debug!(
                    "Filtering out Add due to it being removed {}, is log {is_log_batch}",
                    add.path
                );
            }
        }

        let result = expression_handler
            .get_evaluator(
                get_log_schema().project(&[ADD_NAME])?,
                self.get_add_transform_expr(),
                SCAN_ROW_DATATYPE.clone(),
            )
            .evaluate(actions)?;
        Ok((result, selection_vector))
    }

    // work shared between process_batch and process_scan_batch
    fn setup_batch_process(
        &mut self,
        selection_vector: Option<Vec<bool>>,
        actions: &dyn EngineData,
        is_log_batch: bool,
    ) -> DeltaResult<AddRemoveVisitor> {
        let schema_to_use = if is_log_batch {
            // NB: We _must_ pass these in the order `ADD_NAME, REMOVE_NAME` as the visitor assumes
            // the Add action comes first. The [`project`] method honors this order, so this works
            // as long as we keep this order here.
            get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So no need to load them here.
            get_log_schema().project(&[ADD_NAME])?
        };
        let mut visitor = AddRemoveVisitor::new(selection_vector, is_log_batch);
        actions.extract(schema_to_use, &mut visitor)?;

        for remove in visitor.removes.drain(..) {
            let dv_id = remove.dv_unique_id();
            self.seen.insert((remove.path, dv_id));
        }

        Ok(visitor)
    }
}

/// Given an iterator of (engine_data, bool) tuples and a predicate, returns an iterator of `Adds`.
/// The boolean flag indicates whether the record batch is a log or checkpoint batch.
pub fn log_replay_iter(
    engine_client: &dyn EngineInterface,
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>>,
    table_schema: &SchemaRef,
    predicate: &Option<Expression>,
) -> impl Iterator<Item = DeltaResult<Add>> {
    let mut log_scanner = LogReplayScanner::new(engine_client, table_schema, predicate);

    action_iter.flat_map(move |actions| match actions {
        Ok((batch, is_log_batch)) => {
            match log_scanner.process_batch(batch.as_ref(), is_log_batch) {
                Ok(adds) => Either::Left(adds.into_iter().map(Ok)),
                Err(err) => Either::Right(std::iter::once(Err(err))),
            }
        }
        Err(err) => Either::Right(std::iter::once(Err(err))),
    })
}

/// Given an iterator of (engine_data, bool) tuples and a predicate, returns an iterator of
/// `(engine_data, selection_vec)`. Each row that is selected in the returned `engine_data` _must_
/// be processed to complete the scan. Non-selected rows _must_ be ignored. The boolean flag
/// indicates whether the record batch is a log or checkpoint batch.
pub fn scan_action_iter(
    engine_interface: &dyn EngineInterface,
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>>,
    table_schema: &SchemaRef,
    predicate: &Option<Expression>,
) -> impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, Vec<bool>)>> {
    let mut log_scanner = LogReplayScanner::new(engine_interface, table_schema, predicate);
    let expression_handler = engine_interface.get_expression_handler();
    action_iter.map(move |action_res| {
        action_res.and_then(|(batch, is_log_batch)| {
            log_scanner.process_scan_batch(
                expression_handler.as_ref(),
                batch.as_ref(),
                is_log_batch,
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::{
        client::arrow_data::ArrowEngineData,
        client::sync::{json::SyncJsonHandler, SyncEngineInterface},
        EngineData, JsonHandler,
    };

    // TODO(nick): Merge all copies of this into one "test utils" thing
    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    fn add_batch() -> Box<ArrowEngineData> {
        let handler = SyncJsonHandler {};
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues": {"date": "2017-12-10"},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}"deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(get_log_schema().clone());
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        ArrowEngineData::try_from_engine_data(parsed).unwrap()
    }

    #[test]
    fn test_scan_action_iter() {
        let batch = vec![add_batch()];
        let interface = SyncEngineInterface::new();
        // doesn't matter here
        let table_schema = Arc::new(StructType::new(vec![StructField::new(
            "foo",
            crate::schema::DataType::STRING,
            false,
        )]));
        let iter = scan_action_iter(
            &interface,
            batch.into_iter().map(|batch| Ok((batch as _, false))),
            &table_schema,
            &None,
        );
        for res in iter {
            let (batch, sel) = res.unwrap();
            let record_batch: RecordBatch = batch
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))
                .unwrap()
                .into();
            println!("{:#?}", record_batch);
            println!("{:#?}", sel);
        }
    }
}
