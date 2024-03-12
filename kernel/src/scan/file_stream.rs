use std::collections::HashSet;

use either::Either;
use tracing::debug;

use super::data_skipping::DataSkippingFilter;
use crate::actions::{get_log_schema, ADD_NAME, REMOVE_NAME};
use crate::actions::{visitors::AddVisitor, visitors::RemoveVisitor, Add, Remove};
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::Expression;
use crate::schema::SchemaRef;
use crate::{DataVisitor, DeltaResult, EngineData, EngineInterface};

struct LogReplayScanner {
    filter: Option<DataSkippingFilter>,

    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<(String, Option<String>)>,
}

#[derive(Default)]
struct AddRemoveVisitor {
    adds: Vec<Add>,
    removes: Vec<Remove>,
}

const ADD_FIELD_COUNT: usize = 15;

impl DataVisitor for AddRemoveVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Add will have a path at index 0 if it is valid
            if let Some(path) = getters[0].get_opt(i, "add.path")? {
                self.adds
                    .push(AddVisitor::visit_add(i, path, &getters[..ADD_FIELD_COUNT])?);
            }
            // Remove will have a path at index 15 if it is valid
            // TODO(nick): Should count the fields in Add to ensure we don't get this wrong if more
            // are added
            else if let Some(path) = getters[ADD_FIELD_COUNT].get_opt(i, "remove.path")? {
                let remove_getters = &getters[ADD_FIELD_COUNT..];
                self.removes
                    .push(RemoveVisitor::visit_remove(i, path, remove_getters)?);
            }
        }
        Ok(())
    }
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
        let filtered_actions = self
            .filter
            .as_ref()
            .map(|filter| filter.apply(actions))
            .transpose()?;
        let actions = match filtered_actions {
            Some(ref filtered_actions) => filtered_actions.as_ref(),
            None => actions,
        };

        let schema_to_use = if is_log_batch {
            get_log_schema().project_as_schema(&[ADD_NAME, REMOVE_NAME])?
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So no need to load them here.
            get_log_schema().project_as_schema(&[ADD_NAME])?
        };
        let mut visitor = AddRemoveVisitor::default();
        actions.extract(schema_to_use, &mut visitor)?;

        for remove in visitor.removes.into_iter() {
            self.seen
                .insert((remove.path.clone(), remove.dv_unique_id()));
        }

        visitor
            .adds
            .into_iter()
            .filter_map(|add| {
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
}

/// Given an iterator of (record batch, bool) tuples and a predicate, returns an iterator of `Adds`.
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
