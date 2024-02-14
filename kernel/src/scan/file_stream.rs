use std::collections::HashSet;
use std::sync::Arc;

use super::data_skipping::DataSkippingFilter;
use crate::actions::action_definitions::Add;
use crate::expressions::Expression;
use crate::schema::{SchemaRef, StructType};
use crate::{DataExtractor, DeltaResult, EngineData};

use either::Either;
use tracing::debug;

struct LogReplayScanner {
    filter: Option<DataSkippingFilter>,

    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<(String, Option<String>)>,
}

impl LogReplayScanner {
    /// Create a new [`LogReplayStream`] instance
    fn new(table_schema: &SchemaRef, predicate: &Option<Expression>) -> Self {
        Self {
            filter: DataSkippingFilter::new(table_schema, predicate),
            seen: Default::default(),
        }
    }

    /// Extract Add actions from a single batch. This will filter out rows that
    /// don't match the predicate and Add actions that have corresponding Remove
    /// actions in the log.
    fn process_batch(
        &mut self,
        actions: &dyn EngineData,
        data_extractor: &Arc<dyn DataExtractor>,
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

        use crate::actions::action_definitions::{visit_add, visit_remove, MultiVisitor};
        let add_schema = StructType::new(vec![crate::actions::schemas::ADD_FIELD.clone()]);
        let mut multi_add_visitor = MultiVisitor::new(visit_add);
        data_extractor.extract(actions, Arc::new(add_schema), &mut multi_add_visitor)?;

        let mut multi_remove_visitor = MultiVisitor::new(visit_remove);
        let remove_schema = StructType::new(vec![crate::actions::schemas::REMOVE_FIELD.clone()]);
        if is_log_batch {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So only load them if we're not a checkpoint
            data_extractor.extract(actions, Arc::new(remove_schema), &mut multi_remove_visitor)?;
        }

        for remove in multi_remove_visitor.extracted.into_iter().flatten() {
            self.seen
                .insert((remove.path.clone(), remove.dv_unique_id()));
        }

        let adds: Vec<DeltaResult<Add>> = multi_add_visitor.extracted;
        adds.into_iter()
            .filter_map(|action| {
                match action {
                Ok(add)
                // Note: each (add.path + add.dv_unique_id()) pair has a
                // unique Add + Remove pair in the log. For example:
                // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
                    if !self
                    .seen
                    .contains(&(add.path.clone(), add.dv_unique_id())) =>
                {
                    debug!("Found file: {}, is log {}", &add.path, is_log_batch);
                    if is_log_batch {
                        // Remember file actions from this batch so we can ignore duplicates
                        // as we process batches from older commit and/or checkpoint files. We
                        // don't need to track checkpoint batches because they are already the
                        // oldest actions and can never replace anything.
                        self.seen.insert((add.path.clone(), add.dv_unique_id()));
                    }
                    Some(Ok(add))
                }
                _ => None
            }
            })
            .collect()
    }
}

/// Given an iterator of (record batch, bool) tuples and a predicate, returns an iterator of `Adds`.
/// The boolean flag indicates whether the record batch is a log or checkpoint batch.
pub fn log_replay_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>>,
    data_extractor: Arc<dyn DataExtractor>,
    table_schema: &SchemaRef,
    predicate: &Option<Expression>,
) -> impl Iterator<Item = DeltaResult<Add>> {
    let mut log_scanner = LogReplayScanner::new(table_schema, predicate);

    action_iter.flat_map(move |actions| match actions {
        Ok((batch, is_log_batch)) => {
            match log_scanner.process_batch(batch.as_ref(), &data_extractor, is_log_batch) {
                Ok(adds) => Either::Left(adds.into_iter().map(Ok)),
                Err(err) => Either::Right(std::iter::once(Err(err))),
            }
        }
        Err(err) => Either::Right(std::iter::once(Err(err))),
    })
}
