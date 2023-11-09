use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use either::Either;

use super::data_skipping::data_skipping_filter;
use crate::actions::{parse_actions, Action, ActionType, Add};
use crate::expressions::Expression;
use crate::{DeltaResult, ExpressionEvaluator};

struct LogReplayScanner {
    predicate: Option<Expression>,
    evaluator: Option<Arc<dyn ExpressionEvaluator>>,
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<(String, Option<String>)>,
}

impl LogReplayScanner {
    /// Create a new [`LogReplayStream`] instance
    fn new(predicate: Option<Expression>, evaluator: Option<Arc<dyn ExpressionEvaluator>>) -> Self {
        Self {
            predicate,
            evaluator,
            seen: Default::default(),
        }
    }

    /// Extract Add actions from a single batch. This will filter out rows that
    /// don't match the predicate and Add actions that have corresponding Remove
    /// actions in the log.
    fn process_batch(&mut self, actions: RecordBatch) -> DeltaResult<Vec<Add>> {
        let actions = if let (Some(predicate), Some(evaluator)) = (&self.predicate, &self.evaluator)
        {
            data_skipping_filter(actions, predicate)?
        } else {
            actions
        };

        let adds: Vec<Add> = parse_actions(&actions, &[ActionType::Remove, ActionType::Add])?
            .filter_map(|action| match action {
                Action::Add(add)
                    // Note: each (add.path + add.dv_unique_id()) pair has a
                    // unique Add + Remove pair in the log. For example:
                    // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json
                    if !self
                        .seen
                        .contains(&(add.path.clone(), add.dv_unique_id())) =>
                {
                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                    Some(add)
                }
                Action::Remove(remove) => {
                    self.seen
                        .insert((remove.path.clone(), remove.dv_unique_id()));
                    None
                }
                _ => None,
            })
            .collect();

        Ok(adds)
    }
}

/// Given an iterator of actions and a predicate, returns a stream of [Add]
pub fn log_replay_iter(
    action_iter: impl Iterator<Item = DeltaResult<RecordBatch>>,
    predicate: Option<Expression>,
    evaluator: Option<Arc<dyn ExpressionEvaluator>>,
) -> impl Iterator<Item = DeltaResult<Add>> {
    let mut log_scanner = LogReplayScanner::new(predicate, evaluator);

    action_iter.flat_map(move |actions| match actions {
        Ok(actions) => match log_scanner.process_batch(actions) {
            Ok(adds) => Either::Left(adds.into_iter().map(Ok)),
            Err(err) => Either::Right(std::iter::once(Err(err))),
        },
        Err(err) => Either::Right(std::iter::once(Err(err))),
    })
}
