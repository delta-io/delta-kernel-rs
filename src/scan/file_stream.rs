use std::collections::HashSet;
use std::pin::Pin;

use arrow_arith::boolean::{is_not_null, or};
use arrow_array::{BooleanArray, RecordBatch};
use arrow_select::filter::filter_record_batch;
use futures::prelude::*;
use futures::stream::BoxStream;
use futures::task::{Context, Poll};

use super::data_skipping::data_skipping_filter;
use crate::actions::{parse_actions, Action, ActionType};
use crate::expressions::Expression;
use crate::{DeltaResult, Error};

/// A stream of [`RecordBatch`]es that represent actions in the delta log.
pub struct LogReplayStream {
    stream: BoxStream<'static, DeltaResult<RecordBatch>>,
    predicate: Option<Expression>,
    seen: HashSet<(String, Option<String>)>,
    // ages: HashMap<Version, HashSet<PathBuf>>
}

impl std::fmt::Debug for LogReplayStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("LogStream")
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl LogReplayStream {
    /// Create a new [`LogReplayStream`] instance
    pub(crate) fn new(
        stream: BoxStream<'static, DeltaResult<RecordBatch>>,
        predicate: Option<Expression>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            predicate,
            stream,
            seen: Default::default(),
        })
    }
}

impl Stream for LogReplayStream {
    type Item = DeltaResult<Vec<Action>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<<Self as futures::Stream>::Item>> {
        let stream = Pin::new(&mut self.stream);
        match stream.poll_next(ctx) {
            futures::task::Poll::Ready(value) => match value {
                Some(Ok(actions)) => {
                    let skipped = if let Some(predicate) = &self.predicate {
                        data_skipping_filter(actions, predicate)?
                    } else {
                        let predicate = filter_nulls(&actions)?;
                        filter_record_batch(&actions, &predicate)?
                    };
                    let filtered_actions =
                        parse_actions(&skipped, &[ActionType::Remove, ActionType::Add])?
                            .filter_map(|action| match &action {
                                Action::Add(add)
                                    // TODO right now this may not work as expected if we have the same add
                                    // file with different deletetion vectors in the log - i.e. additional
                                    // rows were deleted in spearate op. Is this a case to consider?
                                    if !self
                                        .seen
                                        .contains(&(add.path.clone(), add.dv_unique_id())) =>
                                {
                                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                                    Some(action)
                                }
                                Action::Add(add) => {
                                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                                    None
                                }
                                Action::Remove(remove) => {
                                    self.seen
                                        .insert((remove.path.clone(), remove.dv_unique_id()));
                                    None
                                }
                                _ => None,
                            })
                            .collect();
                    futures::task::Poll::Ready(Some(Ok(filtered_actions)))
                }
                Some(Err(err)) => futures::task::Poll::Ready(Some(Err(err))),
                None => futures::task::Poll::Ready(None),
            },
            _ => futures::task::Poll::Pending,
        }
    }
}

fn filter_nulls(batch: &RecordBatch) -> DeltaResult<BooleanArray> {
    let add_array = batch
        .column_by_name("add")
        .ok_or(Error::MissingData("expected add column".into()))?;
    let remove_array = batch
        .column_by_name("remove")
        .ok_or(Error::MissingData("expected remove column".into()))?;
    Ok(or(&is_not_null(add_array)?, &is_not_null(remove_array)?)?)
}
