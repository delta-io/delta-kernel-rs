use std::collections::HashSet;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_select::filter::filter_record_batch;
use tracing::debug;

use crate::actions::{parse_actions, Action, ActionType};
use crate::DeltaResult;

#[derive(Debug)]
pub(crate) struct LogReplay {
    // TODO change to HashSet<DataFile>
    seen: HashSet<(String, Option<String>)>,
    // ages: HashMap<Version, HashSet<PathBuf>>
}

impl LogReplay {
    pub(crate) fn new() -> Self {
        LogReplay {
            seen: HashSet::new(),
        }
    }

    /// replay the batch of actions: note (1) and (2) below are independent
    /// 1. add all RemoveFile actions to seen_set
    /// 2. return(AddFiles - seen_set)
    /// or by-row:
    /// for each action => if add and not seen, add to seen and yield; if remove, add to seen
    ///
    /// this batch-wise log replay should take a record batch of actions and replay all
    /// simultaneously. there is no ordering of actions within a commit, so as long as all actions
    /// come from the same commit (or checkpoint) then they can be applied to replay in any order.
    ///
    /// note commit/checkpoint read chunking determines parallelism.
    ///
    /// commit2 -> actions_batch_1 -> actions_batch_1_after_data_skipping -> files_batch_1
    ///         -> actions_batch_2 -> actions_batch_2_after_data_skipping -> files_batch_2
    /// commit1 -> ...
    ///
    /// in the future, this can be implemented as a join for step (1) above and anti-join for step
    /// (2) when integrated with query engines that support joins.
    pub(crate) fn replay(&mut self, actions_batch: &RecordBatch) -> DeltaResult<RecordBatch> {
        debug!("replay actions: {:?}", actions_batch);
        debug!("last seen set: {:?}", self.seen);

        // TODO for the internal implementation of log replay, we should implement 'ages' to reduce
        // the size of the seen set
        let filter_vec = parse_actions(actions_batch, &[ActionType::Add, ActionType::Remove])?
            .map(|action| match action {
                Action::Add(add)
                    if !self.seen.contains(&(add.path.clone(), add.dv_unique_id())) =>
                {
                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                    true
                }
                Action::Add(add) => {
                    self.seen.insert((add.path.clone(), add.dv_unique_id()));
                    false
                }
                Action::Remove(remove) => {
                    self.seen
                        .insert((remove.path.clone(), remove.dv_unique_id()));
                    false
                }
                _ => false,
            })
            .collect::<Vec<_>>();

        Ok(filter_record_batch(
            &actions_batch,
            &BooleanArray::from(filter_vec),
        )?)
    }
}
