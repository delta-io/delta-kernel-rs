use crate::{engine_data::GetData, DeltaResult};

pub(crate) trait Deduplicator {
    type Key;
    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<(Self::Key, bool)>>;
    fn check_and_record_seen(&mut self, key: Self::Key) -> bool;
    fn is_log_batch(&self) -> bool;
}
