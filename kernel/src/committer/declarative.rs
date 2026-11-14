use std::sync::LazyLock;

use delta_kernel_derive::internal_api;
use url::Url;

use crate::engine_data::TypedGetData as _;
use crate::expressions::{ColumnName, column_name};
use crate::plans::{Operation, PlanBuilder, RelationRef, ScopedPlanExecutor};
use crate::schema::DataType;
use crate::{
    EngineData, FileMeta, GetData, KernelError, KernelResult, KernelResultIterator, RowVisitor as _,
};

/// Writes out ordered [`RelationRef`] inputs as JSON and returns metadata for the written file.
///
/// - `executor` holds the relations and runs the write plan.
/// - `actions` yields the commit action relations in write order.
/// - `path` is the destination JSON file URL.
#[internal_api]
pub(crate) fn write_commit_json_declarative(
    executor: &dyn ScopedPlanExecutor,
    actions: impl IntoIterator<Item = RelationRef>,
    path: Url,
) -> KernelResult<FileMeta> {
    let plan = PlanBuilder::write_json(
        actions.into_iter().map(PlanBuilder::relation_source),
        path.clone(),
        false, // overwrite
        true,  // ordered
    )?
    .build()?;
    let file_meta = file_meta_from_batches(
        executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?,
    )?;
    debug_assert_eq!(file_meta.location, path);
    Ok(file_meta)
}

/// Decodes a file write's single metadata row, consuming the full stream.
pub(crate) fn file_meta_from_batches(
    batches: impl KernelResultIterator<Box<dyn EngineData>>,
) -> KernelResult<FileMeta> {
    let mut visitor = FileMetaVisitor::default();
    for batch in batches {
        let batch = batch?;
        if !batch.is_empty() {
            visitor.visit_rows_of(batch.as_ref())?;
        }
    }
    visitor
        .file_meta
        .ok_or_else(|| KernelError::generic("Write returned no file metadata"))
}

#[derive(Default)]
struct FileMetaVisitor {
    file_meta: Option<FileMeta>,
}

impl crate::RowVisitor for FileMetaVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 3]> = LazyLock::new(|| {
            [
                column_name!("location"),
                column_name!("size"),
                column_name!("last_modified"),
            ]
        });
        static TYPES: [DataType; 3] = [DataType::STRING, DataType::LONG, DataType::LONG];
        (&*NAMES, &TYPES)
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..row_count {
            if self.file_meta.is_some() {
                return Err(KernelError::internal_error(
                    "Write returned multiple file metadata rows",
                ));
            }
            let location: String = getters[0].get(row, "location")?;
            let location = Url::parse(&location).map_err(|err| {
                KernelError::internal_error(format!("Invalid written file URL: {err}"))
            })?;
            let size: i64 = getters[1].get(row, "size")?;
            let size = u64::try_from(size)
                .map_err(|_| KernelError::internal_error("Write returned negative file size"))?;
            let last_modified: i64 = getters[2].get(row, "last_modified")?;
            self.file_meta = Some(FileMeta::new(location, last_modified, size));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::Engine;
    use crate::engine::sync::SyncEngine;
    use crate::plans::PlanResult;
    use crate::plans::ir::nodes::FILE_META_SCHEMA;

    #[rstest]
    #[case::valid("memory:///file", 12, vec![0, 1, 0], true)]
    #[case::empty("memory:///file", 12, vec![], false)]
    #[case::multiple("memory:///file", 12, vec![2], false)]
    #[case::multiple_batches("memory:///file", 12, vec![1, 1], false)]
    #[case::negative_size("memory:///file", -1, vec![1], false)]
    #[case::invalid_url("invalid URL", 12, vec![1], false)]
    fn written_file_metadata(
        #[case] path: &str,
        #[case] size: i64,
        #[case] rows_per_batch: Vec<usize>,
        #[case] valid: bool,
        #[values(false, true)] late_error: bool,
    ) -> KernelResult<()> {
        let evaluation = SyncEngine::new().evaluation_handler();
        let values = vec![path.into(), size.into(), 7i64.into()];
        let batches = rows_per_batch.into_iter().map(move |rows| {
            evaluation.create_many(FILE_META_SCHEMA.clone(), vec![values.clone(); rows])
        });
        let result = file_meta_from_batches(
            batches.chain(late_error.then(|| Err(KernelError::generic("late result error")))),
        );
        if valid && !late_error {
            assert_eq!(result?, FileMeta::new(Url::parse(path)?, 7, size as u64));
        } else {
            assert!(result.is_err());
        }
        Ok(())
    }

    #[rstest]
    #[case::unit(PlanResult::Unit)]
    #[case::listing(PlanResult::FileMeta(Box::new(std::iter::empty())))]
    fn written_file_metadata_rejects_other_results(#[case] result: PlanResult) {
        assert!(matches!(
            result.into_data().and_then(file_meta_from_batches),
            Err(KernelError::PlanResultTypeMismatch {
                expected: "Data",
                ..
            })
        ));
    }
}
