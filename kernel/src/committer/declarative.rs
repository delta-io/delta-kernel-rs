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

fn file_meta_from_batches(
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
        .ok_or_else(|| KernelError::generic("Commit write returned no file metadata"))
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
                    "Commit write returned multiple file metadata rows",
                ));
            }
            let location: String = getters[0].get(row, "location")?;
            let location = Url::parse(&location).map_err(|err| {
                KernelError::internal_error(format!("Invalid written file URL: {err}"))
            })?;
            let size: i64 = getters[1].get(row, "size")?;
            let size = u64::try_from(size).map_err(|_| {
                KernelError::internal_error("Commit write returned negative file size")
            })?;
            let last_modified: i64 = getters[2].get(row, "last_modified")?;
            self.file_meta = Some(FileMeta::new(location, last_modified, size));
        }
        Ok(())
    }
}
