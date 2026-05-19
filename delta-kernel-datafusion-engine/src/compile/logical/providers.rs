//! [`TableProvider`] for
//! [`DeclarativePlanNode::FileListing`](delta_kernel::plans::ir::DeclarativePlanNode::FileListing)
//! nodes, emitting one `(path, size, modification_time)` row per object under a URL prefix.

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion_common::arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::plans::ir::nodes::FileListingNode;

use crate::exec::FileListingExec;

/// [`TableProvider`] for
/// [`DeclarativePlanNode::FileListing`](delta_kernel::plans::ir::DeclarativePlanNode::FileListing):
/// enumerates a storage prefix via the object store registered for the path's scheme/host and
/// emits a `(path, size, modification_time)` row per object. The actual listing happens inside
/// the returned [`ExecutionPlan`] at execute time; planning is fast.
#[derive(Debug)]
struct FileListingTableProvider {
    path: url::Url,
    schema: Arc<ArrowSchema>,
}

impl FileListingTableProvider {
    fn new(path: url::Url) -> Self {
        use datafusion_common::arrow::datatypes::{DataType, Field};
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modification_time", DataType::Int64, false),
        ]));
        Self { path, schema }
    }
}

#[async_trait::async_trait]
impl TableProvider for FileListingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FileListingExec::new(self.path.clone())))
    }
}

pub(super) fn file_listing_to_logical_plan(
    node: &FileListingNode,
) -> Result<LogicalPlan, DataFusionError> {
    let provider: Arc<dyn TableProvider> =
        Arc::new(FileListingTableProvider::new(node.path.clone()));
    LogicalPlanBuilder::scan("file_listing", provider_as_source(provider), None)?.build()
}
