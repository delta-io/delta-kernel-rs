use std::sync::Arc;

use datafusion_catalog::Session;
use datafusion_common::error::Result;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::scan::state::Stats;
use delta_kernel::schema::SchemaRef as DeltaSchemaRef;
use delta_kernel::{Expression, ExpressionRef, Version};
use url::Url;

pub struct ScanFileContext {
    pub selection_vector: Option<Vec<bool>>,
    pub transform: Option<ExpressionRef>,
    pub file_url: Url,
    pub stats: Option<Stats>,
    pub size: u64,
}

/// The metadata required to plan a table scan.
pub struct TableScan {
    /// Files included in the scan.
    ///
    /// These are filtered on a best effort basis to match the predicate passed to the scan.
    /// Files are grouped by the object store they are stored in.
    pub files: Vec<ScanFileContext>,
    /// The physical schema of the table.
    ///
    /// Data read from disk should be presented in this schema.
    /// While in most cases this corresponds to the data present in the file,
    /// the reader may be required to cast data to include columns not present
    /// in the file (i.e. the table may have undergone schema evolution),
    /// or type widening may have been applied.
    pub physical_schema: ArrowSchemaRef,
    /// The logical schema of the table.
    ///
    /// This is the schema as it is presented to the end user.
    /// This includes any geneated or implicit columns, mapped names, etc.
    pub logical_schema: ArrowSchemaRef,
}

#[async_trait::async_trait]
pub trait TableSnapshot: std::fmt::Debug + Send + Sync {
    /// The logical schema of the table.
    ///
    /// This is the fully resolved schema as it is presented to the end user.
    /// This includes any geneated or implicit columns, mapped names, etc.
    fn table_schema(&self) -> &ArrowSchemaRef;

    fn schema(&self) -> DeltaSchemaRef;

    fn version(&self) -> Version;

    fn metadata(&self) -> &Metadata;

    fn protocol(&self) -> &Protocol;

    /// Produce the metadata required to plan a table scan.
    async fn scan_metadata(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        predicate: Arc<Expression>,
    ) -> Result<TableScan>;
}
