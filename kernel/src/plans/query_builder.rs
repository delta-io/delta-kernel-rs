//! Builder API for constructing a [`QueryPlan`].

use super::ir::{QueryPlan, QueryPlanNode, ScanFileFormat};
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, FileMeta, PredicateRef};

/// Builder for constructing a [`QueryPlan`].
///
/// TODO: We expect this to evolve to support multi-node plans. For now it just supports a
/// single node.
#[derive(Debug, Default)]
pub struct QueryPlanBuilder {
    node: Option<QueryPlanNode>,
}

impl QueryPlanBuilder {
    /// Create a new, empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the builder's node to a [`QueryPlanNode::Scan`] over the given files.
    ///
    /// See [`QueryPlanNode::Scan`] for the parameter semantics. Calling this method overwrites
    /// any previously configured node.
    pub fn scan(
        mut self,
        format: ScanFileFormat,
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        self.node = Some(QueryPlanNode::Scan {
            format,
            files,
            physical_schema,
            predicate,
        });
        self
    }

    /// Consume the builder and produce a [`QueryPlan`].
    ///
    /// Returns [`Error::Generic`] if no node was configured on the builder.
    pub fn build(self) -> DeltaResult<QueryPlan> {
        self.node
            .ok_or_else(|| Error::generic("QueryPlanBuilder: no node configured"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_utils::assert_result_error_with_message;
    use url::Url;

    use super::*;
    use crate::schema::{DataType, StructField, StructType};
    use crate::FileMeta;

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "id",
            DataType::LONG,
        )]))
    }

    fn test_file(path: &str) -> FileMeta {
        FileMeta {
            location: Url::parse(path).unwrap(),
            last_modified: 0,
            size: 0,
        }
    }

    #[test]
    fn build_without_node_errors() {
        let result = QueryPlanBuilder::new().build();
        assert_result_error_with_message(result, "no node configured");
    }

    #[test]
    fn build_scan_node() {
        let schema = test_schema();
        let files = vec![
            test_file("file:///a.parquet"),
            test_file("file:///b.parquet"),
        ];

        let plan = QueryPlanBuilder::new()
            .scan(ScanFileFormat::Parquet, files.clone(), schema.clone(), None)
            .build()
            .unwrap();

        let QueryPlanNode::Scan {
            format,
            files: scan_files,
            physical_schema,
            predicate,
        } = plan;
        assert_eq!(format, ScanFileFormat::Parquet);
        assert_eq!(scan_files, files);
        assert!(Arc::ptr_eq(&physical_schema, &schema));
        assert!(predicate.is_none());
    }
}
