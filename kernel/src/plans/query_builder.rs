//! Builder API for constructing a [`QueryPlan`].

use super::ir::QueryPlanNode;
use crate::schema::SchemaRef;
use crate::{DeltaResult, FileMeta, PredicateRef, QueryPlan};

/// Builder for constructing a [`QueryPlan`].
///
/// TODO: We expect this to evolve to support multi-node plans. For now it just supports a
/// single node.
#[derive(Debug)]
pub struct QueryPlanBuilder {
    node: QueryPlanNode,
}

impl QueryPlanBuilder {
    /// Construct a [`QueryPlanNode::ScanJson`] over the given files.
    ///
    /// See [`QueryPlanNode::ScanJson`] for the parameter semantics.
    pub fn scan_json(
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self {
            node: QueryPlanNode::ScanJson {
                files,
                physical_schema,
                predicate,
            },
        }
    }

    /// Construct a [`QueryPlanNode::ScanParquet`] over the given files.
    ///
    /// See [`QueryPlanNode::ScanParquet`] for the parameter semantics.
    pub fn scan_parquet(
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self {
            node: QueryPlanNode::ScanParquet {
                files,
                physical_schema,
                predicate,
            },
        }
    }

    /// Consume the builder and produce a [`QueryPlan`].
    pub fn build(self) -> DeltaResult<QueryPlan> {
        Ok(self.node)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
    fn scan_parquet_constructs_scan_parquet_node() {
        let schema = test_schema();
        let files = vec![
            test_file("file:///a.parquet"),
            test_file("file:///b.parquet"),
        ];

        let node = QueryPlanBuilder::scan_parquet(files.clone(), schema.clone(), None)
            .build()
            .unwrap();

        let QueryPlanNode::ScanParquet {
            files: scan_files,
            physical_schema,
            predicate,
        } = node
        else {
            panic!("expected ScanParquet, got {node:?}");
        };
        assert_eq!(scan_files, files);
        assert!(Arc::ptr_eq(&physical_schema, &schema));
        assert!(predicate.is_none());
    }

    #[test]
    fn scan_json_constructs_scan_json_node() {
        let schema = test_schema();
        let files = vec![test_file("file:///a.json"), test_file("file:///b.json")];

        let node = QueryPlanBuilder::scan_json(files.clone(), schema.clone(), None)
            .build()
            .unwrap();

        let QueryPlanNode::ScanJson {
            files: scan_files,
            physical_schema,
            predicate,
        } = node
        else {
            panic!("expected ScanJson, got {node:?}");
        };
        assert_eq!(scan_files, files);
        assert!(Arc::ptr_eq(&physical_schema, &schema));
        assert!(predicate.is_none());
    }
}
