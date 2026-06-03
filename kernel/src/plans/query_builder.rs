//! Builder API for constructing a [`QueryPlan`].

use super::ir::QueryPlanNode;
use super::ir::ScanFile;
use super::validate::validate_scan_files_schema;
use crate::expressions::ColumnName;
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
    /// See [`QueryPlanNode::ScanJson`] and [`crate::plans::ir::nodes::ScanJson`] for parameter
    /// semantics. Validates file-constant invariants in [`Self::build`].
    pub fn scan_json(
        files: Vec<ScanFile>,
        file_constant_columns: Vec<ColumnName>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self {
            node: QueryPlanNode::ScanJson {
                files,
                file_constant_columns,
                physical_schema,
                predicate,
            },
        }
    }

    /// Construct a [`QueryPlanNode::ScanParquet`] over the given files.
    ///
    /// See [`QueryPlanNode::ScanParquet`] and [`crate::plans::ir::nodes::ScanParquet`] for
    /// parameter semantics. Validates file-constant invariants in [`Self::build`].
    pub fn scan_parquet(
        files: Vec<ScanFile>,
        file_constant_columns: Vec<ColumnName>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self {
            node: QueryPlanNode::ScanParquet {
                files,
                file_constant_columns,
                physical_schema,
                predicate,
            },
        }
    }

    /// Like [`Self::scan_parquet`], but wraps bare [`FileMeta`] entries with no file-constant
    /// columns.
    pub fn scan_parquet_files(
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self::scan_parquet(
            files.into_iter().map(ScanFile::from).collect(),
            Vec::new(),
            physical_schema,
            predicate,
        )
    }

    /// Like [`Self::scan_json`], but wraps bare [`FileMeta`] entries with no file-constant columns.
    pub fn scan_json_files(
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self::scan_json(
            files.into_iter().map(ScanFile::from).collect(),
            Vec::new(),
            physical_schema,
            predicate,
        )
    }

    /// Consume the builder and produce a [`QueryPlan`].
    ///
    /// # Errors
    ///
    /// [`crate::Error::Schema`] if scan file-constant invariants are violated.
    pub fn build(self) -> DeltaResult<QueryPlan> {
        match &self.node {
            QueryPlanNode::ScanParquet {
                files,
                file_constant_columns,
                physical_schema,
                ..
            }
            | QueryPlanNode::ScanJson {
                files,
                file_constant_columns,
                physical_schema,
                ..
            } => {
                validate_scan_files_schema(files, file_constant_columns, physical_schema.clone())?;
            }
        }
        Ok(self.node)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use url::Url;

    use super::*;
    use crate::expressions::{column_name, Scalar};
    use crate::schema::{DataType, StructField, StructType};
    use crate::FileMeta;

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "id",
            DataType::LONG,
        )]))
    }

    fn test_file(path: &str) -> ScanFile {
        ScanFile::new(FileMeta {
            location: Url::parse(path).unwrap(),
            last_modified: 0,
            size: 0,
        })
    }

    #[test]
    fn scan_parquet_constructs_scan_parquet_node() {
        let schema = test_schema();
        let files = vec![
            test_file("file:///a.parquet"),
            test_file("file:///b.parquet"),
        ];

        let node = QueryPlanBuilder::scan_parquet_files(
            files.iter().map(|f| f.meta.clone()).collect(),
            schema.clone(),
            None,
        )
        .build()
        .unwrap();

        let QueryPlanNode::ScanParquet {
            files: scan_files,
            file_constant_columns,
            physical_schema,
            predicate,
        } = node
        else {
            panic!("expected ScanParquet, got {node:?}");
        };
        assert_eq!(scan_files.len(), 2);
        assert!(file_constant_columns.is_empty());
        assert!(Arc::ptr_eq(&physical_schema, &schema));
        assert!(predicate.is_none());
    }

    #[test]
    fn scan_json_constructs_scan_json_node() {
        let schema = test_schema();
        let files = vec![test_file("file:///a.json"), test_file("file:///b.json")];

        let node = QueryPlanBuilder::scan_json_files(
            files.iter().map(|f| f.meta.clone()).collect(),
            schema.clone(),
            None,
        )
        .build()
        .unwrap();

        let QueryPlanNode::ScanJson {
            files: scan_files,
            file_constant_columns,
            physical_schema,
            predicate,
        } = node
        else {
            panic!("expected ScanJson, got {node:?}");
        };
        assert_eq!(scan_files.len(), 2);
        assert!(file_constant_columns.is_empty());
        assert!(Arc::ptr_eq(&physical_schema, &schema));
        assert!(predicate.is_none());
    }

    #[test]
    fn scan_parquet_build_validates_file_constants() {
        let schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("id", DataType::LONG),
            StructField::not_null("part", DataType::STRING),
        ]));
        let files = vec![ScanFile {
            meta: test_file("file:///a.parquet").meta,
            file_constants: vec![Scalar::String("p1".into())],
        }];
        QueryPlanBuilder::scan_parquet(files, vec![column_name!("part")], schema, None)
            .build()
            .unwrap();
    }

    #[test]
    fn scan_parquet_build_rejects_mismatched_constant_count() {
        let schema = test_schema();
        let files = vec![test_file("file:///a.parquet")];
        let err = QueryPlanBuilder::scan_parquet(
            files,
            vec![column_name!("id")],
            schema,
            None,
        )
        .build()
        .unwrap_err();
        assert!(err.to_string().contains("expected 1"));
    }
}
