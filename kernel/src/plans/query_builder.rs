//! Builder API for constructing a [`Plan`] over a single relational node.
//!
//! The builder produces single-node [`Plan`]s today (one scan source, no transforms);
//! it will grow to support multi-node plans. Until then, multi-node [`Plan`]s are
//! constructed directly via [`Plan`] / [`PlanNode`].

use super::ir::nodes::{Operator, ScanFile, ScanJson, ScanParquet};
use super::ir::plan::{Plan, PlanNode, RefId};
use crate::schema::SchemaRef;
use crate::{DeltaResult, FileMeta};

/// Builder for constructing a single-node [`Plan`].
#[derive(Debug)]
pub struct QueryPlanBuilder {
    op: Operator,
}

impl QueryPlanBuilder {
    /// Construct a [`ScanJson`] over the given files.
    ///
    /// See [`ScanJson`] for parameter semantics.
    pub fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self {
            op: Operator::ScanJson(ScanJson {
                files: files.into_iter().map(ScanFile::from).collect(),
                file_constant_columns: vec![],
                schema,
            }),
        }
    }

    /// Construct a [`ScanParquet`] over the given files.
    ///
    /// See [`ScanParquet`] for parameter semantics.
    pub fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self {
            op: Operator::ScanParquet(ScanParquet {
                files: files.into_iter().map(ScanFile::from).collect(),
                file_constant_columns: vec![],
                schema,
            }),
        }
    }

    /// Consume the builder and produce a [`Plan`] with a single node whose output is `RefId(0)`.
    ///
    /// # Errors
    ///
    /// Currently infallible; returns `DeltaResult` for a stable signature as the builder
    /// grows multi-node validation in follow-up work.
    pub fn build(self) -> DeltaResult<Plan> {
        Ok(Plan {
            nodes: vec![PlanNode {
                op: self.op,
                inputs: vec![],
                output: RefId(0),
            }],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
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

    enum Format {
        Json,
        Parquet,
    }

    #[rstest]
    #[case::json(Format::Json, &["file:///a.json", "file:///b.json"])]
    #[case::parquet(Format::Parquet, &["file:///a.parquet", "file:///b.parquet"])]
    fn build_constructs_scan_node(#[case] format: Format, #[case] urls: &[&str]) {
        let schema = test_schema();
        let files: Vec<FileMeta> = urls.iter().map(|u| test_file(u)).collect();

        let plan = match format {
            Format::Json => QueryPlanBuilder::scan_json(files.clone(), schema.clone()),
            Format::Parquet => QueryPlanBuilder::scan_parquet(files.clone(), schema.clone()),
        }
        .build()
        .unwrap();

        let result = plan.result();
        let [node] = <[_; 1]>::try_from(plan.nodes).expect("single-node plan");
        assert_eq!(Some(node.output), result);
        let (Operator::ScanJson(ScanJson {
            files: scan_files,
            schema: node_schema,
            ..
        })
        | Operator::ScanParquet(ScanParquet {
            files: scan_files,
            schema: node_schema,
            ..
        })) = node.op
        else {
            panic!("expected ScanJson / ScanParquet, got {:?}", node.op);
        };
        let scan_metas: Vec<FileMeta> = scan_files.into_iter().map(|f| f.meta).collect();
        assert_eq!(scan_metas, files);
        assert!(Arc::ptr_eq(&node_schema, &schema));
    }
}
