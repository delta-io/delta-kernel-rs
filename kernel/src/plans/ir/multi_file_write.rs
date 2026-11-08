//! The distributed file-write contract and its transportable naming policy.

use std::num::NonZero;
use std::sync::Arc;

use prost::Message;
use url::Url;
use uuid::Uuid;

use super::nodes::{Agg, Aggregate, AggregateBuilder};
use crate::expressions::Scalar;
use crate::partition::serialization::serialize_partition_value;
use crate::partition::write_directory_prefix;
use crate::schema::{lazy_schema_ref, DataType, SchemaRef, StructType};
use crate::{KernelError, KernelResult};

/// Writes its single input to Parquet files, returning one descriptor
/// per successfully closed file. Engines choose distribution, compression and file boundaries.
/// Empty input creates no files and returns no descriptors. Output order is unspecified.
///
/// `write_schema` selects top-level input fields by name, in file order. Each selected field must
/// match the input's type, nullability and metadata. Statistics consume the full input, including
/// fields omitted from the file, and apply independently to exactly the rows in each output file.
/// Each file contains one `partition_columns` tuple. A logical partition may span tasks and
/// files; NULL partition values group together. An empty column list defines one partition.
/// Descriptors contain all partition keys in `partition_columns` order, followed by named
/// statistics in declaration order. Their types and nullability are derived from the input.
///
/// The output is `[path: STRING, size: LONG, file_modification: LONG, ...partitions,
/// ...statistics]`. The fixed metadata fields are non-nullable. `path` is the URI-safe relative
/// name supplied by `file_namer`, `size` is the completed file size in bytes, and
/// `file_modification` is milliseconds since the Unix epoch.
///
/// Executors must validate the contract before creating files, create files without overwrite,
/// and report errors from input evaluation, aggregation, encoding and close. LONG sum and count
/// overflow is an error, independent of session settings. Only successful attempts contribute
/// descriptors; failed attempts may leave orphan files. Shared consumers of a node must reuse its
/// completed result instead of repeating its writes. Callers must keep required writes reachable
/// from the terminal node: ordinary builder elimination can discard unreachable side effects.
#[derive(Debug, Clone)]
pub struct MultiFileWrite {
    /// Physical fields written to each file, in file order.
    pub write_schema: SchemaRef,
    /// Top-level input fields whose values must be constant within each file.
    pub partition_columns: Vec<String>,
    /// Hierarchical destination directory URL, ending in `/`, without a query or fragment.
    pub base_url: Url,
    /// Kernel-owned naming policy. Values follow `partition_columns` order.
    pub file_namer: FileNamer,
    /// Aggregates over each file's full input rows, paired with their output names.
    pub stats: Vec<(Agg, String)>,
}

impl MultiFileWrite {
    /// Builds a write over `input_schema`, selecting `write_schema` fields under `base_url` with
    /// names from `file_namer`. `partition_columns` supplies grouping keys; all are returned
    /// in the derived schema. `stats` adds named per-file aggregates.
    ///
    /// Returns a validated operator with a derived descriptor schema. Returns an error for an
    /// invalid directory URL, incompatible write fields, missing or duplicate partition keys,
    /// grouped or invalid statistics, or colliding output names.
    pub fn try_new(
        input_schema: &SchemaRef,
        write_schema: SchemaRef,
        partition_columns: Vec<String>,
        base_url: Url,
        file_namer: FileNamer,
        stats: impl FnOnce(AggregateBuilder) -> AggregateBuilder,
    ) -> KernelResult<Self> {
        let aggregate = stats(Aggregate::ungrouped(Arc::clone(input_schema))).build()?;
        if !aggregate.group_by.is_empty() {
            return Err(KernelError::schema(
                "multi-file write: statistics must be ungrouped",
            ));
        }
        let names = aggregate.schema.fields().map(|field| field.name().clone());
        let stats = aggregate.aggs.into_iter().zip(names).collect();
        let write = Self {
            write_schema,
            partition_columns,
            base_url,
            file_namer,
            stats,
        };
        write.output_schema(input_schema)?;
        Ok(write)
    }

    /// Validates this operator and derives its fixed metadata, partition and statistics fields.
    ///
    /// Returns an error under the conditions documented on [`try_new`](Self::try_new), including
    /// fields changed after construction. No output schema is stored on the operator.
    pub fn output_schema(&self, input_schema: &SchemaRef) -> KernelResult<SchemaRef> {
        if self.base_url.cannot_be_a_base()
            || !self.base_url.path().ends_with('/')
            || self.base_url.query().is_some()
            || self.base_url.fragment().is_some()
        {
            return Err(KernelError::generic(
                "multi-file write: base URL must be a directory without a query or fragment",
            ));
        }
        if self.file_namer.partition_columns.len() != self.partition_columns.len() {
            return Err(KernelError::schema(
                "multi-file write: naming partition count mismatch",
            ));
        }
        for field in self.write_schema.fields() {
            if input_schema.field(field.name()) != Some(field) {
                return Err(KernelError::schema(format!(
                    "multi-file write: field `{}` must match an input field exactly",
                    field.name(),
                )));
            }
        }
        let partition_schema = input_schema.project_as_struct(&self.partition_columns)?;
        let mut expected = Aggregate::ungrouped(Arc::clone(input_schema));
        for (agg, name) in &self.stats {
            if let Agg::Sum(value) = agg {
                if input_schema.field_at(value)?.data_type() != &DataType::LONG {
                    return Err(KernelError::schema(
                        "multi-file write: sum requires a LONG input",
                    ));
                }
            }
            expected = expected.aggregate_as(agg.clone(), name);
        }
        let stats_schema = expected.build()?.schema;
        Ok(Arc::new(StructType::try_new(
            DESCRIPTOR_SCHEMA
                .fields()
                .chain(partition_schema.fields())
                .chain(stats_schema.fields())
                .cloned(),
        )?))
    }
}

static DESCRIPTOR_SCHEMA: std::sync::LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null "path": STRING,
    not_null "size": LONG,
    not_null "file_modification": LONG,
};

/// Kernel-owned naming policy matching the ordinary table writer's directory layout.
///
/// Names use Hive partition directories unless a random-prefix length is configured, followed by
/// a fresh UUID and `.parquet`. The policy contains no transaction or write state.
#[derive(Debug, Clone)]
pub struct FileNamer {
    partition_columns: Vec<String>,
    random_prefix_length: Option<NonZero<usize>>,
}

impl FileNamer {
    /// Creates a naming policy for ordered partition columns. Random prefixes suppress Hive
    /// directories, as for column-mapped tables and `delta.randomizeFilePrefixes`.
    /// Table callers can obtain this policy directly from
    /// [`crate::transaction::WriteState::file_namer`].
    pub fn new(
        partition_columns: Vec<String>,
        random_prefix_length: Option<NonZero<usize>>,
    ) -> Self {
        Self {
            partition_columns,
            random_prefix_length,
        }
    }

    /// Returns a fresh URI-encoded relative path for values in partition-column order.
    ///
    /// Returns an error for incorrect arity or a value that Kernel cannot serialize as a
    /// partition value. NULL and empty values use the ordinary Hive default-partition directory.
    pub fn next_file_name(&self, partition_values: Vec<Scalar>) -> KernelResult<String> {
        if partition_values.len() != self.partition_columns.len() {
            return Err(KernelError::generic(
                "file namer: partition value count mismatch",
            ));
        }
        let serialized = partition_values
            .iter()
            .map(serialize_partition_value)
            .collect::<KernelResult<Vec<_>>>()?;
        let columns: Vec<_> = self
            .partition_columns
            .iter()
            .zip(&serialized)
            .map(|(name, value)| (name.as_str(), value.as_deref()))
            .collect();
        let directory = write_directory_prefix(&columns, self.random_prefix_length);
        Ok(format!("{directory}{}.parquet", Uuid::new_v4()))
    }

    /// Encodes this policy as opaque, versioned bytes for worker transport.
    ///
    /// The format belongs to Kernel. Engines must not inspect it or persist it across upgrades.
    pub fn encode(&self) -> Vec<u8> {
        FileNamerWire {
            version: FILE_NAMER_VERSION,
            partition_columns: self.partition_columns.clone(),
            random_prefix_length: self.random_prefix_length.map(|length| length.get() as u64),
        }
        .encode_to_vec()
    }

    /// Decodes a policy produced by [`encode`](Self::encode), validating its naming rules.
    ///
    /// Returns an error for malformed bytes, unsupported versions, or invalid naming rules.
    pub fn decode(bytes: &[u8]) -> KernelResult<Self> {
        let wire = FileNamerWire::decode(bytes)
            .map_err(|error| KernelError::generic(format!("invalid file namer: {error}")))?;
        if wire.version != FILE_NAMER_VERSION {
            return Err(KernelError::generic(format!(
                "unsupported file namer format version {}; expected {FILE_NAMER_VERSION}",
                wire.version,
            )));
        }
        let random_prefix_length = wire
            .random_prefix_length
            .map(|length| {
                usize::try_from(length)
                    .ok()
                    .and_then(NonZero::new)
                    .ok_or_else(|| KernelError::generic("file namer: invalid random prefix length"))
            })
            .transpose()?;
        Ok(Self::new(wire.partition_columns, random_prefix_length))
    }
}

const FILE_NAMER_VERSION: u32 = 2;

#[derive(Message)]
struct FileNamerWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, repeated, tag = "2")]
    partition_columns: Vec<String>,
    #[prost(uint64, optional, tag = "3")]
    random_prefix_length: Option<u64>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rstest::rstest;

    use super::*;
    use crate::expressions::column_name;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::proto::plan;
    use crate::plans::PlanBuilder;
    use crate::schema::schema_ref;

    fn input_schema() -> SchemaRef {
        schema_ref! {
            not_null "id": LONG,
            nullable "amount": LONG,
        }
    }

    fn write_with_stats(
        stats: impl FnOnce(AggregateBuilder) -> AggregateBuilder,
    ) -> KernelResult<MultiFileWrite> {
        MultiFileWrite::try_new(
            &input_schema(),
            schema_ref! { not_null "id": LONG },
            Vec::new(),
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(Vec::new(), None),
            stats,
        )
    }

    #[rstest]
    fn shared_write_and_statistics_survive_plan_wire_roundtrip(
        #[values(false, true)] with_stats: bool,
        #[values(false, true)] partitioned: bool,
    ) {
        let partition_columns = if partitioned {
            vec!["id".into()]
        } else {
            vec![]
        };
        let write = MultiFileWrite::try_new(
            &input_schema(),
            schema_ref! { not_null "id": LONG },
            partition_columns.clone(),
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(partition_columns.clone(), None),
            |a| {
                if with_stats {
                    a.aggregate_as(Agg::count_star(), "rows")
                        .aggregate_as(Agg::sum(column_name!("amount")), "total")
                } else {
                    a
                }
            },
        )
        .unwrap();
        let output_schema = write.output_schema(&input_schema()).unwrap();
        let input =
            PlanBuilder::values(input_schema(), vec![vec![1i64.into(), 2i64.into()]]).unwrap();
        let written = input.multi_file_write(write).unwrap();
        let original = PlanBuilder::union_all([written.clone(), written])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(original.nodes.len(), 3);
        assert_eq!(original.nodes[2].inputs, [1, 1]);
        let wire = plan::Plan::from(&original);
        let decoded = plan::Plan::decode(wire.encode_to_vec().as_slice()).unwrap();
        assert_eq!(wire, decoded);
        assert_eq!(decoded.nodes[2].inputs, [1, 1]);
        let plan::operator::Op::MultiFileWrite(write) =
            decoded.nodes[1].op.as_ref().unwrap().op.as_ref().unwrap()
        else {
            panic!("expected MultiFileWrite")
        };
        assert_eq!(write.partition_columns, partition_columns);
        assert_eq!(
            write.write_schema,
            Some(schema_ref! { not_null "id": LONG }.as_ref().into())
        );
        if with_stats {
            assert_eq!(write.stats.len(), 2);
            assert!(matches!(
                write.stats[0].aggregate.as_ref().unwrap().func,
                Some(plan::agg::Func::CountStar(_))
            ));
            assert_eq!(
                write.stats[1].aggregate.as_ref().unwrap(),
                &plan::Agg {
                    func: Some(plan::agg::Func::Sum(plan::SumAgg {
                        value: Some((&column_name!("amount")).into()),
                    })),
                }
            );
        } else {
            assert!(write.stats.is_empty());
            let expected_names = if partitioned {
                vec!["path", "size", "file_modification", "id"]
            } else {
                vec!["path", "size", "file_modification"]
            };
            assert_eq!(
                output_schema
                    .fields()
                    .map(|field| field.name())
                    .collect::<Vec<_>>(),
                expected_names,
            );
        }
        FileNamer::decode(&write.file_namer).unwrap();
    }

    #[rstest]
    fn empty_input_produces_no_write_node_but_retains_descriptor_schema(
        #[values(false, true)] partitioned: bool,
    ) {
        let write = MultiFileWrite::try_new(
            &input_schema(),
            schema_ref! { not_null "id": LONG },
            if partitioned {
                vec!["id".into()]
            } else {
                vec![]
            },
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(
                if partitioned {
                    vec!["id".into()]
                } else {
                    vec![]
                },
                None,
            ),
            |a| a.count_star(),
        )
        .unwrap();
        let output_schema = write.output_schema(&input_schema()).unwrap();
        let empty = PlanBuilder::values(input_schema(), vec![])
            .unwrap()
            .multi_file_write(write)
            .unwrap();
        assert!(empty.clone().build_opt().unwrap().is_none());
        let plan = empty.build().unwrap();
        assert_eq!(plan.schema, output_schema);
        assert_eq!(plan.nodes.len(), 1);
        assert!(matches!(&plan.nodes[0].op, Operator::Values(values) if values.rows.is_empty()));
    }

    #[test]
    fn attaching_to_a_different_input_revalidates_the_contract() {
        let write = write_with_stats(|a| a.sum(column_name!("amount"))).unwrap();
        let other_input = schema_ref! { not_null "id": LONG, nullable "amount": STRING };
        assert!(PlanBuilder::values(other_input, vec![])
            .unwrap()
            .multi_file_write(write)
            .is_err());
    }

    #[test]
    fn statistics_use_full_input_and_keep_declared_names_types_and_order() {
        let write = write_with_stats(|a| {
            a.aggregate_as(Agg::count_star(), "rows")
                .aggregate_as(Agg::count(column_name!("amount")), "non_null")
                .aggregate_as(Agg::sum(column_name!("amount")), "total")
        })
        .unwrap();
        assert_eq!(
            write.output_schema(&input_schema()).unwrap(),
            schema_ref! {
                not_null "path": STRING,
                not_null "size": LONG,
                not_null "file_modification": LONG,
                not_null "rows": LONG,
                not_null "non_null": LONG,
                nullable "total": LONG,
            }
        );
        assert!(write.write_schema.field("amount").is_none());
    }

    fn partitioned_input_schema() -> SchemaRef {
        schema_ref! {
            not_null "id": LONG,
            nullable "region": STRING,
            not_null "bucket": LONG,
            nullable "amount": LONG,
        }
    }

    #[rstest]
    fn all_partition_fields_and_statistics_are_independent_of_file_selection(
        #[values(false, true)] store_region: bool,
    ) {
        let input = partitioned_input_schema();
        let file_fields = if store_region {
            vec!["region", "id"]
        } else {
            vec!["id"]
        };
        let write = MultiFileWrite::try_new(
            &input,
            input.project(&file_fields).unwrap(),
            vec!["bucket".into(), "region".into()],
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(vec!["bucket".into(), "region".into()], None),
            |a| {
                a.aggregate_as(Agg::count_star(), "rows")
                    .aggregate_as(Agg::sum(column_name!("amount")), "total")
            },
        )
        .unwrap();
        let output = write.output_schema(&input).unwrap();
        assert_eq!(
            output,
            schema_ref! {
                not_null "path": STRING,
                not_null "size": LONG,
                not_null "file_modification": LONG,
                not_null "bucket": LONG,
                nullable "region": STRING,
                not_null "rows": LONG,
                nullable "total": LONG,
            }
        );
        assert_eq!(write.write_schema, input.project(&file_fields).unwrap());
        let plan = PlanBuilder::values(input, vec![])
            .unwrap()
            .multi_file_write(write)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(plan.schema, output);
    }

    #[rstest]
    #[case::missing(&["missing"])]
    #[case::duplicate(&["id", "id"])]
    #[case::case_mismatch(&["id", "ID"])]
    #[case::nested_path(&["amount.value"])]
    fn rejects_invalid_partition_columns(#[case] keys: &[&str]) {
        let result = MultiFileWrite::try_new(
            &input_schema(),
            schema_ref! { not_null "id": LONG },
            keys.iter().map(|key| (*key).into()).collect(),
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(keys.iter().map(|key| (*key).into()).collect(), None),
            |a| a,
        );
        assert!(result.is_err());
    }

    #[test]
    fn rejects_statistics_names_colliding_with_returned_partition_fields() {
        let input = partitioned_input_schema();
        assert!(MultiFileWrite::try_new(
            &input,
            input.project(&["id"]).unwrap(),
            vec!["region".into()],
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(vec!["region".into()], None),
            |a| a.aggregate_as(Agg::count_star(), "region"),
        )
        .is_err());
    }

    #[test]
    fn rejects_partition_keys_colliding_with_metadata_names() {
        let input = schema_ref! { not_null "id": LONG, nullable "size": STRING };
        assert!(MultiFileWrite::try_new(
            &input,
            input.project(&["id"]).unwrap(),
            vec!["size".into()],
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(vec!["size".into()], None),
            |a| a,
        )
        .is_err());
    }

    #[rstest]
    #[case::reserved("path")]
    #[case::reserved_case_insensitive("SIZE")]
    #[case::duplicate("rows")]
    fn rejects_colliding_statistics_names(#[case] name: &str) {
        assert!(write_with_stats(|a| {
            a.aggregate_as(Agg::count_star(), "rows")
                .aggregate_as(Agg::count_star(), name)
        })
        .is_err());
    }

    #[test]
    fn rejects_missing_statistics_operand() {
        assert!(write_with_stats(|a| a.count(column_name!("missing"))).is_err());
    }

    #[rstest]
    #[case::missing(schema_ref! { nullable "missing": LONG })]
    #[case::wrong_type(schema_ref! { not_null "id": STRING })]
    #[case::wrong_nullability(schema_ref! { nullable "id": LONG })]
    fn rejects_incompatible_write_fields(#[case] schema: SchemaRef) {
        let mut write = write_with_stats(|a| a).unwrap();
        write.write_schema = schema;
        assert!(write.output_schema(&input_schema()).is_err());
    }

    #[rstest]
    #[case("mailto:writer@example.com")]
    #[case("memory:///output")]
    #[case("memory:///output/?query=1")]
    #[case("memory:///output/#fragment")]
    fn rejects_invalid_base_url(#[case] url: &str) {
        let mut write = write_with_stats(|a| a).unwrap();
        write.base_url = Url::parse(url).unwrap();
        assert!(write.output_schema(&input_schema()).is_err());
    }

    #[test]
    fn rejects_grouped_statistics_at_construction() {
        let result = write_with_stats(|_| {
            Aggregate::group_by(input_schema(), [column_name!("id")]).count_star()
        });
        assert!(result.is_err());
    }

    #[test]
    fn rejects_sum_over_non_long_input() {
        let input = schema_ref! { nullable "amount": STRING };
        let result = MultiFileWrite::try_new(
            &input,
            Arc::clone(&input),
            Vec::new(),
            Url::parse("memory:///output/").unwrap(),
            FileNamer::new(Vec::new(), None),
            |a| a.sum(column_name!("amount")),
        );
        assert!(result.is_err());
    }

    #[rstest]
    #[case::plain("abc", "p=abc/")]
    #[case::escape("a/b%", "p=a%252Fb%2525/")]
    #[case::space("a ", "p=a%20/")]
    #[case::empty("", "p=__HIVE_DEFAULT_PARTITION__/")]
    fn file_names_use_kernel_partition_serialization_and_encoding(
        #[case] value: &str,
        #[case] directory: &str,
    ) {
        let namer = FileNamer::new(vec!["p".into()], None);
        let name = namer.next_file_name(vec![value.into()]).unwrap();
        assert!(name.starts_with(directory), "{name}");
        let null_name = namer
            .next_file_name(vec![Scalar::Null(DataType::STRING)])
            .unwrap();
        assert!(null_name.starts_with("p=__HIVE_DEFAULT_PARTITION__/"));
        assert!(namer.next_file_name(vec![]).is_err());
    }

    #[rstest]
    fn independently_decoded_namers_generate_fresh_relative_names(
        #[values(false, true)] partitioned: bool,
        #[values(None, NonZero::new(4))] random_prefix_length: Option<NonZero<usize>>,
    ) {
        let columns = if partitioned {
            vec!["p".into()]
        } else {
            vec![]
        };
        let values: Vec<Scalar> = if partitioned {
            vec!["region".into()]
        } else {
            vec![]
        };
        let namer = FileNamer::new(columns, random_prefix_length);
        let bytes = namer.encode();
        let base = Url::parse("memory:///output/").unwrap();
        let mut names = HashSet::new();
        for _ in 0..16 {
            let copy = FileNamer::decode(&bytes).unwrap();
            for policy in [&namer, &copy, &copy.clone()] {
                let name = policy.next_file_name(values.clone()).unwrap();
                let filename = name.rsplit('/').next().unwrap();
                let uuid = filename.strip_suffix(".parquet").unwrap();
                assert_eq!(Uuid::parse_str(uuid).unwrap().get_version_num(), 4);
                let directory = &name[..name.len() - filename.len()];
                if let Some(length) = random_prefix_length {
                    assert_eq!(directory.len(), length.get() + 1);
                    assert!(directory[..length.get()]
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric()));
                } else {
                    assert_eq!(directory, if partitioned { "p=region/" } else { "" });
                }
                assert!(base
                    .join(&name)
                    .unwrap()
                    .as_str()
                    .starts_with(base.as_str()));
                assert!(names.insert(name));
            }
        }
    }

    #[rstest]
    #[case::unknown_version(3, None)]
    #[case::zero_length(FILE_NAMER_VERSION, Some(0))]
    fn rejects_invalid_naming_payload(#[case] version: u32, #[case] length: Option<u64>) {
        assert!(FileNamer::decode(b"not protobuf").is_err());
        assert!(FileNamer::decode(&[]).is_err());
        let wire = FileNamerWire {
            version,
            partition_columns: vec![],
            random_prefix_length: length,
        };
        assert!(FileNamer::decode(&wire.encode_to_vec()).is_err());
    }
}
