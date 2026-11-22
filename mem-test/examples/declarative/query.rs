use std::sync::Arc;

use delta_kernel::expressions::{
    ArrayData, BinaryPredicateOp, ColumnName, Expression, Predicate, PredicateRef, Scalar, lit,
};
use delta_kernel::scan::{PartitionValuesOptions, ScanBuilder, StatsOptions};
use delta_kernel::schema::{ArrayType, DataType, StructType};
use delta_kernel::{KernelError, KernelResult, SnapshotRef};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    #[default]
    None,
    Range,
    And,
    Or,
    In,
    Nulls,
    Nested,
    String,
    Partition,
    ConstantFalse,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stats {
    None,
    Json,
    #[default]
    Struct,
    Both,
    Selected,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Query {
    pub filter: Filter,
    pub terms: usize,
    pub literal_bytes: usize,
    pub projection: usize,
    pub stats: Stats,
    pub stats_projection: usize,
    pub parsed_partitions: bool,
    pub relations: usize,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            filter: Filter::None,
            terms: 1,
            literal_bytes: 16,
            projection: 0,
            stats: Stats::Struct,
            stats_projection: 8,
            parsed_partitions: false,
            relations: 1,
        }
    }
}

impl Query {
    pub fn from_env() -> KernelResult<Self> {
        if let Ok(value) = std::env::var("KERNEL_MEMORY_QUERY") {
            return serde_json::from_str(&value).map_err(KernelError::from);
        }
        let stats = match std::env::var("KERNEL_DECLARATIVE_STATS").as_deref() {
            Ok("json") => Stats::Json,
            Ok("both") => Stats::Both,
            _ => Stats::Struct,
        };
        let relations = std::env::var("KERNEL_DECLARATIVE_RELATIONS")
            .ok()
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| KernelError::generic("invalid relation count"))?
            .unwrap_or(1);
        Ok(Self {
            stats,
            relations,
            ..Self::default()
        })
    }

    pub fn builder(&self, snapshot: SnapshotRef) -> KernelResult<ScanBuilder> {
        if self.terms == 0 || self.terms > 100_000 {
            return Err(KernelError::generic(
                "predicate terms must be in 1..=100000",
            ));
        }
        let schema = snapshot.schema();
        let mut columns = Columns::default();
        if !matches!(
            self.filter,
            Filter::None | Filter::ConstantFalse | Filter::Partition
        ) || matches!(self.stats, Stats::Selected)
        {
            columns.visit(schema.as_ref(), &mut Vec::new());
        }
        let stats = match self.stats {
            Stats::None => StatsOptions::none(),
            Stats::Json => StatsOptions::json_only(),
            Stats::Struct => StatsOptions::all_struct(),
            Stats::Both => StatsOptions::all(),
            Stats::Selected => StatsOptions::struct_columns(
                columns
                    .numeric
                    .iter()
                    .chain(&columns.strings)
                    .take(self.stats_projection)
                    .cloned()
                    .collect(),
            ),
        };
        let predicate = self.predicate(&columns)?;
        let mut builder = snapshot
            .scan_builder()
            .with_stats(stats)
            .with_predicate(predicate);
        if self.projection != 0 {
            let names: Vec<_> = schema
                .fields()
                .take(self.projection)
                .map(|f| f.name())
                .collect();
            builder = builder.with_schema(schema.project(&names)?);
        }
        if self.parsed_partitions {
            builder = builder.with_partition_values(PartitionValuesOptions::with_struct());
        }
        Ok(builder)
    }

    fn predicate(&self, columns: &Columns) -> KernelResult<Option<PredicateRef>> {
        if matches!(self.filter, Filter::None) {
            return Ok(None);
        }
        if matches!(self.filter, Filter::ConstantFalse) {
            return Ok(Some(Arc::new(Predicate::FALSE)));
        }
        if matches!(self.filter, Filter::Partition) {
            return Ok(Some(Arc::new(Predicate::eq(
                Expression::column(["p"]),
                lit(1i64),
            ))));
        }
        let numeric = |index: usize| -> KernelResult<Expression> {
            let column = columns
                .numeric
                .get(index % columns.numeric.len().max(1))
                .ok_or_else(|| KernelError::generic("fixture has no numeric filter column"))?;
            Ok(Expression::column(column.clone()))
        };
        let predicate = match self.filter {
            Filter::Range => Predicate::and(
                Predicate::gt(numeric(0)?, lit(10i64)),
                Predicate::lt(numeric(0)?, lit(90_000i64)),
            ),
            Filter::In => Predicate::binary(
                BinaryPredicateOp::In,
                numeric(0)?,
                Expression::Literal(Scalar::Array(ArrayData::try_new(
                    ArrayType::new(DataType::LONG, false),
                    (0..self.terms).map(|i| i as i64),
                )?)),
            ),
            Filter::And | Filter::Or | Filter::Nulls => {
                let terms = (0..self.terms)
                    .map(|i| {
                        Ok(if matches!(self.filter, Filter::Nulls) {
                            Predicate::is_not_null(numeric(i)?)
                        } else {
                            Predicate::eq(numeric(i)?, lit(i as i64))
                        })
                    })
                    .collect::<KernelResult<Vec<_>>>()?;
                if matches!(self.filter, Filter::Or) {
                    Predicate::or_from(terms)
                } else {
                    Predicate::and_from(terms)
                }
            }
            Filter::Nested => {
                if self.terms > 64 {
                    return Err(KernelError::generic("nested predicate depth exceeds 64"));
                }
                let mut predicate = Predicate::eq(numeric(0)?, lit(0i64));
                for i in 1..self.terms {
                    let leaf = Predicate::eq(numeric(i)?, lit(i as i64));
                    predicate = if i % 2 == 0 {
                        Predicate::and(predicate, leaf)
                    } else {
                        Predicate::or(predicate, leaf)
                    };
                }
                predicate
            }
            Filter::String => {
                let column = columns
                    .strings
                    .first()
                    .ok_or_else(|| KernelError::generic("fixture has no string filter column"))?;
                Predicate::eq(
                    Expression::column(column.clone()),
                    lit("q".repeat(self.literal_bytes)),
                )
            }
            _ => return Err(KernelError::generic("unsupported filter configuration")),
        };
        Ok(Some(Arc::new(predicate)))
    }
}

#[derive(Default)]
struct Columns {
    numeric: Vec<ColumnName>,
    strings: Vec<ColumnName>,
}

impl Columns {
    fn visit(&mut self, schema: &StructType, prefix: &mut Vec<String>) {
        for field in schema.fields().filter(|f| f.name() != "p") {
            prefix.push(field.name().clone());
            match field.data_type() {
                DataType::Struct(inner) => self.visit(inner, prefix),
                value if *value == DataType::LONG => {
                    self.numeric.push(ColumnName::new(prefix.clone()))
                }
                value if *value == DataType::STRING => {
                    self.strings.push(ColumnName::new(prefix.clone()))
                }
                _ => {}
            }
            prefix.pop();
        }
    }
}
