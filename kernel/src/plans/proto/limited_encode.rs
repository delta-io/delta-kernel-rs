//! Borrowed IR encoding: no owned protobuf mirror is built before the fallible wire buffer.

use std::io::Write as _;

use bytes::Bytes;

use super::{expressions as expr_proto, plan as plan_proto, schema as schema_proto};
use crate::expressions::{Expression, Predicate, Scalar, UnaryExpressionOp};
use crate::memory::{LimitedBytes, MemoryBudget, MemoryError};
use crate::plans::ir::nodes::{Agg, Operator, ScanFile};
use crate::plans::{IoOperation, Operation};
use crate::schema::{
    ArrayType, ColumnName, DataType, MapType, MetadataValue, StructField, StructType,
};
use crate::{FileMeta, KernelResult};

pub(crate) fn encode(operation: &Operation, budget: &MemoryBudget) -> KernelResult<Bytes> {
    let mut output = Encoder {
        bytes: LimitedBytes::new(budget),
    };
    match operation {
        Operation::IoOperation(io) => output.message(1, |out| out.io(io))?,
        Operation::QueryPlan(plan) => output.message(2, |out| {
            for node in &plan.nodes {
                out.message(1, |out| {
                    out.message(1, |out| out.operator(&node.op))?;
                    for &input in &node.inputs {
                        // Unpacked repeated integers are wire-compatible with prost's packed form.
                        out.integer(2, input as u32 as u64)?;
                    }
                    Ok(())
                })?;
            }
            Ok(())
        })?,
    }
    Ok(Bytes::from_owner(output.bytes))
}

struct Encoder {
    bytes: LimitedBytes,
}

impl Encoder {
    // Reserve a maximal length prefix, then compact it after encoding the child in place. All
    // temporary prefix space and buffer growth is covered by the same allocator as the result.
    fn message(
        &mut self,
        tag: u32,
        body: impl FnOnce(&mut Self) -> KernelResult<()>,
    ) -> KernelResult<()> {
        self.varint(u64::from(tag) << 3 | 2)?;
        let prefix = self.bytes.len();
        self.bytes.try_extend_from_slice(&[0; 10])?;
        body(self)?;
        let end = self.bytes.len();
        let mut encoded = [0; 10];
        let count = varint_bytes((end - prefix - 10) as u64, &mut encoded);
        self.bytes.as_mut_slice()[prefix..prefix + count].copy_from_slice(&encoded[..count]);
        self.bytes
            .as_mut_slice()
            .copy_within(prefix + 10..end, prefix + count);
        self.bytes.truncate(end - (10 - count));
        Ok(())
    }

    fn varint(&mut self, value: u64) -> KernelResult<()> {
        let mut encoded = [0; 10];
        let count = varint_bytes(value, &mut encoded);
        self.bytes.try_extend_from_slice(&encoded[..count])?;
        Ok(())
    }

    fn integer(&mut self, tag: u32, value: u64) -> KernelResult<()> {
        self.varint(u64::from(tag) << 3)?;
        self.varint(value)
    }

    fn boolean(&mut self, tag: u32, value: bool) -> KernelResult<()> {
        if value {
            self.integer(tag, 1)?;
        }
        Ok(())
    }

    fn data(&mut self, tag: u32, value: &[u8]) -> KernelResult<()> {
        self.varint(u64::from(tag) << 3 | 2)?;
        self.varint(value.len() as u64)?;
        self.bytes.try_extend_from_slice(value)?;
        Ok(())
    }

    fn text(&mut self, tag: u32, value: &str) -> KernelResult<()> {
        self.data(tag, value.as_bytes())
    }

    fn io(&mut self, io: &IoOperation) -> KernelResult<()> {
        match io {
            IoOperation::FileListing { url, end } => self.message(1, |out| {
                out.text(1, url.as_str())?;
                if let Some(end) = end {
                    out.text(2, end.as_str())?;
                }
                Ok(())
            }),
            IoOperation::ReadBytes { files } => self.message(2, |out| {
                for (url, range) in files {
                    out.message(1, |out| {
                        out.text(1, url.as_str())?;
                        if let Some(range) = range {
                            out.integer(2, range.start)?;
                            out.integer(3, range.end)?;
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            }),
            IoOperation::WriteBytes {
                url,
                data,
                overwrite,
            } => self.message(3, |out| {
                out.text(1, url.as_str())?;
                out.data(2, data)?;
                out.boolean(3, *overwrite)
            }),
            IoOperation::HeadFile { url } => self.message(4, |out| out.text(1, url.as_str())),
            IoOperation::AtomicCopy {
                source,
                destination,
            } => self.message(5, |out| {
                out.text(1, source.as_str())?;
                out.text(2, destination.as_str())
            }),
            IoOperation::ParquetFooter { file } => {
                self.message(6, |out| out.message(1, |out| out.file(file)))
            }
        }
    }

    fn file(&mut self, file: &FileMeta) -> KernelResult<()> {
        self.text(1, file.location.as_str())?;
        self.integer(2, file.size)?;
        self.integer(3, file.last_modified as u64)
    }

    fn operator(&mut self, operator: &Operator) -> KernelResult<()> {
        match operator {
            Operator::ScanParquet(node) => self.message(1, |out| {
                out.scan(&node.files, &node.file_constant_columns, &node.schema)
            }),
            Operator::ScanJson(node) => self.message(2, |out| {
                out.scan(&node.files, &node.file_constant_columns, &node.schema)
            }),
            Operator::Values(node) => self.message(3, |out| {
                out.message(1, |out| out.schema(&node.schema))?;
                for row in &node.rows {
                    out.message(2, |out| {
                        for value in row {
                            out.message(1, |out| out.scalar(value))?;
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            }),
            Operator::Project(node) => self.message(4, |out| {
                out.message(1, |out| out.expression(&node.expr))?;
                out.message(2, |out| out.schema(&node.schema))
            }),
            Operator::Filter(node) => self.message(5, |out| {
                out.message(1, |out| out.predicate(&node.predicate))
            }),
            Operator::UnionAll(_) => self.message(6, |_| Ok(())),
            Operator::DynamicScan(node) => self.message(7, |out| {
                out.message(1, |out| out.schema(&node.schema))?;
                out.integer(2, plan_proto::FileType::from(node.file_type) as u64)?;
                out.text(3, node.base_url.as_str())?;
                for name in &node.file_constant_columns {
                    out.text(4, name)?;
                }
                out.message(5, |out| out.column(&node.path_column))?;
                out.message(6, |out| out.column(&node.file_size_column))?;
                out.message(7, |out| out.column(&node.last_modified_column))?;
                if let Some(column) = &node.dv_column {
                    out.message(8, |out| out.column(column))?;
                }
                Ok(())
            }),
            Operator::Aggregate(node) => self.message(8, |out| {
                for column in &node.group_by {
                    out.message(1, |out| out.column(column))?;
                }
                for agg in &node.aggs {
                    out.message(2, |out| out.agg(agg))?;
                }
                out.message(3, |out| out.schema(&node.schema))
            }),
            Operator::SemiJoin(node) => self.message(9, |out| {
                out.boolean(1, node.inverted)?;
                for column in &node.probe_keys {
                    out.message(2, |out| out.column(column))?;
                }
                for column in &node.build_keys {
                    out.message(3, |out| out.column(column))?;
                }
                Ok(())
            }),
            Operator::RelationSource(node) => self.message(10, |out| {
                out.text(1, node.id())?;
                out.message(2, |out| out.schema(node.schema()))
            }),
            Operator::WriteJson(node) => self.message(11, |out| {
                out.text(1, node.file_path.as_str())?;
                out.message(2, |out| out.schema(&node.schema))?;
                out.boolean(3, node.overwrite)?;
                out.boolean(4, node.ordered)
            }),
            Operator::PrefixSum(node) => self.message(12, |out| {
                out.message(1, |out| out.column(&node.input))?;
                out.text(2, &node.output)?;
                out.message(3, |out| out.schema(&node.schema))
            }),
        }
    }

    fn scan(
        &mut self,
        files: &[ScanFile],
        columns: &[String],
        schema: &StructType,
    ) -> KernelResult<()> {
        for file in files {
            self.message(1, |out| {
                out.message(1, |out| out.file(&file.meta))?;
                for value in &file.file_constants {
                    out.message(2, |out| out.scalar(value))?;
                }
                Ok(())
            })?;
        }
        for column in columns {
            self.text(2, column)?;
        }
        self.message(3, |out| out.schema(schema))
    }

    fn agg(&mut self, agg: &Agg) -> KernelResult<()> {
        let (tag, value) = match agg {
            Agg::Min(value) => (1, value),
            Agg::Max(value) => (2, value),
            Agg::Sum(value) => (5, value),
            Agg::Count(value) => (6, value),
            Agg::FirstNonNull(value) => (8, value),
            Agg::CountStar => return self.message(7, |_| Ok(())),
            Agg::MinNonNullBy(operands) | Agg::MaxNonNullBy(operands) => {
                let tag = if matches!(agg, Agg::MinNonNullBy(_)) {
                    3
                } else {
                    4
                };
                return self.message(tag, |out| {
                    out.message(1, |out| out.column(&operands.value))?;
                    out.message(2, |out| out.column(&operands.null_sentinel))?;
                    out.message(3, |out| out.column(&operands.key))
                });
            }
        };
        self.message(tag, |out| out.message(1, |out| out.column(value)))
    }

    fn column(&mut self, column: &ColumnName) -> KernelResult<()> {
        for part in column.path() {
            self.text(1, part)?;
        }
        Ok(())
    }

    fn expression(&mut self, expression: &Expression) -> KernelResult<()> {
        stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || {
            self.expression_inner(expression)
        })
    }

    fn expression_inner(&mut self, expression: &Expression) -> KernelResult<()> {
        match expression {
            Expression::Literal(value) => self.message(1, |out| out.scalar(value)),
            Expression::Column(column) => self.message(2, |out| out.column(column)),
            Expression::Predicate(predicate) => self.message(3, |out| out.predicate(predicate)),
            Expression::Struct(exprs, nullability) => self.message(4, |out| {
                for expr in exprs {
                    out.message(1, |out| out.expression(expr))?;
                }
                if let Some(expr) = nullability {
                    out.message(2, |out| out.expression(expr))?;
                }
                Ok(())
            }),
            Expression::StructPatch(patch) => self.message(5, |out| {
                if let Some(column) = &patch.input_path {
                    out.message(1, |out| out.column(column))?;
                }
                for (name, patch) in &patch.field_patches {
                    out.message(2, |out| {
                        out.text(1, name)?;
                        out.message(2, |out| {
                            for expr in &patch.insertions {
                                out.message(1, |out| out.expression(expr))?;
                            }
                            out.boolean(2, !patch.keep_input)?;
                            out.boolean(3, patch.optional)
                        })
                    })?;
                }
                for expr in &patch.prepended_fields {
                    out.message(3, |out| out.expression(expr))?;
                }
                for expr in &patch.appended_fields {
                    out.message(4, |out| out.expression(expr))?;
                }
                Ok(())
            }),
            Expression::Unary(node) => self.message(6, |out| {
                out.integer(1, expr_proto::UnaryExpressionOp::from(node.op) as u64)?;
                out.message(2, |out| out.expression(&node.expr))?;
                out.boolean(
                    3,
                    matches!(
                        node.op,
                        UnaryExpressionOp::ToJson {
                            full_precision_timestamps: true
                        }
                    ),
                )
            }),
            Expression::Binary(node) => self.message(7, |out| {
                out.integer(1, expr_proto::BinaryExpressionOp::from(node.op) as u64)?;
                out.message(2, |out| out.expression(&node.left))?;
                out.message(3, |out| out.expression(&node.right))
            }),
            Expression::Variadic(node) => self.message(8, |out| {
                out.integer(1, expr_proto::VariadicExpressionOp::from(node.op) as u64)?;
                for expr in &node.exprs {
                    out.message(2, |out| out.expression(expr))?;
                }
                Ok(())
            }),
            Expression::CaseWhen(node) => self.message(9, |out| {
                for (predicate, expr) in &node.branches {
                    out.message(1, |out| {
                        out.message(1, |out| out.predicate(predicate))?;
                        out.message(2, |out| out.expression(expr))
                    })?;
                }
                out.message(2, |out| out.expression(&node.else_expr))
            }),
            Expression::Opaque(node) => self.message(10, |out| {
                out.text(1, node.op.name())?;
                for expr in &node.exprs {
                    out.message(2, |out| out.expression(expr))?;
                }
                Ok(())
            }),
            Expression::ParseJson(node) => self.message(11, |out| {
                out.message(1, |out| out.expression(&node.json_expr))?;
                out.message(2, |out| out.schema(&node.output_schema))
            }),
            Expression::MapToStruct(node) => self.message(12, |out| {
                out.message(1, |out| out.expression(&node.map_expr))?;
                if !node.options.is_default() {
                    out.message(2, |out| {
                        if let Some(timezone) = node.options.timestamp_timezone() {
                            out.text(1, timezone)?;
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            }),
            Expression::Unknown(name) => self.text(13, name),
            Expression::Cast(node) => self.message(13, |out| {
                let mut writer = FormatOutput { out, failure: None };
                write!(&mut writer, "cast_to_{}", node.target)
                    .map_err(|_| writer.failure.unwrap_or(MemoryError::Serialization))?;
                Ok(())
            }),
        }
    }

    fn predicate(&mut self, predicate: &Predicate) -> KernelResult<()> {
        stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || match predicate {
            Predicate::BooleanExpression(expr) => self.message(1, |out| out.expression(expr)),
            Predicate::Not(predicate) => self.message(2, |out| out.predicate(predicate)),
            Predicate::Unary(node) => self.message(3, |out| {
                out.integer(1, expr_proto::UnaryPredicateOp::from(node.op) as u64)?;
                out.message(2, |out| out.expression(&node.expr))
            }),
            Predicate::Binary(node) => self.message(4, |out| {
                out.integer(1, expr_proto::BinaryPredicateOp::from(node.op) as u64)?;
                out.message(2, |out| out.expression(&node.left))?;
                out.message(3, |out| out.expression(&node.right))
            }),
            Predicate::Junction(node) => self.message(5, |out| {
                out.integer(1, expr_proto::JunctionPredicateOp::from(node.op) as u64)?;
                for predicate in &node.preds {
                    out.message(2, |out| out.predicate(predicate))?;
                }
                Ok(())
            }),
            Predicate::Opaque(node) => self.message(6, |out| {
                out.text(1, node.op.name())?;
                for expr in &node.exprs {
                    out.message(2, |out| out.expression(expr))?;
                }
                Ok(())
            }),
            Predicate::Unknown(name) => self.text(7, name),
        })
    }

    fn scalar(&mut self, scalar: &Scalar) -> KernelResult<()> {
        stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || match scalar {
            Scalar::Integer(value) => self.integer(1, *value as u64),
            Scalar::Long(value) => self.integer(2, *value as u64),
            Scalar::Short(value) => self.integer(3, *value as u64),
            Scalar::Byte(value) => self.integer(4, *value as u64),
            Scalar::Float(value) => {
                self.varint(5 << 3 | 5)?;
                self.bytes.try_extend_from_slice(&value.to_le_bytes())?;
                Ok(())
            }
            Scalar::Double(value) => {
                self.varint(6 << 3 | 1)?;
                self.bytes.try_extend_from_slice(&value.to_le_bytes())?;
                Ok(())
            }
            Scalar::String(value) => self.text(7, value),
            Scalar::Boolean(value) => self.integer(8, u64::from(*value)),
            Scalar::Timestamp(value) => self.integer(9, *value as u64),
            Scalar::TimestampNtz(value) => self.integer(10, *value as u64),
            Scalar::Date(value) => self.integer(11, *value as u64),
            Scalar::Binary(value) => self.data(12, value),
            Scalar::Decimal(value) => self.message(13, |out| {
                out.data(1, &value.bits().to_be_bytes())?;
                out.message(2, |out| {
                    out.integer(1, u64::from(value.ty().precision()))?;
                    out.integer(2, u64::from(value.ty().scale()))
                })
            }),
            Scalar::Null(data_type) => self.message(14, |out| out.data_type(data_type)),
            Scalar::Struct(value) => self.message(15, |out| {
                for field in value.fields() {
                    out.message(1, |out| out.field(field))?;
                }
                for value in value.values() {
                    out.message(2, |out| out.scalar(value))?;
                }
                Ok(())
            }),
            Scalar::Array(value) => self.message(16, |out| {
                out.message(1, |out| out.array_type(value.array_type()))?;
                for value in value.array_elements() {
                    out.message(2, |out| out.scalar(value))?;
                }
                Ok(())
            }),
            Scalar::Map(value) => self.message(17, |out| {
                out.message(1, |out| out.map_type(value.map_type()))?;
                for (key, value) in value.pairs() {
                    out.message(2, |out| {
                        out.message(1, |out| out.scalar(key))?;
                        out.message(2, |out| out.scalar(value))
                    })?;
                }
                Ok(())
            }),
            Scalar::IntervalYearMonth(value) => self.integer(18, value.months() as u64),
            Scalar::IntervalDayTime(value) => self.integer(19, value.micros() as u64),
        })
    }

    fn schema(&mut self, schema: &StructType) -> KernelResult<()> {
        for field in schema.fields() {
            self.message(1, |out| out.field(field))?;
        }
        Ok(())
    }

    fn field(&mut self, field: &StructField) -> KernelResult<()> {
        self.text(1, &field.name)?;
        self.message(2, |out| out.data_type(&field.data_type))?;
        self.boolean(3, field.nullable)?;
        for (key, value) in &field.metadata {
            self.message(4, |out| {
                out.text(1, key.as_ref())?;
                out.message(2, |out| match value {
                    MetadataValue::Number(value) => out.integer(1, *value as u64),
                    MetadataValue::String(value) => out.text(2, value),
                    MetadataValue::LimitedString(value) => out.text(2, value.as_str()),
                    MetadataValue::Boolean(value) => out.integer(3, u64::from(*value)),
                    MetadataValue::Other(value) => {
                        let value = crate::memory::try_to_json(out.bytes.budget(), value)?;
                        out.data(4, &value)
                    }
                })
            })?;
        }
        Ok(())
    }

    fn data_type(&mut self, data_type: &DataType) -> KernelResult<()> {
        stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || match data_type {
            DataType::Primitive(value) => {
                // Primitive conversion has no schema-sized allocation. Geography CRS text is
                // emitted directly so even unusually long annotations do not require a copy.
                #[cfg(feature = "geo-type-in-dev")]
                match value {
                    crate::schema::PrimitiveType::Geometry(value) => {
                        return self
                            .message(1, |out| out.message(3, |out| out.text(1, value.crs())));
                    }
                    crate::schema::PrimitiveType::Geography(value) => {
                        return self.message(1, |out| {
                            out.message(4, |out| {
                                out.text(1, value.crs())?;
                                out.integer(
                                    2,
                                    schema_proto::EdgeInterpolationAlgorithm::from(
                                        value.algorithm(),
                                    ) as u64,
                                )
                            })
                        });
                    }
                    _ => {}
                }
                let value = schema_proto::PrimitiveType::from(value);
                self.message(1, |out| match value.kind {
                    Some(schema_proto::primitive_type::Kind::Simple(value)) => {
                        out.integer(1, value as u64)
                    }
                    Some(schema_proto::primitive_type::Kind::Decimal(value)) => {
                        out.message(2, |out| {
                            out.integer(1, u64::from(value.precision))?;
                            out.integer(2, u64::from(value.scale))
                        })
                    }
                    _ => Err(MemoryError::Serialization.into()),
                })
            }
            DataType::Array(value) => self.message(2, |out| out.array_type(value)),
            DataType::Struct(value) => self.message(3, |out| out.schema(value)),
            DataType::Map(value) => self.message(4, |out| out.map_type(value)),
            DataType::Variant(_) => self.message(5, |_| Ok(())),
            #[cfg(feature = "udt-in-dev")]
            DataType::UserDefined(value) => self.message(6, |out| {
                out.message(1, |out| out.data_type(value.sql_type()))?;
                for (key, value) in value.annotation() {
                    out.message(2, |out| {
                        out.text(1, key)?;
                        out.message(2, |out| {
                            if let Some(value) = value {
                                out.text(1, value)?;
                            }
                            Ok(())
                        })
                    })?;
                }
                Ok(())
            }),
            DataType::File(_) => self.message(7, |_| Ok(())),
        })
    }

    fn array_type(&mut self, value: &ArrayType) -> KernelResult<()> {
        self.message(1, |out| out.data_type(value.element_type()))?;
        self.boolean(2, value.contains_null())
    }

    fn map_type(&mut self, value: &MapType) -> KernelResult<()> {
        self.message(1, |out| out.data_type(value.key_type()))?;
        self.message(2, |out| out.data_type(value.value_type()))?;
        self.boolean(3, value.value_contains_null())
    }
}

fn varint_bytes(mut value: u64, output: &mut [u8; 10]) -> usize {
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = value as u8;
        value >>= 7;
        if value == 0 {
            return index + 1;
        }
        *byte |= 0x80;
    }
    10
}

struct FormatOutput<'a> {
    out: &'a mut Encoder,
    failure: Option<MemoryError>,
}

impl std::io::Write for FormatOutput<'_> {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        self.out
            .bytes
            .try_extend_from_slice(input)
            .map_err(|error| {
                self.failure = Some(error);
                std::io::Error::from(std::io::ErrorKind::OutOfMemory)
            })?;
        Ok(input.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(super) mod equivalence {
    use prost::Message;

    use super::*;

    macro_rules! check {
        ($name:ident, $input:ty, $proto:ty, $method:ident) => {
            pub(crate) fn $name(value: &$input) {
                let expected = <$proto>::from(value);
                let budget = MemoryBudget::new(64 * 1024 * 1024);
                let mut encoder = Encoder {
                    bytes: LimitedBytes::new(&budget),
                };
                encoder.$method(value).unwrap();
                let actual = <$proto>::decode(&*encoder.bytes).unwrap();
                assert_eq!(actual, expected);
                drop(encoder);
                assert_eq!(budget.usage().reserved, 0);
            }
        };
    }
    check!(expression, Expression, expr_proto::Expression, expression);
    check!(predicate, Predicate, expr_proto::Predicate, predicate);
    check!(scalar, Scalar, expr_proto::Scalar, scalar);
    check!(data_type, DataType, schema_proto::DataType, data_type);
    check!(schema, StructType, schema_proto::StructType, schema);
    check!(field, StructField, schema_proto::StructField, field);
    check!(operator, Operator, plan_proto::Operator, operator);
    check!(agg, Agg, plan_proto::Agg, agg);
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use url::Url;

    use super::*;
    use crate::plans::proto::operation as proto;

    #[test]
    fn rejected_encoding_releases_partial_output_and_wire_keeps_charge_across_threads() {
        let operation = Operation::IoOperation(IoOperation::write_bytes(
            Url::parse("memory:///out").unwrap(),
            Bytes::from(vec![42; 4096]),
            true,
        ));
        let insufficient = MemoryBudget::new(512);
        let error = encode(&operation, &insufficient).unwrap_err();
        assert!(matches!(
            error.without_backtrace(),
            crate::KernelError::Memory(_)
        ));
        assert_eq!(insufficient.usage().reserved, 0);
        assert!(insufficient.usage().rejected > 0);

        let budget = MemoryBudget::new(64 * 1024);
        let wire = encode(&operation, &budget).unwrap();
        let expected = proto::Operation::from(&operation);
        assert_eq!(proto::Operation::decode(wire.as_ref()).unwrap(), expected);
        drop(operation);
        let other = wire.clone();
        drop(wire);
        assert!(budget.usage().reserved >= other.len());
        std::thread::spawn(move || drop(other)).join().unwrap();
        assert_eq!(budget.usage().reserved, 0);
    }
}
