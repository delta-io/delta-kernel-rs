//! Conversions from the kernel plan IR into the prost-generated proto wire types.
//!
//! [`operation_to_proto_bytes`] is the entry point for serializing a top-level [`Operation`].

use prost::Message;

use super::{
    expressions as proto_expr, operation as proto_op, plan as proto_plan, schema as proto_schema,
};
use crate::expressions::{
    ArrayData, BinaryExpression, BinaryExpressionOp, BinaryPredicate, BinaryPredicateOp,
    ColumnName, DecimalData, Expression, ExpressionFieldPatch, ExpressionStructPatch,
    JunctionPredicate, JunctionPredicateOp, MapData, MapToStructExpression, OpaqueExpression,
    OpaquePredicate, ParseJsonExpression, Predicate, Scalar, StructData, UnaryExpression,
    UnaryExpressionOp, UnaryPredicate, UnaryPredicateOp, VariadicExpression, VariadicExpressionOp,
};
use crate::plans::ir::nodes::{
    FileType, Filter, Load, LoadColumnFileMeta, MaxByVersion, Operator, Project, ScanFile,
    ScanJson, ScanParquet, SemiJoin, Values,
};
use crate::plans::ir::plan::{Plan, PlanNode, RefId};
use crate::plans::{IoOperation, Operation};
use crate::schema::{
    ArrayType, DataType, DecimalType, MapType, MetadataValue, PrimitiveType, StructField,
    StructType,
};
use crate::{FileMeta, FileSlice};

/// Serializes an [`Operation`] into its proto wire representation.
pub fn operation_to_proto_bytes(op: &Operation) -> Vec<u8> {
    proto_op::Operation::from(op).encode_to_vec()
}

// === Helpers ===

/// Converts each element of `items` via [`From`].
fn convert_vec<'a, T, U>(items: &'a [T]) -> Vec<U>
where
    U: From<&'a T>,
{
    items.iter().map(U::from).collect()
}

/// Converts a slice of [`Expression`] references into proto.
fn convert_expr_vec<E>(items: &[E]) -> Vec<proto_expr::Expression>
where
    E: AsRef<Expression>,
{
    items.iter().map(|e| e.as_ref().into()).collect()
}

// === Operation / IoOperation ===

impl From<&Operation> for proto_op::Operation {
    fn from(op: &Operation) -> Self {
        let op = match op {
            Operation::IoOperation(io) => proto_op::operation::Op::Io(io.into()),
            Operation::QueryPlan(plan) => proto_op::operation::Op::QueryPlan(plan.into()),
        };
        proto_op::Operation { op: Some(op) }
    }
}

impl From<&IoOperation> for proto_op::IoOperation {
    fn from(io: &IoOperation) -> Self {
        use proto_op::io_operation::Op;
        let op = match io {
            IoOperation::FileListing { url } => Op::FileListing(proto_op::FileListing {
                url: url.to_string(),
            }),
            IoOperation::ReadBytes { files } => Op::ReadBytes(proto_op::ReadBytes {
                files: convert_vec(files),
            }),
            IoOperation::WriteBytes {
                url,
                data,
                overwrite,
            } => Op::WriteBytes(proto_op::WriteBytes {
                url: url.to_string(),
                data: data.to_vec(),
                overwrite: *overwrite,
            }),
            IoOperation::HeadFile { url } => Op::HeadFile(proto_op::HeadFile {
                url: url.to_string(),
            }),
            IoOperation::AtomicCopy {
                source,
                destination,
            } => Op::AtomicCopy(proto_op::AtomicCopy {
                source: source.to_string(),
                destination: destination.to_string(),
            }),
            IoOperation::ParquetFooter { file } => Op::ParquetFooter(proto_op::ParquetFooter {
                file: Some(file.into()),
            }),
        };
        proto_op::IoOperation { op: Some(op) }
    }
}

impl From<&FileSlice> for proto_op::FileSlice {
    fn from(slice: &FileSlice) -> Self {
        let (url, range) = slice;
        proto_op::FileSlice {
            url: url.to_string(),
            range_start: range.as_ref().map(|r| r.start),
            range_end: range.as_ref().map(|r| r.end),
        }
    }
}

impl From<&FileMeta> for proto_plan::FileMeta {
    fn from(meta: &FileMeta) -> Self {
        proto_plan::FileMeta {
            location: meta.location.to_string(),
            size: meta.size,
            last_modified: Some(meta.last_modified),
        }
    }
}

// === Plan / nodes ===

impl From<&Plan> for proto_plan::Plan {
    fn from(plan: &Plan) -> Self {
        proto_plan::Plan {
            nodes: convert_vec(&plan.nodes),
        }
    }
}

impl From<&PlanNode> for proto_plan::PlanNode {
    fn from(node: &PlanNode) -> Self {
        proto_plan::PlanNode {
            op: Some((&node.op).into()),
            inputs: convert_vec(&node.inputs),
            output: Some((&node.output).into()),
        }
    }
}

impl From<&RefId> for proto_plan::RefId {
    fn from(ref_id: &RefId) -> Self {
        proto_plan::RefId { id: ref_id.0 }
    }
}

impl From<&Operator> for proto_plan::Operator {
    fn from(op: &Operator) -> Self {
        use proto_plan::operator::Op;
        let op = match op {
            Operator::ScanParquet(n) => Op::ScanParquet(n.into()),
            Operator::ScanJson(n) => Op::ScanJson(n.into()),
            Operator::Values(n) => Op::Values(n.into()),
            Operator::Project(n) => Op::Project(n.into()),
            Operator::Filter(n) => Op::Filter(n.into()),
            Operator::Load(n) => Op::Load(n.into()),
            Operator::MaxByVersion(n) => Op::MaxByVersion(n.into()),
            Operator::SemiJoin(n) => Op::SemiJoin(n.into()),
            Operator::UnionAll(_) => Op::UnionAll(proto_plan::UnionAllNode {}),
        };
        proto_plan::Operator { op: Some(op) }
    }
}

impl From<&ScanFile> for proto_plan::ScanFile {
    fn from(file: &ScanFile) -> Self {
        proto_plan::ScanFile {
            meta: Some((&file.meta).into()),
            file_constants: convert_vec(&file.file_constants),
        }
    }
}

impl From<&ScanParquet> for proto_plan::ScanParquetNode {
    fn from(node: &ScanParquet) -> Self {
        proto_plan::ScanParquetNode {
            files: convert_vec(&node.files),
            file_constant_columns: convert_vec(&node.file_constant_columns),
            schema: Some(node.schema.as_ref().into()),
        }
    }
}

impl From<&ScanJson> for proto_plan::ScanJsonNode {
    fn from(node: &ScanJson) -> Self {
        proto_plan::ScanJsonNode {
            files: convert_vec(&node.files),
            file_constant_columns: convert_vec(&node.file_constant_columns),
            schema: Some(node.schema.as_ref().into()),
        }
    }
}

impl From<&Values> for proto_plan::ValuesNode {
    fn from(node: &Values) -> Self {
        let rows = node
            .rows
            .iter()
            .map(|row| proto_plan::ValuesRow {
                values: convert_vec(row),
            })
            .collect();
        proto_plan::ValuesNode {
            schema: Some(node.schema.as_ref().into()),
            rows,
        }
    }
}

impl From<&Project> for proto_plan::ProjectNode {
    fn from(node: &Project) -> Self {
        proto_plan::ProjectNode {
            expr: Some(node.expr.as_ref().into()),
            schema: Some(node.schema.as_ref().into()),
        }
    }
}

impl From<&Filter> for proto_plan::FilterNode {
    fn from(node: &Filter) -> Self {
        proto_plan::FilterNode {
            predicate: Some(node.predicate.as_ref().into()),
        }
    }
}

impl From<&Load> for proto_plan::LoadNode {
    fn from(node: &Load) -> Self {
        proto_plan::LoadNode {
            schema: Some(node.schema.as_ref().into()),
            file_type: proto_plan::FileType::from(node.file_type) as i32,
            base_url: node.base_url.as_ref().map(ToString::to_string),
            file_constant_columns: convert_vec(&node.file_constant_columns),
            file_meta: Some((&node.file_meta).into()),
            dv_column: Some((&node.dv_column).into()),
        }
    }
}

impl From<&LoadColumnFileMeta> for proto_plan::LoadColumnFileMeta {
    fn from(meta: &LoadColumnFileMeta) -> Self {
        proto_plan::LoadColumnFileMeta {
            path_column: Some((&meta.path_column).into()),
            file_size_column: Some((&meta.file_size_column).into()),
            num_records_column: Some((&meta.num_records_column).into()),
        }
    }
}

impl From<&MaxByVersion> for proto_plan::MaxByVersionNode {
    fn from(node: &MaxByVersion) -> Self {
        proto_plan::MaxByVersionNode {
            group_by: convert_expr_vec(&node.group_by),
            version_column: Some((&node.version_column).into()),
            schema: Some(node.schema.as_ref().into()),
        }
    }
}

impl From<&SemiJoin> for proto_plan::SemiJoinNode {
    fn from(node: &SemiJoin) -> Self {
        proto_plan::SemiJoinNode {
            inverted: node.inverted,
            probe_keys: convert_vec(&node.probe_keys),
            build_keys: convert_vec(&node.build_keys),
        }
    }
}

impl From<FileType> for proto_plan::FileType {
    fn from(file_type: FileType) -> Self {
        match file_type {
            FileType::Parquet => proto_plan::FileType::Parquet,
            FileType::Json => proto_plan::FileType::Json,
        }
    }
}

// === Expressions ===

impl From<&Expression> for proto_expr::Expression {
    fn from(expr: &Expression) -> Self {
        use proto_expr::expression::Kind;
        let kind = match expr {
            Expression::Literal(scalar) => Kind::Literal(scalar.into()),
            Expression::Column(column) => Kind::Column(column.into()),
            Expression::Predicate(pred) => Kind::Predicate(Box::new(pred.as_ref().into())),
            Expression::Struct(exprs, nullability) => {
                let nullability_predicate =
                    nullability.as_ref().map(|n| Box::new(n.as_ref().into()));
                Kind::StructExpr(Box::new(proto_expr::StructExpression {
                    exprs: convert_expr_vec(exprs),
                    nullability_predicate,
                }))
            }
            Expression::StructPatch(patch) => Kind::Transform(patch.into()),
            Expression::Unary(unary) => Kind::Unary(Box::new(unary.into())),
            Expression::Binary(binary) => Kind::Binary(Box::new(binary.into())),
            Expression::Variadic(variadic) => Kind::Variadic(variadic.into()),
            Expression::Opaque(opaque) => Kind::Opaque(opaque.into()),
            Expression::Unknown(name) => Kind::Unknown(name.clone()),
            Expression::ParseJson(parse_json) => Kind::ParseJson(Box::new(parse_json.into())),
            Expression::MapToStruct(map_to_struct) => {
                Kind::MapToStruct(Box::new(map_to_struct.into()))
            }
        };
        proto_expr::Expression { kind: Some(kind) }
    }
}

impl From<&Predicate> for proto_expr::Predicate {
    fn from(pred: &Predicate) -> Self {
        use proto_expr::predicate::Kind;
        let kind = match pred {
            Predicate::BooleanExpression(expr) => Kind::BooleanExpression(Box::new(expr.into())),
            Predicate::Not(inner) => Kind::Not(Box::new(inner.as_ref().into())),
            Predicate::Unary(unary) => Kind::Unary(Box::new(unary.into())),
            Predicate::Binary(binary) => Kind::Binary(Box::new(binary.into())),
            Predicate::Junction(junction) => Kind::Junction(junction.into()),
            Predicate::Opaque(opaque) => Kind::Opaque(opaque.into()),
            Predicate::Unknown(name) => Kind::Unknown(name.clone()),
        };
        proto_expr::Predicate { kind: Some(kind) }
    }
}

impl From<&ColumnName> for proto_expr::ColumnName {
    fn from(column: &ColumnName) -> Self {
        proto_expr::ColumnName {
            path: column.path().to_vec(),
        }
    }
}

impl From<&UnaryExpression> for proto_expr::UnaryExpression {
    fn from(unary: &UnaryExpression) -> Self {
        proto_expr::UnaryExpression {
            op: proto_expr::UnaryExpressionOp::from(unary.op) as i32,
            expr: Some(Box::new(unary.expr.as_ref().into())),
        }
    }
}

impl From<&BinaryExpression> for proto_expr::BinaryExpression {
    fn from(binary: &BinaryExpression) -> Self {
        proto_expr::BinaryExpression {
            op: proto_expr::BinaryExpressionOp::from(binary.op) as i32,
            left: Some(Box::new(binary.left.as_ref().into())),
            right: Some(Box::new(binary.right.as_ref().into())),
        }
    }
}

impl From<&VariadicExpression> for proto_expr::VariadicExpression {
    fn from(variadic: &VariadicExpression) -> Self {
        proto_expr::VariadicExpression {
            op: proto_expr::VariadicExpressionOp::from(variadic.op) as i32,
            exprs: convert_vec(&variadic.exprs),
        }
    }
}

impl From<&OpaqueExpression> for proto_expr::OpaqueExpression {
    fn from(opaque: &OpaqueExpression) -> Self {
        proto_expr::OpaqueExpression {
            name: opaque.op.name().to_string(),
            exprs: convert_vec(&opaque.exprs),
        }
    }
}

impl From<&ParseJsonExpression> for proto_expr::ParseJsonExpression {
    fn from(parse_json: &ParseJsonExpression) -> Self {
        proto_expr::ParseJsonExpression {
            json_expr: Some(Box::new(parse_json.json_expr.as_ref().into())),
            output_schema: Some(parse_json.output_schema.as_ref().into()),
        }
    }
}

impl From<&MapToStructExpression> for proto_expr::MapToStructExpression {
    fn from(map_to_struct: &MapToStructExpression) -> Self {
        proto_expr::MapToStructExpression {
            map_expr: Some(Box::new(map_to_struct.map_expr.as_ref().into())),
        }
    }
}

impl From<&UnaryPredicate> for proto_expr::UnaryPredicate {
    fn from(unary: &UnaryPredicate) -> Self {
        proto_expr::UnaryPredicate {
            op: proto_expr::UnaryPredicateOp::from(unary.op) as i32,
            expr: Some(Box::new(unary.expr.as_ref().into())),
        }
    }
}

impl From<&BinaryPredicate> for proto_expr::BinaryPredicate {
    fn from(binary: &BinaryPredicate) -> Self {
        proto_expr::BinaryPredicate {
            op: proto_expr::BinaryPredicateOp::from(binary.op) as i32,
            left: Some(Box::new(binary.left.as_ref().into())),
            right: Some(Box::new(binary.right.as_ref().into())),
        }
    }
}

impl From<&JunctionPredicate> for proto_expr::JunctionPredicate {
    fn from(junction: &JunctionPredicate) -> Self {
        proto_expr::JunctionPredicate {
            op: proto_expr::JunctionPredicateOp::from(junction.op) as i32,
            preds: convert_vec(&junction.preds),
        }
    }
}

impl From<&OpaquePredicate> for proto_expr::OpaquePredicate {
    fn from(opaque: &OpaquePredicate) -> Self {
        proto_expr::OpaquePredicate {
            name: opaque.op.name().to_string(),
            exprs: convert_vec(&opaque.exprs),
        }
    }
}

// The proto `Transform` struct mirrors `ExpressionStructPatch`
impl From<&ExpressionStructPatch> for proto_expr::Transform {
    fn from(patch: &ExpressionStructPatch) -> Self {
        let field_transforms = patch
            .field_patches
            .iter()
            .map(|(name, field_patch)| (name.clone(), field_patch.into()))
            .collect();
        proto_expr::Transform {
            input_path: patch.input_path.as_ref().map(Into::into),
            field_transforms,
            prepended_fields: convert_expr_vec(&patch.prepended_fields),
            appended_fields: convert_expr_vec(&patch.appended_fields),
        }
    }
}

// The proto `FieldTransform` struct mirrors `ExpressionFieldPatch`.
impl From<&ExpressionFieldPatch> for proto_expr::FieldTransform {
    fn from(field_patch: &ExpressionFieldPatch) -> Self {
        proto_expr::FieldTransform {
            exprs: convert_expr_vec(&field_patch.insertions),
            is_replace: !field_patch.keep_input,
            optional: field_patch.optional,
        }
    }
}

impl From<UnaryExpressionOp> for proto_expr::UnaryExpressionOp {
    fn from(op: UnaryExpressionOp) -> Self {
        match op {
            UnaryExpressionOp::ToJson => proto_expr::UnaryExpressionOp::ToJson,
        }
    }
}

impl From<BinaryExpressionOp> for proto_expr::BinaryExpressionOp {
    fn from(op: BinaryExpressionOp) -> Self {
        match op {
            BinaryExpressionOp::Plus => proto_expr::BinaryExpressionOp::Plus,
            BinaryExpressionOp::Minus => proto_expr::BinaryExpressionOp::Minus,
            BinaryExpressionOp::Multiply => proto_expr::BinaryExpressionOp::Multiply,
            BinaryExpressionOp::Divide => proto_expr::BinaryExpressionOp::Divide,
        }
    }
}

impl From<VariadicExpressionOp> for proto_expr::VariadicExpressionOp {
    fn from(op: VariadicExpressionOp) -> Self {
        match op {
            VariadicExpressionOp::Coalesce => proto_expr::VariadicExpressionOp::Coalesce,
            VariadicExpressionOp::Array => proto_expr::VariadicExpressionOp::Array,
        }
    }
}

impl From<UnaryPredicateOp> for proto_expr::UnaryPredicateOp {
    fn from(op: UnaryPredicateOp) -> Self {
        match op {
            UnaryPredicateOp::IsNull => proto_expr::UnaryPredicateOp::IsNull,
        }
    }
}

impl From<BinaryPredicateOp> for proto_expr::BinaryPredicateOp {
    fn from(op: BinaryPredicateOp) -> Self {
        match op {
            BinaryPredicateOp::LessThan => proto_expr::BinaryPredicateOp::LessThan,
            BinaryPredicateOp::GreaterThan => proto_expr::BinaryPredicateOp::GreaterThan,
            BinaryPredicateOp::Equal => proto_expr::BinaryPredicateOp::Equal,
            BinaryPredicateOp::Distinct => proto_expr::BinaryPredicateOp::Distinct,
            BinaryPredicateOp::In => proto_expr::BinaryPredicateOp::In,
        }
    }
}

impl From<JunctionPredicateOp> for proto_expr::JunctionPredicateOp {
    fn from(op: JunctionPredicateOp) -> Self {
        match op {
            JunctionPredicateOp::And => proto_expr::JunctionPredicateOp::And,
            JunctionPredicateOp::Or => proto_expr::JunctionPredicateOp::Or,
        }
    }
}

// === Scalars ===

impl From<&Scalar> for proto_expr::Scalar {
    fn from(scalar: &Scalar) -> Self {
        use proto_expr::scalar::Value;
        let value = match scalar {
            Scalar::Integer(v) => Value::Integer(*v),
            Scalar::Long(v) => Value::Long(*v),
            Scalar::Short(v) => Value::Short(*v as i32),
            Scalar::Byte(v) => Value::Byte(*v as i32),
            Scalar::Float(v) => Value::Float(*v),
            Scalar::Double(v) => Value::Double(*v),
            Scalar::String(v) => Value::String(v.clone()),
            Scalar::Boolean(v) => Value::Boolean(*v),
            Scalar::Timestamp(v) => Value::Timestamp(*v),
            Scalar::TimestampNtz(v) => Value::TimestampNtz(*v),
            Scalar::Date(v) => Value::Date(*v),
            Scalar::Binary(v) => Value::Binary(v.clone()),
            Scalar::Decimal(decimal) => Value::Decimal(decimal.into()),
            Scalar::Null(data_type) => Value::Null(data_type.into()),
            Scalar::Struct(struct_data) => Value::Struct(struct_data.into()),
            Scalar::Array(array_data) => Value::Array(array_data.into()),
            Scalar::Map(map_data) => Value::Map(map_data.into()),
        };
        proto_expr::Scalar { value: Some(value) }
    }
}

impl From<&DecimalData> for proto_expr::DecimalData {
    fn from(decimal: &DecimalData) -> Self {
        proto_expr::DecimalData {
            bits: decimal.bits().to_be_bytes().to_vec(),
            decimal_type: Some((*decimal.ty()).into()),
        }
    }
}

impl From<&StructData> for proto_expr::StructData {
    fn from(struct_data: &StructData) -> Self {
        proto_expr::StructData {
            fields: convert_vec(struct_data.fields()),
            values: convert_vec(struct_data.values()),
        }
    }
}

impl From<&ArrayData> for proto_expr::ArrayData {
    fn from(array_data: &ArrayData) -> Self {
        proto_expr::ArrayData {
            array_type: Some(array_data.array_type().into()),
            elements: convert_vec(array_data.array_elements()),
        }
    }
}

impl From<&MapData> for proto_expr::MapData {
    fn from(map_data: &MapData) -> Self {
        let pairs = map_data
            .pairs()
            .iter()
            .map(|(key, value)| proto_expr::MapEntry {
                key: Some(key.into()),
                value: Some(value.into()),
            })
            .collect();
        proto_expr::MapData {
            map_type: Some(map_data.map_type().into()),
            pairs,
        }
    }
}

// === Schema ===

impl From<&DataType> for proto_schema::DataType {
    fn from(data_type: &DataType) -> Self {
        use proto_schema::data_type::Kind;
        let kind = match data_type {
            DataType::Primitive(primitive) => Kind::Primitive(primitive.into()),
            DataType::Array(array) => Kind::Array(Box::new(array.as_ref().into())),
            DataType::Struct(struct_type) => Kind::Struct(struct_type.as_ref().into()),
            DataType::Map(map) => Kind::Map(Box::new(map.as_ref().into())),
            // The proto `VariantType` is intentionally empty: variants are opaque on the wire.
            DataType::Variant(_) => Kind::Variant(proto_schema::VariantType {}),
        };
        proto_schema::DataType { kind: Some(kind) }
    }
}

impl From<&PrimitiveType> for proto_schema::PrimitiveType {
    fn from(primitive: &PrimitiveType) -> Self {
        use proto_schema::primitive_type::Kind;
        use proto_schema::SimplePrimitiveType as Simple;
        let kind = match primitive {
            PrimitiveType::String => Kind::Simple(Simple::String as i32),
            PrimitiveType::Long => Kind::Simple(Simple::Long as i32),
            PrimitiveType::Integer => Kind::Simple(Simple::Integer as i32),
            PrimitiveType::Short => Kind::Simple(Simple::Short as i32),
            PrimitiveType::Byte => Kind::Simple(Simple::Byte as i32),
            PrimitiveType::Float => Kind::Simple(Simple::Float as i32),
            PrimitiveType::Double => Kind::Simple(Simple::Double as i32),
            PrimitiveType::Boolean => Kind::Simple(Simple::Boolean as i32),
            PrimitiveType::Binary => Kind::Simple(Simple::Binary as i32),
            PrimitiveType::Date => Kind::Simple(Simple::Date as i32),
            PrimitiveType::Timestamp => Kind::Simple(Simple::Timestamp as i32),
            PrimitiveType::TimestampNtz => Kind::Simple(Simple::TimestampNtz as i32),
            PrimitiveType::Decimal(decimal) => Kind::Decimal((*decimal).into()),
            PrimitiveType::Void => Kind::Simple(Simple::Void as i32),
        };
        proto_schema::PrimitiveType { kind: Some(kind) }
    }
}

impl From<DecimalType> for proto_schema::DecimalType {
    fn from(decimal: DecimalType) -> Self {
        proto_schema::DecimalType {
            precision: u32::from(decimal.precision()),
            scale: u32::from(decimal.scale()),
        }
    }
}

impl From<&ArrayType> for proto_schema::ArrayType {
    fn from(array: &ArrayType) -> Self {
        proto_schema::ArrayType {
            element_type: Some(Box::new(array.element_type().into())),
            contains_null: array.contains_null(),
        }
    }
}

impl From<&MapType> for proto_schema::MapType {
    fn from(map: &MapType) -> Self {
        proto_schema::MapType {
            key_type: Some(Box::new(map.key_type().into())),
            value_type: Some(Box::new(map.value_type().into())),
            value_contains_null: map.value_contains_null(),
        }
    }
}

impl From<&StructType> for proto_schema::StructType {
    fn from(struct_type: &StructType) -> Self {
        proto_schema::StructType {
            fields: struct_type.fields().map(Into::into).collect(),
        }
    }
}

impl From<&StructField> for proto_schema::StructField {
    fn from(field: &StructField) -> Self {
        let metadata = field
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.into()))
            .collect();
        proto_schema::StructField {
            name: field.name.clone(),
            data_type: Some((&field.data_type).into()),
            nullable: field.nullable,
            metadata,
        }
    }
}

impl From<&MetadataValue> for proto_schema::MetadataValue {
    fn from(metadata: &MetadataValue) -> Self {
        use proto_schema::metadata_value::Value;
        let value = match metadata {
            MetadataValue::Number(n) => Value::Number(*n),
            MetadataValue::String(s) => Value::String(s.clone()),
            MetadataValue::Boolean(b) => Value::Boolean(*b),
            MetadataValue::Other(json) => Value::OtherJson(json.to_string()),
        };
        proto_schema::MetadataValue { value: Some(value) }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rstest::rstest;
    use url::Url;

    use super::operation_to_proto_bytes;
    use crate::expressions::{
        lit, ArrayData, BinaryExpressionOp, ColumnName, DecimalData, Expression,
        ExpressionStructPatchBuilder, MapData, Predicate, Scalar, StructData, UnaryExpressionOp,
    };
    use crate::plans::ir::nodes::{
        FileType, Filter, Load, LoadColumnFileMeta, MaxByVersion, Operator, Project, ScanFile,
        ScanJson, ScanParquet, SemiJoin, UnionAll, Values,
    };
    use crate::plans::ir::plan::{Plan, PlanNode, RefId};
    use crate::plans::proto::{
        expressions as proto_expr, operation as proto_op, plan as proto_plan,
    };
    use crate::plans::{IoOperation, Operation};
    use crate::schema::{
        ArrayType, DataType, DecimalType, MapType, SchemaRef, StructField, StructType,
    };
    use crate::FileMeta;

    fn sample_file_meta() -> FileMeta {
        FileMeta {
            location: Url::parse("memory:///table/part-0.parquet").unwrap(),
            last_modified: 123,
            size: 456,
        }
    }

    fn sample_schema() -> SchemaRef {
        Arc::new(StructType::try_new(vec![StructField::nullable("id", DataType::INTEGER)]).unwrap())
    }

    fn decode(op: &Operation) -> proto_op::Operation {
        let bytes = operation_to_proto_bytes(op);
        prost::Message::decode(bytes.as_slice()).expect("decode succeeds")
    }

    fn io_op(op: &Operation) -> proto_op::io_operation::Op {
        let Some(proto_op::operation::Op::Io(io)) = decode(op).op else {
            panic!("expected an IoOperation");
        };
        io.op.expect("io op present")
    }

    // === IoOperation variants ===

    #[test]
    fn io_file_listing_round_trips() {
        let url = "memory:///table/";
        let op = Operation::IoOperation(IoOperation::file_listing(Url::parse(url).unwrap()));
        let proto_op::io_operation::Op::FileListing(file_listing) = io_op(&op) else {
            panic!("expected FileListing");
        };
        assert_eq!(file_listing.url, url);
    }

    #[test]
    fn io_read_bytes_round_trips() {
        let files = vec![
            (Url::parse("memory:///a").unwrap(), Some(0u64..10u64)),
            (Url::parse("memory:///b").unwrap(), None),
        ];
        let op = Operation::IoOperation(IoOperation::read_bytes(files));
        let proto_op::io_operation::Op::ReadBytes(read_bytes) = io_op(&op) else {
            panic!("expected ReadBytes");
        };
        assert_eq!(read_bytes.files.len(), 2);
        assert_eq!(read_bytes.files[0].url, "memory:///a");
        assert_eq!(read_bytes.files[0].range_start, Some(0));
        assert_eq!(read_bytes.files[0].range_end, Some(10));
        assert_eq!(read_bytes.files[1].range_start, None);
        assert_eq!(read_bytes.files[1].range_end, None);
    }

    #[test]
    fn io_write_bytes_round_trips() {
        let op = Operation::IoOperation(IoOperation::write_bytes(
            Url::parse("memory:///out").unwrap(),
            Bytes::from_static(b"hello"),
            true,
        ));
        let proto_op::io_operation::Op::WriteBytes(write_bytes) = io_op(&op) else {
            panic!("expected WriteBytes");
        };
        assert_eq!(write_bytes.url, "memory:///out");
        assert_eq!(write_bytes.data, b"hello");
        assert!(write_bytes.overwrite);
    }

    #[test]
    fn io_atomic_copy_round_trips() {
        let op = Operation::IoOperation(IoOperation::atomic_copy(
            Url::parse("memory:///src").unwrap(),
            Url::parse("memory:///dst").unwrap(),
        ));
        let proto_op::io_operation::Op::AtomicCopy(atomic_copy) = io_op(&op) else {
            panic!("expected AtomicCopy");
        };
        assert_eq!(atomic_copy.source, "memory:///src");
        assert_eq!(atomic_copy.destination, "memory:///dst");
    }

    #[test]
    fn io_head_file_round_trips() {
        let op = Operation::IoOperation(IoOperation::head_file(
            Url::parse("memory:///head").unwrap(),
        ));
        let proto_op::io_operation::Op::HeadFile(head_file) = io_op(&op) else {
            panic!("expected HeadFile");
        };
        assert_eq!(head_file.url, "memory:///head");
    }

    #[test]
    fn io_parquet_footer_round_trips() {
        let op = Operation::IoOperation(IoOperation::parquet_footer(sample_file_meta()));
        let proto_op::io_operation::Op::ParquetFooter(parquet_footer) = io_op(&op) else {
            panic!("expected ParquetFooter");
        };
        let file = parquet_footer.file.expect("file meta present");
        assert_eq!(file.location, "memory:///table/part-0.parquet");
        assert_eq!(file.size, 456);
        assert_eq!(file.last_modified, Some(123));
    }

    // === Multi-node plan ===

    #[test]
    fn query_plan_two_nodes_round_trips() {
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("id", DataType::INTEGER),
                StructField::not_null("name", DataType::STRING),
            ])
            .unwrap(),
        );
        let plan = Plan {
            nodes: vec![
                PlanNode {
                    op: Operator::ScanParquet(ScanParquet {
                        files: vec![ScanFile::new(sample_file_meta())],
                        file_constant_columns: vec![],
                        schema: schema.clone(),
                    }),
                    inputs: vec![],
                    output: RefId(0),
                },
                PlanNode {
                    op: Operator::Filter(Filter {
                        predicate: Arc::new(Predicate::gt(
                            Expression::Column(ColumnName::new(["id"])),
                            lit(5i32),
                        )),
                    }),
                    inputs: vec![RefId(0)],
                    output: RefId(1),
                },
            ],
        };

        let Some(proto_op::operation::Op::QueryPlan(plan)) = decode(&Operation::QueryPlan(plan)).op
        else {
            panic!("expected a QueryPlan");
        };
        assert_eq!(plan.nodes.len(), 2);

        // Source node: ScanParquet, output RefId(0), no inputs.
        let source = &plan.nodes[0];
        assert_eq!(source.output.as_ref().unwrap().id, 0);
        assert!(source.inputs.is_empty());
        let Some(proto_plan::operator::Op::ScanParquet(scan)) = &source.op.as_ref().unwrap().op
        else {
            panic!("expected ScanParquet");
        };
        assert_eq!(scan.files.len(), 1);
        assert_eq!(scan.schema.as_ref().unwrap().fields.len(), 2);

        // Filter node: consumes RefId(0), emits RefId(1), carries a `id > 5` binary predicate.
        let filter_node = &plan.nodes[1];
        assert_eq!(filter_node.output.as_ref().unwrap().id, 1);
        assert_eq!(filter_node.inputs.len(), 1);
        assert_eq!(filter_node.inputs[0].id, 0);
        let Some(proto_plan::operator::Op::Filter(filter)) = &filter_node.op.as_ref().unwrap().op
        else {
            panic!("expected Filter");
        };
        let predicate = filter.predicate.as_ref().unwrap();
        let Some(proto_expr::predicate::Kind::Binary(binary)) = &predicate.kind else {
            panic!("expected a binary predicate");
        };
        assert_eq!(binary.op, proto_expr::BinaryPredicateOp::GreaterThan as i32);
        assert!(matches!(
            binary.left.as_ref().unwrap().kind,
            Some(proto_expr::expression::Kind::Column(_))
        ));
    }

    // === Operator variants ===

    fn operator_kind(op: &Operator) -> &'static str {
        use proto_plan::operator::Op;
        match proto_plan::Operator::from(op).op.unwrap() {
            Op::ScanParquet(_) => "scan_parquet",
            Op::ScanJson(_) => "scan_json",
            Op::Values(_) => "values",
            Op::Project(_) => "project",
            Op::Filter(_) => "filter",
            Op::Load(_) => "load",
            Op::MaxByVersion(_) => "max_by_version",
            Op::SemiJoin(_) => "semi_join",
            Op::UnionAll(_) => "union_all",
        }
    }

    fn load_column_file_meta() -> LoadColumnFileMeta {
        LoadColumnFileMeta {
            path_column: ColumnName::new(["path"]),
            file_size_column: ColumnName::new(["size"]),
            num_records_column: ColumnName::new(["num_records"]),
        }
    }

    #[rstest]
    #[case(
        Operator::ScanParquet(ScanParquet {
            files: vec![],
            file_constant_columns: vec![],
            schema: sample_schema(),
        }),
        "scan_parquet"
    )]
    #[case(
        Operator::ScanJson(ScanJson {
            files: vec![],
            file_constant_columns: vec![],
            schema: sample_schema(),
        }),
        "scan_json"
    )]
    #[case(Operator::Values(Values { schema: sample_schema(), rows: vec![] }), "values")]
    #[case(
        Operator::Project(Project {
            expr: Arc::new(Expression::struct_from([lit(1)])),
            schema: sample_schema(),
        }),
        "project"
    )]
    #[case(Operator::Filter(Filter { predicate: Arc::new(Predicate::literal(true)) }), "filter")]
    #[case(
        Operator::Load(Load {
            schema: sample_schema(),
            file_type: FileType::Parquet,
            base_url: None,
            file_constant_columns: vec![],
            file_meta: load_column_file_meta(),
            dv_column: ColumnName::new(["dv"]),
        }),
        "load"
    )]
    #[case(
        Operator::MaxByVersion(MaxByVersion {
            group_by: vec![],
            version_column: ColumnName::new(["version"]),
            schema: sample_schema(),
        }),
        "max_by_version"
    )]
    #[case(
        Operator::SemiJoin(SemiJoin { inverted: false, probe_keys: vec![], build_keys: vec![] }),
        "semi_join"
    )]
    #[case(Operator::UnionAll(UnionAll), "union_all")]
    fn operator_kind_maps(#[case] op: Operator, #[case] expected: &str) {
        assert_eq!(operator_kind(&op), expected);
    }

    // === Expression variants ===

    fn expr_kind(expr: Expression) -> &'static str {
        use proto_expr::expression::Kind;
        match proto_expr::Expression::from(&expr).kind.unwrap() {
            Kind::Literal(_) => "literal",
            Kind::Column(_) => "column",
            Kind::Predicate(_) => "predicate",
            Kind::StructExpr(_) => "struct_expr",
            Kind::Transform(_) => "transform",
            Kind::Unary(_) => "unary",
            Kind::Binary(_) => "binary",
            Kind::Variadic(_) => "variadic",
            Kind::IfExpr(_) => "if_expr",
            Kind::Opaque(_) => "opaque",
            Kind::ParseJson(_) => "parse_json",
            Kind::MapToStruct(_) => "map_to_struct",
            Kind::Unknown(_) => "unknown",
        }
    }

    #[rstest]
    #[case(lit(1), "literal")]
    #[case(Expression::Column(ColumnName::new(["a"])), "column")]
    #[case(Expression::Predicate(Box::new(Predicate::literal(true))), "predicate")]
    #[case(Expression::struct_from([lit(1)]), "struct_expr")]
    #[case(
        Expression::StructPatch(
            ExpressionStructPatchBuilder::new().append(lit(1)).build().unwrap(),
        ),
        "transform"
    )]
    #[case(Expression::unary(UnaryExpressionOp::ToJson, lit(1)), "unary")]
    #[case(Expression::binary(BinaryExpressionOp::Plus, lit(1), lit(2)), "binary")]
    #[case(Expression::coalesce([lit(1), lit(2)]), "variadic")]
    #[case(Expression::parse_json(lit("{}"), sample_schema()), "parse_json")]
    #[case(Expression::map_to_struct(Expression::Column(ColumnName::new(["m"]))), "map_to_struct")]
    #[case(Expression::unknown("x"), "unknown")]
    fn expression_kind_maps(#[case] expr: Expression, #[case] expected: &str) {
        assert_eq!(expr_kind(expr), expected);
    }

    #[test]
    fn struct_patch_maps_keep_input_to_is_replace_and_preserves_appended() {
        let patch = ExpressionStructPatchBuilder::new()
            .replace("a", lit(1))
            .insert_after("b", lit(2))
            .append(lit(3))
            .build()
            .unwrap();

        let transform = proto_expr::Transform::from(&patch);

        // `replace` drops the input field, so `is_replace` is true.
        assert!(transform.field_transforms["a"].is_replace);
        // `insert_after` keeps the input field, so `is_replace` is false.
        assert!(!transform.field_transforms["b"].is_replace);
        assert_eq!(transform.appended_fields.len(), 1);
    }

    // === Predicate variants ===

    fn pred_kind(pred: Predicate) -> &'static str {
        use proto_expr::predicate::Kind;
        match proto_expr::Predicate::from(&pred).kind.unwrap() {
            Kind::BooleanExpression(_) => "boolean_expression",
            Kind::Not(_) => "not",
            Kind::Unary(_) => "unary",
            Kind::Binary(_) => "binary",
            Kind::Junction(_) => "junction",
            Kind::Opaque(_) => "opaque",
            Kind::Unknown(_) => "unknown",
        }
    }

    #[rstest]
    #[case(Predicate::literal(true), "boolean_expression")]
    #[case(Predicate::not(Predicate::literal(true)), "not")]
    #[case(Predicate::is_null(lit(1)), "unary")]
    #[case(Predicate::gt(lit(1), lit(2)), "binary")]
    #[case(
        Predicate::and(Predicate::literal(true), Predicate::literal(false)),
        "junction"
    )]
    #[case(Predicate::Unknown("x".to_string()), "unknown")]
    fn predicate_kind_maps(#[case] pred: Predicate, #[case] expected: &str) {
        assert_eq!(pred_kind(pred), expected);
    }

    // === Scalar variants ===

    fn scalar_kind(scalar: Scalar) -> &'static str {
        use proto_expr::scalar::Value;
        match proto_expr::Scalar::from(&scalar).value.unwrap() {
            Value::Integer(_) => "integer",
            Value::Long(_) => "long",
            Value::Short(_) => "short",
            Value::Byte(_) => "byte",
            Value::Float(_) => "float",
            Value::Double(_) => "double",
            Value::String(_) => "string",
            Value::Boolean(_) => "boolean",
            Value::Timestamp(_) => "timestamp",
            Value::TimestampNtz(_) => "timestamp_ntz",
            Value::Date(_) => "date",
            Value::Binary(_) => "binary",
            Value::Decimal(_) => "decimal",
            Value::Null(_) => "null",
            Value::Struct(_) => "struct",
            Value::Array(_) => "array",
            Value::Map(_) => "map",
        }
    }

    #[rstest]
    #[case(Scalar::Integer(7), "integer")]
    #[case(Scalar::Long(7), "long")]
    #[case(Scalar::Short(7), "short")]
    #[case(Scalar::Byte(7), "byte")]
    #[case(Scalar::Float(1.5), "float")]
    #[case(Scalar::Double(1.5), "double")]
    #[case(Scalar::String("hi".into()), "string")]
    #[case(Scalar::Boolean(true), "boolean")]
    #[case(Scalar::Timestamp(9), "timestamp")]
    #[case(Scalar::TimestampNtz(9), "timestamp_ntz")]
    #[case(Scalar::Date(9), "date")]
    #[case(Scalar::Binary(vec![1, 2, 3]), "binary")]
    #[case(
        Scalar::Decimal(
            DecimalData::try_new(1234i128, DecimalType::try_new(10, 2).unwrap()).unwrap(),
        ),
        "decimal"
    )]
    #[case(Scalar::Null(DataType::LONG), "null")]
    #[case(
        Scalar::Struct(
            StructData::try_new(
                vec![StructField::nullable("a", DataType::INTEGER)],
                vec![Scalar::Integer(1)],
            )
            .unwrap(),
        ),
        "struct"
    )]
    #[case(
        Scalar::Array(
            ArrayData::try_new(ArrayType::new(DataType::INTEGER, false), [1, 2, 3]).unwrap(),
        ),
        "array"
    )]
    #[case(
        Scalar::Map(
            MapData::try_new(MapType::new(DataType::STRING, DataType::INTEGER, false), [("k", 1)])
                .unwrap(),
        ),
        "map"
    )]
    fn scalar_variant_maps(#[case] scalar: Scalar, #[case] expected: &str) {
        assert_eq!(scalar_kind(scalar), expected);
    }
}
