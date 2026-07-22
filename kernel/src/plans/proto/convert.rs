//! Conversions from the kernel plan IR into the prost-generated proto wire types.

use super::plan::agg as proto_agg;
use super::schema::data_type::Kind as DataTypeKind;
use super::schema::metadata_value::Value as MetadataValueKind;
use super::schema::primitive_type::Kind as PrimitiveTypeKind;
#[cfg(feature = "geo-type-in-dev")]
use super::schema::EdgeInterpolationAlgorithm as EdgeAlgo;
use super::schema::SimplePrimitiveType as Simple;
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
    Agg, Aggregate, FileType, Filter, Load, LoadColumnFileMeta, Operator, Project, ScanFile,
    ScanJson, ScanParquet, SemiJoin, Values,
};
use crate::plans::ir::plan::{Plan, PlanNode};
use crate::plans::{IoOperation, Operation};
use crate::schema::{
    ArrayType, DataType, DecimalType, MapType, MetadataValue, PrimitiveType, StructField,
    StructType,
};
#[cfg(feature = "geo-type-in-dev")]
use crate::schema::{EdgeInterpolationAlgorithm, GeographyType, GeometryType};
use crate::{DeltaResult, Error, FileMeta, FileSlice};

// === Helpers ===

/// Converts each element of `items` into proto via [`From`].
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

// === Operation / IoOperation to Proto ===

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
            last_modified: meta.last_modified,
        }
    }
}

// === Plan / nodes to Proto ===

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
            inputs: node.inputs.iter().map(|&i| i as u32).collect(),
        }
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
            Operator::Aggregate(n) => Op::Aggregate(n.into()),
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
            file_constant_columns: node.file_constant_columns.clone(),
            schema: Some(node.schema.as_ref().into()),
        }
    }
}

impl From<&ScanJson> for proto_plan::ScanJsonNode {
    fn from(node: &ScanJson) -> Self {
        proto_plan::ScanJsonNode {
            files: convert_vec(&node.files),
            file_constant_columns: node.file_constant_columns.clone(),
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
            file_constant_columns: node.file_constant_columns.clone(),
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

impl From<&Aggregate> for proto_plan::AggregateNode {
    fn from(node: &Aggregate) -> Self {
        proto_plan::AggregateNode {
            group_by: convert_vec(&node.group_by),
            aggs: convert_vec(&node.aggs),
            schema: Some(node.schema.as_ref().into()),
        }
    }
}

impl From<&Agg> for proto_plan::Agg {
    fn from(agg: &Agg) -> Self {
        let func = match agg {
            Agg::Min { value } => proto_agg::Func::Min(proto_plan::MinAgg {
                value: Some(value.into()),
            }),
            Agg::Max { value } => proto_agg::Func::Max(proto_plan::MaxAgg {
                value: Some(value.into()),
            }),
            Agg::MinNonNullBy { value, key } => {
                proto_agg::Func::MinNonNullBy(proto_plan::MinNonNullByAgg {
                    value: Some(value.into()),
                    key: Some(key.into()),
                })
            }
            Agg::MaxNonNullBy { value, key } => {
                proto_agg::Func::MaxNonNullBy(proto_plan::MaxNonNullByAgg {
                    value: Some(value.into()),
                    key: Some(key.into()),
                })
            }
        };
        proto_plan::Agg { func: Some(func) }
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

// === Expressions to Proto ===

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

// The proto `Transform` struct mirrors `ExpressionStructPatch`.
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

// === Scalars to Proto ===

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

// === Schema to Proto ===

impl From<&DataType> for proto_schema::DataType {
    fn from(data_type: &DataType) -> Self {
        let kind = match data_type {
            DataType::Primitive(primitive) => DataTypeKind::Primitive(primitive.into()),
            DataType::Array(array) => DataTypeKind::Array(Box::new(array.as_ref().into())),
            DataType::Struct(struct_type) => DataTypeKind::Struct(struct_type.as_ref().into()),
            DataType::Map(map) => DataTypeKind::Map(Box::new(map.as_ref().into())),
            // The proto `VariantType` is intentionally empty: variants are opaque on the wire.
            DataType::Variant(_) => DataTypeKind::Variant(proto_schema::VariantType {}),
        };
        proto_schema::DataType { kind: Some(kind) }
    }
}

impl From<&PrimitiveType> for proto_schema::PrimitiveType {
    fn from(primitive: &PrimitiveType) -> Self {
        let kind = match primitive {
            PrimitiveType::String => PrimitiveTypeKind::Simple(Simple::String as i32),
            PrimitiveType::Long => PrimitiveTypeKind::Simple(Simple::Long as i32),
            PrimitiveType::Integer => PrimitiveTypeKind::Simple(Simple::Integer as i32),
            PrimitiveType::Short => PrimitiveTypeKind::Simple(Simple::Short as i32),
            PrimitiveType::Byte => PrimitiveTypeKind::Simple(Simple::Byte as i32),
            PrimitiveType::Float => PrimitiveTypeKind::Simple(Simple::Float as i32),
            PrimitiveType::Double => PrimitiveTypeKind::Simple(Simple::Double as i32),
            PrimitiveType::Boolean => PrimitiveTypeKind::Simple(Simple::Boolean as i32),
            PrimitiveType::Binary => PrimitiveTypeKind::Simple(Simple::Binary as i32),
            PrimitiveType::Date => PrimitiveTypeKind::Simple(Simple::Date as i32),
            PrimitiveType::Timestamp => PrimitiveTypeKind::Simple(Simple::Timestamp as i32),
            PrimitiveType::TimestampNtz => PrimitiveTypeKind::Simple(Simple::TimestampNtz as i32),
            PrimitiveType::Decimal(decimal) => PrimitiveTypeKind::Decimal((*decimal).into()),
            #[cfg(feature = "geo-type-in-dev")]
            PrimitiveType::Geometry(geometry) => {
                PrimitiveTypeKind::Geometry(geometry.as_ref().into())
            }
            #[cfg(feature = "geo-type-in-dev")]
            PrimitiveType::Geography(geography) => {
                PrimitiveTypeKind::Geography(geography.as_ref().into())
            }
            PrimitiveType::Void => PrimitiveTypeKind::Simple(Simple::Void as i32),
            PrimitiveType::IntervalYearMonth => {
                PrimitiveTypeKind::Simple(Simple::IntervalYearMonth as i32)
            }
            PrimitiveType::IntervalDayTime => {
                PrimitiveTypeKind::Simple(Simple::IntervalDayTime as i32)
            }
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

#[cfg(feature = "geo-type-in-dev")]
impl From<&GeometryType> for proto_schema::GeometryType {
    fn from(geometry: &GeometryType) -> Self {
        proto_schema::GeometryType {
            crs: geometry.crs().to_string(),
        }
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl From<&GeographyType> for proto_schema::GeographyType {
    fn from(geography: &GeographyType) -> Self {
        proto_schema::GeographyType {
            crs: geography.crs().to_string(),
            algorithm: EdgeAlgo::from(geography.algorithm()) as i32,
        }
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl From<&EdgeInterpolationAlgorithm> for EdgeAlgo {
    fn from(algorithm: &EdgeInterpolationAlgorithm) -> Self {
        match algorithm {
            EdgeInterpolationAlgorithm::Spherical => EdgeAlgo::Spherical,
            EdgeInterpolationAlgorithm::Vincenty => EdgeAlgo::Vincenty,
            EdgeInterpolationAlgorithm::Thomas => EdgeAlgo::Thomas,
            EdgeInterpolationAlgorithm::Andoyer => EdgeAlgo::Andoyer,
            EdgeInterpolationAlgorithm::Karney => EdgeAlgo::Karney,
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
        let value = match metadata {
            MetadataValue::Number(n) => MetadataValueKind::Number(*n),
            MetadataValue::String(s) => MetadataValueKind::String(s.clone()),
            MetadataValue::Boolean(b) => MetadataValueKind::Boolean(*b),
            MetadataValue::Other(json) => MetadataValueKind::OtherJson(json.to_string()),
        };
        proto_schema::MetadataValue { value: Some(value) }
    }
}

// === Schema from Proto ===

impl TryFrom<proto_schema::StructType> for StructType {
    type Error = Error;
    fn try_from(proto: proto_schema::StructType) -> DeltaResult<Self> {
        let fields = proto
            .fields
            .into_iter()
            .map(StructField::try_from)
            .collect::<DeltaResult<Vec<_>>>()?;
        StructType::try_new(fields)
    }
}

impl TryFrom<proto_schema::StructField> for StructField {
    type Error = Error;

    fn try_from(proto: proto_schema::StructField) -> DeltaResult<Self> {
        let data_type = proto
            .data_type
            .ok_or_else(|| Error::schema("StructField proto missing data_type"))?;
        let metadata = proto
            .metadata
            .into_iter()
            .map(|(key, value)| Ok::<_, Error>((key, MetadataValue::try_from(value)?)))
            .collect::<DeltaResult<std::collections::HashMap<_, _>>>()?;
        Ok(StructField {
            name: proto.name,
            data_type: DataType::try_from(data_type)?,
            nullable: proto.nullable,
            metadata,
        })
    }
}

impl TryFrom<proto_schema::DataType> for DataType {
    type Error = Error;
    fn try_from(proto: proto_schema::DataType) -> DeltaResult<Self> {
        let kind = proto
            .kind
            .ok_or_else(|| Error::schema("DataType proto missing kind"))?;
        let data_type = match kind {
            DataTypeKind::Primitive(primitive) => DataType::Primitive(primitive.try_into()?),
            DataTypeKind::Array(array) => DataType::Array(Box::new((*array).try_into()?)),
            DataTypeKind::Struct(struct_type) => {
                DataType::Struct(Box::new(struct_type.try_into()?))
            }
            DataTypeKind::Map(map) => DataType::Map(Box::new((*map).try_into()?)),
            // Kernel does not support shredded variants, so always decode as unshredded.
            DataTypeKind::Variant(_) => DataType::unshredded_variant(),
        };
        Ok(data_type)
    }
}

impl TryFrom<proto_schema::PrimitiveType> for PrimitiveType {
    type Error = Error;
    fn try_from(proto: proto_schema::PrimitiveType) -> DeltaResult<Self> {
        let kind = proto
            .kind
            .ok_or_else(|| Error::schema("PrimitiveType proto missing kind"))?;
        let primitive = match kind {
            PrimitiveTypeKind::Simple(simple) => {
                let simple = Simple::try_from(simple).map_err(|_| {
                    Error::schema(format!("unknown SimplePrimitiveType value: {simple}"))
                })?;
                match simple {
                    Simple::String => PrimitiveType::String,
                    Simple::Long => PrimitiveType::Long,
                    Simple::Integer => PrimitiveType::Integer,
                    Simple::Short => PrimitiveType::Short,
                    Simple::Byte => PrimitiveType::Byte,
                    Simple::Float => PrimitiveType::Float,
                    Simple::Double => PrimitiveType::Double,
                    Simple::Boolean => PrimitiveType::Boolean,
                    Simple::Binary => PrimitiveType::Binary,
                    Simple::Date => PrimitiveType::Date,
                    Simple::Timestamp => PrimitiveType::Timestamp,
                    Simple::TimestampNtz => PrimitiveType::TimestampNtz,
                    Simple::Void => PrimitiveType::Void,
                    Simple::IntervalYearMonth => PrimitiveType::IntervalYearMonth,
                    Simple::IntervalDayTime => PrimitiveType::IntervalDayTime,
                    Simple::Unspecified => {
                        return Err(Error::schema("SimplePrimitiveType is unspecified"))
                    }
                }
            }
            PrimitiveTypeKind::Decimal(decimal) => PrimitiveType::Decimal(decimal.try_into()?),
            #[cfg(feature = "geo-type-in-dev")]
            PrimitiveTypeKind::Geometry(geometry) => {
                PrimitiveType::Geometry(Box::new(geometry.try_into()?))
            }
            #[cfg(feature = "geo-type-in-dev")]
            PrimitiveTypeKind::Geography(geography) => {
                PrimitiveType::Geography(Box::new(geography.try_into()?))
            }
            // The proto oneof always carries the geo variants, but kernel only knows how to decode
            // them when the geo feature is enabled.
            #[cfg(not(feature = "geo-type-in-dev"))]
            PrimitiveTypeKind::Geometry(_) | PrimitiveTypeKind::Geography(_) => {
                return Err(Error::schema(
                    "geometry/geography types require the 'geo-type-in-dev' feature",
                ))
            }
        };
        Ok(primitive)
    }
}

impl TryFrom<proto_schema::DecimalType> for DecimalType {
    type Error = Error;
    fn try_from(proto: proto_schema::DecimalType) -> DeltaResult<Self> {
        let precision = u8::try_from(proto.precision).map_err(|_| {
            Error::invalid_decimal(format!("precision out of range: {}", proto.precision))
        })?;
        let scale = u8::try_from(proto.scale)
            .map_err(|_| Error::invalid_decimal(format!("scale out of range: {}", proto.scale)))?;
        DecimalType::try_new(precision, scale)
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl TryFrom<proto_schema::GeometryType> for GeometryType {
    type Error = Error;
    fn try_from(proto: proto_schema::GeometryType) -> DeltaResult<Self> {
        GeometryType::try_new(&proto.crs)
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl TryFrom<proto_schema::GeographyType> for GeographyType {
    type Error = Error;
    fn try_from(proto: proto_schema::GeographyType) -> DeltaResult<Self> {
        let algorithm = EdgeAlgo::try_from(proto.algorithm)
            .map_err(|_| {
                Error::invalid_geo_params(format!(
                    "unknown EdgeInterpolationAlgorithm value: {}",
                    proto.algorithm
                ))
            })?
            .try_into()?;
        GeographyType::try_new(&proto.crs, algorithm)
    }
}

#[cfg(feature = "geo-type-in-dev")]
impl TryFrom<EdgeAlgo> for EdgeInterpolationAlgorithm {
    type Error = Error;
    fn try_from(proto: EdgeAlgo) -> DeltaResult<Self> {
        let algorithm = match proto {
            EdgeAlgo::Spherical => EdgeInterpolationAlgorithm::Spherical,
            EdgeAlgo::Vincenty => EdgeInterpolationAlgorithm::Vincenty,
            EdgeAlgo::Thomas => EdgeInterpolationAlgorithm::Thomas,
            EdgeAlgo::Andoyer => EdgeInterpolationAlgorithm::Andoyer,
            EdgeAlgo::Karney => EdgeInterpolationAlgorithm::Karney,
            EdgeAlgo::Unspecified => {
                return Err(Error::invalid_geo_params(
                    "EdgeInterpolationAlgorithm is unspecified",
                ))
            }
        };
        Ok(algorithm)
    }
}

impl TryFrom<proto_schema::ArrayType> for ArrayType {
    type Error = Error;
    fn try_from(proto: proto_schema::ArrayType) -> DeltaResult<Self> {
        let element_type = proto
            .element_type
            .ok_or_else(|| Error::schema("ArrayType proto missing element_type"))?;
        Ok(ArrayType::new(
            DataType::try_from(*element_type)?,
            proto.contains_null,
        ))
    }
}

impl TryFrom<proto_schema::MapType> for MapType {
    type Error = Error;
    fn try_from(proto: proto_schema::MapType) -> DeltaResult<Self> {
        let key_type = proto
            .key_type
            .ok_or_else(|| Error::schema("MapType proto missing key_type"))?;
        let value_type = proto
            .value_type
            .ok_or_else(|| Error::schema("MapType proto missing value_type"))?;
        Ok(MapType::new(
            DataType::try_from(*key_type)?,
            DataType::try_from(*value_type)?,
            proto.value_contains_null,
        ))
    }
}

impl TryFrom<proto_schema::MetadataValue> for MetadataValue {
    type Error = Error;
    fn try_from(proto: proto_schema::MetadataValue) -> DeltaResult<Self> {
        let value = proto
            .value
            .ok_or_else(|| Error::schema("MetadataValue proto missing value"))?;
        let metadata = match value {
            MetadataValueKind::Number(n) => MetadataValue::Number(n),
            MetadataValueKind::String(s) => MetadataValue::String(s),
            MetadataValueKind::Boolean(b) => MetadataValue::Boolean(b),
            MetadataValueKind::OtherJson(json) => {
                MetadataValue::Other(serde_json::from_str(&json)?)
            }
        };
        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rstest::rstest;
    use url::Url;

    use crate::expressions::{
        lit, ArrayData, BinaryExpressionOp, BinaryPredicateOp, ColumnName, DecimalData, Expression,
        ExpressionStructPatchBuilder, JunctionPredicateOp, MapData, OpaqueExpressionOp,
        OpaquePredicateOp, Predicate, Scalar, ScalarExpressionEvaluator, StructData,
        UnaryExpressionOp, UnaryPredicateOp, VariadicExpressionOp,
    };
    use crate::kernel_predicates::{
        DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
        IndirectDataSkippingPredicateEvaluator,
    };
    use crate::plans::ir::nodes::{
        Agg, Aggregate, FileType, Filter, Load, LoadColumnFileMeta, Operator, Project, ScanFile,
        ScanJson, ScanParquet, SemiJoin, UnionAll, Values,
    };
    use crate::plans::ir::plan::{Plan, PlanNode};
    use crate::plans::proto::{
        expressions as proto_expr, operation as proto_op, plan as proto_plan,
        schema as proto_schema,
    };
    use crate::plans::{IoOperation, Operation};
    use crate::schema::{
        ArrayType, DataType, DecimalType, MapType, MetadataValue, PrimitiveType, SchemaRef,
        StructField, StructType,
    };
    #[cfg(feature = "geo-type-in-dev")]
    use crate::schema::{EdgeInterpolationAlgorithm, GeographyType, GeometryType};
    use crate::{DeltaResult, FileMeta, FileSlice};

    // === Test helpers ===

    #[derive(Debug, PartialEq)]
    struct TestOpaqueExprOp;

    impl OpaqueExpressionOp for TestOpaqueExprOp {
        fn name(&self) -> &str {
            "test_opaque_expr"
        }
        fn eval_expr_scalar(
            &self,
            _eval_expr: &ScalarExpressionEvaluator<'_>,
            _exprs: &[Expression],
        ) -> DeltaResult<Scalar> {
            Ok(Scalar::Integer(0))
        }
    }

    #[derive(Debug, PartialEq)]
    struct TestOpaquePredOp;

    impl OpaquePredicateOp for TestOpaquePredOp {
        fn name(&self) -> &str {
            "test_opaque_pred"
        }
        fn eval_pred_scalar(
            &self,
            _eval_expr: &ScalarExpressionEvaluator<'_>,
            _eval_pred: &DirectPredicateEvaluator<'_>,
            _exprs: &[Expression],
            _inverted: bool,
        ) -> DeltaResult<Option<bool>> {
            Ok(Some(true))
        }
        fn eval_as_data_skipping_predicate(
            &self,
            _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
            _exprs: &[Expression],
            _inverted: bool,
        ) -> Option<bool> {
            None
        }
        fn as_data_skipping_predicate(
            &self,
            _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
            _exprs: &[Expression],
            _inverted: bool,
        ) -> Option<Predicate> {
            None
        }
    }

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
        let bytes = op.to_proto_bytes();
        prost::Message::decode(bytes.as_slice()).expect("decode succeeds")
    }

    fn io_op(op: &Operation) -> proto_op::io_operation::Op {
        let Some(proto_op::operation::Op::Io(io)) = decode(op).op else {
            panic!("expected an IoOperation");
        };
        io.op.expect("io op present")
    }

    fn expr_kind_of(expr: Expression) -> proto_expr::expression::Kind {
        proto_expr::Expression::from(&expr)
            .kind
            .expect("expression kind present")
    }

    fn pred_kind_of(pred: Predicate) -> proto_expr::predicate::Kind {
        proto_expr::Predicate::from(&pred)
            .kind
            .expect("predicate kind present")
    }

    fn scalar_value_of(scalar: Scalar) -> proto_expr::scalar::Value {
        proto_expr::Scalar::from(&scalar)
            .value
            .expect("scalar value present")
    }

    // === Operation / IoOperation ===

    #[rstest]
    #[case(
        Operation::IoOperation(IoOperation::head_file(Url::parse("memory:///h").unwrap())),
        "io"
    )]
    #[case(Operation::QueryPlan(Plan { nodes: vec![] }), "query_plan")]
    fn from_operation(#[case] op: Operation, #[case] expected: &str) {
        use proto_op::operation::Op;
        let kind = match decode(&op).op.unwrap() {
            Op::Io(_) => "io",
            Op::QueryPlan(_) => "query_plan",
        };
        assert_eq!(kind, expected);
    }

    #[test]
    fn from_io_operation_file_listing() {
        let url = "memory:///table/";
        let op = Operation::IoOperation(IoOperation::file_listing(Url::parse(url).unwrap()));
        let proto_op::io_operation::Op::FileListing(file_listing) = io_op(&op) else {
            panic!("expected FileListing");
        };
        assert_eq!(file_listing.url, url);
    }

    #[test]
    fn from_io_operation_read_bytes() {
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
    }

    #[test]
    fn from_io_operation_write_bytes() {
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
    fn from_io_operation_head_file() {
        let op = Operation::IoOperation(IoOperation::head_file(
            Url::parse("memory:///head").unwrap(),
        ));
        let proto_op::io_operation::Op::HeadFile(head_file) = io_op(&op) else {
            panic!("expected HeadFile");
        };
        assert_eq!(head_file.url, "memory:///head");
    }

    #[test]
    fn from_io_operation_atomic_copy() {
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
    fn from_io_operation_parquet_footer() {
        let op = Operation::IoOperation(IoOperation::parquet_footer(sample_file_meta()));
        let proto_op::io_operation::Op::ParquetFooter(parquet_footer) = io_op(&op) else {
            panic!("expected ParquetFooter");
        };
        let file = parquet_footer.file.expect("file meta present");
        assert_eq!(file.location, "memory:///table/part-0.parquet");
        assert_eq!(file.size, 456);
        assert_eq!(file.last_modified, 123);
    }

    #[rstest]
    #[case(Some(0u64..10u64), Some(0), Some(10))]
    #[case(None, None, None)]
    fn from_file_slice(
        #[case] range: Option<std::ops::Range<u64>>,
        #[case] expected_start: Option<u64>,
        #[case] expected_end: Option<u64>,
    ) {
        let slice: FileSlice = (Url::parse("memory:///a").unwrap(), range);
        let proto = proto_op::FileSlice::from(&slice);
        assert_eq!(proto.url, "memory:///a");
        assert_eq!(proto.range_start, expected_start);
        assert_eq!(proto.range_end, expected_end);
    }

    #[test]
    fn from_file_meta() {
        let proto = proto_plan::FileMeta::from(&sample_file_meta());
        assert_eq!(proto.location, "memory:///table/part-0.parquet");
        assert_eq!(proto.size, 456);
        assert_eq!(proto.last_modified, 123);
    }

    // === Plan / nodes ===

    #[test]
    fn from_plan() {
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
                },
                PlanNode {
                    op: Operator::Filter(Filter {
                        predicate: Arc::new(Predicate::gt(
                            Expression::Column(ColumnName::new(["id"])),
                            lit(5i32),
                        )),
                    }),
                    inputs: vec![0],
                },
            ],
        };

        let Some(proto_op::operation::Op::QueryPlan(plan)) = decode(&Operation::QueryPlan(plan)).op
        else {
            panic!("expected a QueryPlan");
        };
        assert_eq!(plan.nodes.len(), 2);

        // Source node: ScanParquet at index 0, no inputs.
        let source = &plan.nodes[0];
        assert!(source.inputs.is_empty());
        let Some(proto_plan::operator::Op::ScanParquet(scan)) = &source.op.as_ref().unwrap().op
        else {
            panic!("expected ScanParquet");
        };
        assert_eq!(scan.files.len(), 1);
        assert_eq!(scan.schema.as_ref().unwrap().fields.len(), 2);

        // Filter node at index 1: consumes node 0, carries a `id > 5` binary predicate.
        let filter_node = &plan.nodes[1];
        assert_eq!(filter_node.inputs.len(), 1);
        assert_eq!(filter_node.inputs[0], 0);
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
            file_meta: sample_load_column_file_meta(),
            dv_column: ColumnName::new(["dv"]),
        }),
        "load"
    )]
    #[case(
        Operator::Aggregate(Aggregate {
            group_by: vec![],
            aggs: vec![],
            schema: sample_schema(),
        }),
        "aggregate"
    )]
    #[case(
        Operator::SemiJoin(SemiJoin { inverted: false, probe_keys: vec![], build_keys: vec![] }),
        "semi_join"
    )]
    #[case(Operator::UnionAll(UnionAll), "union_all")]
    fn from_operator(#[case] op: Operator, #[case] expected: &str) {
        use proto_plan::operator::Op;
        let kind = match proto_plan::Operator::from(&op).op.unwrap() {
            Op::ScanParquet(_) => "scan_parquet",
            Op::ScanJson(_) => "scan_json",
            Op::Values(_) => "values",
            Op::Project(_) => "project",
            Op::Filter(_) => "filter",
            Op::Load(_) => "load",
            Op::Aggregate(_) => "aggregate",
            Op::SemiJoin(_) => "semi_join",
            Op::UnionAll(_) => "union_all",
        };
        assert_eq!(kind, expected);
    }

    #[test]
    fn from_scan_file() {
        let scan_file = ScanFile {
            meta: sample_file_meta(),
            file_constants: vec![Scalar::Integer(1), Scalar::String("x".into())],
        };
        let proto = proto_plan::ScanFile::from(&scan_file);
        assert!(proto.meta.is_some());
        assert_eq!(proto.file_constants.len(), 2);
    }

    #[test]
    fn from_scan_parquet() {
        let node = ScanParquet {
            files: vec![ScanFile::new(sample_file_meta())],
            file_constant_columns: vec!["c".to_string()],
            schema: sample_schema(),
        };
        let proto = proto_plan::ScanParquetNode::from(&node);
        assert_eq!(proto.files.len(), 1);
        assert_eq!(proto.file_constant_columns.len(), 1);
        assert!(proto.schema.is_some());
    }

    #[test]
    fn from_scan_json() {
        let node = ScanJson {
            files: vec![ScanFile::new(sample_file_meta())],
            file_constant_columns: vec!["c".to_string()],
            schema: sample_schema(),
        };
        let proto = proto_plan::ScanJsonNode::from(&node);
        assert_eq!(proto.files.len(), 1);
        assert_eq!(proto.file_constant_columns.len(), 1);
        assert!(proto.schema.is_some());
    }

    #[test]
    fn from_values() {
        let node = Values {
            schema: sample_schema(),
            rows: vec![vec![Scalar::Integer(1)], vec![Scalar::Integer(2)]],
        };
        let proto = proto_plan::ValuesNode::from(&node);
        assert!(proto.schema.is_some());
        assert_eq!(proto.rows.len(), 2);
        assert_eq!(proto.rows[0].values.len(), 1);
    }

    #[test]
    fn from_project() {
        let node = Project {
            expr: Arc::new(Expression::struct_from([lit(1)])),
            schema: sample_schema(),
        };
        let proto = proto_plan::ProjectNode::from(&node);
        assert!(proto.expr.is_some());
        assert!(proto.schema.is_some());
    }

    #[test]
    fn from_filter() {
        let node = Filter {
            predicate: Arc::new(Predicate::literal(true)),
        };
        let proto = proto_plan::FilterNode::from(&node);
        assert!(proto.predicate.is_some());
    }

    fn sample_load_column_file_meta() -> LoadColumnFileMeta {
        LoadColumnFileMeta {
            path_column: ColumnName::new(["path"]),
            file_size_column: ColumnName::new(["size"]),
            num_records_column: ColumnName::new(["num_records"]),
        }
    }

    #[rstest]
    #[case(
        FileType::Json,
        Some(Url::parse("memory:///base/").unwrap()),
        Some("memory:///base/")
    )]
    #[case(FileType::Parquet, None, None)]
    fn from_load(
        #[case] file_type: FileType,
        #[case] base_url: Option<Url>,
        #[case] expected_base_url: Option<&str>,
    ) {
        let node = Load {
            schema: sample_schema(),
            file_type,
            base_url,
            file_constant_columns: vec!["c".to_string()],
            file_meta: sample_load_column_file_meta(),
            dv_column: ColumnName::new(["dv"]),
        };
        let proto = proto_plan::LoadNode::from(&node);
        assert!(proto.schema.is_some());
        assert_eq!(
            proto.file_type,
            proto_plan::FileType::from(file_type) as i32
        );
        assert_eq!(proto.base_url.as_deref(), expected_base_url);
        assert_eq!(proto.file_constant_columns.len(), 1);
        assert!(proto.dv_column.is_some());

        let file_meta = proto.file_meta.expect("file_meta present");
        assert!(file_meta.path_column.is_some());
        assert!(file_meta.file_size_column.is_some());
        assert!(file_meta.num_records_column.is_some());
    }

    #[test]
    fn from_aggregate() {
        let node = Aggregate {
            group_by: vec![ColumnName::new(["g"])],
            aggs: vec![Agg::max(ColumnName::new(["a"]))],
            schema: sample_schema(),
        };
        let proto = proto_plan::AggregateNode::from(&node);
        assert_eq!(proto.group_by.len(), 1);
        assert_eq!(proto.aggs.len(), 1);
        assert!(proto.aggs[0].func.is_some());
        assert!(proto.schema.is_some());
    }

    #[rstest]
    #[case(Agg::min(ColumnName::new(["a"])), "min")]
    #[case(Agg::max(ColumnName::new(["a"])), "max")]
    #[case(Agg::min_non_null_by(ColumnName::new(["a"]), ColumnName::new(["k"])), "min_non_null_by")]
    #[case(Agg::max_non_null_by(ColumnName::new(["a"]), ColumnName::new(["k"])), "max_non_null_by")]
    fn from_agg(#[case] agg: Agg, #[case] expected: &str) {
        use proto_plan::agg::Func;
        let proto = proto_plan::Agg::from(&agg);
        let kind = match proto.func.unwrap() {
            Func::Min(_) => "min",
            Func::Max(_) => "max",
            Func::MinNonNullBy(_) => "min_non_null_by",
            Func::MaxNonNullBy(_) => "max_non_null_by",
        };
        assert_eq!(kind, expected);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn from_semi_join(#[case] inverted: bool) {
        let node = SemiJoin {
            inverted,
            probe_keys: vec![ColumnName::new(["p"])],
            build_keys: vec![ColumnName::new(["b"])],
        };
        let proto = proto_plan::SemiJoinNode::from(&node);
        assert_eq!(proto.inverted, inverted);
        assert_eq!(proto.probe_keys.len(), 1);
        assert_eq!(proto.build_keys.len(), 1);
    }

    #[rstest]
    #[case(FileType::Parquet, proto_plan::FileType::Parquet)]
    #[case(FileType::Json, proto_plan::FileType::Json)]
    fn from_file_type(#[case] value: FileType, #[case] expected: proto_plan::FileType) {
        assert_eq!(proto_plan::FileType::from(value) as i32, expected as i32);
    }

    // === Expressions ===

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
    #[case(Expression::opaque(TestOpaqueExprOp, [lit(1)]), "opaque")]
    #[case(Expression::parse_json(lit("{}"), sample_schema()), "parse_json")]
    #[case(Expression::map_to_struct(Expression::Column(ColumnName::new(["m"]))), "map_to_struct")]
    #[case(Expression::unknown("x"), "unknown")]
    fn from_expression(#[case] expr: Expression, #[case] expected: &str) {
        use proto_expr::expression::Kind;
        let kind = match proto_expr::Expression::from(&expr).kind.unwrap() {
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
        };
        assert_eq!(kind, expected);
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
    #[case(Predicate::opaque(TestOpaquePredOp, [lit(1)]), "opaque")]
    #[case(Predicate::Unknown("x".to_string()), "unknown")]
    fn from_predicate(#[case] pred: Predicate, #[case] expected: &str) {
        use proto_expr::predicate::Kind;
        let kind = match proto_expr::Predicate::from(&pred).kind.unwrap() {
            Kind::BooleanExpression(_) => "boolean_expression",
            Kind::Not(_) => "not",
            Kind::Unary(_) => "unary",
            Kind::Binary(_) => "binary",
            Kind::Junction(_) => "junction",
            Kind::Opaque(_) => "opaque",
            Kind::Unknown(_) => "unknown",
        };
        assert_eq!(kind, expected);
    }

    #[test]
    fn from_column_name() {
        let proto = proto_expr::ColumnName::from(&ColumnName::new(["a", "b", "c"]));
        assert_eq!(proto.path, vec!["a", "b", "c"]);
    }

    #[test]
    fn from_struct_expression() {
        let proto_expr::expression::Kind::StructExpr(plain) =
            expr_kind_of(Expression::struct_from([lit(1), lit(2)]))
        else {
            panic!("expected a struct expression");
        };
        assert_eq!(plain.exprs.len(), 2);
        assert!(plain.nullability_predicate.is_none());

        let proto_expr::expression::Kind::StructExpr(guarded) = expr_kind_of(
            Expression::struct_with_nullability_from([lit(1)], lit(true)),
        ) else {
            panic!("expected a struct expression");
        };
        assert_eq!(guarded.exprs.len(), 1);
        assert!(guarded.nullability_predicate.is_some());
    }

    #[test]
    fn from_unary_expression() {
        let proto_expr::expression::Kind::Unary(unary) =
            expr_kind_of(Expression::unary(UnaryExpressionOp::ToJson, lit(1)))
        else {
            panic!("expected a unary expression");
        };
        assert_eq!(unary.op, proto_expr::UnaryExpressionOp::ToJson as i32);
        assert!(unary.expr.is_some());
    }

    #[test]
    fn from_binary_expression() {
        let proto_expr::expression::Kind::Binary(binary) =
            expr_kind_of(Expression::binary(BinaryExpressionOp::Plus, lit(1), lit(2)))
        else {
            panic!("expected a binary expression");
        };
        assert_eq!(binary.op, proto_expr::BinaryExpressionOp::Plus as i32);
        assert!(binary.left.is_some());
        assert!(binary.right.is_some());
    }

    #[test]
    fn from_variadic_expression() {
        let proto_expr::expression::Kind::Variadic(variadic) =
            expr_kind_of(Expression::coalesce([lit(1), lit(2), lit(3)]))
        else {
            panic!("expected a variadic expression");
        };
        assert_eq!(
            variadic.op,
            proto_expr::VariadicExpressionOp::Coalesce as i32
        );
        assert_eq!(variadic.exprs.len(), 3);
    }

    #[test]
    fn from_opaque_expression() {
        let proto_expr::expression::Kind::Opaque(opaque) =
            expr_kind_of(Expression::opaque(TestOpaqueExprOp, [lit(1), lit(2)]))
        else {
            panic!("expected an opaque expression");
        };
        assert_eq!(opaque.name, "test_opaque_expr");
        assert_eq!(opaque.exprs.len(), 2);
    }

    #[test]
    fn from_parse_json_expression() {
        let proto_expr::expression::Kind::ParseJson(parse_json) =
            expr_kind_of(Expression::parse_json(lit("{}"), sample_schema()))
        else {
            panic!("expected a parse_json expression");
        };
        assert!(parse_json.json_expr.is_some());
        assert!(parse_json.output_schema.is_some());
    }

    #[test]
    fn from_map_to_struct_expression() {
        let proto_expr::expression::Kind::MapToStruct(map_to_struct) = expr_kind_of(
            Expression::map_to_struct(Expression::Column(ColumnName::new(["m"]))),
        ) else {
            panic!("expected a map_to_struct expression");
        };
        assert!(map_to_struct.map_expr.is_some());
    }

    #[test]
    fn from_unary_predicate() {
        let proto_expr::predicate::Kind::Unary(unary) = pred_kind_of(Predicate::is_null(lit(1)))
        else {
            panic!("expected a unary predicate");
        };
        assert_eq!(unary.op, proto_expr::UnaryPredicateOp::IsNull as i32);
        assert!(unary.expr.is_some());
    }

    #[test]
    fn from_binary_predicate() {
        let proto_expr::predicate::Kind::Binary(binary) =
            pred_kind_of(Predicate::gt(lit(1), lit(2)))
        else {
            panic!("expected a binary predicate");
        };
        assert_eq!(binary.op, proto_expr::BinaryPredicateOp::GreaterThan as i32);
        assert!(binary.left.is_some());
        assert!(binary.right.is_some());
    }

    #[test]
    fn from_junction_predicate() {
        let proto_expr::predicate::Kind::Junction(junction) = pred_kind_of(Predicate::and(
            Predicate::literal(true),
            Predicate::literal(false),
        )) else {
            panic!("expected a junction predicate");
        };
        assert_eq!(junction.op, proto_expr::JunctionPredicateOp::And as i32);
        assert_eq!(junction.preds.len(), 2);
    }

    #[test]
    fn from_opaque_predicate() {
        let proto_expr::predicate::Kind::Opaque(opaque) =
            pred_kind_of(Predicate::opaque(TestOpaquePredOp, [lit(1), lit(2)]))
        else {
            panic!("expected an opaque predicate");
        };
        assert_eq!(opaque.name, "test_opaque_pred");
        assert_eq!(opaque.exprs.len(), 2);
    }

    #[test]
    fn from_struct_patch() {
        let patch = ExpressionStructPatchBuilder::new()
            .replace("a", lit(1))
            .insert_after("b", lit(2))
            .drop_if_exists("c")
            .prepend(lit(0))
            .append(lit(3))
            .build()
            .unwrap();

        let transform = proto_expr::Transform::from(&patch);

        // `replace` drops the input field, so `is_replace` is true and it is not optional.
        assert!(transform.field_transforms["a"].is_replace);
        assert!(!transform.field_transforms["a"].optional);
        // `insert_after` keeps the input field, so `is_replace` is false.
        assert!(!transform.field_transforms["b"].is_replace);
        // `drop_if_exists` drops the input field optionally.
        assert!(transform.field_transforms["c"].is_replace);
        assert!(transform.field_transforms["c"].optional);
        // A top-level transform carries no input path.
        assert!(transform.input_path.is_none());
        assert_eq!(transform.prepended_fields.len(), 1);
        assert_eq!(transform.appended_fields.len(), 1);
    }

    // === Expression and predicate operators ===

    #[rstest]
    #[case(UnaryExpressionOp::ToJson, proto_expr::UnaryExpressionOp::ToJson)]
    fn from_unary_expression_op(
        #[case] op: UnaryExpressionOp,
        #[case] expected: proto_expr::UnaryExpressionOp,
    ) {
        assert_eq!(
            proto_expr::UnaryExpressionOp::from(op) as i32,
            expected as i32
        );
    }

    #[rstest]
    #[case(BinaryExpressionOp::Plus, proto_expr::BinaryExpressionOp::Plus)]
    #[case(BinaryExpressionOp::Minus, proto_expr::BinaryExpressionOp::Minus)]
    #[case(BinaryExpressionOp::Multiply, proto_expr::BinaryExpressionOp::Multiply)]
    #[case(BinaryExpressionOp::Divide, proto_expr::BinaryExpressionOp::Divide)]
    fn from_binary_expression_op(
        #[case] op: BinaryExpressionOp,
        #[case] expected: proto_expr::BinaryExpressionOp,
    ) {
        assert_eq!(
            proto_expr::BinaryExpressionOp::from(op) as i32,
            expected as i32
        );
    }

    #[rstest]
    #[case(
        VariadicExpressionOp::Coalesce,
        proto_expr::VariadicExpressionOp::Coalesce
    )]
    #[case(VariadicExpressionOp::Array, proto_expr::VariadicExpressionOp::Array)]
    fn from_variadic_expression_op(
        #[case] op: VariadicExpressionOp,
        #[case] expected: proto_expr::VariadicExpressionOp,
    ) {
        assert_eq!(
            proto_expr::VariadicExpressionOp::from(op) as i32,
            expected as i32
        );
    }

    #[rstest]
    #[case(UnaryPredicateOp::IsNull, proto_expr::UnaryPredicateOp::IsNull)]
    fn from_unary_predicate_op(
        #[case] op: UnaryPredicateOp,
        #[case] expected: proto_expr::UnaryPredicateOp,
    ) {
        assert_eq!(
            proto_expr::UnaryPredicateOp::from(op) as i32,
            expected as i32
        );
    }

    #[rstest]
    #[case(BinaryPredicateOp::LessThan, proto_expr::BinaryPredicateOp::LessThan)]
    #[case(
        BinaryPredicateOp::GreaterThan,
        proto_expr::BinaryPredicateOp::GreaterThan
    )]
    #[case(BinaryPredicateOp::Equal, proto_expr::BinaryPredicateOp::Equal)]
    #[case(BinaryPredicateOp::Distinct, proto_expr::BinaryPredicateOp::Distinct)]
    #[case(BinaryPredicateOp::In, proto_expr::BinaryPredicateOp::In)]
    fn from_binary_predicate_op(
        #[case] op: BinaryPredicateOp,
        #[case] expected: proto_expr::BinaryPredicateOp,
    ) {
        assert_eq!(
            proto_expr::BinaryPredicateOp::from(op) as i32,
            expected as i32
        );
    }

    #[rstest]
    #[case(JunctionPredicateOp::And, proto_expr::JunctionPredicateOp::And)]
    #[case(JunctionPredicateOp::Or, proto_expr::JunctionPredicateOp::Or)]
    fn from_junction_predicate_op(
        #[case] op: JunctionPredicateOp,
        #[case] expected: proto_expr::JunctionPredicateOp,
    ) {
        assert_eq!(
            proto_expr::JunctionPredicateOp::from(op) as i32,
            expected as i32
        );
    }

    // === Scalars ===

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
    fn from_scalar(#[case] value: Scalar, #[case] expected: &str) {
        use proto_expr::scalar::Value;
        let kind = match proto_expr::Scalar::from(&value).value.unwrap() {
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
        };
        assert_eq!(kind, expected);
    }

    #[test]
    fn from_decimal_data() {
        let decimal = DecimalData::try_new(1234i128, DecimalType::try_new(10, 2).unwrap()).unwrap();
        let proto_expr::scalar::Value::Decimal(decimal) = scalar_value_of(Scalar::Decimal(decimal))
        else {
            panic!("expected a decimal scalar");
        };
        assert_eq!(decimal.bits, 1234i128.to_be_bytes().to_vec());
        let decimal_type = decimal.decimal_type.expect("decimal type present");
        assert_eq!((decimal_type.precision, decimal_type.scale), (10, 2));
    }

    #[test]
    fn from_struct_data() {
        let struct_data = StructData::try_new(
            vec![StructField::nullable("a", DataType::INTEGER)],
            vec![Scalar::Integer(1)],
        )
        .unwrap();
        let proto_expr::scalar::Value::Struct(struct_data) =
            scalar_value_of(Scalar::Struct(struct_data))
        else {
            panic!("expected a struct scalar");
        };
        assert_eq!(struct_data.fields.len(), 1);
        assert_eq!(struct_data.values.len(), 1);
        assert_eq!(struct_data.fields[0].name, "a");
    }

    #[test]
    fn from_array_data() {
        let array_data =
            ArrayData::try_new(ArrayType::new(DataType::INTEGER, false), [1, 2, 3]).unwrap();
        let proto_expr::scalar::Value::Array(array_data) =
            scalar_value_of(Scalar::Array(array_data))
        else {
            panic!("expected an array scalar");
        };
        assert!(array_data.array_type.is_some());
        assert_eq!(array_data.elements.len(), 3);
    }

    #[test]
    fn from_map_data() {
        let map_data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, false),
            [("k", 1)],
        )
        .unwrap();
        let proto_expr::scalar::Value::Map(map_data) = scalar_value_of(Scalar::Map(map_data))
        else {
            panic!("expected a map scalar");
        };
        assert!(map_data.map_type.is_some());
        assert_eq!(map_data.pairs.len(), 1);
        assert!(map_data.pairs[0].key.is_some());
        assert!(map_data.pairs[0].value.is_some());
    }

    // === Schema ===

    #[rstest]
    #[case(DataType::INTEGER, "primitive")]
    #[case(ArrayType::new(DataType::INTEGER, true).into(), "array")]
    #[case(
        StructType::try_new(vec![StructField::nullable("a", DataType::INTEGER)])
            .unwrap()
            .into(),
        "struct"
    )]
    #[case(MapType::new(DataType::STRING, DataType::INTEGER, true).into(), "map")]
    #[case(DataType::unshredded_variant(), "variant")]
    fn from_data_type(#[case] value: DataType, #[case] expected: &str) {
        use proto_schema::data_type::Kind;
        let kind = match proto_schema::DataType::from(&value).kind.unwrap() {
            Kind::Primitive(_) => "primitive",
            Kind::Array(_) => "array",
            Kind::Struct(_) => "struct",
            Kind::Map(_) => "map",
            Kind::Variant(_) => "variant",
        };
        assert_eq!(kind, expected);
    }

    #[rstest]
    #[case(PrimitiveType::String, proto_schema::SimplePrimitiveType::String)]
    #[case(PrimitiveType::Long, proto_schema::SimplePrimitiveType::Long)]
    #[case(PrimitiveType::Integer, proto_schema::SimplePrimitiveType::Integer)]
    #[case(PrimitiveType::Short, proto_schema::SimplePrimitiveType::Short)]
    #[case(PrimitiveType::Byte, proto_schema::SimplePrimitiveType::Byte)]
    #[case(PrimitiveType::Float, proto_schema::SimplePrimitiveType::Float)]
    #[case(PrimitiveType::Double, proto_schema::SimplePrimitiveType::Double)]
    #[case(PrimitiveType::Boolean, proto_schema::SimplePrimitiveType::Boolean)]
    #[case(PrimitiveType::Binary, proto_schema::SimplePrimitiveType::Binary)]
    #[case(PrimitiveType::Date, proto_schema::SimplePrimitiveType::Date)]
    #[case(PrimitiveType::Timestamp, proto_schema::SimplePrimitiveType::Timestamp)]
    #[case(
        PrimitiveType::TimestampNtz,
        proto_schema::SimplePrimitiveType::TimestampNtz
    )]
    #[case(PrimitiveType::Void, proto_schema::SimplePrimitiveType::Void)]
    #[case(
        PrimitiveType::IntervalYearMonth,
        proto_schema::SimplePrimitiveType::IntervalYearMonth
    )]
    #[case(
        PrimitiveType::IntervalDayTime,
        proto_schema::SimplePrimitiveType::IntervalDayTime
    )]
    fn from_primitive_type(
        #[case] primitive: PrimitiveType,
        #[case] expected: proto_schema::SimplePrimitiveType,
    ) {
        let Some(proto_schema::primitive_type::Kind::Simple(simple)) =
            proto_schema::PrimitiveType::from(&primitive).kind
        else {
            panic!("expected a simple primitive type");
        };
        assert_eq!(simple, expected as i32);
    }

    #[test]
    fn from_decimal_type() {
        let primitive = PrimitiveType::decimal(10, 2).unwrap();
        let Some(proto_schema::primitive_type::Kind::Decimal(decimal)) =
            proto_schema::PrimitiveType::from(&primitive).kind
        else {
            panic!("expected a decimal primitive type");
        };
        assert_eq!((decimal.precision, decimal.scale), (10, 2));
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn from_array_type(#[case] contains_null: bool) {
        let proto =
            proto_schema::ArrayType::from(&ArrayType::new(DataType::INTEGER, contains_null));
        assert!(proto.element_type.is_some());
        assert_eq!(proto.contains_null, contains_null);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn from_map_type(#[case] value_contains_null: bool) {
        let proto = proto_schema::MapType::from(&MapType::new(
            DataType::STRING,
            DataType::INTEGER,
            value_contains_null,
        ));
        assert!(proto.key_type.is_some());
        assert!(proto.value_type.is_some());
        assert_eq!(proto.value_contains_null, value_contains_null);
    }

    #[test]
    fn from_struct_type() {
        let struct_type = StructType::try_new(vec![
            StructField::nullable("a", DataType::INTEGER),
            StructField::not_null("b", DataType::STRING),
        ])
        .unwrap();
        let proto = proto_schema::StructType::from(&struct_type);
        assert_eq!(proto.fields.len(), 2);
        assert!(proto.fields[0].nullable);
        assert!(!proto.fields[1].nullable);
    }

    #[test]
    fn from_struct_field() {
        let field = StructField::nullable("a", DataType::INTEGER)
            .with_metadata([("k", MetadataValue::Number(7))]);
        let proto = proto_schema::StructField::from(&field);
        assert_eq!(proto.name, "a");
        assert!(proto.nullable);
        assert!(proto.data_type.is_some());
        assert_eq!(
            proto.metadata["k"].value,
            Some(proto_schema::metadata_value::Value::Number(7))
        );
    }

    #[rstest]
    #[case(
        MetadataValue::Number(5),
        proto_schema::metadata_value::Value::Number(5)
    )]
    #[case(
        MetadataValue::String("s".to_string()),
        proto_schema::metadata_value::Value::String("s".to_string())
    )]
    #[case(
        MetadataValue::Boolean(true),
        proto_schema::metadata_value::Value::Boolean(true)
    )]
    #[case(
        MetadataValue::Other(serde_json::json!([1, 2])),
        proto_schema::metadata_value::Value::OtherJson("[1,2]".to_string())
    )]
    fn from_metadata_value(
        #[case] metadata: MetadataValue,
        #[case] expected: proto_schema::metadata_value::Value,
    ) {
        assert_eq!(
            proto_schema::MetadataValue::from(&metadata).value.unwrap(),
            expected
        );
    }

    // === Schema decode (proto -> kernel) ===

    /// Round-trips a [`DataType`] through its proto form and back, asserting it is unchanged.
    fn assert_data_type_round_trips(data_type: DataType) {
        let decoded = DataType::try_from(proto_schema::DataType::from(&data_type));
        assert_eq!(decoded.expect("decode succeeds"), data_type);
    }

    /// Round-trips a [`StructType`] through its proto form and back, asserting it is unchanged.
    fn assert_schema_round_trips(schema: StructType) {
        let decoded = StructType::try_from(proto_schema::StructType::from(&schema));
        assert_eq!(decoded.expect("decode succeeds"), schema);
    }

    #[rstest]
    fn round_trip_primitive(
        #[values(
            PrimitiveType::String,
            PrimitiveType::Long,
            PrimitiveType::Integer,
            PrimitiveType::Short,
            PrimitiveType::Byte,
            PrimitiveType::Float,
            PrimitiveType::Double,
            PrimitiveType::Boolean,
            PrimitiveType::Binary,
            PrimitiveType::Date,
            PrimitiveType::Timestamp,
            PrimitiveType::TimestampNtz,
            PrimitiveType::Void,
            PrimitiveType::IntervalYearMonth,
            PrimitiveType::IntervalDayTime
        )]
        primitive: PrimitiveType,
    ) {
        assert_data_type_round_trips(DataType::Primitive(primitive));
    }

    #[rstest]
    #[case(1, 0)]
    #[case(10, 2)]
    #[case(38, 0)]
    #[case(38, 38)]
    fn round_trip_decimal(#[case] precision: u8, #[case] scale: u8) {
        assert_data_type_round_trips(DataType::Primitive(
            PrimitiveType::decimal(precision, scale).unwrap(),
        ));
    }

    #[cfg(feature = "geo-type-in-dev")]
    #[rstest]
    #[case(GeometryType::try_new("OGC:CRS84").unwrap())]
    #[case(GeometryType::try_new("EPSG:4326").unwrap())]
    fn round_trip_geometry(#[case] geometry: GeometryType) {
        assert_data_type_round_trips(DataType::Primitive(PrimitiveType::Geometry(Box::new(
            geometry,
        ))));
    }

    #[cfg(feature = "geo-type-in-dev")]
    #[rstest]
    fn round_trip_geography(
        #[values("OGC:CRS84", "EPSG:4326")] crs: &str,
        #[values(
            EdgeInterpolationAlgorithm::Spherical,
            EdgeInterpolationAlgorithm::Vincenty,
            EdgeInterpolationAlgorithm::Thomas,
            EdgeInterpolationAlgorithm::Andoyer,
            EdgeInterpolationAlgorithm::Karney
        )]
        algorithm: EdgeInterpolationAlgorithm,
    ) {
        let geography = GeographyType::try_new(crs, algorithm).unwrap();
        assert_data_type_round_trips(DataType::Primitive(PrimitiveType::Geography(Box::new(
            geography,
        ))));
    }

    #[cfg(feature = "geo-type-in-dev")]
    #[rstest]
    #[case(EdgeInterpolationAlgorithm::Spherical, 1)]
    #[case(EdgeInterpolationAlgorithm::Vincenty, 2)]
    #[case(EdgeInterpolationAlgorithm::Thomas, 3)]
    #[case(EdgeInterpolationAlgorithm::Andoyer, 4)]
    #[case(EdgeInterpolationAlgorithm::Karney, 5)]
    fn edge_algo_proto_discriminant_is_stable(
        #[case] algorithm: EdgeInterpolationAlgorithm,
        #[case] proto_tag: i32,
    ) {
        assert_eq!(super::EdgeAlgo::from(&algorithm) as i32, proto_tag);
    }

    #[rstest]
    #[case(DataType::from(ArrayType::new(DataType::INTEGER, true)))]
    #[case(DataType::from(ArrayType::new(DataType::STRING, false)))]
    #[case(DataType::from(MapType::new(DataType::STRING, DataType::LONG, true)))]
    #[case(DataType::from(MapType::new(DataType::INTEGER, DataType::BOOLEAN, false)))]
    #[case(DataType::from(ArrayType::new(
        MapType::new(DataType::STRING, DataType::LONG, true),
        true
    )))]
    #[case(DataType::from(StructType::try_new(vec![
        StructField::nullable("a", DataType::INTEGER),
        StructField::not_null("b", DataType::STRING),
    ]).unwrap()))]
    fn round_trip_composite(#[case] data_type: DataType) {
        assert_data_type_round_trips(data_type);
    }

    #[rstest]
    #[case(MetadataValue::Number(7))]
    #[case(MetadataValue::String("hello".to_string()))]
    #[case(MetadataValue::Boolean(true))]
    #[case(MetadataValue::Other(serde_json::json!({"nested": [1, 2, 3]})))]
    fn round_trip_field_metadata(#[case] value: MetadataValue) {
        let field = StructField::nullable("f", DataType::INTEGER).with_metadata([("k", value)]);
        let decoded = StructField::try_from(proto_schema::StructField::from(&field));
        assert_eq!(decoded.expect("decode succeeds"), field);
    }

    #[test]
    fn round_trip_full_schema() {
        let schema = StructType::try_new(vec![
            StructField::nullable("id", DataType::LONG)
                .with_metadata([("k", MetadataValue::Number(7))]),
            StructField::not_null("name", DataType::STRING),
            StructField::nullable("scores", ArrayType::new(DataType::INTEGER, true)),
            StructField::nullable(
                "attrs",
                MapType::new(DataType::STRING, DataType::LONG, true),
            ),
            StructField::nullable(
                "price",
                DataType::Primitive(PrimitiveType::decimal(10, 2).unwrap()),
            ),
            StructField::nullable(
                "nested",
                StructType::try_new(vec![StructField::not_null("inner", DataType::BOOLEAN)])
                    .unwrap(),
            ),
        ])
        .unwrap();
        assert_schema_round_trips(schema);
    }

    /// The proto `VariantType` is currently opaque, so any variant (even a shredded one)
    /// decodes to the canonical unshredded form rather than round-tripping its inner struct.
    #[test]
    fn variant_decodes_to_unshredded() {
        let shredded = DataType::Variant(Box::new(
            StructType::try_new(vec![
                StructField::not_null("metadata", DataType::BINARY),
                StructField::not_null("value", DataType::BINARY),
                StructField::nullable("typed_value", DataType::INTEGER),
            ])
            .unwrap(),
        ));
        let decoded = DataType::try_from(proto_schema::DataType::from(&shredded));
        assert_eq!(
            decoded.expect("decode succeeds"),
            DataType::unshredded_variant()
        );
    }

    // Builds a proto `DataType::Primitive` with the given (possibly malformed) primitive kind.
    fn primitive_data_type(
        kind: Option<proto_schema::primitive_type::Kind>,
    ) -> proto_schema::DataType {
        proto_schema::DataType {
            kind: Some(proto_schema::data_type::Kind::Primitive(
                proto_schema::PrimitiveType { kind },
            )),
        }
    }

    fn simple_primitive_data_type(value: i32) -> proto_schema::DataType {
        primitive_data_type(Some(proto_schema::primitive_type::Kind::Simple(value)))
    }

    fn decimal_data_type(precision: u32, scale: u32) -> proto_schema::DataType {
        primitive_data_type(Some(proto_schema::primitive_type::Kind::Decimal(
            proto_schema::DecimalType { precision, scale },
        )))
    }

    #[rstest]
    #[case::missing_data_type_kind(proto_schema::DataType { kind: None })]
    #[case::missing_primitive_kind(primitive_data_type(None))]
    #[case::unspecified_primitive(simple_primitive_data_type(
        proto_schema::SimplePrimitiveType::Unspecified as i32
    ))]
    #[case::unknown_primitive(simple_primitive_data_type(9999))]
    #[case::decimal_zero_precision(decimal_data_type(0, 0))]
    #[case::decimal_precision_too_large(decimal_data_type(39, 0))]
    #[case::decimal_scale_exceeds_precision(decimal_data_type(5, 6))]
    #[case::decimal_precision_out_of_u8_range(decimal_data_type(256, 0))]
    #[case::array_missing_element_type(proto_schema::DataType {
        kind: Some(proto_schema::data_type::Kind::Array(Box::new(proto_schema::ArrayType {
            element_type: None,
            contains_null: true,
        }))),
    })]
    #[case::map_missing_key_type(proto_schema::DataType {
        kind: Some(proto_schema::data_type::Kind::Map(Box::new(proto_schema::MapType {
            key_type: None,
            value_type: Some(Box::new(proto_schema::DataType::from(&DataType::STRING))),
            value_contains_null: true,
        }))),
    })]
    #[case::map_missing_value_type(proto_schema::DataType {
        kind: Some(proto_schema::data_type::Kind::Map(Box::new(proto_schema::MapType {
            key_type: Some(Box::new(proto_schema::DataType::from(&DataType::STRING))),
            value_type: None,
            value_contains_null: true,
        }))),
    })]
    fn data_type_decode_rejects_invalid(#[case] proto: proto_schema::DataType) {
        assert!(DataType::try_from(proto).is_err());
    }

    #[test]
    fn struct_field_decode_rejects_missing_data_type() {
        let proto = proto_schema::StructField {
            name: "f".to_string(),
            data_type: None,
            nullable: true,
            metadata: Default::default(),
        };
        assert!(StructField::try_from(proto).is_err());
    }

    #[rstest]
    #[case::missing_value(proto_schema::MetadataValue { value: None })]
    #[case::invalid_other_json(proto_schema::MetadataValue {
        value: Some(proto_schema::metadata_value::Value::OtherJson("not json".to_string())),
    })]
    fn metadata_value_decode_rejects_invalid(#[case] proto: proto_schema::MetadataValue) {
        assert!(MetadataValue::try_from(proto).is_err());
    }
}
