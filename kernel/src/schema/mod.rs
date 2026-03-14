//! Definitions and functions to create and manipulate kernel schema

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::iter::{DoubleEndedIterator, FusedIterator};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::warn;

// re-export because many call sites that use schemas do not necessarily use expressions
pub(crate) use crate::expressions::{column_name, ColumnName};
use crate::reserved_field_ids::FILE_NAME;
use crate::table_features::get_field_column_mapping_info;
use crate::table_features::ColumnMappingMode;
use crate::transforms::SchemaTransform;
use crate::utils::require;
use crate::{DeltaResult, Error};
use delta_kernel_derive::internal_api;

pub(crate) mod compare;
#[cfg(feature = "schema-diff")]
pub(crate) mod diff;

#[cfg(feature = "internal-api")]
pub mod derive_macro_utils;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod derive_macro_utils;
pub(crate) mod variant_utils;

pub type Schema = StructType;
pub type SchemaRef = Arc<StructType>;

/// Converts a type to a [`Schema`] that represents that type. Derivable for struct types using the
/// [`delta_kernel_derive::ToSchema`] derive macro.
#[internal_api]
pub(crate) trait ToSchema {
    fn to_schema() -> StructType;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged)]
pub enum MetadataValue {
    Number(i64),
    String(String),
    Boolean(bool),
    // The [PROTOCOL](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#struct-field) states
    // only that the metadata is "A JSON map containing information about this column.", so we can
    // actually have any valid json here. `Other` is therefore a catchall for things we don't need
    // to handle.
    Other(serde_json::Value),
}

impl Display for MetadataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataValue::Number(n) => write!(f, "{n}"),
            MetadataValue::String(s) => write!(f, "{s}"),
            MetadataValue::Boolean(b) => write!(f, "{b}"),
            MetadataValue::Other(v) => write!(f, "{v}"), // just write the json back
        }
    }
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&String> for MetadataValue {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<&str> for MetadataValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<i64> for MetadataValue {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<bool> for MetadataValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

#[derive(Debug)]
pub enum ColumnMetadataKey {
    ColumnMappingId,
    ColumnMappingPhysicalName,
    ParquetFieldId,
    GenerationExpression,
    IdentityStart,
    IdentityStep,
    IdentityHighWaterMark,
    IdentityAllowExplicitInsert,
    InternalColumn,
    Invariants,
    MetadataSpec,
}

impl AsRef<str> for ColumnMetadataKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::ColumnMappingId => "delta.columnMapping.id",
            Self::ColumnMappingPhysicalName => "delta.columnMapping.physicalName",
            // "parquet.field.id" is not defined by the Delta protocol, but follows the convention
            // established by delta-spark and other Delta ecosystem implementations for storing
            // Parquet field IDs in StructField metadata.
            Self::ParquetFieldId => "parquet.field.id",
            Self::GenerationExpression => "delta.generationExpression",
            Self::IdentityAllowExplicitInsert => "delta.identity.allowExplicitInsert",
            Self::IdentityHighWaterMark => "delta.identity.highWaterMark",
            Self::IdentityStart => "delta.identity.start",
            Self::IdentityStep => "delta.identity.step",
            Self::InternalColumn => "delta.isInternalColumn",
            Self::Invariants => "delta.invariants",
            Self::MetadataSpec => "delta.metadataSpec",
        }
    }
}

/// Enumeration of metadata columns recognized by Delta Kernel.
///
/// Metadata columns provide additional information about rows in a Delta table.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum MetadataColumnSpec {
    RowIndex,
    RowId,
    RowCommitVersion,
    FilePath,
}

impl MetadataColumnSpec {
    /// A human-readable name for the specified metadata column.
    pub fn text_value(&self) -> &'static str {
        match self {
            Self::RowIndex => "row_index",
            Self::RowId => "row_id",
            Self::RowCommitVersion => "row_commit_version",
            Self::FilePath => "_file",
        }
    }

    /// The data type of the specified metadata column.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::RowIndex => DataType::LONG,
            Self::RowId => DataType::LONG,
            Self::RowCommitVersion => DataType::LONG,
            Self::FilePath => DataType::STRING,
        }
    }

    /// Whether the specified metadata column is nullable.
    pub fn nullable(&self) -> bool {
        match self {
            Self::RowIndex => false,
            Self::RowId => false,
            Self::RowCommitVersion => false,
            Self::FilePath => false,
        }
    }

    /// The reserved field ID for the specified metadata column, if any.
    pub fn reserved_field_id(&self) -> Option<i64> {
        match self {
            Self::FilePath => Some(FILE_NAME),
            _ => None,
        }
    }
}

impl FromStr for MetadataColumnSpec {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "row_index" => Ok(Self::RowIndex),
            "row_id" => Ok(Self::RowId),
            "row_commit_version" => Ok(Self::RowCommitVersion),
            "_file" => Ok(Self::FilePath),
            _ => Err(Error::Schema(format!("Unknown metadata column spec: {s}"))),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct StructField {
    /// Name of this (possibly nested) column
    pub name: String,
    /// The data type of this field
    #[serde(rename = "type")]
    pub data_type: DataType,
    /// Denotes whether this Field can be null
    pub nullable: bool,
    /// A JSON map containing information about this column
    pub metadata: HashMap<String, MetadataValue>,
}

impl StructField {
    /// The name of the default row index metadata column.
    ///
    /// Note that the dot does not indicate a nested field, it is just a separator for the metadata column name.
    const DEFAULT_ROW_INDEX_COLUMN_NAME: &'static str = "_metadata.row_index";

    /////////////////
    // Static methods
    /////////////////

    /// Creates a new field
    pub fn new(name: impl Into<String>, data_type: impl Into<DataType>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            metadata: HashMap::default(),
        }
    }

    /// Creates a new nullable field
    pub fn nullable(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, true)
    }

    /// Creates a new non-nullable field
    pub fn not_null(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self::new(name, data_type, false)
    }

    /// Creates a metadata column of the given spec with the given name.
    pub fn create_metadata_column(name: impl Into<String>, spec: MetadataColumnSpec) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert(
            ColumnMetadataKey::MetadataSpec.as_ref().to_string(),
            MetadataValue::String(spec.text_value().to_string()),
        );

        Self {
            name: name.into(),
            data_type: spec.data_type(),
            nullable: spec.nullable(),
            metadata,
        }
    }

    /// Returns the default row index metadata column used by Kernel.
    pub fn default_row_index_column() -> &'static StructField {
        static DEFAULT_ROW_INDEX_COLUMN: LazyLock<StructField> = LazyLock::new(|| {
            StructField::create_metadata_column(
                StructField::DEFAULT_ROW_INDEX_COLUMN_NAME,
                MetadataColumnSpec::RowIndex,
            )
        });
        &DEFAULT_ROW_INDEX_COLUMN
    }

    ///////////////////
    // Instance methods
    ///////////////////

    /// Replaces `self.metadata` with the list of <key, value> pairs in `metadata`.
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata = metadata
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    /// Extends `self.metadata` to include the <key, value> pairs in `metadata`.
    pub fn add_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<MetadataValue>)>,
    ) -> Self {
        self.metadata
            .extend(metadata.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Returns true if this field is a metadata column.
    pub fn is_metadata_column(&self) -> bool {
        self.metadata
            .contains_key(ColumnMetadataKey::MetadataSpec.as_ref())
    }

    /// Returns the metadata column spec if this is a metadata column, otherwise returns None.
    pub fn get_metadata_column_spec(&self) -> Option<MetadataColumnSpec> {
        match self
            .metadata
            .get(ColumnMetadataKey::MetadataSpec.as_ref())?
        {
            MetadataValue::String(s) => MetadataColumnSpec::from_str(s).ok(),
            _ => None,
        }
    }

    /// Returns true if this field is an internal column added by Kernel.
    ///
    /// Internal columns must be removed before returning scan results to the user.
    pub fn is_internal_column(&self) -> bool {
        matches!(
            self.metadata
                .get(ColumnMetadataKey::InternalColumn.as_ref()),
            Some(MetadataValue::Boolean(true))
        )
    }

    /// Marks this field as an internal column.
    pub fn as_internal_column(self) -> Self {
        self.add_metadata(vec![(
            ColumnMetadataKey::InternalColumn.as_ref().to_string(),
            MetadataValue::Boolean(true),
        )])
    }

    pub fn get_config_value(&self, key: &ColumnMetadataKey) -> Option<&MetadataValue> {
        self.metadata.get(key.as_ref())
    }

    /// Get the physical name for this field as it should be read from parquet.
    ///
    /// When `column_mapping_mode` is `None`, always returns the logical name (even if physical
    /// name metadata is present). When mode is `Id` or `Name`, returns the physical name from
    /// metadata if present, otherwise returns the logical name.
    ///
    /// NOTE: Caller affirms that the schema was already validated by
    /// [`crate::table_configuration::TableConfiguration::try_new`], to ensure that annotations are
    /// always and only present when column mapping mode is enabled.
    #[internal_api]
    pub(crate) fn physical_name(&self, column_mapping_mode: ColumnMappingMode) -> &str {
        match column_mapping_mode {
            ColumnMappingMode::None => &self.name,
            ColumnMappingMode::Id | ColumnMappingMode::Name => {
                match self
                    .metadata
                    .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
                {
                    Some(MetadataValue::String(physical_name)) => physical_name,
                    _ => &self.name,
                }
            }
        }
    }

    /// Returns true if this field has a physical name annotation
    /// in its column mapping metadata.
    pub(crate) fn has_physical_name_annotation(&self) -> bool {
        matches!(
            self.metadata
                .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()),
            Some(MetadataValue::String(_))
        )
    }

    /// Returns true if this field has a column mapping ID annotation
    /// in its column mapping metadata.
    pub(crate) fn has_id_annotation(&self) -> bool {
        matches!(
            self.metadata
                .get(ColumnMetadataKey::ColumnMappingId.as_ref()),
            Some(MetadataValue::Number(_))
        )
    }

    /// Change the name of a field. The field will preserve its data type and nullability. Note that
    /// this allocates a new field.
    pub fn with_name(&self, new_name: impl Into<String>) -> Self {
        StructField {
            name: new_name.into(),
            data_type: self.data_type().clone(),
            nullable: self.nullable,
            metadata: self.metadata.clone(),
        }
    }

    #[inline]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, MetadataValue> {
        &self.metadata
    }

    /// Convert our metadata into a HashMap<String, String>. Note this copies all the data so can be
    /// expensive for large metadata
    pub fn metadata_with_string_values(&self) -> HashMap<String, String> {
        self.metadata
            .iter()
            .map(|(key, val)| (key.clone(), val.to_string()))
            .collect()
    }

    /// Applies physical name and field ID mappings to this field.
    ///
    /// This function sets the field ID for the physical [`StructField`] only if the
    /// `column_mapping_mode` is `Id`. The field ID is specified using the
    /// [`ColumnMetadataKey::ParquetFieldId`] metadata field. Readers should use
    /// [`ColumnMetadataKey::ParquetFieldId`] to match fields to the Parquet schema.
    /// If a physical StructField contains a field ID, the reader must resolve columns
    /// with that ID. Otherwise, the physical StructField's name is used. For details,
    /// see [`read_parquet_files`].
    ///
    /// This function also sets the physical name of a field. If `column_mapping_mode` is
    /// `Id` or `Name`, this is specified in [`ColumnMetadataKey::ColumnMappingPhysicalName`].
    /// Otherwise, the field's logical name is used.
    ///
    /// Returns an error if a field has invalid or inconsistent column mapping annotations (e.g.
    /// missing when column mapping is enabled, present when disabled, or wrong type), or if a
    /// metadata column is encountered (metadata columns should not participate in column mapping).
    ///
    /// [`read_parquet_files`]: crate::ParquetHandler::read_parquet_files
    #[internal_api]
    pub(crate) fn make_physical(
        &self,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<Self> {
        MakePhysical::new(column_mapping_mode).run_field(self)
    }

    fn has_invariants(&self) -> bool {
        self.metadata
            .contains_key(ColumnMetadataKey::Invariants.as_ref())
    }

    /// Converts logical schema StructField metadata to physical schema metadata
    /// based on the specified `column_mapping_mode`.
    ///
    /// NOTE: Must not be called on metadata columns, which are not subject to column mapping.
    ///
    /// NOTE: Caller affirms that `self` was already validated by
    /// [`crate::table_features::get_field_column_mapping_info`], to ensure that annotations are
    /// always and only present when column mapping mode is enabled.
    fn logical_to_physical_metadata(
        &self,
        column_mapping_mode: ColumnMappingMode,
    ) -> HashMap<String, MetadataValue> {
        let mut base_metadata = self.metadata.clone();
        let physical_name_key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
        let field_id_key = ColumnMetadataKey::ColumnMappingId.as_ref();
        let parquet_field_id_key = ColumnMetadataKey::ParquetFieldId.as_ref();
        let field_id = base_metadata.get(ColumnMetadataKey::ColumnMappingId.as_ref());
        match column_mapping_mode {
            ColumnMappingMode::Id => {
                let Some(MetadataValue::Number(fid)) = field_id else {
                    // `get_field_column_mapping_info` should have verified that this has a field Id
                    warn!("StructField with name {} is missing field id in the Id column mapping mode", self.name());
                    debug_assert!(false);
                    return base_metadata;
                };
                // Insert the parquet field id matching the column mapping id
                base_metadata.insert(
                    parquet_field_id_key.to_string(),
                    MetadataValue::Number(*fid),
                );
                // Ensure that physical name is present
                debug_assert!(base_metadata.contains_key(physical_name_key));
            }
            ColumnMappingMode::Name => {
                // Logical metadata should have the column mapping metadata keys
                debug_assert!(base_metadata.contains_key(physical_name_key));
                debug_assert!(base_metadata.contains_key(field_id_key));

                // Retain column mapping id and insert parquet field id so that
                // Parquet files carry field IDs in Name mode as well (matching
                // the Delta protocol requirement and Delta Spark behaviour).
                let Some(MetadataValue::Number(fid)) = field_id else {
                    warn!("StructField with name {} is missing field id in the Name column mapping mode", self.name());
                    debug_assert!(false);
                    return base_metadata;
                };
                base_metadata.insert(
                    parquet_field_id_key.to_string(),
                    MetadataValue::Number(*fid),
                );
                // TODO(#1070): Remove nested column ids when they are supported in kernel
            }
            ColumnMappingMode::None => {
                base_metadata.remove(physical_name_key);
                base_metadata.remove(field_id_key);
                base_metadata.remove(parquet_field_id_key);
                // TODO(#1070): Remove nested column ids when they are supported in kernel
            }
        }
        base_metadata
    }
}

impl Display for StructField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut metadata_str = String::from("{");
        let mut first = true;
        for (k, v) in self.metadata.iter() {
            if !first {
                metadata_str.push_str(", ");
            }
            first = false;
            metadata_str.push_str(&format!("{k}: {v:?}"));
        }
        metadata_str.push('}');
        write!(
            f,
            "{}: {} (is nullable: {}, metadata: {})",
            self.name, self.data_type, self.nullable, metadata_str,
        )
    }
}

/// A struct is used to represent both the top-level schema of the table
/// as well as struct columns that contain nested columns.
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct StructType {
    type_name: String,
    /// The fields stored in this struct
    // We use indexmap to preserve the order of fields as they are defined in the schema
    // while also allowing for fast lookup by name. The alternative is to do a linear search
    // for each field by name would be potentially quite expensive for large schemas.
    fields: IndexMap<String, StructField>,
    /// The metadata columns in this struct
    // We use a dedicated map for metadata columns to allow for fast lookup without having to iterate
    // over all fields.
    metadata_columns: HashMap<MetadataColumnSpec, usize>,
}

pub struct StructTypeBuilder {
    fields: IndexMap<String, StructField>,
}

impl Default for StructTypeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StructTypeBuilder {
    pub fn new() -> Self {
        Self {
            fields: IndexMap::new(),
        }
    }

    pub fn from_schema(schema: &StructType) -> Self {
        Self {
            fields: schema.fields.clone(),
        }
    }

    pub fn add_field(mut self, field: StructField) -> Self {
        self.fields.insert(field.name.clone(), field);
        self
    }

    pub fn build(self) -> DeltaResult<StructType> {
        StructType::try_new(self.fields.into_values())
    }

    pub fn build_arc_unchecked(self) -> Arc<StructType> {
        Arc::new(StructType::new_unchecked(self.fields.into_values()))
    }
}

impl StructType {
    /// Creates a new [`StructType`] from the given fields.
    ///
    /// Returns an error if:
    /// - the schema contains duplicate field names (case-insensitive; Delta column names are
    ///   case-insensitive per the protocol)
    /// - the schema contains duplicate metadata columns
    /// - the schema contains nested metadata columns
    pub fn try_new(fields: impl IntoIterator<Item = StructField>) -> DeltaResult<Self> {
        let mut field_map = IndexMap::new();
        let mut metadata_columns = HashMap::new();
        let mut seen_lowercase_names = HashSet::new();

        // Validate each field during insertion
        for (i, field) in fields.into_iter().enumerate() {
            // Verify that there are no nested metadata columns
            if !matches!(field.data_type, DataType::Primitive(_)) {
                Self::ensure_no_metadata_columns_in_field(&field)?;
            }

            // Check for duplicate metadata columns
            if let Some(metadata_column_spec) = field.get_metadata_column_spec() {
                if metadata_columns.insert(metadata_column_spec, i).is_some() {
                    return Err(Error::schema(format!(
                        "Duplicate metadata column: {metadata_column_spec:?}",
                    )));
                }
            }

            // Delta column names are case-insensitive; reject schemas with duplicates that differ only by case.
            let key = field.name.to_lowercase();
            if !seen_lowercase_names.insert(key) {
                return Err(Error::schema(format!(
                    "Duplicate field name (case-insensitive): '{}'",
                    field.name
                )));
            }

            field_map.insert(field.name.clone(), field);
        }

        Ok(Self {
            type_name: "struct".into(),
            fields: field_map,
            metadata_columns,
        })
    }

    /// Creates a new [`StructType`] from a fallible iterator of fields.
    ///
    /// This constructor collects all fields from the iterator, returning the first error
    /// encountered, or a new [`StructType`] if all fields are successfully collected and validated.
    pub fn try_from_results<E: Into<Error>>(
        fields: impl IntoIterator<Item = Result<StructField, E>>,
    ) -> DeltaResult<Self> {
        fields
            .into_iter()
            .map(|result| result.map_err(Into::into))
            .process_results(|iter| Self::try_new(iter))?
    }

    pub fn builder() -> StructTypeBuilder {
        StructTypeBuilder::new()
    }

    /// Creates a new [`StructType`] from the given fields without validating them.
    ///
    /// This should only be used when you are sure that the fields are valid.
    /// Refer to [`StructType::try_new`] for more details on the validation checks.
    #[internal_api]
    pub(crate) fn new_unchecked(fields: impl IntoIterator<Item = StructField>) -> Self {
        let mut field_map = IndexMap::new();
        let mut metadata_columns = HashMap::new();

        for (i, field) in fields.into_iter().enumerate() {
            if let Some(metadata_column_spec) = field.get_metadata_column_spec() {
                metadata_columns.insert(metadata_column_spec, i);
            }
            field_map.insert(field.name.clone(), field);
        }

        Self {
            type_name: "struct".into(),
            fields: field_map,
            metadata_columns,
        }
    }

    /// Gets a [`StructType`] containing [`StructField`]s of the given names. The order of fields in
    /// the returned schema will match the order passed to this function, which can be different
    /// from this order in this schema. Returns an Err if a specified field doesn't exist.
    pub fn project_as_struct(&self, names: &[impl AsRef<str>]) -> DeltaResult<StructType> {
        let fields = names.iter().map(|name| {
            self.fields
                .get(name.as_ref())
                .cloned()
                .ok_or_else(|| Error::missing_column(name.as_ref()))
        });
        Self::try_from_results(fields)
    }

    /// Gets a [`SchemaRef`] containing [`StructField`]s of the given names. The order of fields in
    /// the returned schema will match the order passed to this function, which can be different
    /// from this order in this schema. Returns an Err if a specified field doesn't exist.
    pub fn project(&self, names: &[impl AsRef<str>]) -> DeltaResult<SchemaRef> {
        let struct_type = self.project_as_struct(names)?;
        Ok(Arc::new(struct_type))
    }

    /// Adds fields to this [`StructType`], returning a new [`StructType`].
    pub fn add(&self, fields: impl IntoIterator<Item = StructField>) -> DeltaResult<Self> {
        Self::try_new(self.fields.values().cloned().chain(fields))
    }

    /// Adds a predefined metadata column to this [`StructType`], returning a new [`StructType`].
    pub fn add_metadata_column(
        &self,
        name: impl Into<String>,
        spec: MetadataColumnSpec,
    ) -> DeltaResult<Self> {
        self.add([StructField::create_metadata_column(name, spec)])
    }

    /// Returns the index of the field with the given name, or None if not found.
    pub fn index_of(&self, name: impl AsRef<str>) -> Option<usize> {
        self.fields.get_index_of(name.as_ref())
    }

    /// Returns the index of the metadata column with the given spec, or None if not found.
    pub fn index_of_metadata_column(&self, spec: &MetadataColumnSpec) -> Option<&usize> {
        self.metadata_columns.get(spec)
    }

    /// Checks if the [`StructType`] contains a field with the specified name.
    pub fn contains(&self, name: impl AsRef<str>) -> bool {
        self.fields.contains_key(name.as_ref())
    }

    /// Checks if the [`StructType`] contains a metadata column with the given spec.
    pub fn contains_metadata_column(&self, spec: &MetadataColumnSpec) -> bool {
        self.metadata_columns.contains_key(spec)
    }

    /// Gets the field with the given name.
    pub fn field(&self, name: impl AsRef<str>) -> Option<&StructField> {
        self.fields.get(name.as_ref())
    }

    /// Resolves a column path through nested structs, returning references to all
    /// [`StructField`]s along the path. The last element is the leaf field.
    ///
    /// Each element of the path must resolve to a field in the current struct. All intermediate
    /// (non-leaf) fields must be struct types.
    ///
    /// Returns an error if the path is empty, a field is not found, or an intermediate
    /// field is not a struct type.
    pub(crate) fn walk_column_fields<'a>(
        &'a self,
        col: &ColumnName,
    ) -> DeltaResult<Vec<&'a StructField>> {
        let path = col.path();
        if path.is_empty() {
            return Err(Error::generic("Column path cannot be empty"));
        }
        let mut current_struct = self;
        let mut fields = Vec::with_capacity(path.len());
        for (i, field_name) in path.iter().enumerate() {
            let field = current_struct.field(field_name).ok_or_else(|| {
                Error::generic(format!(
                    "Could not resolve column '{col}': field '{field_name}' not found in schema"
                ))
            })?;
            fields.push(field);
            if i < path.len() - 1 {
                let DataType::Struct(inner) = field.data_type() else {
                    return Err(Error::generic(format!(
                        "Cannot resolve column '{col}': intermediate field '{field_name}' \
                         is not a struct type"
                    )));
                };
                current_struct = inner;
            }
        }
        Ok(fields)
    }

    /// Gets the field with the given name and its index.
    pub fn field_with_index(&self, name: impl AsRef<str>) -> Option<(usize, &StructField)> {
        self.fields
            .get_full(name.as_ref())
            .map(|(index, _, field)| (index, field))
    }

    /// Gets the field at the given index.
    pub fn field_at_index(&self, index: usize) -> Option<&StructField> {
        self.fields.get_index(index).map(|(_, field)| field)
    }

    /// Gets a reference to all the fields in this struct type.
    pub fn fields(
        &self,
    ) -> impl ExactSizeIterator<Item = &StructField> + DoubleEndedIterator + FusedIterator {
        self.fields.values()
    }

    /// Gets an iterator over all the fields in this struct type.
    pub fn into_fields(
        self,
    ) -> impl ExactSizeIterator<Item = StructField> + DoubleEndedIterator + FusedIterator {
        self.fields.into_values()
    }

    /// Gets all the field names in this struct type in the order they are defined.
    pub fn field_names(&self) -> impl ExactSizeIterator<Item = &String> {
        self.fields.keys()
    }

    /// Gets the number of fields in this struct type.
    pub fn num_fields(&self) -> usize {
        // O(1) for indexmap
        self.fields.len()
    }

    /// Recursively counts all [`StructField`] nodes in this schema tree.
    ///
    /// This includes nested struct fields (inside Struct, Array, and Map types) but does not
    /// count Array/Map containers themselves. This matches the traversal pattern used by
    /// `assign_column_mapping_metadata` when assigning column IDs, so the result equals the
    /// expected `delta.columnMapping.maxColumnId` for a newly created table.
    #[allow(unused)] // Only used by integration tests (create_table/column_mapping.rs)
    #[internal_api]
    pub(crate) fn total_struct_fields(&self) -> usize {
        fn count_data_type(dt: &DataType) -> usize {
            match dt {
                DataType::Struct(inner) => inner.total_struct_fields(),
                DataType::Array(array) => count_data_type(array.element_type()),
                DataType::Map(map) => {
                    count_data_type(map.key_type()) + count_data_type(map.value_type())
                }
                _ => 0,
            }
        }
        self.fields()
            .map(|field| 1 + count_data_type(field.data_type()))
            .sum()
    }

    /// Gets a reference to the metadata column with the given spec.
    pub fn metadata_column(&self, spec: &MetadataColumnSpec) -> Option<&StructField> {
        self.metadata_columns
            .get(spec)
            .and_then(|index| self.fields.get_index(*index).map(|(_, field)| field))
    }

    /// Gets an iterator over all the metadata columns in this struct type.
    pub fn metadata_columns(&self) -> impl Iterator<Item = &StructField> {
        self.metadata_columns
            .values()
            .filter_map(|index| self.fields.get_index(*index).map(|(_, field)| field))
    }

    /// Extracts the name and type of all leaf columns, in schema order. Caller should pass Some
    /// `own_name` if this schema is embedded in a larger struct (e.g. `add.*`) and None if the
    /// schema is a top-level result (e.g. `*`).
    ///
    /// NOTE: This method only traverses through `StructType` fields; `MapType` and `ArrayType`
    /// fields are considered leaves even if they contain `StructType` entries/elements.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn leaves<'s>(&self, own_name: impl Into<Option<&'s str>>) -> ColumnNamesAndTypes {
        let mut get_leaves = GetSchemaLeaves::new(own_name.into());
        let _ = get_leaves.transform_struct(self);
        (get_leaves.names, get_leaves.types).into()
    }

    /// Applies physical name mappings to this field. If the `column_mapping_mode` is
    /// [`ColumnMappingMode::Id`], then each StructField will have its parquet field id in the
    /// [`ColumnMetadataKey::ParquetFieldId`] metadata field.
    ///
    /// Uses a single transformer so duplicate column mapping IDs are detected across all
    /// fields in this struct, not just within each field's subtree.
    #[internal_api]
    pub(crate) fn make_physical(
        &self,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<Self> {
        let mut transformer = MakePhysical::new(column_mapping_mode);
        let fields: Vec<StructField> = self
            .fields()
            .map(|field| transformer.run_field(field))
            .try_collect()?;
        Self::try_new(fields)
    }

    /// Validates that there are no metadata columns in the given fields.
    pub(crate) fn ensure_no_metadata_columns(
        fields: &mut dyn Iterator<Item = &StructField>,
    ) -> DeltaResult<()> {
        for field in fields {
            Self::ensure_no_metadata_columns_in_field(field)?;
        }
        Ok(())
    }

    /// Validates that there are no metadata columns in the given field.
    pub(crate) fn ensure_no_metadata_columns_in_field(field: &StructField) -> DeltaResult<()> {
        if field.is_metadata_column() {
            return Err(Error::schema(
                "Metadata columns are only allowed at the top level of a schema".to_string(),
            ));
        }

        match &field.data_type {
            DataType::Struct(struct_type) => {
                // Only check leaf fields; nested structs were validated at their creation
                Self::ensure_no_metadata_columns(&mut struct_type.fields().filter(|f| {
                    !matches!(f.data_type, DataType::Struct(_) | DataType::Variant(_))
                }))?;
            }
            DataType::Array(array_type) => {
                if let DataType::Struct(struct_type) = array_type.element_type() {
                    Self::ensure_no_metadata_columns(&mut struct_type.fields())?;
                }
            }
            DataType::Map(map_type) => {
                if let DataType::Struct(struct_type) = map_type.key_type() {
                    Self::ensure_no_metadata_columns(&mut struct_type.fields())?;
                }
                if let DataType::Struct(struct_type) = map_type.value_type() {
                    Self::ensure_no_metadata_columns(&mut struct_type.fields())?;
                }
            }
            // Primitive types cannot contain nested metadata columns and variant types are validated at creation
            DataType::Primitive(_) | DataType::Variant(_) => {}
        };

        Ok(())
    }

    /// Returns a StructType with `new_field` inserted after the field named `after`.
    /// If `new_field`  already presents in the schema, an error is returned.
    /// If `after` is None, `new_field` is appended to the end.
    /// If `after` is not found, an error is returned.
    pub fn with_field_inserted_after(
        mut self,
        after: Option<&str>,
        new_field: StructField,
    ) -> DeltaResult<Self> {
        // TODO: Upgrade to a case-insensitive duplicate check when this method is used for
        // user-facing operations like ALTER TABLE ADD COLUMN. Currently only used internally
        // for inserting protocol-defined fields (e.g. stats_parsed) where exact-name matching
        // is sufficient.
        if self.fields.contains_key(&new_field.name) {
            return Err(Error::generic(format!(
                "Field {} already exists",
                new_field.name
            )));
        }

        let insert_index = after
            .map(|after| {
                self.fields
                    .get_index_of(after)
                    .map(|index| index + 1)
                    .ok_or_else(|| Error::generic(format!("Field {after} not found")))
            })
            .unwrap_or_else(|| Ok(self.fields.len()))?;

        self.fields
            .insert_before(insert_index, new_field.name.clone(), new_field);
        Ok(self)
    }

    /// Returns a StructType with `new_field` inserted before the field named `before`.
    /// If `new_field` already presents in the schema, an error is returned.
    /// If `before` is None, `new_field` is inserted at the beginning.
    /// If `before` is not found, an error is returned.
    pub fn with_field_inserted_before(
        mut self,
        before: Option<&str>,
        new_field: StructField,
    ) -> DeltaResult<Self> {
        // TODO: Upgrade to a case-insensitive duplicate check when this method is used for
        // user-facing operations like ALTER TABLE ADD COLUMN. Currently only used internally
        // for inserting protocol-defined fields where exact-name matching is sufficient.
        if self.fields.contains_key(&new_field.name) {
            return Err(Error::generic(format!(
                "Field {} already exists",
                new_field.name
            )));
        }

        let index_of_before = before
            .map(|before| {
                self.fields
                    .get_index_of(before)
                    .ok_or_else(|| Error::generic(format!("Field {before} not found")))
            })
            .unwrap_or_else(|| Ok(0))?;

        self.fields
            .insert_before(index_of_before, new_field.name.clone(), new_field);
        Ok(self)
    }

    /// Returns a StructType with the named field removed.
    /// Returns self unchanged if field doesn't exist.
    pub fn with_field_removed(mut self, name: &str) -> Self {
        self.fields.shift_remove(name);
        self
    }

    /// Returns a new [`StructType`] containing only the top-level fields for which `predicate`
    /// returns `true`. This does not recurse into nested [`StructType`] fields.
    pub fn with_fields_filtered(
        &self,
        predicate: impl Fn(&StructField) -> bool,
    ) -> DeltaResult<Self> {
        Self::try_new(self.fields().filter(|f| predicate(f)).cloned())
    }

    /// Returns a StructType with the named field replaced.
    /// Returns an error if field doesn't exist.
    pub fn with_field_replaced(
        mut self,
        name: &str,
        new_field: StructField,
    ) -> DeltaResult<StructType> {
        let replace_field = self
            .fields
            .get_mut(name)
            .ok_or_else(|| Error::generic(format!("Field {name} not found")))?;

        *replace_field = new_field;
        Ok(self)
    }
}

fn write_indent(f: &mut Formatter<'_>, levels: &[bool]) -> std::fmt::Result {
    let mut it = levels.iter().peekable();

    while let Some(is_last) = it.next() {
        // Final level → draw branch
        if it.peek().is_none() {
            write!(f, "{}", if *is_last { "└─" } else { "├─" })?;
        }
        // Parent levels → vertical line or empty space
        else {
            write!(f, "{}", if *is_last { "   " } else { "│  " })?;
        }
    }

    Ok(())
}

fn write_struct_type(
    st: &StructType,
    f: &mut Formatter<'_>,
    levels: &mut Vec<bool>,
) -> std::fmt::Result {
    let len = st.fields.len();

    for (i, (_, field)) in st.fields.iter().enumerate() {
        let is_last = i + 1 == len;
        levels.push(is_last);

        write_indent(f, levels)?;
        writeln!(f, "{field}")?;

        field.data_type.fmt_recursive(f, levels)?;

        levels.pop();
    }
    Ok(())
}

impl Display for StructType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}:", self.type_name)?;
        let mut levels = Vec::new();
        write_struct_type(self, f, &mut levels)
    }
}

impl IntoIterator for StructType {
    type Item = StructField;
    type IntoIter = StructFieldIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        StructFieldIntoIter {
            inner: self.fields.into_values(),
        }
    }
}

impl<'a> IntoIterator for &'a StructType {
    type Item = &'a StructField;
    type IntoIter = StructFieldRefIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        StructFieldRefIter {
            inner: self.fields.values(),
        }
    }
}

/// An iterator that yields owned [`StructField`]s from a [`StructType`].
///
/// This iterator is returned by the [`IntoIterator`] implementation for [`StructType`] and
/// consumes the original struct. It yields each field in the order they were defined in the
/// schema, preserving the insertion order maintained by the underlying [`IndexMap`].
///
/// # Examples
///
/// ```
/// # use delta_kernel::Error;
/// use delta_kernel::schema::{StructType, StructField, DataType};
///
/// let fields = vec![
///     StructField::new("name", DataType::STRING, false),
///     StructField::new("age", DataType::INTEGER, true),
/// ];
/// let struct_type = StructType::try_new(fields)?;
///
/// // Consume the struct_type and iterate over owned fields
/// for field in struct_type {
///     println!("Field: {} ({})", field.name(), field.data_type());
/// }
/// # Ok::<(), Error>(())
/// ```
///
/// [`IndexMap`]: indexmap::IndexMap
#[derive(Debug)]
pub struct StructFieldIntoIter {
    inner: indexmap::map::IntoValues<String, StructField>,
}

impl Iterator for StructFieldIntoIter {
    type Item = StructField;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    fn count(self) -> usize {
        self.inner.count()
    }

    fn last(self) -> Option<Self::Item> {
        self.inner.last()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.inner.nth(n)
    }
}

impl ExactSizeIterator for StructFieldIntoIter {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl FusedIterator for StructFieldIntoIter {}

impl DoubleEndedIterator for StructFieldIntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

/// An iterator that yields references to [`StructField`]s from a [`StructType`].
///
/// This iterator is returned by the [`IntoIterator`] implementation for `&StructType` and by
/// the [`StructType::fields()`] method. Unlike [`StructFieldIntoIter`], this iterator does not
/// consume the original struct and yields references to the fields. It preserves the insertion
/// order maintained by the underlying [`IndexMap`].
///
/// This iterator implements [`Clone`], allowing you to create multiple independent iterators
/// over the same set of fields.
///
/// # Examples
///
/// ```
/// # use delta_kernel::Error;
/// use delta_kernel::schema::{StructType, StructField, DataType};
///
/// let fields = vec![
///     StructField::new("name", DataType::STRING, false),
///     StructField::new("age", DataType::INTEGER, true),
/// ];
/// let struct_type = StructType::try_new(fields)?;
///
/// // Iterate over field references without consuming the struct_type
/// for field in &struct_type {
///     println!("Field: {} ({})", field.name(), field.data_type());
/// }
///
/// // struct_type is still available for use
/// assert_eq!(struct_type.field("name").unwrap().name(), "name");
///
/// // Or use the fields() method explicitly
/// for field in struct_type.fields() {
///     println!("Field type: {}", field.data_type());
/// }
/// # Ok::<(), Error>(())
/// ```
///
/// [`StructType::fields()`]: StructType::fields
/// [`IndexMap`]: indexmap::IndexMap
#[derive(Debug, Clone)]
pub struct StructFieldRefIter<'a> {
    inner: indexmap::map::Values<'a, String, StructField>,
}

impl<'a> Iterator for StructFieldRefIter<'a> {
    type Item = &'a StructField;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for StructFieldRefIter<'_> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl FusedIterator for StructFieldRefIter<'_> {}

impl DoubleEndedIterator for StructFieldRefIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

struct InvariantChecker(bool);

impl<'a> SchemaTransform<'a> for InvariantChecker {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if field.has_invariants() {
            self.0 = true;
        } else if !self.0 {
            let _ = self.recurse_into_struct_field(field);
        }
        Some(Cow::Borrowed(field))
    }
}

/// Checks if any column in the schema (including nested columns) has invariants defined.
///
/// This traverses the entire schema to check for the presence of the `delta.invariants`
/// metadata key.
pub(crate) fn schema_has_invariants(schema: &Schema) -> bool {
    let mut checker = InvariantChecker(false);
    let _ = checker.transform_struct(schema);
    checker.0
}

/// Helper for RowVisitor implementations
#[internal_api]
#[derive(Clone, Default)]
pub(crate) struct ColumnNamesAndTypes(Vec<ColumnName>, Vec<DataType>);
impl ColumnNamesAndTypes {
    #[internal_api]
    pub(crate) fn as_ref(&self) -> (&[ColumnName], &[DataType]) {
        (&self.0, &self.1)
    }
}

impl From<(Vec<ColumnName>, Vec<DataType>)> for ColumnNamesAndTypes {
    fn from((names, fields): (Vec<ColumnName>, Vec<DataType>)) -> Self {
        ColumnNamesAndTypes(names, fields)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StructTypeSerDeHelper {
    #[serde(rename = "type")]
    type_name: String,
    fields: Vec<StructField>,
}

impl Serialize for StructType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StructTypeSerDeHelper {
            type_name: self.type_name.clone(),
            fields: self.fields.values().cloned().collect(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StructType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
        Self: Sized,
    {
        let helper = StructTypeSerDeHelper::deserialize(deserializer)?;
        StructType::try_new(helper.fields).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArrayType {
    #[serde(rename = "type")]
    pub type_name: String,
    /// The type of element stored in this array
    pub element_type: DataType,
    /// Denoting whether this array can contain one or more null values
    pub contains_null: bool,
}

impl ArrayType {
    pub fn new(element_type: DataType, contains_null: bool) -> Self {
        Self {
            type_name: "array".into(),
            element_type,
            contains_null,
        }
    }

    #[inline]
    pub const fn element_type(&self) -> &DataType {
        &self.element_type
    }

    #[inline]
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MapType {
    #[serde(rename = "type")]
    pub type_name: String,
    /// The type of element used for the key of this map
    pub key_type: DataType,
    /// The type of element used for the value of this map
    pub value_type: DataType,
    /// Denoting whether this map can contain one or more null values
    #[serde(default = "default_true")]
    pub value_contains_null: bool,
}

impl MapType {
    pub fn new(
        key_type: impl Into<DataType>,
        value_type: impl Into<DataType>,
        value_contains_null: bool,
    ) -> Self {
        Self {
            type_name: "map".into(),
            key_type: key_type.into(),
            value_type: value_type.into(),
            value_contains_null,
        }
    }

    #[inline]
    pub const fn key_type(&self) -> &DataType {
        &self.key_type
    }

    #[inline]
    pub const fn value_type(&self) -> &DataType {
        &self.value_type
    }

    #[inline]
    pub const fn value_contains_null(&self) -> bool {
        self.value_contains_null
    }

    /// Create a schema assuming the map is stored as a struct with the specified key and value field names
    pub fn as_struct_schema(&self, key_name: String, val_name: String) -> Schema {
        StructType::new_unchecked([
            StructField::not_null(key_name, self.key_type.clone()),
            StructField::new(val_name, self.value_type.clone(), self.value_contains_null),
        ])
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecimalType {
    precision: u8,
    scale: u8,
}

impl DecimalType {
    /// Check if the given precision and scale are valid for a decimal type.
    pub fn try_new(precision: u8, scale: u8) -> DeltaResult<Self> {
        require!(
            0 < precision && precision <= 38,
            Error::invalid_decimal(format!(
                "precision must be in range 1..38 inclusive, found: {precision}."
            ))
        );
        require!(
            scale <= precision,
            Error::invalid_decimal(format!(
                "scale must be in range 0..{precision} inclusive, found: {scale}."
            ))
        );
        Ok(Self { precision, scale })
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }
}

#[derive(Debug, Serialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PrimitiveType {
    /// UTF-8 encoded string of characters
    String,
    /// i64: 8-byte signed integer. Range: -9223372036854775808 to 9223372036854775807
    Long,
    /// i32: 4-byte signed integer. Range: -2147483648 to 2147483647
    Integer,
    /// i16: 2-byte signed integer numbers. Range: -32768 to 32767
    Short,
    /// i8: 1-byte signed integer number. Range: -128 to 127
    Byte,
    /// f32: 4-byte single-precision floating-point numbers
    Float,
    /// f64: 8-byte double-precision floating-point numbers
    Double,
    /// bool: boolean values
    Boolean,
    Binary,
    Date,
    /// Microsecond precision timestamp, adjusted to UTC.
    Timestamp,
    #[serde(rename = "timestamp_ntz")]
    TimestampNtz,
    #[serde(serialize_with = "serialize_decimal", untagged)]
    Decimal(DecimalType),
}

impl PrimitiveType {
    pub fn decimal(precision: u8, scale: u8) -> DeltaResult<Self> {
        Ok(DecimalType::try_new(precision, scale)?.into())
    }

    /// Returns `true` if this primitive type can be widened to the `target` type.
    ///
    /// Widening rules:
    /// - Integer widening: byte -> short -> int -> long (Delta protocol type widening)
    /// - Float widening: float -> double (Delta protocol type widening)
    /// - Timestamp interchangeability: Timestamp <-> TimestampNtz (both are i64 microseconds
    ///   since epoch, differing only in timezone semantics; this is a physical read
    ///   accommodation, not a Delta protocol type widening rule)
    pub(crate) fn can_widen_to(&self, target: &Self) -> bool {
        use PrimitiveType::*;
        matches!(
            (self, target),
            // Integer widening: smaller types can be read as larger ones
            (Byte, Short | Integer | Long)
                | (Short, Integer | Long)
                | (Integer, Long)
                // Float widening: float can be read as double
                | (Float, Double)
                // Timestamp equivalence: both are i64 microseconds since epoch, differing only
                // in timezone semantics. The parquet representation is identical, so reading
                // one as the other is safe at the data layer.
                | (Timestamp, TimestampNtz)
                | (TimestampNtz, Timestamp)
        )
    }

    /// Returns `true` if this primitive type is compatible with `target` for reading
    /// `stats_parsed` columns from checkpoint parquet files.
    ///
    /// This is a superset of [`can_widen_to`]: it includes all Delta protocol type widening
    /// rules plus physical Parquet encoding accommodations for checkpoint interop. Some
    /// external checkpoint writers omit Parquet logical type annotations, producing plain
    /// integer-typed columns for logically date or timestamp values:
    /// - Integer -> Date (int32 stored without DATE annotation)
    /// - Long -> Timestamp/TimestampNtz (int64 stored without TIMESTAMP annotation)
    ///
    /// These reinterpretation cases are intentionally not part of [`can_widen_to`] because
    /// they are not Delta protocol type widening rules and should not apply to general
    /// schema compatibility checks.
    ///
    /// [`can_widen_to`]: PrimitiveType::can_widen_to
    pub(crate) fn is_stats_type_compatible_with(&self, target: &Self) -> bool {
        self == target
            || self.can_widen_to(target)
            || matches!(
                (self, target),
                (Self::Integer, Self::Date) | (Self::Long, Self::Timestamp | Self::TimestampNtz)
            )
    }
}

fn serialize_decimal<S: serde::Serializer>(
    dtype: &DecimalType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("decimal({},{})", dtype.precision(), dtype.scale()))
}

fn serialize_variant<S: serde::Serializer>(
    _: &StructType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str("variant")
}

// Custom Deserialize to provide clear error messages for unsupported types.
// The derived impl would produce: "unknown variant `interval second`, expected one of ..."
// This impl produces: "Unsupported Delta table type: 'interval second'"
impl<'de> serde::Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str_value = String::deserialize(deserializer)?;

        match str_value.as_str() {
            "string" => Ok(PrimitiveType::String),
            "long" => Ok(PrimitiveType::Long),
            "integer" => Ok(PrimitiveType::Integer),
            "short" => Ok(PrimitiveType::Short),
            "byte" => Ok(PrimitiveType::Byte),
            "float" => Ok(PrimitiveType::Float),
            "double" => Ok(PrimitiveType::Double),
            "boolean" => Ok(PrimitiveType::Boolean),
            "binary" => Ok(PrimitiveType::Binary),
            "date" => Ok(PrimitiveType::Date),
            "timestamp" => Ok(PrimitiveType::Timestamp),
            "timestamp_ntz" => Ok(PrimitiveType::TimestampNtz),
            decimal_str if decimal_str.starts_with("decimal(") && decimal_str.ends_with(')') => {
                // Parse decimal type
                let mut parts = decimal_str[8..decimal_str.len() - 1].split(',');
                let precision = parts
                    .next()
                    .and_then(|part| part.trim().parse::<u8>().ok())
                    .ok_or_else(|| {
                        serde::de::Error::custom(format!(
                            "Invalid precision in decimal: {decimal_str}"
                        ))
                    })?;
                let scale = parts
                    .next()
                    .and_then(|part| part.trim().parse::<u8>().ok())
                    .ok_or_else(|| {
                        serde::de::Error::custom(format!("Invalid scale in decimal: {decimal_str}"))
                    })?;
                // Reject extra parts (e.g., decimal(10,2,99))
                if parts.next().is_some() {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid decimal format (expected 2 parts): {decimal_str}"
                    )));
                }
                DecimalType::try_new(precision, scale)
                    .map(PrimitiveType::Decimal)
                    .map_err(serde::de::Error::custom)
            }
            unsupported => Err(serde::de::Error::custom(format!(
                "Unsupported Delta table type: '{unsupported}'"
            ))),
        }
    }
}

impl Display for PrimitiveType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Integer => write!(f, "integer"),
            PrimitiveType::Short => write!(f, "short"),
            PrimitiveType::Byte => write!(f, "byte"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Binary => write!(f, "binary"),
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::TimestampNtz => write!(f, "timestamp_ntz"),
            PrimitiveType::Decimal(dtype) => {
                write!(f, "decimal({},{})", dtype.precision(), dtype.scale())
            }
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Clone, Eq)]
#[serde(untagged, rename_all = "camelCase")]
pub enum DataType {
    /// UTF-8 encoded string of characters
    Primitive(PrimitiveType),
    /// An array stores a variable length collection of items of some type.
    Array(Box<ArrayType>),
    /// A struct is used to represent both the top-level schema of the table as well
    /// as struct columns that contain nested columns.
    Struct(Box<StructType>),
    /// A map stores an arbitrary length collection of key-value pairs
    /// with a single keyType and a single valueType
    Map(Box<MapType>),
    /// The Variant data type. The physical representation can be flexible to support shredded
    /// reads. The unshredded schema is `Variant(StructType<metadata: BINARY, value: BINARY>)`.
    #[serde(serialize_with = "serialize_variant")]
    Variant(Box<StructType>),
}

impl From<DecimalType> for PrimitiveType {
    fn from(dtype: DecimalType) -> Self {
        PrimitiveType::Decimal(dtype)
    }
}
impl From<DecimalType> for DataType {
    fn from(dtype: DecimalType) -> Self {
        PrimitiveType::from(dtype).into()
    }
}
impl From<PrimitiveType> for DataType {
    fn from(ptype: PrimitiveType) -> Self {
        DataType::Primitive(ptype)
    }
}
impl From<MapType> for DataType {
    fn from(map_type: MapType) -> Self {
        DataType::Map(Box::new(map_type))
    }
}

impl From<StructType> for DataType {
    fn from(struct_type: StructType) -> Self {
        DataType::Struct(Box::new(struct_type))
    }
}

impl From<ArrayType> for DataType {
    fn from(array_type: ArrayType) -> Self {
        DataType::Array(Box::new(array_type))
    }
}

impl From<SchemaRef> for DataType {
    fn from(schema: SchemaRef) -> Self {
        Arc::unwrap_or_clone(schema).into()
    }
}

// Custom Deserialize to preserve error messages from PrimitiveType.
// Serde's untagged enum only reports the last variant's error, discarding PrimitiveType's
// clear "Unsupported Delta table type: 'X'" message. We deserialize to Value first, then
// dispatch based on structure (string -> Primitive/Variant, object -> Array/Struct/Map).
impl<'de> serde::Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        use serde_json::Value;

        let value = Value::deserialize(deserializer)?;

        // String values are either primitive types or "variant"
        if let Value::String(s) = &value {
            if s == "variant" {
                return match DataType::unshredded_variant() {
                    DataType::Variant(st) => Ok(DataType::Variant(st)),
                    _ => Err(Error::custom("Failed to create variant type")),
                };
            }

            // Try PrimitiveType - this will give us good error messages for unsupported types
            return PrimitiveType::deserialize(value.clone())
                .map(DataType::Primitive)
                .map_err(|e| Error::custom(e.to_string()));
        }

        // Object values are complex types - dispatch based on "type" field
        if let Value::Object(map) = &value {
            if let Some(Value::String(type_str)) = map.get("type") {
                return match type_str.as_str() {
                    "array" => ArrayType::deserialize(value)
                        .map(|at| DataType::Array(Box::new(at)))
                        .map_err(|e| Error::custom(e.to_string())),
                    "struct" => StructType::deserialize(value)
                        .map(|st| DataType::Struct(Box::new(st)))
                        .map_err(|e| Error::custom(e.to_string())),
                    "map" => MapType::deserialize(value)
                        .map(|mt| DataType::Map(Box::new(mt)))
                        .map_err(|e| Error::custom(e.to_string())),
                    _ => Err(Error::custom(format!("Unknown complex type: '{type_str}'"))),
                };
            }
        }

        // Fallback error with the actual value that failed
        Err(Error::custom(format!(
            "Invalid data type: {}",
            serde_json::to_string(&value).unwrap_or_else(|_| format!("{value:?}"))
        )))
    }
}

/// cbindgen:ignore
impl DataType {
    pub const STRING: Self = DataType::Primitive(PrimitiveType::String);
    pub const LONG: Self = DataType::Primitive(PrimitiveType::Long);
    pub const INTEGER: Self = DataType::Primitive(PrimitiveType::Integer);
    pub const SHORT: Self = DataType::Primitive(PrimitiveType::Short);
    pub const BYTE: Self = DataType::Primitive(PrimitiveType::Byte);
    pub const FLOAT: Self = DataType::Primitive(PrimitiveType::Float);
    pub const DOUBLE: Self = DataType::Primitive(PrimitiveType::Double);
    pub const BOOLEAN: Self = DataType::Primitive(PrimitiveType::Boolean);
    pub const BINARY: Self = DataType::Primitive(PrimitiveType::Binary);
    pub const DATE: Self = DataType::Primitive(PrimitiveType::Date);
    pub const TIMESTAMP: Self = DataType::Primitive(PrimitiveType::Timestamp);
    pub const TIMESTAMP_NTZ: Self = DataType::Primitive(PrimitiveType::TimestampNtz);

    /// Create a new decimal type with the given precision and scale.
    pub fn decimal(precision: u8, scale: u8) -> DeltaResult<Self> {
        Ok(PrimitiveType::decimal(precision, scale)?.into())
    }

    /// Create a new struct type with the given fields.
    pub fn try_struct_type(fields: impl IntoIterator<Item = StructField>) -> DeltaResult<Self> {
        Ok(StructType::try_new(fields)?.into())
    }

    /// Create a new struct type from a fallible iterator of fields.
    pub fn try_struct_type_from_results<E: Into<Error>>(
        fields: impl IntoIterator<Item = Result<StructField, E>>,
    ) -> DeltaResult<Self> {
        StructType::try_from_results(fields).map(Self::from)
    }

    /// Create a new struct type with the given fields without validating them.
    pub(crate) fn struct_type_unchecked(fields: impl IntoIterator<Item = StructField>) -> Self {
        StructType::new_unchecked(fields).into()
    }

    /// Create a new unshredded [`DataType::Variant`]. This data type is a struct of two not-null
    /// binary fields: `metadata` and `value`.
    pub fn unshredded_variant() -> Self {
        DataType::Variant(Box::new(StructType::new_unchecked([
            StructField::not_null("metadata", DataType::BINARY),
            StructField::not_null("value", DataType::BINARY),
        ])))
    }

    /// Create a new [`DataType::Variant`] from the provided fields. For unshredded variants, you
    /// should prefer using [`DataType::unshredded_variant`].
    pub fn variant_type(fields: impl IntoIterator<Item = StructField>) -> DeltaResult<Self> {
        // Different from regular StructTypes, Variants are not allowed to contain metadata columns
        // at all, so we also need to check their top-level primitive types.
        Ok(DataType::Variant(Box::new(StructType::try_from_results(
            fields.into_iter().map(|field| {
                if field.is_metadata_column() {
                    Err(Error::schema(
                        "Metadata columns are not allowed in Variant types".to_string(),
                    ))
                } else {
                    Ok(field)
                }
            }),
        )?)))
    }

    /// Attempt to convert this data type to a [`PrimitiveType`]. Returns `None` if this is a
    /// non-primitive type.
    pub fn as_primitive_opt(&self) -> Option<&PrimitiveType> {
        match self {
            DataType::Primitive(ptype) => Some(ptype),
            _ => None,
        }
    }

    fn fmt_recursive(&self, f: &mut Formatter<'_>, levels: &mut Vec<bool>) -> std::fmt::Result {
        match self {
            DataType::Struct(inner) => write_struct_type(inner, f, levels),

            DataType::Array(inner) => {
                levels.push(true); // only one child → last
                write_indent(f, levels)?;
                writeln!(f, "array_element: {}", inner.element_type)?;
                inner.element_type.fmt_recursive(f, levels)?;
                levels.pop();
                Ok(())
            }

            DataType::Map(inner) => {
                // key
                levels.push(false); // map_key is NOT last
                write_indent(f, levels)?;
                writeln!(f, "map_key: {}", inner.key_type)?;
                inner.key_type.fmt_recursive(f, levels)?;
                levels.pop();

                // value
                levels.push(true); // map_value IS last at this level
                write_indent(f, levels)?;
                writeln!(f, "map_value: {}", inner.value_type)?;
                inner.value_type.fmt_recursive(f, levels)?;
                levels.pop();
                Ok(())
            }

            _ => Ok(()),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Primitive(p) => write!(f, "{p}"),
            DataType::Array(a) => write!(f, "array<{}>", a.element_type),
            DataType::Struct(s) => {
                write!(f, "struct<")?;
                for (i, field) in s.fields().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Map(m) => write!(f, "map<{}, {}>", m.key_type, m.value_type),
            DataType::Variant(_) => write!(f, "variant"),
        }
    }
}

struct GetSchemaLeaves {
    path: Vec<String>,
    names: Vec<ColumnName>,
    types: Vec<DataType>,
}
impl GetSchemaLeaves {
    fn new(own_name: Option<&str>) -> Self {
        Self {
            path: own_name.into_iter().map(|s| s.to_string()).collect(),
            names: vec![],
            types: vec![],
        }
    }
}

impl<'a> SchemaTransform<'a> for GetSchemaLeaves {
    fn transform_struct_field(&mut self, field: &StructField) -> Option<Cow<'a, StructField>> {
        self.path.push(field.name.clone());
        if let DataType::Struct(_) = field.data_type {
            let _ = self.recurse_into_struct_field(field);
        } else {
            self.names.push(ColumnName::new(&self.path));
            self.types.push(field.data_type.clone());
        }
        self.path.pop();
        None
    }
}

struct MakePhysical<'a> {
    column_mapping_mode: ColumnMappingMode,
    path: Vec<&'a str>,
    seen: HashMap<i64, &'a str>,
    err: Option<Error>,
}
impl<'a> MakePhysical<'a> {
    fn new(column_mapping_mode: ColumnMappingMode) -> Self {
        Self {
            column_mapping_mode,
            path: vec![],
            seen: HashMap::new(),
            err: None,
        }
    }

    /// Transforms a single [`StructField`] from logical to physical. Returns the physical
    /// field on success, or the first error encountered during the recursive transformation.
    fn run_field(&mut self, field: &'a StructField) -> DeltaResult<StructField> {
        let result = self.transform_struct_field(field);
        match (self.err.take(), result) {
            (Some(err), _) => Err(err),
            // Theoretically impossible: MakePhysical only returns None when it sets an error
            (None, None) => Err(Error::internal_error(
                "make_physical: transform returned None without setting an error",
            )),
            (None, Some(field)) => Ok(field.into_owned()),
        }
    }

    fn transform_inner<T>(
        &mut self,
        field_name: &'a str,
        transform: impl FnOnce(&mut Self) -> Option<T>,
    ) -> Option<T> {
        if self.err.is_some() {
            return None;
        }
        self.path.push(field_name);
        let result = transform(self);
        self.path.pop();
        result
    }
}
impl<'a> SchemaTransform<'a> for MakePhysical<'a> {
    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner("<array element>", |this| this.transform(etype))
    }
    fn transform_map_key(&mut self, ktype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner("<map key>", |this| this.transform(ktype))
    }
    fn transform_map_value(&mut self, vtype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner("<map value>", |this| this.transform(vtype))
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.transform_inner(field.name(), |this| {
            let (physical_name, _id) = get_field_column_mapping_info(
                field,
                this.column_mapping_mode,
                &this.path,
                Some(&mut this.seen),
            )
            .map_err(|e| this.err = Some(e))
            .ok()?;

            if field.is_metadata_column() {
                return Some(Cow::Borrowed(field));
            }

            let field = this.recurse_into_struct_field(field)?;

            let metadata = field.logical_to_physical_metadata(this.column_mapping_mode);
            let name = physical_name.to_owned();

            Some(Cow::Owned(field.with_name(name).with_metadata(metadata)))
        })
    }

    fn transform_variant(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        // There is no column mapping metadata inside the struct fields of a variant, so
        // we do not recurse into the variant fields
        Some(Cow::Borrowed(stype))
    }
}

#[cfg(test)]
mod tests {
    use crate::table_features::ColumnMappingMode;
    use crate::utils::test_utils::{
        assert_result_error_with_message, test_deep_nested_schema_missing_leaf_cm,
    };

    use super::*;
    use rstest::rstest;
    use serde_json;

    fn example_schema_metadata() -> &'static str {
        r#"
            {
                "name": "e",
                "type": {
                    "type": "array",
                    "elementType": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "d",
                                "type": "integer",
                                "nullable": false,
                                "metadata": {
                                    "delta.columnMapping.id": 5,
                                    "delta.columnMapping.physicalName": "col-a7f4159c-53be-4cb0-b81a-f7e5240cfc49"
                                }
                            }
                        ]
                    },
                    "containsNull": true
                },
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 4,
                    "delta.columnMapping.physicalName": "col-5f422f40-de70-45b2-88ab-1d5c90e94db1",
                    "delta.identity.start": 2147483648
                }
            }"#
    }

    #[test]
    fn test_serde_data_types() {
        let data = r#"
        {
            "name": "a",
            "type": "integer",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::INTEGER));

        let data = r#"
        {
            "name": "c",
            "type": {
                "type": "array",
                "elementType": "integer",
                "containsNull": false
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Array(_)));

        let data = r#"
        {
            "name": "e",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "d",
                            "type": "integer",
                            "nullable": false,
                            "metadata": {}
                        }
                    ]
                },
                "containsNull": true
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Array(_)));
        match field.data_type {
            DataType::Array(array) => assert!(matches!(array.element_type, DataType::Struct(_))),
            _ => unreachable!(),
        }

        let data = r#"
        {
            "name": "f",
            "type": {
                "type": "map",
                "keyType": "string",
                "valueType": "string",
                "valueContainsNull": true
            },
            "nullable": true,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(matches!(field.data_type, DataType::Map(_)));
    }

    #[test]
    fn test_roundtrip_decimal() {
        let data = r#"
        {
            "name": "a",
            "type": "decimal(10, 2)",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert_eq!(field.data_type, DataType::decimal(10, 2).unwrap());

        let json_str = serde_json::to_string(&field).unwrap();
        assert_eq!(
            json_str,
            r#"{"name":"a","type":"decimal(10,2)","nullable":false,"metadata":{}}"#
        );
    }

    #[test]
    fn test_roundtrip_variant() {
        let data = r#"
        {
            "name": "v",
            "type": "variant",
            "nullable": false,
            "metadata": {}
        }
        "#;
        let field: StructField = serde_json::from_str(data).unwrap();
        assert_eq!(field.data_type, DataType::unshredded_variant());

        let json_str = serde_json::to_string(&field).unwrap();
        assert_eq!(
            json_str,
            r#"{"name":"v","type":"variant","nullable":false,"metadata":{}}"#
        );
    }

    #[test]
    fn test_unshredded_variant() {
        let unshredded_variant_type = DataType::unshredded_variant();

        match &unshredded_variant_type {
            DataType::Variant(struct_type) => {
                let fields: Vec<_> = struct_type.fields().collect();
                assert_eq!(fields.len(), 2);

                assert_eq!(fields[0].name, "metadata");
                assert_eq!(fields[0].data_type, DataType::BINARY);
                assert!(!fields[0].nullable);

                assert_eq!(fields[1].name, "value");
                assert_eq!(fields[1].data_type, DataType::BINARY);
                assert!(!fields[1].nullable);
            }
            _ => panic!("Expected DataType::Variant, got {unshredded_variant_type:?}"),
        }
    }

    #[rstest]
    #[case("interval second")]
    #[case("interval day")]
    #[case("money")]
    fn test_unsupported_type_error_message(#[case] unsupported_type: &str) {
        let data = format!(
            r#"{{
                "name": "test_field",
                "type": "{unsupported_type}",
                "nullable": false,
                "metadata": {{}}
            }}"#
        );
        let result: Result<StructField, _> = serde_json::from_str(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let expected_msg = format!("Unsupported Delta table type: '{unsupported_type}'");
        assert!(
            err.to_string().contains(&expected_msg),
            "Expected error message about unsupported type '{unsupported_type}', got: {err}"
        );
    }

    #[rstest]
    #[case("string", DataType::STRING)]
    #[case("long", DataType::LONG)]
    #[case("integer", DataType::INTEGER)]
    #[case("short", DataType::SHORT)]
    #[case("byte", DataType::BYTE)]
    #[case("float", DataType::FLOAT)]
    #[case("double", DataType::DOUBLE)]
    #[case("boolean", DataType::BOOLEAN)]
    #[case("binary", DataType::BINARY)]
    #[case("date", DataType::DATE)]
    #[case("timestamp", DataType::TIMESTAMP)]
    #[case("timestamp_ntz", DataType::TIMESTAMP_NTZ)]
    fn test_primitive_type_deserialization_still_works(
        #[case] type_str: &str,
        #[case] expected_type: DataType,
    ) {
        let data = format!(
            r#"{{
                "name": "test_field",
                "type": "{type_str}",
                "nullable": false,
                "metadata": {{}}
            }}"#
        );
        let field: StructField = serde_json::from_str(&data).unwrap();
        assert_eq!(field.data_type, expected_type);
    }

    #[rstest]
    #[case(10, 2)]
    #[case(16, 4)]
    #[case(38, 10)]
    fn test_decimal_with_primitive_deserializer(#[case] precision: u8, #[case] scale: u8) {
        let data = format!(
            r#"{{
                "name": "test_decimal",
                "type": "decimal({precision},{scale})",
                "nullable": false,
                "metadata": {{}}
            }}"#
        );
        let field: StructField = serde_json::from_str(&data).unwrap();
        assert_eq!(
            field.data_type,
            DataType::decimal(precision, scale).unwrap()
        );
    }

    #[rstest]
    #[case("decimal(invalid)", "Invalid precision in decimal")]
    #[case("decimal(10)", "Invalid scale in decimal")]
    #[case("decimal()", "Invalid precision in decimal")]
    #[case("decimal(10,2,99)", "Invalid decimal format (expected 2 parts)")]
    fn test_invalid_decimal_format(#[case] invalid_type: &str, #[case] expected_error: &str) {
        let data = format!(
            r#"{{
                "name": "invalid",
                "type": "{invalid_type}",
                "nullable": false,
                "metadata": {{}}
            }}"#
        );
        let result: Result<StructField, _> = serde_json::from_str(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains(expected_error),
            "Expected error containing '{expected_error}', got: {err}"
        );
    }

    #[rstest]
    #[case(
        r#"{"type": "array", "elementType": "integer", "containsNull": false}"#,
        DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false)))
    )]
    #[case(
        r#"{"type": "struct", "fields": [{"name": "a", "type": "integer", "nullable": false, "metadata": {}}, {"name": "b", "type": "string", "nullable": true, "metadata": {}}]}"#,
        DataType::Struct(Box::new(StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::STRING, true),
        ])))
    )]
    #[case(
        r#"{"type": "map", "keyType": "string", "valueType": "integer", "valueContainsNull": true}"#,
        DataType::Map(Box::new(MapType::new(DataType::STRING, DataType::INTEGER, true)))
    )]
    #[case("\"string\"", DataType::STRING)]
    #[case("\"long\"", DataType::LONG)]
    #[case("\"integer\"", DataType::INTEGER)]
    #[case("\"short\"", DataType::SHORT)]
    #[case("\"byte\"", DataType::BYTE)]
    #[case("\"float\"", DataType::FLOAT)]
    #[case("\"double\"", DataType::DOUBLE)]
    #[case("\"boolean\"", DataType::BOOLEAN)]
    #[case("\"binary\"", DataType::BINARY)]
    #[case("\"date\"", DataType::DATE)]
    #[case("\"timestamp\"", DataType::TIMESTAMP)]
    #[case("\"timestamp_ntz\"", DataType::TIMESTAMP_NTZ)]
    #[case("\"variant\"", DataType::unshredded_variant())]
    fn test_data_type_deserialization(#[case] type_json: &str, #[case] expected: DataType) {
        let data_type: DataType = serde_json::from_str(type_json).unwrap();
        assert_eq!(data_type, expected);
    }

    #[test]
    fn test_make_physical_no_column_mapping() {
        let field = StructField::nullable(
            "e",
            ArrayType::new(
                StructType::new_unchecked([StructField::not_null("d", DataType::INTEGER)]).into(),
                true,
            ),
        );
        let physical_field = field.make_physical(ColumnMappingMode::None).unwrap();

        assert_eq!(physical_field.name, "e");
        assert!(physical_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .is_none());
        assert!(physical_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .is_none());

        let DataType::Array(atype) = physical_field.data_type else {
            panic!("Expected an Array");
        };
        let DataType::Struct(stype) = atype.element_type else {
            panic!("Expected a Struct");
        };
        let struct_field = stype.fields.get_index(0).unwrap().1;
        assert_eq!(struct_field.name, "d");
    }

    #[test]
    fn test_make_physical_rejects_annotated_fields_when_column_mapping_disabled() {
        let data = example_schema_metadata();
        let field: StructField = serde_json::from_str(data).unwrap();
        assert!(field.make_physical(ColumnMappingMode::None).is_err());
    }

    #[test]
    fn test_make_physical_rejects_unannotated_leaf_in_deep_nesting() {
        let schema = test_deep_nested_schema_missing_leaf_cm();
        let field = schema.fields().next().unwrap();
        let err = field
            .make_physical(ColumnMappingMode::Name)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("top.`<array element>`.mid_field.`<map value>`.leaf"),
            "Expected full nested path in error, got: {err}"
        );
    }

    #[test]
    fn test_make_physical_rejects_duplicate_column_mapping_ids() {
        use crate::schema::ColumnMetadataKey;

        fn cm_field(name: &str, id: i64, data_type: impl Into<DataType>) -> StructField {
            StructField::not_null(name, data_type).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(id),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String(format!("col-{name}")),
                ),
            ])
        }

        let inner = StructType::new_unchecked([
            cm_field("x", 3, DataType::INTEGER),
            cm_field("y", 4, DataType::STRING),
        ]);
        let schema = StructType::new_unchecked([
            cm_field("a", 1, DataType::INTEGER),
            cm_field(
                "b",
                2,
                ArrayType::new(DataType::Struct(Box::new(inner)), true),
            ),
            cm_field("c", 3, DataType::STRING),
        ]);
        assert_result_error_with_message(
            schema.make_physical(ColumnMappingMode::Id),
            "Duplicate column mapping ID",
        );
    }

    #[test]
    fn test_make_physical_column_mapping() {
        [ColumnMappingMode::Name, ColumnMappingMode::Id]
            .into_iter()
            .for_each(|mode| {
                let data = example_schema_metadata();

                let field: StructField = serde_json::from_str(data).unwrap();

                let col_id = field
                    .get_config_value(&ColumnMetadataKey::ColumnMappingId)
                    .unwrap();
                let id_start = field
                    .get_config_value(&ColumnMetadataKey::IdentityStart)
                    .unwrap();
                assert!(matches!(col_id, MetadataValue::Number(num) if *num == 4));
                assert!(matches!(id_start, MetadataValue::Number(num) if *num == 2147483648i64));
                assert_eq!(
                    field.physical_name(mode),
                    "col-5f422f40-de70-45b2-88ab-1d5c90e94db1"
                );
                let physical_field = field.make_physical(mode).unwrap();

                // Parquet field id should only be present in id column mapping mode
                match mode {
                    ColumnMappingMode::Id => {
                        assert!(matches!(
                            physical_field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
                            Some(MetadataValue::Number(4))
                        ));

                        assert!(matches!(
                            physical_field.get_config_value(&ColumnMetadataKey::ColumnMappingId),
                            Some(MetadataValue::Number(4))
                        ));
                    }
                    ColumnMappingMode::Name => {
                        assert!(matches!(
                            physical_field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
                            Some(MetadataValue::Number(4))
                        ));
                        assert!(matches!(
                            physical_field.get_config_value(&ColumnMetadataKey::ColumnMappingId),
                            Some(MetadataValue::Number(4))
                        ));
                    }
                    ColumnMappingMode::None => panic!("unexpected column mapping mode"),
                }

                assert_eq!(
                    physical_field.name,
                    "col-5f422f40-de70-45b2-88ab-1d5c90e94db1"
                );
                let DataType::Array(atype) = physical_field.data_type else {
                    panic!("Expected an Array");
                };
                let DataType::Struct(stype) = atype.element_type else {
                    panic!("Expected a Struct");
                };

                let struct_field = stype.fields.get_index(0).unwrap().1;
                assert_eq!(
                    struct_field.name,
                    "col-a7f4159c-53be-4cb0-b81a-f7e5240cfc49"
                );

                // The subfield should also have ParquetFieldId present it column mapping id mode
                match mode {
                    ColumnMappingMode::Id => {
                        assert!(matches!(
                            struct_field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
                            Some(MetadataValue::Number(5))
                        ));
                        assert!(matches!(
                            struct_field.get_config_value(&ColumnMetadataKey::ColumnMappingId),
                            Some(MetadataValue::Number(5))
                        ));
                    }
                    ColumnMappingMode::Name => {
                        assert!(matches!(
                            struct_field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
                            Some(MetadataValue::Number(5))
                        ));
                        assert!(matches!(
                            struct_field.get_config_value(&ColumnMetadataKey::ColumnMappingId),
                            Some(MetadataValue::Number(5))
                        ));
                    }
                    ColumnMappingMode::None => panic!("unexpected column mapping mode"),
                }
            });
    }

    #[test]
    fn test_make_physical_passes_metadata_column_through() {
        let field = StructField::create_metadata_column(
            "_metadata.row_index",
            MetadataColumnSpec::RowIndex,
        );
        for mode in [
            ColumnMappingMode::None,
            ColumnMappingMode::Name,
            ColumnMappingMode::Id,
        ] {
            let physical = field.make_physical(mode).unwrap();
            assert_eq!(physical.name(), "_metadata.row_index");
            assert!(physical.is_metadata_column());
        }
    }

    #[test]
    fn test_make_physical_rejects_metadata_column_with_cm_annotations() {
        let field = StructField::create_metadata_column(
            "_metadata.row_index",
            MetadataColumnSpec::RowIndex,
        )
        .add_metadata([(
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String("phys".to_string()),
        )]);
        assert_result_error_with_message(
            field.make_physical(ColumnMappingMode::Name),
            "must not have column mapping annotations",
        );
    }

    #[test]
    fn test_read_schemas() {
        let file = std::fs::File::open("./tests/serde/schema.json").unwrap();
        let schema: Result<Schema, _> = serde_json::from_reader(file);
        assert!(schema.is_ok());

        let file = std::fs::File::open("./tests/serde/checkpoint_schema.json").unwrap();
        let schema: Result<Schema, _> = serde_json::from_reader(file);
        assert!(schema.is_ok())
    }

    #[test]
    fn test_invalid_decimal() {
        let data = r#"
        {
            "name": "a",
            "type": "decimal(39, 10)",
            "nullable": false,
            "metadata": {}
        }
        "#;
        assert!(serde_json::from_str::<StructField>(data).is_err());

        let data = r#"
        {
            "name": "a",
            "type": "decimal(10, 39)",
            "nullable": false,
            "metadata": {}
        }
        "#;
        assert!(serde_json::from_str::<StructField>(data).is_err());
    }

    #[test]
    fn test_metadata_value_to_string() {
        assert_eq!(MetadataValue::Number(0).to_string(), "0");
        assert_eq!(
            MetadataValue::String("hello".to_string()).to_string(),
            "hello"
        );
        assert_eq!(MetadataValue::Boolean(true).to_string(), "true");
        assert_eq!(MetadataValue::Boolean(false).to_string(), "false");
        let object_json = serde_json::json!({ "an": "object" });
        assert_eq!(
            MetadataValue::Other(object_json).to_string(),
            "{\"an\":\"object\"}"
        );
        let array_json = serde_json::json!(["an", "array"]);
        assert_eq!(
            MetadataValue::Other(array_json).to_string(),
            "[\"an\",\"array\"]"
        );
    }

    #[test]
    fn test_num_fields() {
        let schema = StructType::new_unchecked([]);
        assert!(schema.num_fields() == 0);
        let schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
            StructField::nullable("d", DataType::LONG),
        ]);
        assert_eq!(schema.num_fields(), 4);
        let schema = StructType::new_unchecked([
            StructField::nullable("b", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
            StructField::nullable("c", DataType::LONG),
        ]);
        assert_eq!(schema.num_fields(), 2);
    }

    #[test]
    fn test_has_invariants() {
        // Schema with no invariants
        let schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
        ]);
        assert!(!schema_has_invariants(&schema));

        // Schema with top-level invariant
        let mut field = StructField::nullable("c", DataType::STRING);
        field.metadata.insert(
            ColumnMetadataKey::Invariants.as_ref().to_string(),
            MetadataValue::String("c > 0".to_string()),
        );

        let schema =
            StructType::new_unchecked([StructField::nullable("a", DataType::STRING), field]);
        assert!(schema_has_invariants(&schema));

        // Schema with nested invariant in a struct
        let nested_field = StructField::nullable(
            "nested_c",
            DataType::try_struct_type([{
                let mut field = StructField::nullable("d", DataType::INTEGER);
                field.metadata.insert(
                    ColumnMetadataKey::Invariants.as_ref().to_string(),
                    MetadataValue::String("d > 0".to_string()),
                );
                field
            }])
            .unwrap(),
        );

        let schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
            nested_field,
        ]);
        assert!(schema_has_invariants(&schema));

        // Schema with nested invariant in an array of structs
        let array_field = StructField::nullable(
            "array_field",
            ArrayType::new(
                DataType::try_struct_type([{
                    let mut field = StructField::nullable("d", DataType::INTEGER);
                    field.metadata.insert(
                        ColumnMetadataKey::Invariants.as_ref().to_string(),
                        MetadataValue::String("d > 0".to_string()),
                    );
                    field
                }])
                .unwrap(),
                true,
            ),
        );

        let schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
            array_field,
        ]);
        assert!(schema_has_invariants(&schema));

        // Schema with nested invariant in a map value that's a struct
        let map_field = StructField::nullable(
            "map_field",
            MapType::new(
                DataType::STRING,
                DataType::try_struct_type([{
                    let mut field = StructField::nullable("d", DataType::INTEGER);
                    field.metadata.insert(
                        ColumnMetadataKey::Invariants.as_ref().to_string(),
                        MetadataValue::String("d > 0".to_string()),
                    );
                    field
                }])
                .unwrap(),
                true,
            ),
        );

        let schema = StructType::new_unchecked([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::INTEGER),
            map_field,
        ]);
        assert!(schema_has_invariants(&schema));
    }

    #[test]
    fn test_struct_type_iterator_basic() {
        let fields = vec![
            StructField::new("field1", DataType::STRING, true),
            StructField::new("field2", DataType::INTEGER, false),
            StructField::new("field3", DataType::BOOLEAN, true),
        ];
        let struct_type = StructType::new_unchecked(fields.clone());

        // Test fields() method returns reference iterator
        let field_names: Vec<_> = struct_type.fields().map(|f| f.name()).collect();
        assert_eq!(field_names, vec!["field1", "field2", "field3"]);

        // Test that we can still access the struct_type after using fields()
        assert_eq!(struct_type.field("field1").unwrap().name, "field1");
    }

    #[test]
    fn test_struct_type_into_iterator_owned() {
        let fields = vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b", DataType::INTEGER, false),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Test owned iteration (consumes the struct)
        let mut field_names = Vec::new();
        for field in struct_type {
            field_names.push(field.name);
        }
        assert_eq!(field_names, vec!["a", "b"]);
    }

    #[test]
    fn test_struct_type_into_iterator_references() {
        let fields = vec![
            StructField::new("x", DataType::DOUBLE, true),
            StructField::new("y", DataType::FLOAT, false),
            StructField::new("z", DataType::LONG, true),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Test reference iteration (does not consume the struct)
        let mut field_names = Vec::new();
        for field in &struct_type {
            field_names.push(field.name().clone());
        }
        assert_eq!(field_names, vec!["x", "y", "z"]);

        // Should still be able to use struct_type after iteration
        assert_eq!(struct_type.field("x").unwrap().name, "x");
    }

    #[test]
    fn test_iterator_exact_size() {
        let fields = vec![
            StructField::new("field1", DataType::STRING, true),
            StructField::new("field2", DataType::INTEGER, false),
            StructField::new("field3", DataType::BOOLEAN, true),
            StructField::new("field4", DataType::DATE, true),
        ];

        // Test ExactSizeIterator for reference iterator
        let struct_type = StructType::new_unchecked(fields.clone());
        let ref_iter = struct_type.fields();
        assert_eq!(ref_iter.len(), 4);

        // Test ExactSizeIterator for &StructType into_iter
        let struct_type = StructType::new_unchecked(fields.clone());
        let into_ref_iter = (&struct_type).into_iter();
        assert_eq!(into_ref_iter.len(), 4);

        // Test ExactSizeIterator for StructType into_iter (consuming)
        let struct_type = StructType::new_unchecked(fields);
        let into_owned_iter = struct_type.into_iter();
        assert_eq!(into_owned_iter.len(), 4);
    }

    #[test]
    fn test_iterator_with_metadata() {
        let field_with_metadata = StructField::new("test_field", DataType::STRING, true)
            .with_metadata([("key1", MetadataValue::String("value1".to_string()))]);

        let struct_type = StructType::new_unchecked([field_with_metadata]);

        // Test that metadata is preserved through iteration
        for field in &struct_type {
            assert_eq!(field.metadata().len(), 1);
            assert_eq!(
                field.metadata().get("key1"),
                Some(&MetadataValue::String("value1".to_string()))
            );
        }

        // Test consuming iterator preserves metadata
        for field in struct_type {
            assert_eq!(field.metadata().len(), 1);
            assert_eq!(
                field.metadata().get("key1"),
                Some(&MetadataValue::String("value1".to_string()))
            );
        }
    }

    #[test]
    fn test_empty_struct_type_iterator() {
        let struct_type = StructType::new_unchecked(std::iter::empty::<StructField>());

        // Test all iterator methods with empty struct
        assert_eq!(struct_type.fields().count(), 0);
        assert_eq!((&struct_type).into_iter().count(), 0);
        assert_eq!(struct_type.into_iter().count(), 0);
    }

    #[test]
    fn test_iterator_order_preservation() {
        let fields = vec![
            StructField::new("zebra", DataType::STRING, true),
            StructField::new("apple", DataType::INTEGER, false),
            StructField::new("banana", DataType::BOOLEAN, true),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // IndexMap should preserve insertion order
        let field_names: Vec<_> = struct_type.fields().map(|f| f.name()).collect();
        assert_eq!(field_names, vec!["zebra", "apple", "banana"]);

        // Order should be the same across different iterator methods
        let ref_names: Vec<_> = (&struct_type).into_iter().map(|f| f.name()).collect();
        assert_eq!(ref_names, vec!["zebra", "apple", "banana"]);

        // Test consuming iterator maintains order too
        let owned_names: Vec<_> = struct_type.into_iter().map(|f| f.name).collect();
        assert_eq!(owned_names, vec!["zebra", "apple", "banana"]);
    }

    #[test]
    fn test_iterator_collect() {
        let original_fields = vec![
            StructField::new("field1", DataType::STRING, true),
            StructField::new("field2", DataType::INTEGER, false),
        ];
        let struct_type = StructType::new_unchecked(original_fields.clone());

        // Test collecting from reference iterator
        let collected_refs: Vec<&StructField> = struct_type.fields().collect();
        assert_eq!(collected_refs.len(), 2);
        assert_eq!(collected_refs[0].name, "field1");
        assert_eq!(collected_refs[1].name, "field2");

        // Test collecting from consuming iterator
        let collected_owned: Vec<StructField> = struct_type.into_iter().collect();
        assert_eq!(collected_owned.len(), 2);
        assert_eq!(collected_owned[0].name, "field1");
        assert_eq!(collected_owned[1].name, "field2");
    }

    #[test]
    fn test_iterator_functional_methods() {
        let fields = vec![
            StructField::new("nullable_string", DataType::STRING, true),
            StructField::new("required_int", DataType::INTEGER, false),
            StructField::new("nullable_bool", DataType::BOOLEAN, true),
            StructField::new("required_long", DataType::LONG, false),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Test filter - find nullable fields
        let nullable_count = struct_type.fields().filter(|f| f.is_nullable()).count();
        assert_eq!(nullable_count, 2);

        // Test map and filter chain
        let required_field_names: Vec<_> = struct_type
            .fields()
            .filter(|f| !f.is_nullable())
            .map(|f| f.name())
            .collect();
        assert_eq!(required_field_names, vec!["required_int", "required_long"]);

        // Test enumerate
        for (index, field) in struct_type.fields().enumerate() {
            match index {
                0 => assert_eq!(field.name, "nullable_string"),
                1 => assert_eq!(field.name, "required_int"),
                2 => assert_eq!(field.name, "nullable_bool"),
                3 => assert_eq!(field.name, "required_long"),
                _ => panic!("Unexpected field index: {index}"),
            }
        }
    }

    #[test]
    fn test_double_ended_iterator_ref() {
        let fields = vec![
            StructField::new("first", DataType::STRING, true),
            StructField::new("second", DataType::INTEGER, false),
            StructField::new("third", DataType::BOOLEAN, true),
            StructField::new("fourth", DataType::LONG, false),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Test iterating from both ends using reference iterator
        let mut iter = struct_type.fields();

        // Forward iteration
        assert_eq!(iter.next().unwrap().name, "first");
        assert_eq!(iter.next().unwrap().name, "second");

        // Backward iteration
        assert_eq!(iter.next_back().unwrap().name, "fourth");
        assert_eq!(iter.next_back().unwrap().name, "third");

        // Should be exhausted
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_double_ended_iterator_owned() {
        let fields = vec![
            StructField::new("alpha", DataType::STRING, true),
            StructField::new("beta", DataType::INTEGER, false),
            StructField::new("gamma", DataType::BOOLEAN, true),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Test iterating from both ends using owned iterator
        let mut iter = struct_type.into_iter();

        // Backward iteration first
        assert_eq!(iter.next_back().unwrap().name, "gamma");

        // Forward iteration
        assert_eq!(iter.next().unwrap().name, "alpha");

        // Backward iteration again
        assert_eq!(iter.next_back().unwrap().name, "beta");

        // Should be exhausted
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_double_ended_iterator_collect_reverse() {
        let fields = vec![
            StructField::new("one", DataType::STRING, true),
            StructField::new("two", DataType::INTEGER, false),
            StructField::new("three", DataType::BOOLEAN, true),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Test collecting in reverse order using DoubleEndedIterator
        let reversed_names: Vec<_> = struct_type.fields().rev().map(|f| f.name()).collect();
        assert_eq!(reversed_names, vec!["three", "two", "one"]);

        // Test that we can still use the original struct
        assert_eq!(struct_type.field("two").unwrap().name, "two");
    }

    #[test]
    fn test_double_ended_iterator_with_into_iter_ref() {
        let fields = vec![
            StructField::new("x", DataType::DOUBLE, true),
            StructField::new("y", DataType::FLOAT, false),
            StructField::new("z", DataType::LONG, true),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Test DoubleEndedIterator with &StructType into_iter
        let mut iter = (&struct_type).into_iter();

        // Mix forward and backward iteration
        assert_eq!(iter.next().unwrap().name, "x");
        assert_eq!(iter.next_back().unwrap().name, "z");
        assert_eq!(iter.next().unwrap().name, "y");

        // Should be exhausted
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_fused_iterator_ref() {
        let fields = vec![
            StructField::new("test1", DataType::STRING, true),
            StructField::new("test2", DataType::INTEGER, false),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Verify that reference iterator implements FusedIterator
        let mut iter = struct_type.fields();

        // Exhaust the iterator
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());

        // FusedIterator guarantees that calling next() after exhaustion always returns None
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_fused_iterator_owned() {
        let fields = vec![
            StructField::new("item1", DataType::STRING, true),
            StructField::new("item2", DataType::INTEGER, false),
        ];
        let struct_type = StructType::new_unchecked(fields);

        // Verify that owned iterator implements FusedIterator
        let mut iter = struct_type.into_iter();

        // Exhaust the iterator
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());

        // FusedIterator guarantees that calling next() after exhaustion always returns None
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_fused_iterator_with_into_iter_ref() {
        let fields = vec![StructField::new("field_a", DataType::BOOLEAN, true)];
        let struct_type = StructType::new_unchecked(fields);

        // Verify that &StructType into_iter implements FusedIterator
        let mut iter = (&struct_type).into_iter();

        // Exhaust the iterator
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());

        // FusedIterator guarantees that calling next() after exhaustion always returns None
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_fused_double_ended_iterator_empty() {
        let struct_type = StructType::new_unchecked(std::iter::empty::<StructField>());

        // Test both forward and backward iteration on empty iterator
        let mut iter = struct_type.fields();

        // Empty iterator should return None immediately
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());

        // FusedIterator guarantees continued None after exhaustion
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_double_ended_iterator_single_element() {
        let fields = vec![StructField::new("single", DataType::STRING, true)];
        let struct_type = StructType::new_unchecked(fields);

        // Test DoubleEndedIterator with single element
        let mut iter = struct_type.fields();

        // Should get the single element from next()
        assert_eq!(iter.next().unwrap().name, "single");
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());

        // Test getting single element from next_back()
        let struct_type =
            StructType::new_unchecked([StructField::new("single2", DataType::INTEGER, false)]);
        let mut iter = struct_type.into_iter();

        assert_eq!(iter.next_back().unwrap().name, "single2");
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_metadata_column_spec() -> DeltaResult<()> {
        // Test text_value
        assert_eq!(MetadataColumnSpec::RowIndex.text_value(), "row_index");
        assert_eq!(MetadataColumnSpec::RowId.text_value(), "row_id");
        assert_eq!(
            MetadataColumnSpec::RowCommitVersion.text_value(),
            "row_commit_version"
        );
        assert_eq!(MetadataColumnSpec::FilePath.text_value(), "_file");

        // Test data_type
        assert_eq!(MetadataColumnSpec::RowIndex.data_type(), DataType::LONG);
        assert_eq!(MetadataColumnSpec::RowId.data_type(), DataType::LONG);
        assert_eq!(
            MetadataColumnSpec::RowCommitVersion.data_type(),
            DataType::LONG
        );
        assert_eq!(MetadataColumnSpec::FilePath.data_type(), DataType::STRING);

        // Test nullable
        assert!(!MetadataColumnSpec::RowIndex.nullable());
        assert!(!MetadataColumnSpec::RowId.nullable());
        assert!(!MetadataColumnSpec::RowCommitVersion.nullable());
        assert!(!MetadataColumnSpec::FilePath.nullable());

        // Test reserved_field_id
        assert_eq!(MetadataColumnSpec::RowIndex.reserved_field_id(), None);
        assert_eq!(MetadataColumnSpec::RowId.reserved_field_id(), None);
        assert_eq!(
            MetadataColumnSpec::RowCommitVersion.reserved_field_id(),
            None
        );
        assert_eq!(
            MetadataColumnSpec::FilePath.reserved_field_id(),
            Some(crate::reserved_field_ids::FILE_NAME)
        );

        // Test from_str
        assert_eq!(
            MetadataColumnSpec::from_str("row_index")?,
            MetadataColumnSpec::RowIndex
        );
        assert_eq!(
            MetadataColumnSpec::from_str("row_id")?,
            MetadataColumnSpec::RowId
        );
        assert_eq!(
            MetadataColumnSpec::from_str("row_commit_version")?,
            MetadataColumnSpec::RowCommitVersion
        );
        assert_eq!(
            MetadataColumnSpec::from_str("_file")?,
            MetadataColumnSpec::FilePath
        );

        // Test invalid from_str
        assert!(MetadataColumnSpec::from_str("invalid").is_err());

        Ok(())
    }

    #[test]
    fn test_create_metadata_column() {
        let field =
            StructField::create_metadata_column("test_row_index", MetadataColumnSpec::RowIndex);

        assert_eq!(field.name(), "test_row_index");
        assert_eq!(field.data_type(), &DataType::LONG);
        assert!(!field.nullable);
        assert!(field.is_metadata_column());
        assert_eq!(
            field.get_metadata_column_spec(),
            Some(MetadataColumnSpec::RowIndex)
        );
    }

    #[test]
    fn test_default_row_index_column() {
        let field = StructField::default_row_index_column();

        assert_eq!(field.name(), "_metadata.row_index");
        assert_eq!(field.data_type(), &DataType::LONG);
        assert!(!field.nullable);
        assert!(field.is_metadata_column());
        assert_eq!(
            field.get_metadata_column_spec(),
            Some(MetadataColumnSpec::RowIndex)
        );
    }

    #[test]
    fn test_add_column() -> DeltaResult<()> {
        let schema = StructType::try_new([StructField::nullable("col1", DataType::STRING)])?;

        let new_field = StructField::nullable("col2", DataType::INTEGER);
        let updated_schema = schema.add([new_field])?;

        assert_eq!(updated_schema.fields().count(), 2);
        assert!(updated_schema.contains("col1"));
        assert!(updated_schema.contains("col2"));
        Ok(())
    }

    #[test]
    fn test_add_metadata_column() -> DeltaResult<()> {
        let schema = StructType::try_new([StructField::nullable("regular_col", DataType::STRING)])?;

        let schema_with_metadata =
            schema.add_metadata_column("my_row_index", MetadataColumnSpec::RowIndex)?;

        assert_eq!(schema_with_metadata.fields().count(), 2);
        assert!(schema_with_metadata.contains_metadata_column(&MetadataColumnSpec::RowIndex));
        assert!(schema_with_metadata.contains("my_row_index"));
        assert_eq!(
            schema_with_metadata.index_of_metadata_column(&MetadataColumnSpec::RowIndex),
            Some(&1)
        );
        Ok(())
    }

    #[test]
    fn test_duplicate_metadata_columns() -> DeltaResult<()> {
        let schema = StructType::try_new([StructField::nullable("regular_col", DataType::STRING)])?;

        let schema_with_metadata =
            schema.add_metadata_column("row_index1", MetadataColumnSpec::RowIndex)?;

        // Adding another row index metadata column should fail
        let result =
            schema_with_metadata.add_metadata_column("row_index2", MetadataColumnSpec::RowIndex);

        assert_result_error_with_message(result, "Duplicate metadata column");
        Ok(())
    }

    #[test]
    fn test_duplicate_field_name_case_insensitive() {
        // Delta column names are case-insensitive per protocol; (Value, value) is invalid
        let result = StructType::try_new([
            StructField::nullable("Value", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ]);
        assert_result_error_with_message(result, "Duplicate field name (case-insensitive)");
    }

    #[test]
    fn test_duplicate_field_name_exact() {
        // Exact duplicate (same name twice) is rejected via the case-insensitive check
        let result = StructType::try_new([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("id", DataType::STRING),
        ]);
        assert_result_error_with_message(result, "Duplicate field name (case-insensitive)");
    }

    #[test]
    fn test_nested_metadata_columns_validation_struct() -> DeltaResult<()> {
        // Test that metadata columns in nested structs are rejected
        let nested_field_with_metadata =
            StructField::create_metadata_column("nested_row_index", MetadataColumnSpec::RowIndex);
        let nested_struct = StructType {
            type_name: "struct".into(),
            fields: [(
                nested_field_with_metadata.name.clone(),
                nested_field_with_metadata,
            )]
            .into_iter()
            .collect(),
            metadata_columns: HashMap::new(),
        };

        let result = StructType::try_new([
            StructField::nullable("regular_col", DataType::STRING),
            StructField::nullable("nested", DataType::Struct(Box::new(nested_struct))),
        ]);

        assert_result_error_with_message(result, "only allowed at the top level");
        Ok(())
    }

    #[test]
    fn test_nested_metadata_columns_validation_array() -> DeltaResult<()> {
        // Test that metadata columns in array element structs are rejected
        let nested_field_with_metadata =
            StructField::create_metadata_column("nested_row_index", MetadataColumnSpec::RowIndex);
        let nested_struct = StructType {
            type_name: "struct".into(),
            fields: [(
                nested_field_with_metadata.name.clone(),
                nested_field_with_metadata,
            )]
            .into_iter()
            .collect(),
            metadata_columns: HashMap::new(),
        };
        let array_type = ArrayType::new(DataType::Struct(Box::new(nested_struct)), true);

        let result = StructType::try_new([
            StructField::nullable("regular_col", DataType::STRING),
            StructField::nullable("array_col", DataType::Array(Box::new(array_type))),
        ]);

        assert_result_error_with_message(result, "only allowed at the top level");
        Ok(())
    }

    #[test]
    fn test_nested_metadata_columns_validation_map() -> DeltaResult<()> {
        // Test that metadata columns in map key structs or map value structs are rejected
        let nested_field_with_metadata =
            StructField::create_metadata_column("nested_row_index", MetadataColumnSpec::RowIndex);
        let nested_struct = StructType {
            type_name: "struct".into(),
            fields: [(
                nested_field_with_metadata.name.clone(),
                nested_field_with_metadata,
            )]
            .into_iter()
            .collect(),
            metadata_columns: HashMap::new(),
        };

        for map_type in [
            MapType::new(
                DataType::Struct(Box::new(nested_struct.clone())),
                DataType::STRING,
                true,
            ),
            MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(nested_struct)),
                true,
            ),
        ] {
            let result = StructType::try_new([
                StructField::nullable("regular_col", DataType::STRING),
                StructField::nullable("map_col", DataType::Map(Box::new(map_type))),
            ]);

            assert_result_error_with_message(result, "only allowed at the top level");
        }

        Ok(())
    }

    #[test]
    fn test_column_identifier_trait() -> DeltaResult<()> {
        let schema = StructType::try_new([
            StructField::nullable("regular_col", DataType::STRING),
            StructField::create_metadata_column("row_index_col", MetadataColumnSpec::RowIndex),
        ])?;

        // Test string identifier
        assert!(schema.contains("regular_col"));
        assert!(schema.contains("row_index_col"));
        assert!(!schema.contains("nonexistent"));

        // Test String identifier
        assert!(schema.contains("regular_col"));
        assert!(schema.contains("row_index_col"));

        // Test MetadataColumnSpec identifier
        assert!(schema.contains_metadata_column(&MetadataColumnSpec::RowIndex));
        assert!(!schema.contains_metadata_column(&MetadataColumnSpec::RowId));
        Ok(())
    }

    #[test]
    fn test_metadata_column_serialization() -> DeltaResult<()> {
        let field = StructField::create_metadata_column("test_row_id", MetadataColumnSpec::RowId);

        // Test that serialization works
        let json = serde_json::to_string(&field)?;
        let deserialized: StructField = serde_json::from_str(&json)?;

        assert_eq!(deserialized.name(), field.name());
        assert_eq!(deserialized.data_type(), field.data_type());
        assert_eq!(deserialized.nullable, field.nullable);
        assert!(deserialized.is_metadata_column());
        assert_eq!(
            deserialized.get_metadata_column_spec(),
            Some(MetadataColumnSpec::RowId)
        );
        Ok(())
    }

    #[test]
    fn test_all_metadata_column_specs() -> DeltaResult<()> {
        let schema = StructType::try_new([StructField::nullable("regular_col", DataType::STRING)])?;

        let schema = schema
            .add_metadata_column("row_index", MetadataColumnSpec::RowIndex)?
            .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
            .add_metadata_column("row_commit_version", MetadataColumnSpec::RowCommitVersion)?;

        assert_eq!(schema.fields().count(), 4);
        assert!(schema.contains_metadata_column(&MetadataColumnSpec::RowIndex));
        assert!(schema.contains_metadata_column(&MetadataColumnSpec::RowId));
        assert!(schema.contains_metadata_column(&MetadataColumnSpec::RowCommitVersion));

        assert_eq!(
            schema.index_of_metadata_column(&MetadataColumnSpec::RowIndex),
            Some(&1)
        );
        assert_eq!(
            schema.index_of_metadata_column(&MetadataColumnSpec::RowId),
            Some(&2)
        );
        assert_eq!(
            schema.index_of_metadata_column(&MetadataColumnSpec::RowCommitVersion),
            Some(&3)
        );
        Ok(())
    }

    #[test]
    fn test_physical_name_with_mode_none() {
        let field_json = r#"{
            "name": "logical_name",
            "type": "string",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.physicalName": "physical_name_col123"
            }
        }"#;
        let field: StructField = serde_json::from_str(field_json).unwrap();

        // With ColumnMappingMode::None, should return logical name even though physical name exists
        assert_eq!(field.physical_name(ColumnMappingMode::None), "logical_name");
    }

    #[test]
    fn test_physical_name_with_mode_id() {
        let field_json = r#"{
            "name": "logical_name",
            "type": "string",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 5,
                "delta.columnMapping.physicalName": "physical_name_col123"
            }
        }"#;
        let field: StructField = serde_json::from_str(field_json).unwrap();

        // With ColumnMappingMode::Id, should return physical name
        assert_eq!(
            field.physical_name(ColumnMappingMode::Id),
            "physical_name_col123"
        );
    }

    #[test]
    fn test_physical_name_with_mode_name() {
        let field_json = r#"{
            "name": "logical_name",
            "type": "string",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.physicalName": "physical_name_col456"
            }
        }"#;
        let field: StructField = serde_json::from_str(field_json).unwrap();

        // With ColumnMappingMode::Name, should return physical name
        assert_eq!(
            field.physical_name(ColumnMappingMode::Name),
            "physical_name_col456"
        );
    }

    #[test]
    fn test_physical_name_fallback_id() {
        let field_json = r#"{
            "name": "logical_name",
            "type": "string",
            "nullable": true,
            "metadata": {}
        }"#;
        let field: StructField = serde_json::from_str(field_json).unwrap();

        // With ColumnMappingMode::Id but no physical name, should fallback to logical name
        assert_eq!(field.physical_name(ColumnMappingMode::Id), "logical_name");
    }

    #[test]
    fn test_physical_name_fallback_name() {
        let field_json = r#"{
            "name": "logical_name",
            "type": "string",
            "nullable": true,
            "metadata": {}
        }"#;
        let field: StructField = serde_json::from_str(field_json).unwrap();

        // With ColumnMappingMode::Name but no physical name, should fallback to logical name
        assert_eq!(field.physical_name(ColumnMappingMode::Name), "logical_name");
    }

    #[test]
    fn test_display_struct_type_stable_output() -> DeltaResult<()> {
        let nested_field_with_metadata =
            StructField::create_metadata_column("nested_row_index", MetadataColumnSpec::RowIndex);
        let inner_struct =
            StructType::new_unchecked([StructField::new("q", DataType::LONG, false)]);
        let nested_struct = StructType::new_unchecked([
            nested_field_with_metadata,
            StructField::new("x", DataType::DOUBLE, true),
            StructField::new(
                "inner_struct",
                DataType::Struct(Box::new(inner_struct)),
                false,
            ),
        ]);
        let array_type = ArrayType::new(DataType::Struct(Box::new(nested_struct.clone())), true);
        let map_type = MapType::new(
            DataType::Struct(Box::new(nested_struct.clone())),
            DataType::Struct(Box::new(nested_struct.clone())), // kek
            true,
        );
        let fields = vec![
            StructField::new("x", DataType::DOUBLE, true),
            StructField::new("y", DataType::FLOAT, false),
            StructField::new("z", DataType::LONG, true),
            StructField::new("s", nested_struct.clone(), false),
            StructField::nullable("array_col", DataType::Array(Box::new(array_type))),
            StructField::nullable("map_col", DataType::Map(Box::new(map_type))),
            StructField::new("a", DataType::LONG, true),
        ];

        let struct_type = StructType::new_unchecked(fields);
        assert_eq!(
            struct_type.to_string(),
            "struct:
├─x: double (is nullable: true, metadata: {})
├─y: float (is nullable: false, metadata: {})
├─z: long (is nullable: true, metadata: {})
├─s: struct<nested_row_index: long, x: double, inner_struct: struct<q: long>> (is nullable: false, metadata: {})
│  ├─nested_row_index: long (is nullable: false, metadata: {delta.metadataSpec: String(\"row_index\")})
│  ├─x: double (is nullable: true, metadata: {})
│  └─inner_struct: struct<q: long> (is nullable: false, metadata: {})
│     └─q: long (is nullable: false, metadata: {})
├─array_col: array<struct<nested_row_index: long, x: double, inner_struct: struct<q: long>>> (is nullable: true, metadata: {})
│  └─array_element: struct<nested_row_index: long, x: double, inner_struct: struct<q: long>>
│     ├─nested_row_index: long (is nullable: false, metadata: {delta.metadataSpec: String(\"row_index\")})
│     ├─x: double (is nullable: true, metadata: {})
│     └─inner_struct: struct<q: long> (is nullable: false, metadata: {})
│        └─q: long (is nullable: false, metadata: {})
├─map_col: map<struct<nested_row_index: long, x: double, inner_struct: struct<q: long>>, struct<nested_row_index: long, x: double, inner_struct: struct<q: long>>> (is nullable: true, metadata: {})
│  ├─map_key: struct<nested_row_index: long, x: double, inner_struct: struct<q: long>>
│  │  ├─nested_row_index: long (is nullable: false, metadata: {delta.metadataSpec: String(\"row_index\")})
│  │  ├─x: double (is nullable: true, metadata: {})
│  │  └─inner_struct: struct<q: long> (is nullable: false, metadata: {})
│  │     └─q: long (is nullable: false, metadata: {})
│  └─map_value: struct<nested_row_index: long, x: double, inner_struct: struct<q: long>>
│     ├─nested_row_index: long (is nullable: false, metadata: {delta.metadataSpec: String(\"row_index\")})
│     ├─x: double (is nullable: true, metadata: {})
│     └─inner_struct: struct<q: long> (is nullable: false, metadata: {})
│        └─q: long (is nullable: false, metadata: {})
└─a: long (is nullable: true, metadata: {})
"
        );

        let schema = StructType::try_new([StructField::nullable("regular_col", DataType::STRING)])?;
        let schema = schema
            .add_metadata_column("row_index", MetadataColumnSpec::RowIndex)?
            .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
            .add_metadata_column("row_commit_version", MetadataColumnSpec::RowCommitVersion)?;
        assert_eq!(schema.to_string(), "struct:
├─regular_col: string (is nullable: true, metadata: {})
├─row_index: long (is nullable: false, metadata: {delta.metadataSpec: String(\"row_index\")})
├─row_id: long (is nullable: false, metadata: {delta.metadataSpec: String(\"row_id\")})
└─row_commit_version: long (is nullable: false, metadata: {delta.metadataSpec: String(\"row_commit_version\")})
");
        Ok(())
    }

    #[test]
    fn test_builder_empty() {
        let schema = StructType::builder().build().unwrap();
        assert_eq!(schema.num_fields(), 0)
    }

    #[test]
    fn test_builder_add_fields() {
        let schema = StructType::builder()
            .add_field(StructField::new("id", DataType::INTEGER, false))
            .add_field(StructField::new("name", DataType::STRING, true))
            .build()
            .unwrap();

        assert_eq!(schema.num_fields(), 2);
        assert_eq!(schema.field_at_index(0).unwrap().name(), "id");
        assert_eq!(schema.field_at_index(1).unwrap().name(), "name");
    }

    #[test]
    fn test_builder_from_schema() {
        let base_schema =
            StructType::try_new([StructField::new("id", DataType::INTEGER, false)]).unwrap();

        let extended_schema = StructTypeBuilder::from_schema(&base_schema)
            .add_field(StructField::new("name", DataType::STRING, true))
            .build()
            .unwrap();

        assert_eq!(extended_schema.num_fields(), 2);
        assert_eq!(extended_schema.field_at_index(0).unwrap().name(), "id");
        assert_eq!(extended_schema.field_at_index(1).unwrap().name(), "name");
    }

    #[test]
    fn test_parquet_field_id_key_value() {
        // Verify the string value of ColumnMetadataKey::ParquetFieldId matches the convention
        // used by delta-spark and other Delta ecosystem implementations. This is not part of
        // the Delta protocol spec, so we pin the value here to catch accidental changes.
        assert_eq!(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            "parquet.field.id"
        );
    }

    #[test]
    fn test_with_field_inserted_empty_struct() {
        let schema = StructType::try_new([]).unwrap();
        let schema = schema
            .with_field_inserted_after(None, StructField::new("age", DataType::STRING, true))
            .expect("with field inserted should produce a valid schema");
        assert_eq!(schema.num_fields(), 1);
        assert_eq!(schema.field_at_index(0).unwrap().name(), "age");
    }

    #[test]
    fn test_with_field_inserted() {
        let schema = StructType::try_new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        let schema = schema
            .with_field_inserted_after(Some("id"), StructField::new("age", DataType::STRING, true))
            .expect("with field inserted should produce a valid schema");
        assert_eq!(schema.num_fields(), 3);
        assert_eq!(schema.field_at_index(0).unwrap().name(), "id");
        assert_eq!(schema.field_at_index(1).unwrap().name(), "age");
        assert_eq!(schema.field_at_index(2).unwrap().name(), "name");
    }

    #[test]
    fn test_with_field_inserted_append_to_end() {
        let schema = StructType::try_new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        let schema = schema
            .with_field_inserted_after(None, StructField::new("age", DataType::STRING, true))
            .expect("with field inserted should produce a valid schema");

        assert_eq!(schema.num_fields(), 3);
        assert_eq!(schema.field_at_index(0).unwrap().name(), "id");
        assert_eq!(schema.field_at_index(1).unwrap().name(), "name");
        assert_eq!(schema.field_at_index(2).unwrap().name(), "age");
    }

    #[test]
    fn test_with_field_inserted_after_non_existent_field() {
        let schema =
            StructType::try_new([StructField::new("id", DataType::INTEGER, false)]).unwrap();
        let new_schema = schema.with_field_inserted_after(
            Some("nonexistent"),
            StructField::new("name", DataType::STRING, true),
        );
        assert!(new_schema.is_err());
    }

    #[test]
    fn test_with_field_inserted_after_duplicate_field() {
        let schema = StructType::try_new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        let new_schema = schema.with_field_inserted_after(
            Some("name"),
            StructField::new("id", DataType::STRING, true),
        );
        assert!(new_schema.is_err());
        assert_result_error_with_message(new_schema, "Field id already exists");
    }

    #[test]
    fn test_with_field_inserted_before() {
        let schema = StructType::try_new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        let schema = schema
            .with_field_inserted_before(
                Some("name"),
                StructField::new("age", DataType::STRING, true),
            )
            .expect("with field inserted before should produce a valid schema");
        assert_eq!(schema.num_fields(), 3);
        assert_eq!(schema.field_at_index(0).unwrap().name(), "id");
        assert_eq!(schema.field_at_index(1).unwrap().name(), "age");
        assert_eq!(schema.field_at_index(2).unwrap().name(), "name");
    }

    #[test]
    fn test_with_field_inserted_before_duplicate_field() {
        let schema = StructType::try_new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        let new_schema = schema.with_field_inserted_before(
            Some("name"),
            StructField::new("id", DataType::STRING, true),
        );
        assert!(new_schema.is_err());
        assert_result_error_with_message(new_schema, "Field id already exists");
    }

    #[test]
    fn test_with_field_inserted_before_at_beginning() {
        let schema = StructType::try_new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap();
        let schema = schema
            .with_field_inserted_before(None, StructField::new("age", DataType::STRING, true))
            .expect("with field inserted before should produce a valid schema");
        assert_eq!(schema.num_fields(), 3);
        assert_eq!(schema.field_at_index(0).unwrap().name(), "age");
        assert_eq!(schema.field_at_index(1).unwrap().name(), "id");
        assert_eq!(schema.field_at_index(2).unwrap().name(), "name");
    }

    #[test]
    fn test_with_field_inserted_before_non_existent_field() {
        let schema =
            StructType::try_new([StructField::new("id", DataType::INTEGER, false)]).unwrap();
        let new_schema = schema.with_field_inserted_before(
            Some("nonexistent"),
            StructField::new("name", DataType::STRING, true),
        );
        assert!(new_schema.is_err());
    }

    #[test]
    fn test_with_field_inserted_before_empty_struct() {
        let schema = StructType::try_new([]).unwrap();
        let schema = schema
            .with_field_inserted_before(None, StructField::new("age", DataType::STRING, true))
            .expect("with field inserted before on empty struct should succeed");
        assert_eq!(schema.num_fields(), 1);
        assert_eq!(schema.field_at_index(0).unwrap().name(), "age");
    }

    #[test]
    fn test_with_field_removed() {
        let schema =
            StructType::try_new([StructField::new("id", DataType::INTEGER, false)]).unwrap();
        let new_schema = schema.with_field_removed("id");
        assert_eq!(new_schema.num_fields(), 0);
    }

    #[test]
    fn test_with_field_removed_non_existent_field() {
        let schema =
            StructType::try_new([StructField::new("id", DataType::INTEGER, false)]).unwrap();
        let new_schema = schema.with_field_removed("nonexistent");
        assert_eq!(new_schema.num_fields(), 1);
        assert_eq!(new_schema.field_at_index(0).unwrap().name(), "id");
    }

    #[test]
    fn test_with_field_replaced() {
        let schema =
            StructType::try_new([StructField::new("id", DataType::INTEGER, false)]).unwrap();
        let new_schema = schema
            .with_field_replaced("id", StructField::new("name", DataType::STRING, true))
            .unwrap();

        assert_eq!(new_schema.num_fields(), 1);
        assert_eq!(new_schema.field_at_index(0).unwrap().name(), "name");
    }

    #[test]
    fn test_with_field_replaced_non_existent_field() {
        let schema =
            StructType::try_new([StructField::new("id", DataType::INTEGER, false)]).unwrap();
        let new_schema = schema.with_field_replaced(
            "nonexistent",
            StructField::new("name", DataType::STRING, true),
        );
        assert!(new_schema.is_err(), "Expected error for non-existent field");
    }

    /// Schema: { a: { b: { c: double } } } — supports walks at depths 1, 2, and 3.
    fn walk_test_schema() -> StructType {
        let l3 = StructType::new_unchecked([StructField::new("c", DataType::DOUBLE, false)]);
        let l2 = StructType::new_unchecked([StructField::new(
            "b",
            DataType::Struct(Box::new(l3)),
            false,
        )]);
        StructType::new_unchecked([StructField::new("a", DataType::Struct(Box::new(l2)), false)])
    }

    #[rstest::rstest]
    #[case::single_level(vec!["a"], vec!["a"], DataType::Struct(Box::new(
        StructType::new_unchecked([StructField::new("b", DataType::Struct(Box::new(
            StructType::new_unchecked([StructField::new("c", DataType::DOUBLE, false)])
        )), false)])
    )))]
    #[case::nested_2(vec!["a", "b"], vec!["a", "b"], DataType::Struct(Box::new(
        StructType::new_unchecked([StructField::new("c", DataType::DOUBLE, false)])
    )))]
    #[case::nested_3(vec!["a", "b", "c"], vec!["a", "b", "c"], DataType::DOUBLE)]
    #[test]
    fn test_walk_column_fields_happy(
        #[case] col_path: Vec<&str>,
        #[case] expected_names: Vec<&str>,
        #[case] expected_leaf_type: DataType,
    ) {
        let schema = walk_test_schema();
        let fields = schema
            .walk_column_fields(&ColumnName::new(col_path.iter().copied()))
            .unwrap();
        assert_eq!(fields.len(), expected_names.len());
        for (field, name) in fields.iter().zip(expected_names.iter()) {
            assert_eq!(field.name(), *name);
        }
        assert_eq!(fields.last().unwrap().data_type(), &expected_leaf_type);
    }

    #[rstest::rstest]
    #[case::empty_path(vec![], "Column path cannot be empty")]
    #[case::not_found_top(vec!["x"], "not found in schema")]
    #[case::not_found_nested(vec!["a", "x"], "not found in schema")]
    #[case::intermediate_not_struct(vec!["a", "b", "c", "d"], "not a struct type")]
    #[test]
    fn test_walk_column_fields_error(#[case] col_path: Vec<&str>, #[case] expected_error: &str) {
        let schema = walk_test_schema();
        let result = schema.walk_column_fields(&ColumnName::new(col_path.iter().copied()));
        assert_result_error_with_message(result, expected_error);
    }
}
