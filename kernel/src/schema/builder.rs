//! Fluent [`SchemaBuilder`] for incrementally constructing or augmenting [`StructType`] /
//! [`SchemaRef`] values.
//!
//! Plan builders frequently start from a kernel-defined action schema and add or replace one or
//! two fields (`__fsr_join_k`, `version`, `partitionValues_parsed`, `stats_parsed`). The
//! builder collapses the otherwise-verbose `Vec::collect().push().push().try_new()` ritual to
//! a method chain:
//!
//! ```ignore
//! let augmented = action_read_schema()
//!     .build_on()
//!     .with_nullable(FSR_JOIN_KEY_COL, DataType::Array(Box::new(ArrayType::new(
//!         DataType::STRING, true))))
//!     .with_not_null("version", DataType::LONG)
//!     .build()?;
//! ```
//!
//! Finalize via [`SchemaBuilder::build`] (validating) or [`SchemaBuilder::build_unchecked`]
//! (when the caller knows there are no duplicates).

use std::sync::Arc;

use crate::error::Error;
use crate::schema::{DataType, SchemaRef, StructField, StructType};

/// Append-only builder used to construct a [`StructType`] / [`SchemaRef`]. Open one via
/// [`SchemaBuilder::new`] or [`StructType::build_on`]; finalize via [`Self::build`]
/// (validated) or [`Self::build_unchecked`] (caller-asserted unique names).
pub struct SchemaBuilder {
    fields: Vec<StructField>,
}

impl SchemaBuilder {
    /// Open an empty [`SchemaBuilder`]. Equivalent to calling [`StructType::build_on`] on an
    /// empty struct but reads better when constructing a schema from scratch.
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Open a builder seeded with the fields of `schema`.
    pub fn from_schema(schema: &StructType) -> Self {
        Self {
            fields: schema.fields().cloned().collect(),
        }
    }

    /// Add a field. The field's nullability is taken from the [`StructField`] itself; this is
    /// the lowest-level append.
    pub fn with_field(mut self, field: StructField) -> Self {
        self.fields.push(field);
        self
    }

    /// Shorthand for [`Self::with_field`]`(StructField::nullable(name, ty))`.
    pub fn with_nullable(self, name: impl Into<String>, ty: impl Into<DataType>) -> Self {
        self.with_field(StructField::nullable(name, ty))
    }

    /// Shorthand for [`Self::with_field`]`(StructField::not_null(name, ty))`.
    pub fn with_not_null(self, name: impl Into<String>, ty: impl Into<DataType>) -> Self {
        self.with_field(StructField::not_null(name, ty))
    }

    /// Append every field from `other` in order. Useful for "this schema plus those columns".
    pub fn extend_from(mut self, other: &StructType) -> Self {
        self.fields.extend(other.fields().cloned());
        self
    }

    /// Validating finalize -- fails if field names are duplicated. Returns an [`Arc`]-wrapped
    /// schema so the result is directly assignable to [`SchemaRef`] slots.
    pub fn build(self) -> Result<SchemaRef, Error> {
        StructType::try_new(self.fields).map(Arc::new)
    }

    /// Unchecked finalize. Use only when the field set is known-unique at the call site
    /// (e.g. all field names are local consts that don't collide).
    pub fn build_unchecked(self) -> SchemaRef {
        Arc::new(StructType::new_unchecked(self.fields))
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::nullable("b", DataType::STRING),
        ]))
    }

    #[test]
    fn build_on_extends_existing_schema_fields() {
        let schema = base()
            .build_on()
            .with_not_null("c", DataType::LONG)
            .build()
            .unwrap();
        assert_eq!(schema.fields().count(), 3);
        assert_eq!(schema.field("c").unwrap().data_type(), &DataType::LONG);
    }

    #[test]
    fn build_unchecked_dedups_duplicate_field_names() {
        // `StructType::new_unchecked` silently deduplicates (last writer wins).
        let schema = SchemaBuilder::new()
            .with_not_null("x", DataType::LONG)
            .with_not_null("x", DataType::STRING)
            .build_unchecked();
        assert_eq!(schema.fields().count(), 1);
        assert_eq!(schema.field("x").unwrap().data_type(), &DataType::STRING);
    }

    #[test]
    fn build_validates_duplicate_field_names() {
        let err = base()
            .build_on()
            .with_not_null("a", DataType::LONG)
            .build()
            .unwrap_err();
        assert!(format!("{err}").contains("a"), "{err}");
    }

    #[test]
    fn extend_from_appends_all_fields_in_order() {
        let extra = StructType::new_unchecked([
            StructField::not_null("c", DataType::LONG),
            StructField::not_null("d", DataType::LONG),
        ]);
        let schema = base().build_on().extend_from(&extra).build().unwrap();
        let names: Vec<_> = schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c", "d"]);
    }
}
