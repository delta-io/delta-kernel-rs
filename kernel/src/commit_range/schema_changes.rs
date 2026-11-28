//! [`SchemaChange`]: how a commit changed the schema of its table.

use std::collections::{HashMap, HashSet};

use crate::expressions::ColumnName;
use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
use crate::table_features::ColumnMappingMode;
use crate::{KernelError, KernelResult};

/// A change that a commit made to the schema of its table.
///
/// Paths use the field names from before the commit, even if the commit also renamed the field
/// or one of its ancestors: a change to `a.x` stays `a.x` when `a` is renamed to `b`. Array
/// elements and map keys and values use the segments `element`, `key`, and `value`, both as the
/// target of a change (`tags.element` for a changed element type) and on the way to a nested
/// field (`tags.element.id`). The changes describe the difference between two schemas; they are
/// not a sequence to apply in order. The order of the fields of a struct is not part of that
/// difference: a commit that only reorders fields changes nothing.
///
/// The changes are only reported: which changes a reader of the commits tolerates is up to
/// the connector.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaChange {
    /// A column or nested field was added. A struct added with all its fields is one change.
    AddColumn {
        /// The struct that received the field; `None` is the root schema.
        parent: Option<ColumnName>,
        /// The added field.
        field: StructField,
    },
    /// A column or nested field was removed. A struct removed with all its fields is one change.
    DropColumn { column: ColumnName },
    /// A column or nested field was given the name `new_name` within the same struct.
    RenameColumn {
        column: ColumnName,
        new_name: String,
    },
    /// The data type of a column, nested field, array element, map key, or map value changed
    /// from `old_type` to `new_type`. The change is reported at the innermost path where the two
    /// types differ in kind.
    ChangeColumnType {
        column: ColumnName,
        old_type: DataType,
        new_type: DataType,
    },
    /// A column, nested field, array element, or map value changed from NOT NULL to nullable.
    SetNullable { column: ColumnName },
    /// A column, nested field, array element, or map value changed from nullable to NOT NULL.
    SetNotNull { column: ColumnName },
    /// The column mapping mode of the table changed from `old_mode` to `new_mode`. The mode
    /// decides how the columns of the schema map to the columns of the table's data files, so
    /// files written before and after the commit can name the same column differently even if
    /// no column of the schema changed.
    /// This is not a schema change in a strict sense (but a table's protocol change). Connectors
    /// still need this information to determine if a range is valid.
    ChangeColumnMappingMode {
        old_mode: ColumnMappingMode,
        new_mode: ColumnMappingMode,
    },
}

/// Returns the changes between the schemas `before` and `after`.
///
/// Fields are matched within the struct that holds them: by their column mapping ID with
/// `by_field_id`, otherwise by name. With `by_field_id`, a field dropped and added back under the
/// same name is reported as a [`SchemaChange::DropColumn`] and a [`SchemaChange::AddColumn`], and
/// a field without an ID or with the ID of another field of its struct is an error.
///
/// Names are matched exactly. Without column mapping IDs, a field whose name changes cannot be
/// told from a field that was replaced, so the change is reported as the pair that asks more of
/// a reader: a [`SchemaChange::DropColumn`] and a [`SchemaChange::AddColumn`]. That includes a
/// name that only changes case, although Delta column names are case-insensitive.
///
/// The order of the fields of a struct is not compared, and changes to field metadata are not
/// reported. Column mapping mode changes are derived in `TableShape::changes_since`.
pub(super) fn diff_schemas(
    before: &StructType,
    after: &StructType,
    by_field_id: bool,
) -> KernelResult<Vec<SchemaChange>> {
    let mut changes = Vec::new();
    SchemaComparison { by_field_id }.compare_structs(
        before,
        after,
        &ColumnName::default(),
        &mut changes,
    )?;
    Ok(changes)
}

/// What identifies a field among the fields of its struct.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum FieldKey<'a> {
    Id(i64),
    Name(&'a str),
}

struct SchemaComparison {
    by_field_id: bool,
}

impl SchemaComparison {
    /// The key of `field`, a field of the struct at `parent`.
    fn key<'a>(&self, parent: &ColumnName, field: &'a StructField) -> KernelResult<FieldKey<'a>> {
        if !self.by_field_id {
            return Ok(FieldKey::Name(field.name()));
        }
        let Some(MetadataValue::Number(id)) =
            field.get_config_value(&ColumnMetadataKey::ColumnMappingId)
        else {
            return Err(KernelError::schema(format!(
                "Column mapping is in effect but field '{}' has no column mapping ID",
                field_path(parent, field)
            )));
        };
        Ok(FieldKey::Id(*id))
    }

    /// Appends the changes between the structs at `path` to `changes`.
    fn compare_structs(
        &self,
        before: &StructType,
        after: &StructType,
        path: &ColumnName,
        changes: &mut Vec<SchemaChange>,
    ) -> KernelResult<()> {
        let after_fields: Vec<_> = after
            .fields()
            .map(|field| Ok((self.key(path, field)?, field)))
            .collect::<KernelResult<_>>()?;
        let mut unmatched = HashMap::with_capacity(after_fields.len());
        for &(key, field) in &after_fields {
            if unmatched.insert(key, field).is_some() {
                return Err(shared_key_error(path, field, key));
            }
        }
        let mut seen = HashSet::new();
        for before_field in before.fields() {
            let key = self.key(path, before_field)?;
            if !seen.insert(key) {
                return Err(shared_key_error(path, before_field, key));
            }
            let column = field_path(path, before_field);
            match unmatched.remove(&key) {
                Some(after_field) => {
                    self.compare_fields(before_field, after_field, column, changes)?
                }
                None => changes.push(SchemaChange::DropColumn { column }),
            }
        }
        let parent = (!path.is_empty()).then(|| path.clone());
        for (key, field) in after_fields {
            if unmatched.contains_key(&key) {
                changes.push(SchemaChange::AddColumn {
                    parent: parent.clone(),
                    field: field.clone(),
                });
            }
        }
        Ok(())
    }

    /// Appends the changes between two versions of the field at `column` to `changes`.
    fn compare_fields(
        &self,
        before: &StructField,
        after: &StructField,
        column: ColumnName,
        changes: &mut Vec<SchemaChange>,
    ) -> KernelResult<()> {
        if before.name() != after.name() {
            changes.push(SchemaChange::RenameColumn {
                column: column.clone(),
                new_name: after.name().clone(),
            });
        }
        compare_nullability(before.is_nullable(), after.is_nullable(), &column, changes);
        self.compare_types(before.data_type(), after.data_type(), &column, changes)
    }

    /// Appends the changes between two versions of the type at `path` to `changes`.
    fn compare_types(
        &self,
        before: &DataType,
        after: &DataType,
        path: &ColumnName,
        changes: &mut Vec<SchemaChange>,
    ) -> KernelResult<()> {
        let nested = |segment: &str| path.join(&ColumnName::new([segment]));
        match (before, after) {
            (DataType::Struct(before), DataType::Struct(after)) => {
                self.compare_structs(before, after, path, changes)?
            }
            (DataType::Array(before), DataType::Array(after)) => {
                let element = nested("element");
                compare_nullability(
                    before.contains_null(),
                    after.contains_null(),
                    &element,
                    changes,
                );
                self.compare_types(
                    before.element_type(),
                    after.element_type(),
                    &element,
                    changes,
                )?
            }
            (DataType::Map(before), DataType::Map(after)) => {
                self.compare_types(before.key_type(), after.key_type(), &nested("key"), changes)?;
                let value = nested("value");
                compare_nullability(
                    before.value_contains_null(),
                    after.value_contains_null(),
                    &value,
                    changes,
                );
                self.compare_types(before.value_type(), after.value_type(), &value, changes)?
            }
            (before, after) if before == after => {}
            (before, after) => changes.push(SchemaChange::ChangeColumnType {
                column: path.clone(),
                old_type: before.clone(),
                new_type: after.clone(),
            }),
        }
        Ok(())
    }
}

/// The path of `field`, a field of the struct at `parent`.
fn field_path(parent: &ColumnName, field: &StructField) -> ColumnName {
    parent.join(&ColumnName::new([field.name()]))
}

fn shared_key_error(parent: &ColumnName, field: &StructField, key: FieldKey<'_>) -> KernelError {
    let shared = match key {
        FieldKey::Id(_) => "its column mapping ID",
        FieldKey::Name(_) => "its name",
    };
    KernelError::schema(format!(
        "Field '{}' shares {shared} with another field of its struct",
        field_path(parent, field)
    ))
}

/// Appends the change in nullability of the field, array element, or map value at `column` to
/// `changes`.
fn compare_nullability(
    before: bool,
    after: bool,
    column: &ColumnName,
    changes: &mut Vec<SchemaChange>,
) {
    match (before, after) {
        (false, true) => changes.push(SchemaChange::SetNullable {
            column: column.clone(),
        }),
        (true, false) => changes.push(SchemaChange::SetNotNull {
            column: column.clone(),
        }),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::commit_range::tests::{mapped, mapped_int};
    use crate::schema::{ArrayType, MapType, schema};

    fn column(path: &[&str]) -> ColumnName {
        ColumnName::new(path.iter().copied())
    }

    fn drop_column(path: &[&str]) -> SchemaChange {
        SchemaChange::DropColumn {
            column: column(path),
        }
    }

    fn add(parent: &[&str], field: StructField) -> SchemaChange {
        SchemaChange::AddColumn {
            parent: (!parent.is_empty()).then(|| column(parent)),
            field,
        }
    }

    fn rename(path: &[&str], new_name: &str) -> SchemaChange {
        SchemaChange::RenameColumn {
            column: column(path),
            new_name: new_name.to_string(),
        }
    }

    fn change_type(
        path: &[&str],
        old_type: impl Into<DataType>,
        new_type: impl Into<DataType>,
    ) -> SchemaChange {
        SchemaChange::ChangeColumnType {
            column: column(path),
            old_type: old_type.into(),
            new_type: new_type.into(),
        }
    }

    fn set_nullable(path: &[&str]) -> SchemaChange {
        SchemaChange::SetNullable {
            column: column(path),
        }
    }

    fn set_not_null(path: &[&str]) -> SchemaChange {
        SchemaChange::SetNotNull {
            column: column(path),
        }
    }

    /// A schema whose column `map` maps `key` to `value`.
    fn map_schema(key: DataType, value: DataType, value_contains_null: bool) -> StructType {
        let map = MapType::new(key, value, value_contains_null);
        StructType::try_new([StructField::nullable("map", map)]).unwrap()
    }

    fn mapped_struct(name: &str, id: i64, fields: Vec<StructField>) -> StructField {
        let fields = StructType::try_new(fields).unwrap();
        mapped(StructField::nullable(name, fields), id)
    }

    fn mapped_array_of_struct(name: &str, id: i64, fields: Vec<StructField>) -> StructField {
        let element = StructType::try_new(fields).unwrap();
        mapped(
            StructField::nullable(name, ArrayType::new(element, true)),
            id,
        )
    }

    #[rstest]
    #[case::unchanged(
        schema! { nullable "a": INTEGER },
        schema! { nullable "a": INTEGER },
        vec![],
    )]
    #[case::added_and_dropped(
        schema! { nullable "a": INTEGER, nullable "b": STRING },
        schema! { nullable "a": INTEGER, nullable "c": STRING },
        vec![drop_column(&["b"]), add(&[], StructField::nullable("c", DataType::STRING))],
    )]
    #[case::nullability_and_type(
        schema! { not_null "a": INTEGER, nullable "b": INTEGER },
        schema! { nullable "a": LONG, not_null "b": INTEGER },
        vec![
            set_nullable(&["a"]),
            change_type(&["a"], DataType::INTEGER, DataType::LONG),
            set_not_null(&["b"]),
        ],
    )]
    #[case::nested_struct_is_compared_field_by_field(
        schema! {
            nullable "s": { nullable "x": INTEGER, not_null "y": INTEGER, nullable "z": INTEGER },
        },
        schema! {
            nullable "s": { nullable "x": LONG, nullable "y": INTEGER, nullable "new": INTEGER },
        },
        vec![
            change_type(&["s", "x"], DataType::INTEGER, DataType::LONG),
            set_nullable(&["s", "y"]),
            drop_column(&["s", "z"]),
            add(&["s"], StructField::nullable("new", DataType::INTEGER)),
        ],
    )]
    #[case::struct_field_added_inside_an_array(
        schema! { nullable "list": [ nullable { nullable "x": INTEGER } ] },
        schema! { nullable "list": [ nullable { nullable "x": INTEGER, nullable "y": INTEGER } ] },
        vec![add(&["list", "element"], StructField::nullable("y", DataType::INTEGER))],
    )]
    #[case::element_type_and_nullability_change_at_the_element(
        schema! { nullable "list": [ not_null INTEGER ] },
        schema! { nullable "list": [ nullable LONG ] },
        vec![
            set_nullable(&["list", "element"]),
            change_type(&["list", "element"], DataType::INTEGER, DataType::LONG),
        ],
    )]
    #[case::map_key_and_value_change_at_the_key_and_value(
        map_schema(DataType::INTEGER, DataType::INTEGER, true),
        map_schema(DataType::LONG, DataType::STRING, false),
        vec![
            change_type(&["map", "key"], DataType::INTEGER, DataType::LONG),
            set_not_null(&["map", "value"]),
            change_type(&["map", "value"], DataType::INTEGER, DataType::STRING),
        ],
    )]
    #[case::different_kind_of_type_is_one_change(
        schema! { nullable "a": { nullable "x": INTEGER } },
        schema! { nullable "a": [ nullable INTEGER ] },
        vec![change_type(
            &["a"],
            StructType::try_new([StructField::nullable("x", DataType::INTEGER)]).unwrap(),
            ArrayType::new(DataType::INTEGER, true),
        )],
    )]
    #[case::void_column_given_a_type(
        schema! { nullable "a": VOID },
        schema! { nullable "a": STRING },
        vec![change_type(&["a"], DataType::VOID, DataType::STRING)],
    )]
    #[case::field_moved_to_another_struct_is_dropped_and_added(
        schema! {
            nullable "s": { nullable "x": INTEGER },
            nullable "t": { nullable "y": INTEGER },
        },
        schema! {
            nullable "s": {},
            nullable "t": { nullable "y": INTEGER, nullable "x": INTEGER },
        },
        vec![drop_column(&["s", "x"]), add(&["t"], StructField::nullable("x", DataType::INTEGER))],
    )]
    #[case::name_that_only_changes_case_is_dropped_and_added(
        schema! { nullable "id": INTEGER },
        schema! { nullable "ID": INTEGER },
        vec![drop_column(&["id"]), add(&[], StructField::nullable("ID", DataType::INTEGER))],
    )]
    #[case::reordered_fields_are_not_a_change(
        schema! { nullable "a": INTEGER, nullable "b": INTEGER },
        schema! { nullable "b": INTEGER, nullable "a": INTEGER },
        vec![],
    )]
    fn changes_by_name(
        #[case] before: StructType,
        #[case] after: StructType,
        #[case] expected: Vec<SchemaChange>,
    ) {
        assert_eq!(diff_schemas(&before, &after, false).unwrap(), expected);
    }

    #[rstest]
    #[case::unchanged(vec![mapped_int("a", 1)], vec![mapped_int("a", 1)], vec![])]
    #[case::dropped_and_added_back_under_the_same_name(
        vec![mapped_int("a", 1)],
        vec![mapped_int("a", 2)],
        vec![drop_column(&["a"]), add(&[], mapped_int("a", 2))],
    )]
    #[case::names_swapped(
        vec![mapped_int("a", 1), mapped_int("b", 2)],
        vec![mapped_int("b", 1), mapped_int("a", 2)],
        vec![rename(&["a"], "b"), rename(&["b"], "a")],
    )]
    #[case::paths_under_a_renamed_parent_use_its_name_before_the_commit(
        vec![mapped_struct("z", 1, vec![mapped_int("x", 2), mapped_int("gone", 3)])],
        vec![mapped_struct("s", 1, vec![mapped_int("y", 2), mapped_int("new", 4)])],
        vec![
            rename(&["z"], "s"),
            rename(&["z", "x"], "y"),
            drop_column(&["z", "gone"]),
            add(&["z"], mapped_int("new", 4)),
        ],
    )]
    #[case::type_and_nullability(
        vec![mapped(StructField::not_null("a", DataType::INTEGER), 1)],
        vec![mapped(StructField::nullable("a", DataType::LONG), 1)],
        vec![
            set_nullable(&["a"]),
            change_type(&["a"], DataType::INTEGER, DataType::LONG),
        ],
    )]
    #[case::field_renamed_inside_an_array(
        vec![mapped_array_of_struct("list", 1, vec![mapped_int("x", 2)])],
        vec![mapped_array_of_struct("list", 1, vec![mapped_int("y", 2)])],
        vec![rename(&["list", "element", "x"], "y")],
    )]
    #[case::field_moved_to_another_struct_is_dropped_and_added(
        vec![mapped_struct("s", 1, vec![mapped_int("x", 3)]), mapped_struct("t", 2, vec![])],
        vec![mapped_struct("s", 1, vec![]), mapped_struct("t", 2, vec![mapped_int("x", 3)])],
        vec![drop_column(&["s", "x"]), add(&["t"], mapped_int("x", 3))],
    )]
    fn changes_by_field_id(
        #[case] before: Vec<StructField>,
        #[case] after: Vec<StructField>,
        #[case] expected: Vec<SchemaChange>,
    ) {
        let before = StructType::try_new(before).unwrap();
        let after = StructType::try_new(after).unwrap();
        assert_eq!(diff_schemas(&before, &after, true).unwrap(), expected);
    }

    #[rstest]
    #[case::field_without_an_id_before(
        vec![StructField::nullable("a", DataType::INTEGER)],
        vec![mapped_int("a", 1)],
        "field 'a' has no column mapping ID",
    )]
    #[case::field_without_an_id_after(
        vec![mapped_int("a", 1)],
        vec![StructField::nullable("a", DataType::INTEGER)],
        "field 'a' has no column mapping ID",
    )]
    #[case::two_fields_share_an_id_before(
        vec![mapped_int("a", 1), mapped_int("b", 1)],
        vec![mapped_int("a", 1)],
        "Field 'b' shares its column mapping ID",
    )]
    #[case::two_fields_share_an_id_after(
        vec![mapped_int("a", 1)],
        vec![mapped_int("a", 1), mapped_int("b", 1)],
        "Field 'b' shares its column mapping ID",
    )]
    #[case::nested_field_without_an_id(
        vec![mapped_struct("s", 1, vec![mapped_int("x", 2)])],
        vec![mapped_struct("s", 1, vec![StructField::nullable("x", DataType::INTEGER)])],
        "field 's.x' has no column mapping ID",
    )]
    fn changes_by_field_id_fail_without_unique_ids(
        #[case] before: Vec<StructField>,
        #[case] after: Vec<StructField>,
        #[case] message: &str,
    ) {
        let before = StructType::try_new(before).unwrap();
        let after = StructType::try_new(after).unwrap();
        assert_result_error_with_message(diff_schemas(&before, &after, true), message);
    }
}
