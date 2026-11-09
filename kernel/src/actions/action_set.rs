//! Action identities and predicates used by declarative metadata plans.

use std::sync::LazyLock;

use super::{ADD_NAME, REMOVE_NAME};
use crate::expressions::{
    ColumnName, Expression as Expr, Predicate, col, column_name, joined_column_expr,
};
use crate::schema::{StructField, schema};
use crate::struct_patch::ProjectionStructPatchBuilder;

/// Add and remove share a path and DV identity (storageType, pathOrInlineDv, offset).
/// Materialize it once for the aggregate and anti-join operators.
pub(crate) const ACTION_KEY: &str = "action_key";

/// Adds the file identity, coalescing add/remove fields only when removes are included.
pub(crate) fn with_file_action_key(
    patch: ProjectionStructPatchBuilder<'_>,
    includes_removes: bool,
) -> ProjectionStructPatchBuilder<'_> {
    patch.append(
        FILE_ACTION_KEY_FIELD.clone(),
        file_action_key_expr(includes_removes),
    )
}

/// Selects file actions using their required path fields.
pub(crate) fn file_action_predicate(includes_removes: bool) -> Predicate {
    let add = col!(ADD_NAME, "path").is_not_null();
    if includes_removes {
        Predicate::or(add, col!(REMOVE_NAME, "path").is_not_null())
    } else {
        add
    }
}

/// File identity used for replay.
static FILE_ACTION_KEY_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    let schema = schema! {
        nullable "path": STRING,
        nullable "deletionVector": {
            not_null "storageType": STRING,
            not_null "pathOrInlineDv": STRING,
            nullable "offset": INTEGER,
        },
    };
    StructField::nullable(ACTION_KEY, schema)
});

/// Build a file identity from path and deletion vector.
fn file_action_key_expr(includes_removes: bool) -> Expr {
    let key_col_expr = |col: ColumnName| {
        let add = joined_column_expr!("add", col);
        if includes_removes {
            Expr::coalesce([add, joined_column_expr!("remove", col)])
        } else {
            add
        }
    };
    let storage_type = key_col_expr(column_name!("deletionVector.storageType"));
    Expr::struct_from([
        key_col_expr(column_name!("path")),
        Expr::struct_with_nullability_from(
            [
                storage_type.clone(),
                key_col_expr(column_name!("deletionVector.pathOrInlineDv")),
                key_col_expr(column_name!("deletionVector.offset")),
            ],
            Expr::from_pred(storage_type.is_not_null()),
        ),
    ])
}
