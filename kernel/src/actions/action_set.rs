//! Action selections shared by declarative sources and replay.

#![allow(dead_code)]

use std::sync::LazyLock;

use super::{
    ADD_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME, action_presence_leaf, get_all_actions_schema,
};
use crate::KernelResult;
use crate::expressions::{
    ColumnName, Expression as Expr, Predicate, col, column_name, joined_column_expr,
};
use crate::schema::{DataType, SchemaRef, StructField, StructType, schema};

/// Add and remove share a path and DV identity (storageType, pathOrInlineDv, offset).
/// Materialize it once for the aggregate and anti-join operators.
pub(crate) const ACTION_KEY: &str = "action_key";

/// The actions selected for source reads and reconciliation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ActionSet {
    pub(crate) file_actions: bool,
    /// Whether selected file actions include removes alongside adds.
    pub(crate) includes_removes: bool,
    pub(crate) metadata: bool,
    pub(crate) protocol: bool,
    pub(crate) transactions: bool,
    pub(crate) domain_metadata: bool,
}

impl ActionSet {
    pub(crate) const ALL: Self = Self {
        file_actions: true,
        includes_removes: true,
        metadata: true,
        protocol: true,
        transactions: true,
        domain_metadata: true,
    };

    /// Selects add actions only.
    pub(crate) fn adds() -> Self {
        Self {
            file_actions: true,
            ..Default::default()
        }
    }

    /// Selects both add and remove actions.
    pub(crate) fn files() -> Self {
        Self {
            includes_removes: true,
            ..Self::adds()
        }
    }

    pub(crate) fn contains_content_actions(self) -> bool {
        self.file_actions
    }

    pub(crate) fn contains_non_content_actions(self) -> bool {
        self.metadata || self.protocol || self.transactions || self.domain_metadata
    }

    /// Required non-null leaves identifying the selected action types.
    pub(crate) fn required_leaves(self) -> impl Iterator<Item = ColumnName> {
        self.names()
            .map(|name| ColumnName::new(std::iter::once(name).chain(action_presence_leaf(name))))
    }

    /// Selects actions using required leaves so readers can prune row groups.
    pub(crate) fn predicate(self) -> Predicate {
        Predicate::or_from(
            self.required_leaves()
                .map(|column| Expr::column(column).is_not_null()),
        )
    }

    /// Selects requested file actions.
    pub(crate) fn content_predicate(self) -> Predicate {
        Self {
            file_actions: self.file_actions,
            includes_removes: self.includes_removes,
            ..Default::default()
        }
        .predicate()
    }

    /// Selects requested non-file actions.
    pub(crate) fn non_content_predicate(self) -> Predicate {
        Self {
            file_actions: false,
            includes_removes: false,
            ..self
        }
        .predicate()
    }

    /// Physical columns carried by this selection.
    pub(crate) fn names(self) -> impl Iterator<Item = &'static str> {
        [
            (self.file_actions, ADD_NAME),
            (self.file_actions && self.includes_removes, REMOVE_NAME),
            (self.metadata, METADATA_NAME),
            (self.protocol, PROTOCOL_NAME),
            (self.transactions, SET_TRANSACTION_NAME),
            (self.domain_metadata, DOMAIN_METADATA_NAME),
        ]
        .into_iter()
        .filter_map(|(selected, name)| selected.then_some(name))
    }

    /// Canonical schema containing only the selected actions.
    pub(crate) fn schema(self) -> KernelResult<SchemaRef> {
        get_all_actions_schema().project(&self.names().collect::<Vec<_>>())
    }

    /// Builds the identity field and expression for the selected actions.
    /// File, transaction, and domain lanes use identities; singleton lanes use presence flags.
    pub(crate) fn key(self) -> KernelResult<(StructField, Expr)> {
        let actions = self;
        let mut lanes = vec![];
        if actions.file_actions {
            lanes.push((
                FILE_ACTION_KEY_FIELD.clone(),
                file_action_key_expr(actions.includes_removes),
            ));
        }
        if actions.metadata {
            lanes.push((
                StructField::not_null("metadata", DataType::BOOLEAN),
                Expr::from_pred(col!("metaData.id").is_not_null()),
            ));
        }
        if actions.protocol {
            lanes.push((
                StructField::not_null("protocol", DataType::BOOLEAN),
                Expr::from_pred(col!("protocol.minReaderVersion").is_not_null()),
            ));
        }
        if actions.transactions {
            lanes.push((
                StructField::nullable("transaction", DataType::STRING),
                col!("txn.appId"),
            ));
        }
        if actions.domain_metadata {
            lanes.push((
                StructField::nullable("domain", DataType::STRING),
                col!("domainMetadata.domain"),
            ));
        }
        let key_schema = StructType::try_new(lanes.iter().map(|(field, _)| field.clone()))?;
        Ok((
            StructField::not_null(ACTION_KEY, key_schema),
            Expr::struct_from(lanes.into_iter().map(|(_, expr)| expr)),
        ))
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
    let path = key_col_expr(column_name!("path"));
    let storage_type = key_col_expr(column_name!("deletionVector.storageType"));
    let fields = [
        path.clone(),
        Expr::struct_with_nullability_from(
            [
                storage_type.clone(),
                key_col_expr(column_name!("deletionVector.pathOrInlineDv")),
                key_col_expr(column_name!("deletionVector.offset")),
            ],
            Expr::from_pred(storage_type.is_not_null()),
        ),
    ];
    Expr::struct_with_nullability_from(fields, Expr::from_pred(path.is_not_null()))
}
