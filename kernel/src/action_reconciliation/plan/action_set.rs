//! Replay intent and the source reads needed to reconstruct it.

#![allow(dead_code)]

use std::sync::LazyLock;

use crate::KernelResult;
use crate::actions::{
    ADD_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME, action_presence_leaf, get_all_actions_schema,
};
use crate::expressions::{
    ColumnName, Expression as Expr, Predicate, col, column_name, joined_column_expr,
};
use crate::schema::{DataType, SchemaRef, StructField, StructType, schema};

/// Name of the column used to identify actions during log replay.
pub(crate) const ACTION_KEY: &str = "action_key";

/// The reconstructed actions requested from log replay.
/// Live adds require reading commit removes; checkpoint tombstones are read only when requested.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ActionSet {
    /// Reconstruct live adds by replaying add and remove actions.
    pub(crate) live_adds: bool,
    /// Also include winning remove actions in the reconstructed result.
    pub(crate) include_tombstones: bool,
    pub(crate) metadata: bool,
    pub(crate) protocol: bool,
    pub(crate) transactions: bool,
    pub(crate) domain_metadata: bool,
}

/// The source supplying actions for log replay.
#[derive(Clone, Copy)]
pub(crate) enum ActionSource {
    Commit,
    Checkpoint,
    Crc,
}

impl ActionSet {
    pub(crate) const ALL: Self = Self {
        live_adds: true,
        include_tombstones: true,
        metadata: true,
        protocol: true,
        transactions: true,
        domain_metadata: true,
    };

    /// Requests live adds without tombstones.
    pub(crate) fn live_adds() -> Self {
        Self {
            live_adds: true,
            ..Default::default()
        }
    }

    /// Requests live adds and tombstones.
    pub(crate) fn live_adds_with_tombstones() -> Self {
        Self {
            include_tombstones: true,
            ..Self::live_adds()
        }
    }

    pub(crate) fn contains_content_actions(self) -> bool {
        self.live_adds
    }

    pub(crate) fn contains_non_content_actions(self) -> bool {
        self.metadata || self.protocol || self.transactions || self.domain_metadata
    }

    /// Whether this source must supply removes for the requested replay.
    pub(crate) fn reads_removes(self, source: ActionSource) -> bool {
        match source {
            ActionSource::Commit => self.live_adds,
            ActionSource::Checkpoint => self.live_adds && self.include_tombstones,
            ActionSource::Crc => false,
        }
    }

    /// Selects source actions using required leaves so readers can prune row groups.
    pub(crate) fn predicate_from_source(self, source: ActionSource) -> Predicate {
        action_predicate(self.read_action_names(source))
    }

    /// Selects requested reconstructed actions after replay.
    pub(crate) fn output_predicate(self) -> Predicate {
        action_predicate(self.output_action_names())
    }

    /// Selects the content actions needed from a replay source.
    pub(crate) fn content_predicate(self, source: ActionSource) -> Predicate {
        Self {
            live_adds: self.live_adds,
            include_tombstones: self.include_tombstones,
            ..Default::default()
        }
        .predicate_from_source(source)
    }

    /// Selects requested non-content metadata actions.
    pub(crate) fn non_content_predicate(self) -> Predicate {
        Self {
            live_adds: false,
            include_tombstones: false,
            ..self
        }
        .output_predicate()
    }

    /// Action types needed to reconstruct the requested state, including commit removes.
    pub(crate) fn replay_action_names(self) -> impl Iterator<Item = &'static str> {
        [
            (self.live_adds, ADD_NAME),
            (self.live_adds, REMOVE_NAME),
            (self.metadata, METADATA_NAME),
            (self.protocol, PROTOCOL_NAME),
            (self.transactions, SET_TRANSACTION_NAME),
            (self.domain_metadata, DOMAIN_METADATA_NAME),
        ]
        .into_iter()
        .filter_map(|(selected, name)| selected.then_some(name))
    }

    /// Action columns retained in the reconstructed result.
    pub(crate) fn output_action_names(self) -> impl Iterator<Item = &'static str> {
        self.replay_action_names()
            .filter(move |name| *name != REMOVE_NAME || self.include_tombstones)
    }

    /// Canonical schema containing the actions read from a source.
    pub(crate) fn schema(self, source: ActionSource) -> KernelResult<SchemaRef> {
        get_all_actions_schema().project(&self.read_action_names(source).collect::<Vec<_>>())
    }

    /// Builds action identities using the fields read from a replay source.
    /// File, transaction, and domain lanes use identities; singleton lanes use presence flags.
    pub(crate) fn key(self, source: ActionSource) -> KernelResult<(StructField, Expr)> {
        let actions = self;
        let mut lanes = vec![];
        if actions.live_adds {
            lanes.push((
                FILE_ACTION_KEY_FIELD.clone(),
                file_action_key_expr(actions.reads_removes(source)),
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

    fn read_action_names(self, source: ActionSource) -> impl Iterator<Item = &'static str> {
        let read_removes = self.reads_removes(source);
        self.replay_action_names()
            .filter(move |name| *name != REMOVE_NAME || read_removes)
    }
}

fn action_predicate(names: impl Iterator<Item = &'static str>) -> Predicate {
    Predicate::or_from(names.map(|name| {
        let column = ColumnName::new(std::iter::once(name).chain(action_presence_leaf(name)));
        Expr::column(column).is_not_null()
    }))
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

/// Builds the path/DV identity, referencing remove fields only when the source reads them.
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
