//! Discovery and alteration of write expressions: expressions stored in the table that a
//! connector must enforce on every write, such as CHECK constraints.
//!
//! CHECK constraints are boolean SQL expressions stored in the table configuration under
//! `delta.constraints.<name>`. The Delta protocol requires every row in the table to satisfy every
//! one. Kernel never sees row data on the write path, so it does not evaluate constraints itself.
//! It exposes each constraint's raw SQL verbatim (via [`TableWriteExpressions`]) for a connector to
//! parse and enforce, and gates `write_state` and every commit on a table with constraints on the
//! connector acknowledging that responsibility (see [`Transaction::ack_check_constraints`]).
//! ALTER TABLE adds and drops constraints through
//! [`AlterTableTransactionBuilder`](crate::transaction::builder::alter_table::AlterTableTransactionBuilder).
//!
//! [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints

use crate::actions::{Metadata, Protocol};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::table_properties::{
    strip_check_constraint_prefix, CheckConstraint, CHECK_CONSTRAINT_PREFIX,
};
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// Read-only discovery of a table's CHECK constraints, implemented by both [`Snapshot`] and
/// [`Transaction`] so a connector can discover from either.
///
/// A [`Snapshot`] returns the constraints at its version. A [`Transaction`] returns the
/// constraints the table has once the transaction commits. For a create-table transaction these
/// are the declared ones, with names lowercased.
///
/// Discovery has no side effect: it does not acknowledge anything. See
/// [`Transaction::ack_check_constraints`] for which operations require the acknowledgement.
///
/// [`Snapshot`]: crate::Snapshot
/// [`Transaction`]: crate::transaction::Transaction
/// [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints
pub trait TableWriteExpressions {
    /// The table's CHECK constraints, sorted by name and then SQL. Empty when the table declares
    /// none.
    fn check_constraints(&self) -> &[CheckConstraint];
}

/// A CHECK-constraint change queued on an ALTER TABLE builder.
#[derive(Debug, Clone)]
pub(crate) enum CheckConstraintOperation {
    /// Add a constraint. The name is stored lowercased.
    Add { name: String, raw_sql: String },
    /// Drop every constraint whose name matches case-insensitively. With `if_exists`, a missing
    /// constraint is skipped instead of rejected.
    Drop { name: String, if_exists: bool },
}

/// Applies `operations` in order to `table_config`'s metadata and returns the validated
/// configuration. When the table ends up declaring a constraint without supporting the
/// `checkConstraints` writer feature, the protocol is upgraded to support it.
///
/// # Errors
///
/// Returns an error if an added constraint has an empty expression, if an added name matches an
/// existing constraint case-insensitively, if a dropped name matches no constraint, or if the
/// resulting metadata and protocol are invalid.
pub(crate) fn apply_check_constraint_operations(
    table_config: &TableConfiguration,
    operations: Vec<CheckConstraintOperation>,
) -> KernelResult<TableConfiguration> {
    let mut metadata = table_config.metadata().clone();
    for operation in operations {
        metadata = match operation {
            CheckConstraintOperation::Add { name, raw_sql } => {
                // Other writers look a constraint up by its lowercased key, so store it that way.
                let name = name.to_lowercase();
                require!(
                    !raw_sql.trim().is_empty(),
                    KernelError::generic(format!(
                        "CHECK constraint '{name}' has an empty expression"
                    ))
                );
                require!(
                    find_constraint_keys(&metadata, &name).is_empty(),
                    KernelError::generic(format!("CHECK constraint '{name}' already exists"))
                );
                metadata
                    .with_configuration_entry(format!("{CHECK_CONSTRAINT_PREFIX}{name}"), raw_sql)
            }
            CheckConstraintOperation::Drop { name, if_exists } => {
                let keys = find_constraint_keys(&metadata, &name);
                require!(
                    if_exists || !keys.is_empty(),
                    KernelError::generic(format!("CHECK constraint '{name}' does not exist"))
                );
                keys.iter().fold(metadata, |metadata, key| {
                    metadata.without_configuration_entry(key)
                })
            }
        };
    }

    let declares_constraints = metadata
        .configuration()
        .keys()
        .any(|key| strip_check_constraint_prefix(key).is_some());
    let protocol = if declares_constraints
        && !table_config.is_feature_supported(&TableFeature::CheckConstraints)
    {
        Some(protocol_with_check_constraints(table_config.protocol())?)
    } else {
        None
    };
    TableConfiguration::try_new_from(
        table_config,
        Some(metadata),
        protocol,
        table_config.version(),
    )
}

/// Returns the configuration keys of every constraint named `name`, compared case-insensitively.
/// Other writers can store one name under keys that differ only in case.
fn find_constraint_keys(metadata: &Metadata, name: &str) -> Vec<String> {
    let name = name.to_lowercase();
    metadata
        .configuration()
        .keys()
        .filter(|key| {
            strip_check_constraint_prefix(key)
                .is_some_and(|existing| existing.to_lowercase() == name)
        })
        .cloned()
        .collect()
}

/// Returns `protocol` upgraded to support the `checkConstraints` writer feature. A table-features
/// protocol lists the feature. A legacy protocol is raised to the lowest writer version that
/// implies it.
fn protocol_with_check_constraints(protocol: &Protocol) -> KernelResult<Protocol> {
    let reader_features = protocol.reader_features().map(<[_]>::to_vec);
    match protocol.writer_features() {
        Some(writer_features) => Protocol::try_new(
            protocol.min_reader_version(),
            protocol.min_writer_version(),
            reader_features,
            Some(
                writer_features
                    .iter()
                    .cloned()
                    .chain([TableFeature::CheckConstraints]),
            ),
        ),
        None => {
            let min_writer_version = TableFeature::CheckConstraints
                .info()
                .min_legacy_version
                .as_ref()
                .map_or(protocol.min_writer_version(), |version| version.writer);
            Protocol::try_new(
                protocol.min_reader_version(),
                protocol.min_writer_version().max(min_writer_version),
                reader_features,
                None::<Vec<TableFeature>>,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::unit_test_utils::{MockProtocolBuilder, MockTableConfigurationBuilder};

    #[test]
    fn legacy_protocol_is_raised_to_writer_version_implying_check_constraints() {
        let protocol = Protocol::try_new_legacy(1, 2).unwrap();
        let upgraded = protocol_with_check_constraints(&protocol).unwrap();
        let expected = Protocol::try_new_legacy(1, 3).unwrap();
        assert_eq!(upgraded, expected);
    }

    #[test]
    fn table_features_protocol_lists_check_constraints() {
        let protocol =
            Protocol::try_new_modern(Vec::<TableFeature>::new(), [TableFeature::AppendOnly])
                .unwrap();
        let upgraded = protocol_with_check_constraints(&protocol).unwrap();
        let expected = Protocol::try_new_modern(
            Vec::<TableFeature>::new(),
            [TableFeature::AppendOnly, TableFeature::CheckConstraints],
        )
        .unwrap();
        assert_eq!(upgraded, expected);
    }

    #[rstest]
    #[case::feature_not_supported(Vec::new(), true)]
    #[case::feature_already_supported(vec![TableFeature::CheckConstraints], false)]
    fn adding_a_constraint_changes_protocol_only_when_unsupported(
        #[case] writer_features: Vec<TableFeature>,
        #[case] expect_protocol_change: bool,
    ) {
        let table_config = MockTableConfigurationBuilder::new()
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_writer_features(writer_features)
                    .build(),
            )
            .build();
        let add = CheckConstraintOperation::Add {
            name: "positive".to_string(),
            raw_sql: "value > 0".to_string(),
        };

        let altered = apply_check_constraint_operations(&table_config, vec![add]).unwrap();
        let supported = altered.is_feature_supported(&TableFeature::CheckConstraints);
        assert!(supported);
        let protocol_changed = altered.protocol() != table_config.protocol();
        assert_eq!(protocol_changed, expect_protocol_change);
    }

    #[rstest]
    fn drop_removes_every_constraint_whose_name_matches_case_insensitively(
        #[values(false, true)] if_exists: bool,
    ) {
        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([
                ("delta.constraints.positive", "value > 0"),
                ("delta.constraints.POSITIVE", "value > 2"),
                ("delta.constraints.other", "value < 10"),
            ])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_writer_features([TableFeature::CheckConstraints])
                    .build(),
            )
            .build();
        let drop = CheckConstraintOperation::Drop {
            name: "Positive".to_string(),
            if_exists,
        };

        let dropped = apply_check_constraint_operations(&table_config, vec![drop]).unwrap();

        let remaining: Vec<_> = dropped
            .table_properties()
            .check_constraints
            .iter()
            .map(|constraint| constraint.name())
            .collect();
        assert_eq!(remaining, ["other"]);
    }

    #[test]
    fn non_ascii_names_compare_case_insensitively() {
        let table_config = MockTableConfigurationBuilder::new()
            .with_properties([("delta.constraints.\u{e4}", "value > 0")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_writer_features([TableFeature::CheckConstraints])
                    .build(),
            )
            .build();

        let add = CheckConstraintOperation::Add {
            name: "\u{c4}".to_string(),
            raw_sql: "value > 1".to_string(),
        };
        let add_result = apply_check_constraint_operations(&table_config, vec![add]);
        let rejected_as_duplicate = matches!(add_result, Err(KernelError::Generic(_)));
        assert!(rejected_as_duplicate);

        let drop = CheckConstraintOperation::Drop {
            name: "\u{c4}".to_string(),
            if_exists: false,
        };
        let dropped = apply_check_constraint_operations(&table_config, vec![drop]).unwrap();
        let remaining = &dropped.table_properties().check_constraints;
        assert!(remaining.is_empty());
    }
}
