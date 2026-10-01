//! Discovery and alteration of CHECK constraints.
//!
//! CHECK constraints are boolean SQL expressions stored in the table configuration under
//! `delta.constraints.<name>`. The Delta protocol requires every added row to satisfy every one.
//! Kernel never sees row data on the write path, so it does not evaluate constraints itself. It
//! exposes each constraint's raw SQL verbatim (via [`TableWriteExpressions`]) for a connector to
//! parse and enforce, and gates any commit that adds data or introduces a constraint on the
//! connector acknowledging that responsibility (see [`Transaction::ack_check_constraints`]).
//! ALTER TABLE adds and drops constraints through
//! [`AlterTableTransactionBuilder`](crate::transaction::builder::alter_table::AlterTableTransactionBuilder).
//!
//! [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::actions::{Metadata, Protocol};
use crate::schema::SchemaRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::table_properties::{strip_check_constraint_prefix, CHECK_CONSTRAINT_PREFIX};
use crate::utils::require;
use crate::{DeltaResult, Error, Snapshot};

/// Read-only discovery of a table's CHECK constraints, implemented by both [`Snapshot`] and
/// [`Transaction`] so a connector can discover from either.
///
/// A [`Snapshot`] returns the constraints at its version. A [`Transaction`] returns the
/// constraints the table has once the transaction commits, which for a create-table transaction
/// are the declared ones.
///
/// Discovery has no side effect: it does not acknowledge anything. A data-adding commit to a
/// constrained table still requires [`Transaction::ack_check_constraints`].
///
/// [`Transaction`]: crate::transaction::Transaction
/// [`Transaction::ack_check_constraints`]: crate::transaction::Transaction::ack_check_constraints
pub trait TableWriteExpressions {
    /// The table's CHECK constraints. Empty when the table declares none.
    fn check_constraints(&self) -> CheckConstraints;
}

impl TableWriteExpressions for Snapshot {
    fn check_constraints(&self) -> CheckConstraints {
        self.table_configuration().check_constraints()
    }
}

/// One CHECK constraint: its name, the raw SQL stored under `delta.constraints.<name>`, and the
/// logical schema of the table that declares it.
///
/// Two constraints are equal when their names match case-insensitively and their raw SQL matches.
/// The schema is not part of the constraint's identity.
#[derive(Debug, Clone)]
pub struct CheckConstraint {
    name: String,
    raw_sql: String,
    // The logical schema of the table that declares this constraint.
    #[allow(dead_code)]
    schema: SchemaRef,
}

impl PartialEq for CheckConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.name.to_lowercase() == other.name.to_lowercase() && self.raw_sql == other.raw_sql
    }
}

impl Eq for CheckConstraint {}

impl CheckConstraint {
    /// The constraint's name: the `<name>` suffix of its `delta.constraints.<name>` config key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The constraint's boolean SQL expression, verbatim from the table configuration. This is the
    /// authoritative form, and kernel does not parse or interpret it.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }
}

/// All CHECK constraints on a table. Dereferences to a slice for iteration.
#[derive(Debug, Clone, Default)]
pub struct CheckConstraints(Arc<[CheckConstraint]>);

impl CheckConstraints {
    /// Builds [`CheckConstraints`] from a table's already-parsed constraints: a map from each
    /// constraint's name to its raw SQL (the
    /// [`check_constraints`](crate::table_properties::TableProperties::check_constraints) field of
    /// [`TableProperties`](crate::table_properties::TableProperties)). Each entry becomes a
    /// [`CheckConstraint`] verbatim, carrying `schema`, the table's logical schema. The
    /// `delta.constraints.` prefix is already stripped from the names and non-constraint keys are
    /// already filtered out during property parsing, so this does no filtering of its own.
    pub(crate) fn from_parsed(constraints: &HashMap<String, String>, schema: &SchemaRef) -> Self {
        Self(
            constraints
                .iter()
                .map(|(name, raw_sql)| CheckConstraint {
                    name: name.clone(),
                    raw_sql: raw_sql.clone(),
                    schema: schema.clone(),
                })
                .collect(),
        )
    }
}

impl Deref for CheckConstraints {
    type Target = [CheckConstraint];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A CHECK-constraint change queued on an ALTER TABLE builder.
#[derive(Debug, Clone)]
pub(crate) enum CheckConstraintOperation {
    /// Add a constraint. The name is stored lowercased.
    Add { name: String, raw_sql: String },
    /// Drop a constraint, matching its name case-insensitively.
    Drop { name: String },
}

/// Applies `operations` in order to `table_config`'s metadata and returns the validated
/// configuration. When the table ends up declaring a constraint without supporting the
/// `checkConstraints` writer feature, the protocol is upgraded to support it.
///
/// # Errors
///
/// Returns an error if an added constraint has an empty name or an empty expression, if an added
/// name matches an existing constraint case-insensitively, if a dropped name matches no constraint,
/// or if the resulting metadata and protocol are invalid.
pub(crate) fn apply_check_constraint_operations(
    table_config: &TableConfiguration,
    operations: Vec<CheckConstraintOperation>,
) -> DeltaResult<TableConfiguration> {
    let mut metadata = table_config.metadata().clone();
    for operation in operations {
        metadata = match operation {
            CheckConstraintOperation::Add { name, raw_sql } => {
                // Other writers look a constraint up by its lowercased key, so store it that way.
                let name = name.to_lowercase();
                require!(
                    !name.is_empty(),
                    Error::generic("CHECK constraint name must not be empty")
                );
                require!(
                    !raw_sql.trim().is_empty(),
                    Error::generic(format!("CHECK constraint '{name}' has an empty expression"))
                );
                require!(
                    find_constraint_key(&metadata, &name).is_none(),
                    Error::generic(format!("CHECK constraint '{name}' already exists"))
                );
                metadata
                    .with_configuration_entry(format!("{CHECK_CONSTRAINT_PREFIX}{name}"), raw_sql)
            }
            CheckConstraintOperation::Drop { name } => {
                let key = find_constraint_key(&metadata, &name).ok_or_else(|| {
                    Error::generic(format!("CHECK constraint '{name}' does not exist"))
                })?;
                metadata.without_configuration_entry(&key)
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

/// Returns the configuration key of the constraint named `name`, compared case-insensitively.
fn find_constraint_key(metadata: &Metadata, name: &str) -> Option<String> {
    let name = name.to_lowercase();
    metadata
        .configuration()
        .keys()
        .find(|key| {
            strip_check_constraint_prefix(key)
                .is_some_and(|existing| existing.to_lowercase() == name)
        })
        .cloned()
}

/// Returns `protocol` upgraded to support the `checkConstraints` writer feature. A table-features
/// protocol lists the feature. A legacy protocol is raised to the lowest writer version that
/// implies it.
fn protocol_with_check_constraints(protocol: &Protocol) -> DeltaResult<Protocol> {
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
    use crate::schema::schema_ref;
    use crate::unit_test_utils::{MockProtocolBuilder, MockTableConfigurationBuilder};

    fn parsed(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn test_schema() -> SchemaRef {
        schema_ref! {
            nullable "amount": LONG,
            nullable "name": STRING,
        }
    }

    #[test]
    fn maps_parsed_entries_to_constraints() {
        let schema = test_schema();
        let constraints = CheckConstraints::from_parsed(
            &parsed(&[("positive", "amount > 0"), ("named", "name = 'a'")]),
            &schema,
        );
        let mut discovered: Vec<_> = constraints
            .iter()
            .map(|c| (c.name(), c.raw_sql()))
            .collect();
        discovered.sort();
        let expected = [("named", "name = 'a'"), ("positive", "amount > 0")];
        assert_eq!(discovered, expected);

        let carries_schema = constraints.iter().all(|c| c.schema == schema);
        assert!(carries_schema);
    }

    #[test]
    fn empty_parsed_map_yields_no_constraints() {
        let constraints = CheckConstraints::from_parsed(&parsed(&[]), &test_schema());
        assert!(constraints.is_empty());
    }

    #[test]
    fn equality_ignores_schema() {
        let entries = parsed(&[("positive", "amount > 0")]);
        let with_table_schema = CheckConstraints::from_parsed(&entries, &test_schema());
        let with_other_schema =
            CheckConstraints::from_parsed(&entries, &schema_ref! { nullable "amount": LONG });
        assert_eq!(with_table_schema[0], with_other_schema[0]);
    }

    #[test]
    fn equality_folds_name_case_but_not_raw_sql() {
        let schema = test_schema();
        let lower = CheckConstraints::from_parsed(&parsed(&[("positive", "amount > 0")]), &schema);
        let upper = CheckConstraints::from_parsed(&parsed(&[("POSITIVE", "amount > 0")]), &schema);
        assert_eq!(lower[0], upper[0]);

        let other_sql =
            CheckConstraints::from_parsed(&parsed(&[("positive", "amount > 1")]), &schema);
        assert_ne!(lower[0], other_sql[0]);

        let non_ascii_lower =
            CheckConstraints::from_parsed(&parsed(&[("\u{e4}", "amount > 0")]), &schema);
        let non_ascii_upper =
            CheckConstraints::from_parsed(&parsed(&[("\u{c4}", "amount > 0")]), &schema);
        assert_eq!(non_ascii_lower[0], non_ascii_upper[0]);
    }

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
        let rejected_as_duplicate = matches!(add_result, Err(Error::Generic(_)));
        assert!(rejected_as_duplicate);

        let drop = CheckConstraintOperation::Drop {
            name: "\u{c4}".to_string(),
        };
        let dropped = apply_check_constraint_operations(&table_config, vec![drop]).unwrap();
        let remaining = dropped.check_constraints();
        assert!(remaining.is_empty());
    }
}
