//! Error conditions from the Delta error catalog.
//!
//! A condition is a stable identity from Delta's error catalog that connectors match on instead
//! of message text. [`DeltaErrorCondition`] is generated from the vendored catalog in
//! `kernel/error_catalog/` by `cargo xtask update-delta-error-catalog`.

mod generated;

pub use self::generated::DeltaErrorCondition;

#[derive(Clone, Copy)]
struct ConditionMetadata {
    name: &'static str,
    sql_state: Option<&'static str>,
    parameter_names: &'static [&'static str],
    message_template: &'static str,
}

impl DeltaErrorCondition {
    /// Returns the catalog name of this condition, such as `DELTA_VERSIONS_NOT_CONTIGUOUS` or
    /// `PARENT.SUB` for a subclass.
    pub const fn name(self) -> &'static str {
        self.metadata().name
    }

    /// Returns the five-character SQLSTATE of this condition, if the catalog defines one.
    pub const fn sql_state(self) -> Option<&'static str> {
        self.metadata().sql_state
    }

    /// Returns the names of the `<name>` placeholders in [`Self::message_template`], in
    /// first-appearance order and without duplicates.
    pub const fn parameter_names(self) -> &'static [&'static str] {
        self.metadata().parameter_names
    }

    /// Returns the message template of this condition, with `<name>` placeholders for its
    /// parameters.
    ///
    /// The text is diagnostic and may change when the catalog is updated; match on the
    /// condition, not on its message.
    pub const fn message_template(self) -> &'static str {
        self.metadata().message_template
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::top_level(
        DeltaErrorCondition::DeltaVersionsNotContiguous,
        "DELTA_VERSIONS_NOT_CONTIGUOUS",
        Some("KD00C"),
        &["versionList", "startVersion", "endVersion", "versionToLoad"],
        "Versions (<versionList>) are not contiguous. \nA gap in the delta log between versions \
         <startVersion> and <endVersion> was detected while trying to load version \
         <versionToLoad>."
    )]
    #[case::subclass_inherits_sql_state(
        DeltaErrorCondition::DeltaCannotWriteEmptySchemaStructNoFields,
        "DELTA_CANNOT_WRITE_EMPTY_SCHEMA.STRUCT_NO_FIELDS",
        Some("428GU"),
        &["columnPath"],
        "The Delta table is not writable because parts of the schema are not supported.\nReason: \
         The column <columnPath> is a struct with no fields."
    )]
    fn condition_metadata(
        #[case] condition: DeltaErrorCondition,
        #[case] name: &str,
        #[case] sql_state: Option<&str>,
        #[case] parameter_names: &[&str],
        #[case] message_template: &str,
    ) {
        assert_eq!(condition.name(), name);
        assert_eq!(condition.sql_state(), sql_state);
        assert_eq!(condition.parameter_names(), parameter_names);
        assert_eq!(condition.message_template(), message_template);
    }

    #[test]
    fn all_conditions_have_unique_names_and_parameters_in_template() {
        let mut names = HashSet::new();
        for condition in DeltaErrorCondition::ALL {
            assert!(names.insert(condition.name()), "{}", condition.name());
            let mut parameters = HashSet::new();
            for parameter in condition.parameter_names() {
                assert!(
                    parameters.insert(parameter),
                    "{}: {parameter}",
                    condition.name()
                );
                let placeholder = format!("<{parameter}>");
                assert!(
                    condition.message_template().contains(&placeholder),
                    "{}: {placeholder}",
                    condition.name()
                );
            }
        }
    }
}
