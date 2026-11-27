//! Selected error conditions from the Delta error catalog.
//!
//! A condition is a stable identity from Delta's error catalog that connectors match on instead
//! of message text. [`DeltaErrorCondition`] is generated from the pinned catalog and explicit
//! allowlist in `kernel/error_catalog/` by `cargo xtask generate-delta-error-conditions`.

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
