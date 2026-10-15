//! Catalog-backed Delta failures and their diagnostic parameters.

use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt;

use super::DeltaErrorCondition;

/// A catalog-defined Delta failure that terminates the current operation.
///
/// Match on [`Self::condition`] and [`Self::sql_state`], not on diagnostic messages or parameter
/// values. A failure does not invalidate the snapshot or require engine shutdown.
///
/// [`fmt::Display`] renders only the catalog message. The original cause is available through
/// [`StdError::source`], and [`fmt::Debug`] includes the cause and backtrace. Diagnostic values are
/// not redacted; connectors are responsible for redacting telemetry.
#[derive(Debug)]
pub struct DeltaError {
    condition: DeltaErrorCondition,
    parameters: Box<[DeltaErrorParameter]>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    backtrace: Backtrace,
}

/// A named, stringified diagnostic value for a [`DeltaError`] message template.
///
/// The name is a stable catalog identifier; the value is diagnostic text, not a classification key.
#[derive(Debug, Eq, PartialEq)]
pub struct DeltaErrorParameter {
    name: &'static str,
    value: String,
}

impl DeltaError {
    /// Returns the typed Delta condition; [`DeltaErrorCondition::name`] provides its stable name.
    pub fn condition(&self) -> DeltaErrorCondition {
        self.condition
    }

    /// Returns the condition's five-character SQLSTATE, if the catalog defines one.
    pub fn sql_state(&self) -> Option<&'static str> {
        self.condition.sql_state()
    }

    /// Returns the named diagnostic parameters in catalog template order.
    pub fn parameters(&self) -> &[DeltaErrorParameter] {
        &self.parameters
    }

    /// Renders the catalog message, inserting parameter values verbatim.
    ///
    /// Inserted values are not interpreted as further placeholders. The message excludes the cause
    /// and backtrace and is diagnostic text, not a stable classification key.
    pub fn message(&self) -> String {
        self.to_string()
    }

    /// Returns the backtrace captured at construction, subject to Rust's backtrace configuration.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    /// Creates an error with an optional diagnostic cause and captures its backtrace.
    ///
    /// `parameters` must contain exactly [`DeltaErrorCondition::parameter_names`], in that order.
    /// Condition-specific factories must maintain this invariant.
    #[allow(
        dead_code,
        reason = "Construction is reserved for condition-specific factories."
    )]
    fn new(
        condition: DeltaErrorCondition,
        parameters: Vec<DeltaErrorParameter>,
        source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            condition,
            parameters: parameters.into_boxed_slice(),
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

impl fmt::Display for DeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        render_template(
            self.condition.message_template(),
            &self.parameters,
            formatter,
        )
    }
}

impl StdError for DeltaError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

impl DeltaErrorParameter {
    /// Returns the parameter's stable catalog name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the parameter's diagnostic string value without redaction.
    pub fn value(&self) -> &str {
        &self.value
    }

    #[allow(
        dead_code,
        reason = "Construction is reserved for condition-specific factories."
    )]
    fn new(name: &'static str, value: impl ToString) -> Self {
        Self {
            name,
            value: value.to_string(),
        }
    }
}

fn render_template(
    template: &str,
    parameters: &[DeltaErrorParameter],
    output: &mut impl fmt::Write,
) -> fmt::Result {
    let mut remaining = template;
    while let Some(start) = remaining.find('<') {
        output.write_str(&remaining[..start])?;
        remaining = &remaining[start + 1..];
        if let Some(end) = remaining.find('>') {
            let name = &remaining[..end];
            if let Some(parameter) = parameters.iter().find(|parameter| parameter.name == name) {
                output.write_str(&parameter.value)?;
                remaining = &remaining[end + 1..];
                continue;
            }
        }
        output.write_char('<')?;
    }
    output.write_str(remaining)
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use rstest::rstest;

    use super::{render_template, DeltaErrorParameter};

    struct LimitedWriter {
        remaining_bytes: usize,
    }

    impl fmt::Write for LimitedWriter {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.remaining_bytes = self
                .remaining_bytes
                .checked_sub(text.len())
                .ok_or(fmt::Error)?;
            Ok(())
        }
    }

    #[rstest]
    #[case::zero(0, "0")]
    #[case::positive(42, "42")]
    #[case::negative(-42, "-42")]
    fn parameter_stringifies_diagnostic_value(#[case] value: i64, #[case] expected: &str) {
        let parameter = DeltaErrorParameter::new("version", value);
        assert_eq!(parameter.name(), "version");
        assert_eq!(parameter.value(), expected);
    }

    #[rstest]
    #[case::empty_value("<first>", "", "")]
    #[case::adjacent_parameters("<first><second>", "value", "valuesecond")]
    #[case::placeholder_in_value(
        "<first> <second> <first>",
        "<second>",
        "<second> second <second>"
    )]
    #[case::unicode("π<first>東京", "Δé\n文字", "πΔé\n文字東京")]
    #[case::literal_angle_brackets(
        "a<> < first > <unknown> <<first>> <first",
        "value",
        "a<> < first > <unknown> <value> <first"
    )]
    #[case::literal_comparison("a < b > c <first>", "value", "a < b > c value")]
    fn rendering_preserves_literal_text(
        #[case] template: &str,
        #[case] first_value: &str,
        #[case] expected: &str,
    ) {
        let parameters = [
            DeltaErrorParameter::new("first", first_value),
            DeltaErrorParameter::new("second", "second"),
        ];
        let mut message = String::new();
        render_template(template, &parameters, &mut message).unwrap();
        assert_eq!(message, expected);
    }

    #[rstest]
    #[case::literal_prefix("prefix<first>", 5)]
    #[case::parameter_value("<first>", 0)]
    #[case::literal_delimiter("<", 0)]
    #[case::literal_suffix("<first>suffix", 5)]
    fn rendering_propagates_output_errors(#[case] template: &str, #[case] remaining_bytes: usize) {
        let parameters = [DeltaErrorParameter::new("first", "value")];
        let mut output = LimitedWriter { remaining_bytes };
        assert_eq!(
            render_template(template, &parameters, &mut output),
            Err(fmt::Error)
        );
    }
}
