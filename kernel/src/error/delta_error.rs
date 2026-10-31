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
/// Use [`Self::name`] and [`Self::sql_state`] to read the condition's classification directly.
/// Catalog template details are available on [`DeltaErrorCondition`].
///
/// [`fmt::Display`] renders only the catalog message. The original cause is available through
/// [`StdError::source`], and [`fmt::Debug`] includes every field and the rendered message.
/// Diagnostic values are not redacted. Connectors are responsible for redacting telemetry.
pub struct DeltaError {
    condition: DeltaErrorCondition,
    parameters: Box<[DeltaErrorParameter]>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    backtrace: Backtrace,
}

/// A named, stringified diagnostic value for a [`DeltaError`] message template.
///
/// The name is a stable catalog identifier. The value is diagnostic text, not a classification key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaErrorParameter {
    name: &'static str,
    value: String,
}

impl DeltaError {
    /// Returns the typed Delta condition.
    pub fn condition(&self) -> DeltaErrorCondition {
        self.condition
    }

    /// Returns the condition's stable catalog name.
    pub fn name(&self) -> &'static str {
        self.condition.name()
    }

    /// Returns the condition's five-character SQLSTATE, if the catalog defines one.
    pub fn sql_state(&self) -> Option<&'static str> {
        self.condition.sql_state()
    }

    /// Returns the named diagnostic parameters in catalog template order.
    pub fn parameters(&self) -> &[DeltaErrorParameter] {
        &self.parameters
    }

    /// Returns the rendered catalog message, equivalent to `self.to_string()`.
    ///
    /// See the [`fmt::Display`] implementation for the rendering contract.
    pub fn message(&self) -> String {
        self.to_string()
    }

    /// Returns the backtrace captured at construction, subject to Rust's backtrace configuration.
    ///
    /// Every construction calls [`Backtrace::capture`] with no per-error opt-out. Capture is
    /// disabled unless `RUST_BACKTRACE` or `RUST_LIB_BACKTRACE` enables it.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    /// Creates an error with an optional diagnostic cause and captures its backtrace.
    ///
    /// `parameters` must contain exactly [`DeltaErrorCondition::parameter_names`], in that order.
    /// Condition-specific factories must maintain this invariant.
    // TODO(#3421): Remove this dead code attribute when the first delta error factory is introduced
    #[allow(
        dead_code,
        reason = "Reserved for condition-specific factories (see #3421)."
    )]
    fn new(
        condition: DeltaErrorCondition,
        parameters: Vec<DeltaErrorParameter>,
        source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    ) -> Self {
        debug_assert!(
            parameters
                .iter()
                .map(DeltaErrorParameter::name)
                .eq(condition.parameter_names().iter().copied()),
            "{} requires parameters {:?}, got {:?}",
            condition.name(),
            condition.parameter_names(),
            parameters
                .iter()
                .map(DeltaErrorParameter::name)
                .collect::<Vec<_>>(),
        );
        Self {
            condition,
            parameters: parameters.into_boxed_slice(),
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

/// Renders the catalog message, inserting parameter values verbatim.
///
/// Inserted values are not interpreted as further placeholders. The message excludes the cause
/// and backtrace and is diagnostic text, not a stable classification key.
impl fmt::Display for DeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        render_template(
            self.condition.message_template(),
            &self.parameters,
            formatter,
        )
    }
}

impl fmt::Debug for DeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaError")
            .field("message", &self.to_string())
            .field("condition", &self.condition)
            .field("parameters", &self.parameters)
            .field("source", &self.source)
            .field("backtrace", &self.backtrace)
            .finish()
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

    // TODO(#3421): Remove this dead code attribute when the first delta error factory is introduced
    #[allow(
        dead_code,
        reason = "Reserved for condition-specific factories (see #3421)."
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
    use std::backtrace::{Backtrace, BacktraceStatus};
    use std::error::Error as StdError;
    use std::{fmt, io};

    use rstest::rstest;

    use super::{render_template, DeltaError, DeltaErrorParameter};
    use crate::{DeltaErrorCondition, Error, Result};

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

    #[test]
    fn display_and_accessors_expose_condition_metadata_and_substituted_message() {
        let delta = version_gap_error("0, 2", None);
        assert_eq!(
            delta.condition(),
            DeltaErrorCondition::DeltaVersionsNotContiguous
        );
        assert_eq!(delta.name(), "DELTA_VERSIONS_NOT_CONTIGUOUS");
        assert_eq!(delta.sql_state(), Some("KD00C"));
        let parameters: Vec<_> = delta
            .parameters()
            .iter()
            .map(|parameter| (parameter.name(), parameter.value()))
            .collect();
        assert_eq!(
            parameters,
            [
                ("versionList", "0, 2"),
                ("startVersion", "0"),
                ("endVersion", "2"),
                ("versionToLoad", "2"),
            ]
        );
        assert_eq!(
            delta.to_string(),
            "Versions (0, 2) are not contiguous. \n\
            A gap in the delta log between versions 0 and 2 was detected while trying to load \
            version 2."
        );
        assert_eq!(delta.message(), delta.to_string());
        assert!(delta.source().is_none());
    }

    #[rstest]
    #[case::compact(false)]
    #[case::pretty(true)]
    fn debug_quotes_and_escapes_message_in_delta_and_envelope(#[case] pretty: bool) {
        let delta = version_gap_error("\"0\",\n2", None);
        let expected = format!("message: {:?}", delta.message());
        let debug = if pretty {
            format!("{delta:#?}")
        } else {
            format!("{delta:?}")
        };
        assert!(debug.contains(&expected), "{debug}");
        let error = Error::from(delta);
        let debug = if pretty {
            format!("{error:#?}")
        } else {
            format!("{error:?}")
        };
        assert!(debug.contains(&expected), "{debug}");
    }

    #[test]
    fn envelope_preserves_original_cause_without_exposing_it_in_display() {
        let source = Box::new(io::Error::other("private cause"));
        let original_source = std::ptr::from_ref(source.as_ref());
        let delta = version_gap_error("0, 2", Some(source));
        let error = Error::from(delta);
        let Error::Delta(delta) = &error else {
            panic!("expected a Delta error");
        };
        assert_eq!(error.to_string(), delta.message());
        assert!(!error.to_string().contains("private cause"));
        assert!(format!("{error:?}").contains("private cause"));
        assert!(std::ptr::eq(
            error.source().unwrap(),
            delta.source().unwrap()
        ));
        assert!(std::ptr::eq(
            error.source().unwrap().downcast_ref::<io::Error>().unwrap(),
            original_source,
        ));
    }

    #[test]
    fn delta_error_propagates_through_result() {
        let propagate = |delta: DeltaError| -> Result<()> {
            Err(delta)?;
            Ok(())
        };
        let error = propagate(version_gap_error("0, 2", None)).unwrap_err();
        let Error::Delta(delta) = error else {
            panic!("expected a Delta error");
        };
        assert_eq!(
            delta.condition(),
            DeltaErrorCondition::DeltaVersionsNotContiguous
        );
        assert_eq!(delta.parameters()[0].value(), "0, 2");
    }

    #[test]
    fn constructor_captures_backtrace_and_envelope_preserves_it() {
        let ambient = Backtrace::capture().status();
        if ambient != BacktraceStatus::Captured {
            eprintln!("skipping: backtrace capture is {ambient:?}; set RUST_BACKTRACE=1 to run");
            return;
        }

        let delta = version_gap_error("0, 2", None);
        assert_eq!(delta.backtrace().status(), BacktraceStatus::Captured);
        let backtrace = delta.backtrace().to_string();

        let error: Error = delta.into();
        let Error::Delta(delta) = error else {
            panic!("expected a Delta error");
        };
        assert_eq!(delta.backtrace().to_string(), backtrace);
        assert!(format!("{delta:?}").contains(&format!("{:?}", delta.backtrace())));
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

    fn version_gap_error(
        version_list: &str,
        source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    ) -> DeltaError {
        DeltaError::new(
            DeltaErrorCondition::DeltaVersionsNotContiguous,
            vec![
                DeltaErrorParameter::new("versionList", version_list),
                DeltaErrorParameter::new("startVersion", 0),
                DeltaErrorParameter::new("endVersion", 2),
                DeltaErrorParameter::new("versionToLoad", 2),
            ],
            source,
        )
    }
}
