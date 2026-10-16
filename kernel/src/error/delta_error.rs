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
    use std::backtrace::Backtrace;
    use std::error::Error as StdError;
    use std::fmt::{self, Write as _};
    use std::io;

    use rstest::rstest;

    use super::{render_template, DeltaError, DeltaErrorParameter};
    use crate::{
        DeltaErrorCondition, Error, KernelError, Result, ResultIterator, ResultIteratorStatic,
    };

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
    #[case::without_parameters(
        DeltaErrorCondition::DeltaActiveSparkSessionNotFound,
        &[],
        Some("08003"),
        "Could not find active SparkSession"
    )]
    #[case::subclass(
        DeltaErrorCondition::DeltaCannotWriteEmptySchemaStructNoFields,
        &[("columnPath", "payload")],
        Some("428GU"),
        "The Delta table is not writable because parts of the schema are not supported.\nReason: \
         The column payload is a struct with no fields."
    )]
    #[case::repeated_parameter(
        DeltaErrorCondition::DeltaCannotUpdateArrayField,
        &[("tableName", "events"), ("fieldName", "items")],
        Some("429BQ"),
        "Cannot update events field items type: update the element by updating `items.element`."
    )]
    #[case::multiple_parameters(
        DeltaErrorCondition::DeltaVersionsNotContiguous,
        &[
            ("versionList", "10,12"),
            ("startVersion", "10"),
            ("endVersion", "12"),
            ("versionToLoad", "12"),
        ],
        Some("KD00C"),
        "Versions (10,12) are not contiguous. \nA gap in the delta log between versions \
         10 and 12 was detected while trying to load version 12."
    )]
    fn catalog_error_exposes_metadata_and_message(
        #[case] condition: DeltaErrorCondition,
        #[case] parameters: &[(&'static str, &str)],
        #[case] sql_state: Option<&str>,
        #[case] expected_message: &str,
    ) {
        let error = DeltaError::new(
            condition,
            parameters
                .iter()
                .map(|(name, value)| DeltaErrorParameter::new(name, value))
                .collect(),
            None,
        );

        assert_eq!(error.condition(), condition);
        assert_eq!(error.sql_state(), sql_state);
        assert_eq!(
            error
                .parameters()
                .iter()
                .map(|parameter| (parameter.name(), parameter.value()))
                .collect::<Vec<_>>(),
            parameters
        );
        assert_eq!(error.message(), expected_message);
        assert_eq!(error.to_string(), expected_message);
        assert!(error.source().is_none());

        let error: Error = error.into();
        assert_eq!(error.to_string(), expected_message);
        assert!(error.source().is_none());
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

    #[test]
    fn display_propagates_output_errors() {
        let error = DeltaError::new(
            DeltaErrorCondition::DeltaActiveSparkSessionNotFound,
            vec![],
            None,
        );
        let mut output = LimitedWriter { remaining_bytes: 0 };
        assert!(write!(&mut output, "{error}").is_err());
    }

    #[test]
    fn all_catalog_templates_render_with_literal_parameter_values() {
        for &condition in DeltaErrorCondition::ALL {
            let parameters: Vec<_> = condition
                .parameter_names()
                .iter()
                .map(|name| DeltaErrorParameter::new(name, format!("value of {name}")))
                .collect();
            let expected = parameters.iter().fold(
                condition.message_template().to_string(),
                |message, parameter| {
                    message.replace(&format!("<{}>", parameter.name()), parameter.value())
                },
            );
            let error = DeltaError::new(condition, parameters, None);
            assert_eq!(error.message(), expected, "{}", condition.name());
        }
    }

    #[rstest]
    #[case::direct(false)]
    #[case::nested_kernel_error(true)]
    fn envelope_preserves_original_cause_without_exposing_it_in_display(#[case] nested: bool) {
        let source = Box::new(io::Error::other("private cause"));
        let original_source = std::ptr::from_ref(source.as_ref());
        let source: Box<dyn StdError + Send + Sync> = if nested {
            Box::new(KernelError::GenericError { source })
        } else {
            source
        };
        let delta = DeltaError::new(
            DeltaErrorCondition::DeltaActiveSparkSessionNotFound,
            vec![],
            Some(source),
        );
        let error = Error::from(delta);
        let Error::Delta(delta) = &error else {
            panic!("expected a Delta error");
        };

        assert_eq!(error.to_string(), delta.message());
        assert!(!error.to_string().contains("private cause"));
        assert!(format!("{error:?}").contains("private cause"));
        let source = error.source().unwrap();
        assert!(std::ptr::eq(source, delta.source().unwrap()));
        let source = if nested {
            source
                .downcast_ref::<KernelError>()
                .unwrap()
                .source()
                .unwrap()
        } else {
            source
        };
        assert!(std::ptr::eq(
            source.downcast_ref::<io::Error>().unwrap(),
            original_source
        ));
    }

    #[test]
    fn constructor_captures_backtrace_and_envelope_preserves_it() {
        let delta = DeltaError::new(
            DeltaErrorCondition::DeltaActiveSparkSessionNotFound,
            vec![],
            None,
        );
        assert_eq!(delta.backtrace().status(), Backtrace::capture().status());
        let backtrace = delta.backtrace().to_string();
        let error: Error = delta.into();
        let Error::Delta(delta) = error else {
            panic!("expected a Delta error");
        };
        assert_eq!(delta.backtrace().to_string(), backtrace);
        assert!(format!("{delta:?}").contains(&format!("{:?}", delta.backtrace())));
    }

    #[test]
    fn delta_error_propagates_through_result() {
        let propagate = |result: std::result::Result<(), DeltaError>| -> Result<()> {
            result?;
            Ok(())
        };
        assert!(propagate(Ok(())).is_ok());
        let delta = DeltaError::new(
            DeltaErrorCondition::DeltaActiveSparkSessionNotFound,
            vec![],
            None,
        );
        let error = propagate(Err(delta)).unwrap_err();
        assert!(matches!(error, Error::Delta(delta)
            if delta.condition() == DeltaErrorCondition::DeltaActiveSparkSessionNotFound));
    }

    #[test]
    fn delta_error_propagates_through_result_iterator() {
        let delta = DeltaError::new(
            DeltaErrorCondition::DeltaActiveSparkSessionNotFound,
            vec![],
            None,
        );
        let errors = std::iter::once(Err::<(), _>(delta));
        let errors: ResultIteratorStatic<()> =
            Box::new(errors.map(|result| result.map_err(Error::from)));
        let mut errors: ResultIterator<'_, ()> = errors;
        assert!(matches!(
            errors.next().unwrap(),
            Err(Error::Delta(delta))
                if delta.condition() == DeltaErrorCondition::DeltaActiveSparkSessionNotFound
        ));
        assert!(errors.next().is_none());
    }
}
