use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

use super::{DeltaErrorCondition, KernelError};

type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// A named parameter used to render a [`DeltaError`] message template.
#[derive(Debug, Eq, PartialEq)]
pub struct DeltaErrorParameter {
    name: &'static str,
    value: String,
}

impl DeltaErrorParameter {
    #[allow(dead_code)]
    pub(crate) fn new(name: &'static str, value: impl ToString) -> Self {
        Self {
            name,
            value: value.to_string(),
        }
    }

    /// Returns the parameter's stable catalog name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the parameter's diagnostic display value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// A structured, user-facing error produced while operating on a Delta table.
///
/// The condition, SQLSTATE, and parameter names are stable identifiers. The rendered message is
/// diagnostic text and may evolve when the pinned Delta error catalog is updated.
#[derive(Debug)]
pub struct DeltaError {
    condition: DeltaErrorCondition,
    parameters: Box<[DeltaErrorParameter]>,
    source: Option<BoxedError>,
    backtrace: Backtrace,
}

impl DeltaError {
    #[allow(dead_code)]
    pub(crate) fn new(
        condition: DeltaErrorCondition,
        parameters: impl Into<Box<[DeltaErrorParameter]>>,
    ) -> Result<Self, KernelError> {
        let parameters = parameters.into();
        Self::validate_parameters(condition, &parameters)?;
        Ok(Self {
            condition,
            parameters,
            source: None,
            backtrace: Backtrace::capture(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn with_source(
        condition: DeltaErrorCondition,
        parameters: impl Into<Box<[DeltaErrorParameter]>>,
        source: impl Into<BoxedError>,
    ) -> Result<Self, KernelError> {
        let parameters = parameters.into();
        Self::validate_parameters(condition, &parameters)?;
        Ok(Self {
            condition,
            parameters,
            source: Some(source.into()),
            backtrace: Backtrace::capture(),
        })
    }

    /// Returns the typed Delta error condition.
    pub fn condition(&self) -> DeltaErrorCondition {
        self.condition
    }

    /// Returns the stable string identity of the Delta error condition.
    pub fn condition_name(&self) -> &'static str {
        self.condition.name()
    }

    /// Returns the SQLSTATE associated with the condition, if the catalog defines one.
    pub fn sql_state(&self) -> Option<&'static str> {
        self.condition.sql_state()
    }

    /// Returns the named message parameters in template order.
    pub fn parameters(&self) -> &[DeltaErrorParameter] {
        &self.parameters
    }

    /// Returns the value of the named message parameter, if present.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|parameter| parameter.name == name)
            .map(|parameter| parameter.value.as_str())
    }

    /// Renders the user-facing message from the catalog template and parameters.
    pub fn message(&self) -> String {
        let mut rendered = String::new();
        let mut remaining = self.condition.message_template();

        while let Some(open) = remaining.find('<') {
            let (prefix, placeholder_and_tail) = remaining.split_at(open);
            rendered.push_str(prefix);

            let Some(close) = placeholder_and_tail.find('>') else {
                rendered.push_str(placeholder_and_tail);
                return rendered;
            };
            let (placeholder, tail) = placeholder_and_tail.split_at(close + 1);
            let name = &placeholder[1..placeholder.len() - 1];
            if let Some(value) = self.parameter(name) {
                rendered.push_str(value);
            } else {
                rendered.push_str(placeholder);
            }
            remaining = tail;
        }

        rendered.push_str(remaining);
        rendered
    }

    /// Returns the backtrace captured when this error was created.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    fn validate_parameters(
        condition: DeltaErrorCondition,
        parameters: &[DeltaErrorParameter],
    ) -> Result<(), KernelError> {
        let expected = condition.parameter_names();
        if parameters.len() == expected.len()
            && parameters
                .iter()
                .zip(expected)
                .all(|(parameter, expected)| parameter.name == *expected)
        {
            return Ok(());
        }

        let actual = parameters
            .iter()
            .map(DeltaErrorParameter::name)
            .collect::<Vec<_>>();
        Err(KernelError::internal_error(format!(
            "invalid parameters for {}: expected {expected:?}, got {actual:?}",
            condition.name()
        )))
    }
}

impl Display for DeltaError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

impl StdError for DeltaError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions_not_contiguous() -> DeltaError {
        DeltaError::new(
            DeltaErrorCondition::DeltaVersionsNotContiguous,
            vec![
                DeltaErrorParameter::new("versionList", "1, 3"),
                DeltaErrorParameter::new("startVersion", 1),
                DeltaErrorParameter::new("endVersion", 3),
                DeltaErrorParameter::new("versionToLoad", 3),
            ],
        )
        .unwrap()
    }

    #[test]
    fn exposes_catalog_metadata_and_renders_parameters() {
        let error = versions_not_contiguous();

        assert_eq!(
            error.condition(),
            DeltaErrorCondition::DeltaVersionsNotContiguous
        );
        assert_eq!(error.condition_name(), "DELTA_VERSIONS_NOT_CONTIGUOUS");
        assert_eq!(error.sql_state(), Some("KD00C"));
        assert_eq!(error.parameter("startVersion"), Some("1"));
        assert_eq!(error.parameter("missing"), None);
        assert_eq!(
            error.to_string(),
            "Versions (1, 3) are not contiguous. \n\
             A gap in the delta log between versions 1 and 3 was detected while trying to load \
             version 3."
        );
    }

    #[test]
    fn preserves_diagnostic_source() {
        let error = DeltaError::with_source(
            DeltaErrorCondition::DeltaActiveSparkSessionNotFound,
            Vec::new(),
            std::io::Error::other("log listing failed"),
        )
        .unwrap();

        assert_eq!(
            StdError::source(&error).map(ToString::to_string),
            Some("log listing failed".to_string())
        );
    }

    #[test]
    fn rendering_does_not_reinterpret_parameter_values() {
        let error = DeltaError::new(
            DeltaErrorCondition::DeltaVersionNotFound,
            vec![
                DeltaErrorParameter::new("userVersion", "<earliest>"),
                DeltaErrorParameter::new("earliest", 1),
                DeltaErrorParameter::new("latest", 3),
            ],
        )
        .unwrap();

        assert_eq!(
            error.to_string(),
            "Cannot time travel Delta table to version <earliest>. Available versions: [1, 3]."
        );
    }

    #[test]
    fn construction_rejects_invalid_parameter_names_or_order() {
        let error = DeltaError::new(
            DeltaErrorCondition::DeltaVersionNotFound,
            vec![
                DeltaErrorParameter::new("earliest", 1),
                DeltaErrorParameter::new("userVersion", 0),
                DeltaErrorParameter::new("unexpected", 3),
            ],
        )
        .unwrap_err();

        assert!(error.to_string().contains(
            "expected [\"userVersion\", \"earliest\", \"latest\"], got \
             [\"earliest\", \"userVersion\", \"unexpected\"]"
        ));
    }
}
