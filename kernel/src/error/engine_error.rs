//! Errors produced by engine operations, before Kernel interprets their Delta meaning.

use std::convert::Infallible;

use crate::schema::DataType;

/// The result of an engine operation.
pub type EngineResult<T> = Result<T, EngineError>;

/// A boxed, `Send` iterator of engine results borrowing data for `'a`.
pub type EngineResultIterator<'a, T> = Box<dyn Iterator<Item = EngineResult<T>> + Send + 'a>;

/// An engine-result iterator that does not borrow non-static data.
pub type EngineResultIteratorStatic<T> = EngineResultIterator<'static, T>;

/// An engine failure that Kernel can interpret without knowing the engine implementation.
///
/// Engines retain native diagnostics in `source`. Kernel decides whether a recognized failure
/// permits recovery or describes a Delta condition; other failures propagate as engine errors.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// A requested file does not exist.
    #[error("file not found: {path}")]
    FileNotFound {
        /// The requested file location.
        path: String,
        /// The underlying engine failure, when available.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// A conditional write collided with an existing file.
    #[error("file already exists: {path}")]
    FileAlreadyExists {
        /// The destination file location.
        path: String,
        /// The underlying engine failure, when available.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// A file cannot be decoded because its contents are corrupt.
    #[error("corrupt file: {path}")]
    CorruptFile {
        /// The corrupt file location.
        path: String,
        /// The underlying decoder failure, when available.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// An engine operation was cancelled rather than completed.
    #[error("operation cancelled")]
    Cancelled,
    /// A value cannot be parsed as the requested data type.
    #[error("failed to parse '{value}' as {data_type}")]
    ParseError {
        /// The value being parsed.
        value: String,
        /// The requested data type.
        data_type: DataType,
        /// The underlying parser failure, when available.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// An engine failure without a more specific Kernel-understood classification.
    #[error("external engine error: {message}")]
    External {
        /// The engine's diagnostic message.
        message: String,
        /// The underlying engine failure, when available.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl EngineError {
    /// Returns an external engine failure retaining `source` and its diagnostic message.
    pub fn external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::External {
            message: source.to_string(),
            source: Some(Box::new(source)),
        }
    }

    /// Returns a missing-file failure for `path` without an underlying source.
    pub fn file_not_found(path: impl ToString) -> Self {
        Self::FileNotFound {
            path: path.to_string(),
            source: None,
        }
    }

    /// Returns a conditional-write collision for `path` without an underlying source.
    pub fn file_already_exists(path: impl ToString) -> Self {
        Self::FileAlreadyExists {
            path: path.to_string(),
            source: None,
        }
    }
}

impl From<Infallible> for EngineError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

macro_rules! external_error {
    ($($source:ty),* $(,)?) => {
        $(impl From<$source> for EngineError {
            fn from(source: $source) -> Self {
                Self::external(source)
            }
        })*
    };
}

external_error!(std::io::Error, serde_json::Error, url::ParseError);

#[cfg(feature = "need-arrow")]
external_error!(crate::arrow::error::ArrowError);

#[cfg(feature = "default-engine-base")]
external_error!(
    crate::parquet::errors::ParquetError,
    crate::object_store::path::Error,
    reqwest::Error,
);

#[cfg(feature = "default-engine-base")]
impl From<crate::object_store::Error> for EngineError {
    fn from(source: crate::object_store::Error) -> Self {
        match &source {
            crate::object_store::Error::NotFound { path, .. } => Self::FileNotFound {
                path: path.clone(),
                source: Some(Box::new(source)),
            },
            crate::object_store::Error::AlreadyExists { path, .. } => Self::FileAlreadyExists {
                path: path.clone(),
                source: Some(Box::new(source)),
            },
            _ => Self::external(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io::{Error as IoError, ErrorKind};

    use rstest::rstest;

    use super::*;
    use crate::{DeltaResult, Error};

    #[rstest]
    #[case::missing(EngineError::FileNotFound {
        path: "memory:///missing".into(),
        source: Some(Box::new(IoError::from(ErrorKind::NotFound))),
    }, ErrorKind::NotFound)]
    #[case::exists(EngineError::FileAlreadyExists {
        path: "memory:///exists".into(),
        source: Some(Box::new(IoError::from(ErrorKind::AlreadyExists))),
    }, ErrorKind::AlreadyExists)]
    #[case::corrupt(EngineError::CorruptFile {
        path: "memory:///corrupt".into(),
        source: Some(Box::new(IoError::from(ErrorKind::InvalidData))),
    }, ErrorKind::InvalidData)]
    #[case::parse(EngineError::ParseError {
        value: "not an integer".into(),
        data_type: DataType::INTEGER,
        source: Some(Box::new(IoError::from(ErrorKind::InvalidData))),
    }, ErrorKind::InvalidData)]
    #[case::external(
        EngineError::external(IoError::from(ErrorKind::PermissionDenied)),
        ErrorKind::PermissionDenied
    )]
    fn native_source_survives_public_error_conversion(
        #[case] error: EngineError,
        #[case] expected: ErrorKind,
    ) {
        let diagnostic = error.to_string();
        let error = Error::from(error);
        assert!(matches!(error, Error::Engine(_)));
        assert_eq!(error.to_string(), diagnostic);
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<IoError>()
                .unwrap()
                .kind(),
            expected
        );
    }

    #[rstest]
    fn eager_and_lazy_errors_keep_engine_origin(#[values(false, true)] lazy: bool) {
        let read = || -> EngineResult<EngineResultIteratorStatic<()>> {
            if lazy {
                Ok(Box::new(std::iter::once(Err(EngineError::Cancelled))))
            } else {
                Err(EngineError::Cancelled)
            }
        };
        let result = (|| -> DeltaResult<()> {
            for item in read()? {
                item?;
            }
            Ok(())
        })();
        assert!(matches!(result, Err(Error::Engine(EngineError::Cancelled))));
    }

    #[test]
    fn context_preserves_recognized_engine_classification() {
        let error = Error::from(EngineError::file_not_found("memory:///missing"))
            .with_context(crate::error::ErrorContext::Operation("read file"));
        assert!(
            matches!(error, Error::Engine(EngineError::FileNotFound { path, source: None })
            if path == "memory:///missing")
        );
    }

    #[cfg(feature = "default-engine-base")]
    #[rstest]
    fn object_store_classification_retains_native_source(#[values(false, true)] exists: bool) {
        let source = Box::new(IoError::from(ErrorKind::Other));
        let native = if exists {
            crate::object_store::Error::AlreadyExists {
                path: "data".into(),
                source,
            }
        } else {
            crate::object_store::Error::NotFound {
                path: "data".into(),
                source,
            }
        };
        let error = EngineError::from(native);
        if exists {
            assert!(
                matches!(&error, EngineError::FileAlreadyExists { path, .. } if path == "data")
            );
        } else {
            assert!(matches!(&error, EngineError::FileNotFound { path, .. } if path == "data"));
        }
        let native = error.source().unwrap();
        assert!(native.is::<crate::object_store::Error>());
        assert!(native.source().unwrap().is::<IoError>());
    }
}
