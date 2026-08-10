//! Various utility functions/macros used throughout the kernel
use std::borrow::Cow;
use std::ops::Deref;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use delta_kernel_derive::internal_api;
use url::Url;

use crate::{DeltaResult, Error};

/// convenient way to return an error if a condition isn't true
macro_rules! require {
    ( $cond:expr, $err:expr ) => {
        if !($cond) {
            return Err($err);
        }
    };
}

pub(crate) use require;

/// Dual of the `FromIterator` trait, similar to how `Into` is the dual of `From`. It is
/// automatically implemented for any iterable whose items collect into `T`, and can drastically
/// simplify type bounds. For example, `CollectInto` allows to write this:
///
/// ```
/// # use delta_kernel::CollectInto;
/// # struct Foo;
/// fn foo(arg: impl CollectInto<Foo>) -> Foo {
///     arg.collect_into()
/// }
/// ```
///
/// instead of the much more verbose:
///
/// ```
/// # struct Foo;
/// fn foo<T>(arg: impl IntoIterator<Item = T>) -> Foo
/// where
///     Foo: FromIterator<T>,
/// {
///     Foo::from_iter(arg)
/// }
/// ```
pub trait CollectInto<T>: IntoIterator + Sized {
    /// Collects this iterable into a `T`
    fn collect_into(self) -> T;
}

// blanket impl
impl<I: IntoIterator, T: FromIterator<I::Item>> CollectInto<T> for I {
    fn collect_into(self) -> T {
        T::from_iter(self)
    }
}

/// Try to parse string uri into a URL for a table path. This will do it's best to handle things
/// like `/local/paths`, and even `../relative/paths`.
#[allow(unused)]
#[internal_api]
pub(crate) fn try_parse_uri(uri: impl AsRef<str>) -> DeltaResult<Url> {
    let uri = uri.as_ref();
    let uri_type = resolve_uri_type(uri)?;
    let url = match uri_type {
        UriType::LocalPath(path) => {
            if !path.exists() {
                // When we support writes, create a directory if we can
                return Err(Error::InvalidTableLocation(format!(
                    "Path does not exist: {path:?}"
                )));
            }
            if !path.is_dir() {
                return Err(Error::InvalidTableLocation(format!(
                    "{path:?} is not a directory"
                )));
            }
            let path = std::fs::canonicalize(path).map_err(|err| {
                let msg = format!("Invalid table location: {uri} Error: {err:?}");
                Error::InvalidTableLocation(msg)
            })?;
            Url::from_directory_path(path.clone()).map_err(|_| {
                let msg = format!(
                    "Could not construct a URL from canonicalized path: {path:?}.\n\
                     Something must be very wrong with the table path."
                );
                Error::InvalidTableLocation(msg)
            })?
        }
        UriType::Url(url) => url,
    };
    Ok(url)
}

#[allow(unused)]
#[derive(Debug)]
enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// Utility function to figure out whether string representation of the path is either local path or
/// some kind or URL.
///
/// Will return an error if the path is not valid.
#[allow(unused)]
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();
    let table_uri = if table_uri.ends_with('/') {
        Cow::Borrowed(table_uri)
    } else {
        Cow::Owned(format!("{table_uri}/"))
    };
    if let Ok(url) = Url::parse(&table_uri) {
        let scheme = url.scheme().to_string();
        if url.scheme() == "file" {
            Ok(UriType::LocalPath(
                url.to_file_path()
                    .map_err(|_| Error::invalid_table_location(table_uri))?,
            ))
        } else if scheme.len() == 1 {
            // NOTE this check is required to support absolute windows paths which may properly
            // parse as url we assume here that a single character scheme is a windows drive letter
            Ok(UriType::LocalPath(PathBuf::from(table_uri.as_ref())))
        } else {
            Ok(UriType::Url(url))
        }
    } else {
        Ok(UriType::LocalPath(table_uri.deref().into()))
    }
}

/// Returns the current time as a Duration since Unix epoch.
pub(crate) fn current_time_duration() -> DeltaResult<Duration> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::generic(format!("System time before Unix epoch: {e}")))
}

/// Returns the current time in milliseconds since Unix epoch.
pub(crate) fn current_time_ms() -> DeltaResult<i64> {
    let duration = current_time_duration()?;
    i64::try_from(duration.as_millis())
        .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))
}

/// Returns the current time in microseconds since Unix epoch.
pub(crate) fn current_time_micros() -> DeltaResult<i64> {
    let duration = current_time_duration()?;
    duration_to_micros(duration)
}

fn duration_to_micros(duration: Duration) -> DeltaResult<i64> {
    i64::try_from(duration.as_micros())
        .map_err(|_| Error::generic("Current timestamp exceeds i64 microsecond range"))
}

/// Extension trait for folding zero or one value from an [`Option`] into a base value.
#[internal_api]
pub(crate) trait FoldWithOption: Sized {
    /// Applies an optional fold operation `f` to `self` if `opt` is [`Some`]; otherwise returns
    /// `self` unchanged.
    ///
    /// Similar to `opt.iter().fold(self, |acc, value| f(acc, value))`, but accepting `FnOnce`
    /// instead of requiring `FnMut`, and with the base value as receiver instead of the option.
    fn fold_with<U>(self, opt: Option<U>, f: impl FnOnce(Self, U) -> Self) -> Self {
        match opt {
            Some(value) => f(self, value),
            None => self,
        }
    }

    /// Fallible [`fold_with`](Self::fold_with): applies `Result`-returning `f` to `self` if `opt`
    /// is [`Some`], otherwise returns `self` unchanged (wrapped in `Ok`).
    fn try_fold_with<U, E>(
        self,
        opt: Option<U>,
        f: impl FnOnce(Self, U) -> Result<Self, E>,
    ) -> Result<Self, E> {
        match opt {
            Some(value) => f(self, value),
            None => Ok(self),
        }
    }
}

// Blanket impl -- every type can fold_with an Option.
impl<T: Sized> FoldWithOption for T {}

/// Extension trait for adding completion callbacks to iterators.
pub(crate) trait IteratorExt: Iterator + Sized {
    /// Wraps this iterator to call a closure when fully exhausted.
    ///
    /// The closure is called only when `next()` returns `None`. If the iterator
    /// is dropped before exhaustion, a warning is logged but the closure is not called.
    fn on_complete<F: FnOnce()>(self, f: F) -> OnComplete<Self, F> {
        OnComplete {
            inner: self,
            on_complete: Some(f),
        }
    }
}

impl<I: Iterator> IteratorExt for I {}

/// Iterator adaptor that executes a closure when fully exhausted.
pub(crate) struct OnComplete<I, F: FnOnce()> {
    inner: I,
    on_complete: Option<F>,
}

impl<I, F: FnOnce()> Drop for OnComplete<I, F> {
    fn drop(&mut self) {
        if self.on_complete.is_some() {
            tracing::debug!(
                "OnComplete iterator dropped before exhaustion; completion callback not called"
            );
        }
    }
}

impl<I, F> Iterator for OnComplete<I, F>
where
    I: Iterator,
    F: FnOnce(),
{
    type Item = I::Item;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(item) => Some(item),
            None => {
                if let Some(f) = self.on_complete.take() {
                    f();
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_time_micros_is_within_system_time_bounds() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let current = u128::try_from(current_time_micros().unwrap()).unwrap();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        assert!(before <= current && current <= after);
    }

    #[test]
    fn duration_to_micros_checks_i64_bounds() {
        let max = Duration::from_micros(i64::MAX as u64);
        assert_eq!(duration_to_micros(max).unwrap(), i64::MAX);

        let overflow = Duration::from_micros(i64::MAX as u64 + 1);
        assert!(duration_to_micros(overflow)
            .unwrap_err()
            .to_string()
            .contains("i64 microsecond range"));
    }

    #[test]
    fn test_path_parsing() {
        for x in [
            // windows parsing of file:/// is... odd
            #[cfg(not(windows))]
            "file:///foo/bar",
            #[cfg(not(windows))]
            "file:///foo/bar/",
            "/foo/bar",
            "/foo/bar/",
            "../foo/bar",
            "../foo/bar/",
            "c:/foo/bar",
            "c:/",
            "file:///C:/",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::LocalPath(_)) => {}
                x => panic!("Should have parsed as a local path {x:?}"),
            }
        }

        for x in [
            "s3://foo/bar",
            "s3a://foo/bar",
            "memory://foo/bar",
            "gs://foo/bar",
            "https://foo/bar/",
            "unknown://foo/bar",
            "s2://foo/bar",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::Url(_)) => {}
                x => panic!("Should have parsed as a url {x:?}"),
            }
        }

        #[cfg(not(windows))]
        resolve_uri_type("file://foo/bar").expect_err("file://foo/bar should not have parsed");
    }

    #[test]
    fn try_from_uri_without_trailing_slash() {
        let location = "s3://foo/__unitystorage/catalogs/cid/tables/tid";
        let url = try_parse_uri(location).unwrap();

        assert_eq!(
            url.to_string(),
            "s3://foo/__unitystorage/catalogs/cid/tables/tid/"
        );
    }

    mod on_complete_tests {
        use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
        use std::sync::Arc;

        use super::*;

        #[test]
        fn test_calls_on_exhaustion() {
            let called = Arc::new(AtomicBool::new(false));
            let called_clone = called.clone();
            let mut iter = vec![1, 2].into_iter().on_complete(move || {
                called_clone.store(true, Ordering::SeqCst);
            });
            assert_eq!(iter.next(), Some(1));
            assert!(!called.load(Ordering::SeqCst));
            assert_eq!(iter.next(), Some(2));
            assert_eq!(iter.next(), None);
            assert!(called.load(Ordering::SeqCst));
        }

        #[test]
        fn test_does_not_call_on_early_drop() {
            let called = Arc::new(AtomicBool::new(false));
            let called_clone = called.clone();
            {
                let mut iter = vec![1, 2].into_iter().on_complete(move || {
                    called_clone.store(true, Ordering::SeqCst);
                });
                assert_eq!(iter.next(), Some(1));
                // Drop without exhausting - callback should NOT be called
            }
            assert!(!called.load(Ordering::SeqCst));
        }

        #[test]
        fn test_calls_only_once() {
            let count = Arc::new(AtomicU32::new(0));
            let count_clone = count.clone();
            {
                let mut iter = vec![1].into_iter().on_complete(move || {
                    count_clone.fetch_add(1, Ordering::SeqCst);
                });
                assert_eq!(iter.next(), Some(1));
                assert_eq!(iter.next(), None); // triggers callback
                assert_eq!(iter.next(), None); // should not trigger again
            } // drop should not trigger again
            assert_eq!(count.load(Ordering::SeqCst), 1);
        }
    }
}
