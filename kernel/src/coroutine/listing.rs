//! Paginated file-listing requests.

use url::Url;

use super::PagedOperation;
use crate::path::may_begin_listable_log_path;
use crate::{DeltaResult, Error, FileMeta, Version};

/// Selects descendants of `prefix` whose full URLs are in the exclusive range `(low, high)`.
///
/// Comparisons use UTF-8 byte order. A bare-version bound sorts before files carrying that version
/// prefix.
pub struct ListingBounds {
    /// Common URL prefix for entries in this listing.
    pub prefix: Url,
    /// Exclusive lower URL bound.
    pub low: Url,
    /// Exclusive upper URL bound.
    pub high: Url,
}

impl ListingBounds {
    /// Return whether `entry` remains within these bounds.
    ///
    /// Errors remain within bounds so the listing consumer can observe them.
    pub fn contains(&self, entry: &DeltaResult<FileMeta>) -> bool {
        is_within_listing_bounds(entry, &self.prefix, &self.high)
    }

    /// Parse the upper bound's final path segment as a Delta version.
    ///
    /// Returns an error if the URL has no final segment or that segment is not a version.
    pub fn high_version(&self) -> DeltaResult<Version> {
        version_from_listing_bound(&self.high)
    }

    /// Select the next descending version window within these bounds.
    ///
    /// `high` is the current exclusive upper version. `window_size` controls how many versions the
    /// returned window spans.
    ///
    /// Returns an error if a bound does not end in a version or `window_size` is zero.
    pub fn backward_window(
        &self,
        high: Version,
        window_size: Version,
    ) -> DeltaResult<BackwardListingWindow> {
        if window_size == 0 {
            return Err(Error::generic(
                "backward listing window size must be greater than zero",
            ));
        }
        let start = version_from_listing_bound(&self.low)?;
        let lower = high.saturating_sub(window_size).max(start);
        Ok(BackwardListingWindow {
            prefix: self.prefix.clone(),
            low: bare_version_path(&self.prefix, lower)?,
            high: bare_version_path(&self.prefix, high)?,
            next_high: (lower > start).then_some(lower),
        })
    }
}

/// A bounded listing whose pages and entries are in ascending lexicographic order.
pub struct ForwardListing(pub Box<ListingBounds>);

impl ForwardListing {
    /// Default number of entries in one listing page.
    pub const DEFAULT_PAGE_SIZE: usize = 1024;
}

impl PagedOperation for ForwardListing {
    type Page = Vec<DeltaResult<FileMeta>>;
}

/// A bounded listing whose page ranges move high to low, with entries ascending within each page.
pub struct BackwardListing(pub Box<ListingBounds>);

impl BackwardListing {
    /// Default number of Delta versions covered by one listing request.
    pub const DEFAULT_WINDOW_SIZE: Version = 1000;
}

impl PagedOperation for BackwardListing {
    type Page = BackwardListingResult;
}

/// One backward-listing page with entries in ascending lexicographic order.
pub struct BackwardListingResult {
    /// Entries in this page.
    pub entries: Vec<DeltaResult<FileMeta>>,
    /// True if all file version numbers in all future pages will be strictly lower than the lowest
    /// file version seen so far.
    ///
    /// False is conservative and always valid.
    pub known_version_boundary: bool,
}

/// URL bounds and continuation state for one backward-listing window.
pub struct BackwardListingWindow {
    /// Common URL prefix for entries in this window.
    pub prefix: Url,
    /// Inclusive lower bound for this window.
    pub low: Url,
    /// Exclusive upper bound for this window.
    pub high: Url,
    /// Upper version for the next lower window, or `None` when this window reaches the range
    /// start.
    pub next_high: Option<Version>,
}

impl BackwardListingWindow {
    /// Return whether `entry` remains within this window.
    ///
    /// Errors remain within bounds so the listing consumer can observe them.
    pub fn contains(&self, entry: &DeltaResult<FileMeta>) -> bool {
        is_within_listing_bounds(entry, &self.prefix, &self.high)
    }
}

fn is_within_listing_bounds(entry: &DeltaResult<FileMeta>, prefix: &Url, high: &Url) -> bool {
    let Ok(entry) = entry else {
        return true;
    };
    let path = entry.location.as_str();
    path < high.as_str()
        && path
            .strip_prefix(prefix.as_str())
            .is_none_or(may_begin_listable_log_path)
}

fn version_from_listing_bound(bound: &Url) -> DeltaResult<Version> {
    bound
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .ok_or_else(|| Error::internal_error("listing bound has no path segment"))?
        .parse()
        .map_err(|_| Error::internal_error("listing bound is not a version"))
}

/// Map an inclusive version range to exclusive bare-version URL bounds.
pub(crate) fn log_listing_bounds(
    log_root: &Url,
    start_version: Version,
    end_version: Version,
) -> DeltaResult<ListingBounds> {
    Ok(ListingBounds {
        prefix: log_root.clone(),
        low: bare_version_path(log_root, start_version)?,
        high: bare_version_path(log_root, end_version.saturating_add(1))?,
    })
}

/// Build the bare version path used as an exclusive listing bound.
fn bare_version_path(log_root: &Url, version: Version) -> DeltaResult<Url> {
    Ok(log_root.join(&format!("{version:020}"))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    fn bounds(start: Version, end: Version) -> ListingBounds {
        log_listing_bounds(&Url::parse("memory:///_delta_log/").unwrap(), start, end).unwrap()
    }

    #[test]
    fn backward_windows_descend_without_overlapping_versions() {
        let bounds = bounds(10, 3010);
        let first = bounds
            .backward_window(
                bounds.high_version().unwrap(),
                BackwardListing::DEFAULT_WINDOW_SIZE,
            )
            .unwrap();
        assert_eq!(first.low.path(), "/_delta_log/00000000000000002011");
        assert_eq!(first.high.path(), "/_delta_log/00000000000000003011");
        assert_eq!(first.next_high, Some(2011));

        let last = bounds
            .backward_window(
                first.next_high.unwrap(),
                BackwardListing::DEFAULT_WINDOW_SIZE * 3,
            )
            .unwrap();
        assert_eq!(last.low.path(), "/_delta_log/00000000000000000010");
        assert_eq!(last.next_high, None);
    }

    #[test]
    fn listing_bounds_keep_errors_and_stop_after_version_named_paths() {
        let bounds = bounds(0, 10);
        let error = Err(Error::generic("listing failed"));
        assert!(bounds.contains(&error));

        let sidecar = Ok(FileMeta::new(
            Url::parse("memory:///_delta_log/_sidecars/part.parquet").unwrap(),
            0,
            0,
        ));
        assert!(!bounds.contains(&sidecar));
    }
}
