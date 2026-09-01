//! Paginated file-listing requests.

use delta_kernel_derive::internal_api;
use url::Url;

use super::core::{PendingPageRequest, PendingRequest};
use super::{Channel, Cursor, Page, PageRequest, PagedOperation};
use crate::path::may_begin_listable_log_path;
use crate::{DeltaResult, Error, FileMeta, Version};

/// Selects descendants of `prefix` whose full URLs are in the exclusive range `(low, high)`.
///
/// Comparisons use UTF-8 byte order. A bare-version bound sorts before files carrying that version
/// prefix.
pub struct ListingBounds {
    pub prefix: Url,
    pub low: Url,
    pub high: Url,
}

/// A bounded listing whose pages and entries are in ascending lexicographic order.
pub struct ForwardListing(pub Box<ListingBounds>);

impl PagedOperation for ForwardListing {
    type Page = Vec<DeltaResult<FileMeta>>;
}

/// A bounded listing whose page ranges move high to low, with entries ascending within each page.
pub struct BackwardListing(pub Box<ListingBounds>);

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

/// Default number of listing entries in one forward-listing page.
#[internal_api]
pub(crate) const DEFAULT_FORWARD_LISTING_PAGE_SIZE: usize = 1024;

/// Default number of Delta versions covered by one backward-listing request.
#[internal_api]
pub(crate) const DEFAULT_BACKWARD_LISTING_WINDOW_SIZE: Version = 1000;

/// URL bounds and continuation state for one backward-listing window.
#[internal_api]
pub(crate) struct BackwardListingWindow {
    /// Inclusive lower bound for this window.
    pub low: Url,
    /// Exclusive upper bound for this window.
    pub high: Url,
    /// Upper version for the next lower window, or `None` when this window reaches the range
    /// start.
    pub next_high: Option<Version>,
}

/// Select the next descending version window within `bounds`.
///
/// `high` is the current exclusive upper version. `window_size` controls how many versions the
/// returned window spans.
///
/// Returns an error if a bound does not end in a version or `window_size` is zero.
#[internal_api]
pub(crate) fn backward_listing_window(
    bounds: &ListingBounds,
    high: Version,
    window_size: Version,
) -> DeltaResult<BackwardListingWindow> {
    if window_size == 0 {
        return Err(Error::generic(
            "backward listing window size must be greater than zero",
        ));
    }
    let start = version_from_listing_bound(&bounds.low)?;
    let lower = high.saturating_sub(window_size).max(start);
    Ok(BackwardListingWindow {
        low: bare_version_path(&bounds.prefix, lower)?,
        high: bare_version_path(&bounds.prefix, high)?,
        next_high: (lower > start).then_some(lower),
    })
}

/// Return whether `entry` remains within a bounded, version-named log listing.
///
/// Errors remain within bounds so the listing consumer can observe them.
#[internal_api]
pub(crate) fn is_within_listing_bounds(
    entry: &DeltaResult<FileMeta>,
    prefix: &Url,
    high: &Url,
) -> bool {
    let Ok(entry) = entry else {
        return true;
    };
    let path = entry.location.as_str();
    path < high.as_str()
        && path
            .strip_prefix(prefix.as_str())
            .is_none_or(may_begin_listable_log_path)
}

/// Parse the final path segment of a listing bound as a Delta version.
///
/// Returns an error if the URL has no final segment or that segment is not a version.
#[internal_api]
pub(crate) fn version_from_listing_bound(bound: &Url) -> DeltaResult<Version> {
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
pub(crate) fn bare_version_path(log_root: &Url, version: Version) -> DeltaResult<Url> {
    Ok(log_root.join(&format!("{version:020}"))?)
}

impl Channel {
    /// Initialize a forward listing and return its first page.
    pub(crate) async fn start_forward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Page<ForwardListing>> {
        self.exchange(ForwardListing(Box::new(bounds)), |request| {
            PendingRequest::ListForward(PendingPageRequest::Start(request))
        })
        .await
    }

    /// Initialize a forward listing without fetching its first page.
    pub(crate) async fn prepare_forward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Cursor<ForwardListing>> {
        self.exchange(ForwardListing(Box::new(bounds)), |request| {
            PendingRequest::ListForward(PendingPageRequest::Prepare(request))
        })
        .await
    }

    /// Continue a forward listing from `cursor`.
    pub(crate) async fn continue_forward_listing(
        &self,
        cursor: Cursor<ForwardListing>,
    ) -> DeltaResult<Page<ForwardListing>> {
        self.exchange(cursor, |request| {
            PendingRequest::ListForward(PendingPageRequest::Continue(request))
        })
        .await
    }

    /// Initialize a backward listing and return its first page.
    pub(crate) async fn start_backward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Page<BackwardListing>> {
        self.exchange(BackwardListing(Box::new(bounds)), |request| {
            PendingRequest::ListBackward(PendingPageRequest::Start(request))
        })
        .await
    }

    /// Initialize a backward listing without fetching its first page.
    pub(crate) async fn prepare_backward_listing(
        &self,
        bounds: ListingBounds,
    ) -> DeltaResult<Cursor<BackwardListing>> {
        self.exchange(BackwardListing(Box::new(bounds)), |request| {
            PendingRequest::ListBackward(PendingPageRequest::Prepare(request))
        })
        .await
    }

    /// Continue a backward listing from `cursor`.
    pub(crate) async fn continue_backward_listing(
        &self,
        cursor: Cursor<BackwardListing>,
    ) -> DeltaResult<Page<BackwardListing>> {
        self.exchange(cursor, |request| {
            PendingRequest::ListBackward(PendingPageRequest::Continue(request))
        })
        .await
    }
}

impl<N: Send + 'static> PageRequest<N, ForwardListing> {
    /// Forward this listing request through `parent`.
    pub(super) async fn forward_to(self, parent: &Channel) -> DeltaResult<N> {
        match self {
            Self::Start(ForwardListing(bounds), resume) => {
                resume.resume(parent.start_forward_listing(*bounds).await)
            }
            Self::Prepare(ForwardListing(bounds), resume) => {
                resume.resume(parent.prepare_forward_listing(*bounds).await)
            }
            Self::Continue(cursor, resume) => {
                resume.resume(parent.continue_forward_listing(cursor).await)
            }
        }
    }
}

impl<N: Send + 'static> PageRequest<N, BackwardListing> {
    /// Forward this listing request through `parent`.
    pub(super) async fn forward_to(self, parent: &Channel) -> DeltaResult<N> {
        match self {
            Self::Start(BackwardListing(bounds), resume) => {
                resume.resume(parent.start_backward_listing(*bounds).await)
            }
            Self::Prepare(BackwardListing(bounds), resume) => {
                resume.resume(parent.prepare_backward_listing(*bounds).await)
            }
            Self::Continue(cursor, resume) => {
                resume.resume(parent.continue_backward_listing(cursor).await)
            }
        }
    }
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
        let first = backward_listing_window(
            &bounds,
            version_from_listing_bound(&bounds.high).unwrap(),
            DEFAULT_BACKWARD_LISTING_WINDOW_SIZE,
        )
        .unwrap();
        assert_eq!(first.low.path(), "/_delta_log/00000000000000002011");
        assert_eq!(first.high.path(), "/_delta_log/00000000000000003011");
        assert_eq!(first.next_high, Some(2011));

        let last = backward_listing_window(
            &bounds,
            first.next_high.unwrap(),
            DEFAULT_BACKWARD_LISTING_WINDOW_SIZE * 3,
        )
        .unwrap();
        assert_eq!(last.low.path(), "/_delta_log/00000000000000000010");
        assert_eq!(last.next_high, None);
    }

    #[test]
    fn listing_bounds_keep_errors_and_stop_after_version_named_paths() {
        let bounds = bounds(0, 10);
        let error = Err(Error::generic("listing failed"));
        assert!(is_within_listing_bounds(
            &error,
            &bounds.prefix,
            &bounds.high
        ));

        let sidecar = Ok(FileMeta::new(
            Url::parse("memory:///_delta_log/_sidecars/part.parquet").unwrap(),
            0,
            0,
        ));
        assert!(!is_within_listing_bounds(
            &sidecar,
            &bounds.prefix,
            &bounds.high
        ));
    }
}
