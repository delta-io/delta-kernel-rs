//! REST endpoint configuration for [`RestObjectStore`](super::RestObjectStore): URL path prefixes
//! and JSON field names for list/put request and response shapes.

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ObjectMeta, Result as ObjectStoreResult};

use super::generic_error;

/// One page of a list response: the objects on this page plus an optional token for the next.
pub(crate) struct RestListPage {
    /// The (file) objects on this page. Directories are filtered out, since
    /// [`ObjectStore::list`](delta_kernel::object_store::ObjectStore::list) yields only objects.
    pub(crate) objects: Vec<ObjectMeta>,
    /// Opaque token to fetch the next page, or `None` when this is the last page.
    pub(crate) next_page_token: Option<String>,
}

/// Per-backend HTTP/JSON settings for a [`RestObjectStore`](super::RestObjectStore).
///
/// Names URL path prefixes and list-response JSON fields so the store can talk to a REST file API
/// without service-specific code. Credentials come from
/// [`AuthHeaderProvider`](super::AuthHeaderProvider).
#[derive(Debug, Clone)]
pub struct RestEndpointConfig {
    /// Path prefix for file (object) operations; the URL is `{base_url}/{files_prefix}/{path}`.
    pub files_prefix: String,
    /// Path prefix for directory (list) operations.
    pub directories_prefix: String,
    /// Query-param name carrying the pagination token.
    pub page_token_param: String,
    /// Query-param name carrying the list start offset.
    pub start_from_param: String,
    /// Query-param name (value `"true"`) requesting a recursive listing.
    pub recursive_param: String,
    /// Query-param name controlling overwrite-vs-create on `PUT` (value `"true"`/`"false"`).
    pub overwrite_param: String,
    /// List-response JSON field holding the array of entries.
    pub contents_field: String,
    /// List-response JSON field holding the next-page token.
    pub next_page_token_field: String,
    /// Per-entry JSON field holding the path.
    pub entry_path_field: String,
    /// Per-entry JSON field holding the size in bytes.
    pub entry_size_field: String,
    /// Per-entry JSON field holding the directory flag; truthy entries are skipped, since
    /// [`ObjectStore::list`](delta_kernel::object_store::ObjectStore::list) yields only objects.
    pub entry_is_directory_field: String,
    /// Per-entry JSON field holding the last-modified time as epoch milliseconds.
    pub entry_last_modified_field: String,
    /// If set, every list-entry path must start with this prefix; it is stripped to yield the
    /// store-relative path, and an entry outside the prefix is rejected (a scope check). Use when
    /// the backend returns absolute keys but the store is rooted at a sub-path.
    pub entry_strip_prefix: Option<String>,
}

/// Join `base_url`, a path `prefix`, and a store-relative `path` into a single URL, trimming
/// stray slashes at each seam.
fn join_url(base_url: &str, prefix: &str, path: &str) -> String {
    format!(
        "{}/{}/{}",
        base_url.trim_end_matches('/'),
        prefix.trim_matches('/'),
        path.trim_start_matches('/')
    )
}

impl RestEndpointConfig {
    /// Build the URL for file (object) operations on `path`.
    pub(crate) fn file_url(&self, base_url: &str, path: &str) -> String {
        join_url(base_url, &self.files_prefix, path)
    }

    /// Build the URL for directory (list) operations on `path`.
    pub(crate) fn directory_url(&self, base_url: &str, path: &str) -> String {
        join_url(base_url, &self.directories_prefix, path)
    }

    /// Query parameters for a list request. `page_token` drives pagination; `start_from` is an
    /// optional offset (see
    /// [`ObjectStore::list_with_offset`](delta_kernel::object_store::ObjectStore::list_with_offset));
    /// `recursive` requests a recursive listing.
    pub(crate) fn list_query(
        &self,
        page_token: Option<&str>,
        start_from: Option<&str>,
        recursive: bool,
    ) -> Vec<(String, String)> {
        let mut q = Vec::new();
        if let Some(t) = page_token {
            q.push((self.page_token_param.clone(), t.to_string()));
        }
        if let Some(s) = start_from {
            q.push((self.start_from_param.clone(), s.to_string()));
        }
        if recursive {
            q.push((self.recursive_param.clone(), "true".to_string()));
        }
        q
    }

    /// Parse a list-response body into a [`RestListPage`].
    ///
    /// Entries **must** be ordered by ascending full path, and that order must hold *across*
    /// pages (a page's first entry sorts after the previous page's last).
    /// [`RestObjectStore`](super::RestObjectStore) relies on this for Delta log replay and does not
    /// re-sort: it checks the order as it streams pages and fails the listing with an error if an
    /// entry arrives out of order, rather than silently producing a wrong snapshot.
    pub(crate) fn parse_list(&self, body: &[u8]) -> ObjectStoreResult<RestListPage> {
        let root: serde_json::Value = serde_json::from_slice(body).map_err(generic_error)?;
        let mut objects = Vec::new();
        if let Some(entries) = root.get(&self.contents_field).and_then(|v| v.as_array()) {
            for entry in entries {
                let is_dir = entry
                    .get(&self.entry_is_directory_field)
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if is_dir {
                    continue;
                }
                let Some(path) = entry.get(&self.entry_path_field).and_then(|v| v.as_str()) else {
                    return Err(generic_error(format!(
                        "list entry is missing the `{}` field",
                        self.entry_path_field
                    )));
                };
                let path = match &self.entry_strip_prefix {
                    Some(prefix) => match path.strip_prefix(prefix.as_str()) {
                        Some(rest) => rest.trim_start_matches('/'),
                        None => {
                            return Err(generic_error(format!(
                                "list entry `{path}` is outside the configured prefix `{prefix}`"
                            )))
                        }
                    },
                    None => path,
                };
                let size = match entry.get(&self.entry_size_field) {
                    None => 0,
                    Some(v) => v.as_u64().ok_or_else(|| {
                        generic_error(format!(
                            "list entry field `{}` is not a non-negative integer",
                            self.entry_size_field
                        ))
                    })?,
                };
                let last_modified = entry
                    .get(&self.entry_last_modified_field)
                    .and_then(|v| v.as_u64())
                    .map(|ms| {
                        (std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(ms))
                            .into()
                    })
                    .unwrap_or(chrono::DateTime::UNIX_EPOCH);
                // Fail loudly on an unparseable path rather than silently dropping it, so a
                // misbehaving backend cannot corrupt log replay with a partial listing.
                let location = Path::parse(path).map_err(|e| {
                    generic_error(format!("list entry path `{path}` is not a valid path: {e}"))
                })?;
                objects.push(ObjectMeta {
                    location,
                    last_modified,
                    size,
                    e_tag: None,
                    version: None,
                });
            }
        }
        let next_page_token = root
            .get(&self.next_page_token_field)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        Ok(RestListPage {
            objects,
            next_page_token,
        })
    }

    /// Query parameters for a `PUT`. `overwrite` selects overwrite-vs-create semantics via the
    /// configured [`overwrite_param`](Self::overwrite_param).
    pub(crate) fn put_query(&self, overwrite: bool) -> Vec<(String, String)> {
        vec![(self.overwrite_param.clone(), overwrite.to_string())]
    }
}
