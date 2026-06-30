//! The REST dialect spoken by [`RestObjectStore`](super::RestObjectStore), described as data.

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{
    Error as ObjectStoreError, ObjectMeta, Result as ObjectStoreResult,
};

use super::{generic_err, generic_msg};

/// One page of a list response: the objects on this page plus an optional token for the next.
pub(crate) struct RestListPage {
    /// The (file) objects on this page. Directories are filtered out, since
    /// [`ObjectStore::list`](delta_kernel::object_store::ObjectStore::list) yields only objects.
    pub(crate) objects: Vec<ObjectMeta>,
    /// Opaque token to fetch the next page, or `None` when this is the last page.
    pub(crate) next_page_token: Option<String>,
}

/// The REST dialect spoken by a [`RestObjectStore`](super::RestObjectStore), described as data:
/// path-prefix templates and configurable JSON field names give the request/response shape, so a
/// backend is configured rather than coded. Holds no credentials -- those come from the
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

impl Default for RestEndpointConfig {
    /// The default dialect mirrors the Databricks Files API: empty path prefixes, `*_param` query
    /// names, and the `contents`/`nextPageToken`/`path`/`fileSize`/`isDirectory`/`lastModified`
    /// list-response field names. A caller targeting a different backend overrides any field.
    fn default() -> Self {
        Self {
            files_prefix: String::new(),
            directories_prefix: String::new(),
            page_token_param: "page_token".into(),
            start_from_param: "start_from".into(),
            recursive_param: "recursive".into(),
            overwrite_param: "overwrite".into(),
            contents_field: "contents".into(),
            next_page_token_field: "nextPageToken".into(),
            entry_path_field: "path".into(),
            entry_size_field: "fileSize".into(),
            entry_is_directory_field: "isDirectory".into(),
            entry_last_modified_field: "lastModified".into(),
            entry_strip_prefix: None,
        }
    }
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
        let root: serde_json::Value = serde_json::from_slice(body).map_err(generic_err)?;
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
                    return Err(generic_msg(format!(
                        "list entry is missing the `{}` field",
                        self.entry_path_field
                    )));
                };
                let path = match &self.entry_strip_prefix {
                    Some(prefix) => match path.strip_prefix(prefix.as_str()) {
                        Some(rest) => rest.trim_start_matches('/'),
                        None => {
                            return Err(generic_msg(format!(
                                "list entry `{path}` is outside the configured prefix `{prefix}`"
                            )))
                        }
                    },
                    None => path,
                };
                let size = entry
                    .get(&self.entry_size_field)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
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
                    generic_msg(format!("list entry path `{path}` is not a valid path: {e}"))
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

    /// Map a non-success HTTP status to an [`ObjectStoreError`], or `None` to fall back to the
    /// generic status handling.
    ///
    /// `404 -> NotFound` is a universal HTTP semantic enforced by the store itself, so it never
    /// reaches here; this maps the remaining non-success codes -- a `Create` collision maps
    /// `409 -> AlreadyExists`.
    pub(crate) fn map_status(
        &self,
        status: reqwest::StatusCode,
        path: &str,
    ) -> Option<ObjectStoreError> {
        match status {
            reqwest::StatusCode::CONFLICT => Some(ObjectStoreError::AlreadyExists {
                path: path.to_string(),
                source: "HTTP 409".into(),
            }),
            _ => None,
        }
    }
}
