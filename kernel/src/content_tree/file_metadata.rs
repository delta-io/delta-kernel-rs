//! File metadata fields: `add` fields the table stores per file in the content tree rather than as
//! table columns. Each one has a field ID. An entry stores its value as the `lower_bound` of the
//! stats struct for that ID in `content_stats`.

/// The field IDs of the file metadata fields a table maps. The default maps none.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileMetadataFieldIds {
    /// The field ID of `add.clusteringProvider`, or `None` when the table does not map it.
    pub(crate) clustering_provider: Option<i32>,
}
