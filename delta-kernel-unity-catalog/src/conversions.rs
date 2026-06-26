//! Shared conversions between kernel types and UC wire models.

use delta_kernel::actions::Protocol as KernelProtocol;
use unity_catalog_delta_client_api::Protocol as ApiProtocol;

/// Logical domain name for clustering metadata. The UC server expects this as a typed shape on
/// both the CREATE and UPDATE paths.
pub(crate) const CLUSTERING_DOMAIN: &str = "delta.clustering";

/// Domains the UC server mirrors into its catalog metadata via the typed `set-domain-metadata` /
/// `remove-domain-metadata` updates on the UPDATE path. The server rejects any other domain with
/// `INVALID_ARGUMENT`.
///
/// The CREATE path is broader: it forwards every domain (user domains included) plus typed
/// clustering, because the server has no log to replay yet. On the UPDATE path the staged commit
/// file already carries non-mirrored domains, so log replay recovers them and only the domains in
/// this list need a catalog-side mirror. Extend this list when UC adds support for a new domain.
pub(crate) const UC_MIRRORED_DOMAINS: &[&str] = &[CLUSTERING_DOMAIN, "delta.rowTracking"];

/// Convert a kernel `Protocol` into the api crate's wire `Protocol`. Feature lists are flattened to
/// `Vec<String>`, keeping the api crate kernel-free.
pub(crate) fn to_api_protocol(protocol: &KernelProtocol) -> ApiProtocol {
    ApiProtocol {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: protocol
            .reader_features()
            .into_iter()
            .flatten()
            .map(|f| f.as_ref().to_string())
            .collect(),
        writer_features: protocol
            .writer_features()
            .into_iter()
            .flatten()
            .map(|f| f.as_ref().to_string())
            .collect(),
    }
}

/// Parse `raw` as JSON, falling back to a JSON string when it is not valid JSON. Used for domain
/// metadata payloads: `delta.*` domains carry well-known JSON shapes; user domains are opaque
/// blobs passed through verbatim.
pub(crate) fn parse_or_string(raw: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(raw)
        .unwrap_or_else(|_| serde_json::Value::String(raw.to_string()))
}
