//! Builds the [`reqwest::Client`] backing a [`RestObjectStore`](super::RestObjectStore), including
//! optional mTLS and DNS overrides.

use std::net::SocketAddr;
use std::time::Duration;

use delta_kernel::object_store::Result as ObjectStoreResult;
use reqwest::Client;
#[cfg(any(feature = "rustls", feature = "native-tls"))]
use reqwest::{Certificate, Identity};

use super::{generic_err, generic_msg};

/// Options for the [`reqwest::Client`] backing a [`RestObjectStore`](super::RestObjectStore). All
/// fields are optional; the default value yields a plain client.
#[derive(Debug, Clone, Default)]
pub struct RestClientOptions {
    /// PEM client-certificate path. mTLS is enabled only when cert, key, and CA are all set.
    pub cert_path: Option<String>,
    /// PEM client-private-key path.
    pub key_path: Option<String>,
    /// PEM CA-bundle path used to verify the server.
    pub ca_path: Option<String>,
    /// DNS overrides as `host=ip:port` entries: connect to the given address for `host`,
    /// bypassing the system resolver. TLS SNI/hostname verification is unchanged.
    pub dns_overrides: Vec<String>,
    /// Request timeout in seconds; `None` uses reqwest's default.
    pub timeout_secs: Option<u64>,
}

// === mTLS configuration ===
//
// reqwest's `Identity` is built differently per backend: rustls wants a single PEM bundle
// (cert followed by key), native-tls takes cert and key as separate PEM buffers. Each `cfg`
// variant of `build_identity` builds it the way its backend expects.

#[cfg(feature = "rustls")]
fn build_identity(cert: &str, key: &str) -> ObjectStoreResult<Identity> {
    let mut bundle = std::fs::read(cert).map_err(generic_err)?;
    bundle.extend_from_slice(&std::fs::read(key).map_err(generic_err)?);
    Identity::from_pem(&bundle).map_err(generic_err)
}

#[cfg(all(feature = "native-tls", not(feature = "rustls")))]
fn build_identity(cert: &str, key: &str) -> ObjectStoreResult<Identity> {
    let cert_pem = std::fs::read(cert).map_err(generic_err)?;
    let key_pem = std::fs::read(key).map_err(generic_err)?;
    Identity::from_pkcs8_pem(&cert_pem, &key_pem).map_err(generic_err)
}

// CA-cert read and builder assembly are backend-agnostic, so they live once here.
#[cfg(any(feature = "rustls", feature = "native-tls"))]
fn configure_mtls(
    builder: reqwest::ClientBuilder,
    cert: &str,
    key: &str,
    ca: &str,
) -> ObjectStoreResult<reqwest::ClientBuilder> {
    let identity = build_identity(cert, key)?;
    let ca_cert =
        Certificate::from_pem(&std::fs::read(ca).map_err(generic_err)?).map_err(generic_err)?;
    Ok(builder.identity(identity).add_root_certificate(ca_cert))
}

#[cfg(not(any(feature = "rustls", feature = "native-tls")))]
fn configure_mtls(
    _builder: reqwest::ClientBuilder,
    _cert: &str,
    _key: &str,
    _ca: &str,
) -> ObjectStoreResult<reqwest::ClientBuilder> {
    Err(generic_msg(
        "mTLS requires the `rustls` or `native-tls` feature to be enabled",
    ))
}

/// Build a [`reqwest::Client`] from [`RestClientOptions`]. Enables mTLS when `cert_path`,
/// `key_path`, and `ca_path` are all present.
pub fn build_rest_client(opts: &RestClientOptions) -> ObjectStoreResult<Client> {
    let mut builder = Client::builder();
    // Pin the TLS backend when both are compiled in: prefer rustls (the crate's default
    // feature), since reqwest otherwise defaults to native-tls and would mismatch the
    // rustls-built identity. native-tls is used only when it is the sole backend.
    #[cfg(feature = "rustls")]
    {
        builder = builder.use_rustls_tls();
    }
    #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
    {
        builder = builder.use_native_tls();
    }
    if let Some(secs) = opts.timeout_secs {
        builder = builder.timeout(Duration::from_secs(secs));
    }
    match (&opts.cert_path, &opts.key_path, &opts.ca_path) {
        (Some(cert), Some(key), Some(ca)) => {
            builder = configure_mtls(builder, cert, key, ca)?;
        }
        (None, None, None) => {}
        // A partial mTLS config is an error, not a silent fall-through to a plain client.
        _ => {
            return Err(generic_msg(
                "partial mTLS config: cert_path, key_path, and ca_path must be set \
                 together (or all omitted)",
            ));
        }
    }
    for entry in &opts.dns_overrides {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (host, addr) = entry.split_once('=').ok_or_else(|| {
            generic_msg(format!(
                "invalid DNS override (expected `host=ip:port`): {entry}"
            ))
        })?;
        let addr: SocketAddr = addr.parse().map_err(generic_err)?;
        builder = builder.resolve(host, addr);
    }
    builder.build().map_err(generic_err)
}
