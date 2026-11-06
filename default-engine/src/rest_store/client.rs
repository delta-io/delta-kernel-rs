//! Builds the [`reqwest::Client`] backing a [`RestObjectStore`](super::RestObjectStore), including
//! optional TLS client auth, DNS overrides, and request timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use delta_kernel::object_store::Result as ObjectStoreResult;
use reqwest::Client;
#[cfg(any(feature = "rustls", feature = "native-tls"))]
use reqwest::{Certificate, ClientBuilder, Identity};

use super::generic_error;

/// Options for the [`reqwest::Client`] backing a [`RestObjectStore`](super::RestObjectStore).
/// By default, server certificates are verified and no client certificate is sent. When
/// `cert_path`, `key_path`, and `ca_path` are all set, mutual TLS is enabled.
#[derive(Debug, Clone, Default)]
pub struct RestClientOptions {
    /// PEM client-certificate path. mTLS is enabled only when cert, key, and CA are all set.
    pub cert_path: Option<String>,
    /// PEM client-private-key path.
    pub key_path: Option<String>,
    /// PEM CA-bundle path used to verify the server.
    pub ca_path: Option<String>,
    /// Accept certificates whose names do not match the server hostname. Defaults to `false`.
    /// Certificate trust and validity checks remain enabled. Intended for test environments.
    /// With rustls, enabling this switches from the platform verifier to webpki, using a snapshot
    /// of system roots loaded by `rustls-native-certs` plus the configured `ca_path` certificate.
    pub allow_invalid_hostnames: bool,
    /// DNS overrides as `host=ip:port` entries: connect to the given address for `host`,
    /// bypassing the system resolver. Does not change TLS SNI or the hostname used for
    /// verification.
    pub dns_overrides: Vec<String>,
    /// Request timeout in seconds; `None` uses reqwest's default.
    pub timeout_secs: Option<u64>,
}

// === mTLS configuration ===
//
// reqwest's `Identity` is built differently per backend: rustls wants a single PEM bundle
// (cert followed by key), native-tls takes cert and key as separate PEM buffers.
#[cfg(any(feature = "rustls", feature = "native-tls"))]
fn configure_mtls(
    builder: ClientBuilder,
    cert: &str,
    key: &str,
    ca: &str,
) -> Result<ClientBuilder, Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "rustls")]
    let identity = {
        let mut bundle = std::fs::read(cert)?;
        bundle.extend_from_slice(&std::fs::read(key)?);
        Identity::from_pem(&bundle)?
    };
    #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
    let identity = Identity::from_pkcs8_pem(&std::fs::read(cert)?, &std::fs::read(key)?)?;
    let ca_cert = Certificate::from_pem(&std::fs::read(ca)?)?;
    Ok(builder.identity(identity).add_root_certificate(ca_cert))
}

// Pin the TLS backend when both are compiled in: prefer rustls (the crate's default feature),
// since reqwest otherwise defaults to native-tls and would mismatch the rustls-built identity.
// native-tls is used only when it is the sole backend.
#[cfg(any(feature = "rustls", feature = "native-tls"))]
fn pin_tls_backend(
    mut builder: ClientBuilder,
    opts: &RestClientOptions,
) -> ObjectStoreResult<ClientBuilder> {
    #[cfg(feature = "rustls")]
    {
        builder = builder.use_rustls_tls();
        if opts.allow_invalid_hostnames {
            // reqwest's rustls backend requires tls_certs_only(...) when
            // tls_danger_accept_invalid_hostnames(true) is set; otherwise build() fails.
            // This selects its webpki verifier instead of the platform verifier, so load system
            // roots explicitly to keep validating the server's certificate chain.
            let roots = hostname_bypass_roots(
                rustls_native_certs::load_native_certs(),
                opts.ca_path.is_some(),
            )?;
            builder = builder.tls_certs_only(roots);
        }
    }
    #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
    {
        let _ = opts;
        builder = builder.use_native_tls();
    }
    Ok(builder)
}

/// Build a [`reqwest::Client`] from [`RestClientOptions`]. Enables mTLS when `cert_path`,
/// `key_path`, and `ca_path` are all present.
///
/// Returns an error for incomplete or unreadable TLS material, malformed DNS overrides, missing
/// TLS support, or HTTP client configuration errors. Disabling hostname verification requires
/// trusted CA certificates from the system or the configured CA bundle.
pub fn build_rest_client(opts: &RestClientOptions) -> ObjectStoreResult<Client> {
    let mut builder = Client::builder();
    if let Some(secs) = opts.timeout_secs {
        builder = builder.timeout(Duration::from_secs(secs));
    }
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    {
        builder = pin_tls_backend(builder, opts)?;
        builder = builder.tls_danger_accept_invalid_hostnames(opts.allow_invalid_hostnames);
        match (&opts.cert_path, &opts.key_path, &opts.ca_path) {
            (Some(cert), Some(key), Some(ca)) => {
                builder = configure_mtls(builder, cert, key, ca).map_err(generic_error)?;
            }
            (None, None, None) => {}
            // A partial mTLS config is an error, not a silent fall-through to a plain client.
            _ => {
                return Err(generic_error(
                    "partial mTLS config: cert_path, key_path, and ca_path must be set \
                     together (or all omitted)",
                ));
            }
        }
    }
    #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
    if opts.allow_invalid_hostnames
        || opts.cert_path.is_some()
        || opts.key_path.is_some()
        || opts.ca_path.is_some()
    {
        return Err(generic_error(
            "TLS options require the `rustls` or `native-tls` feature to be enabled",
        ));
    }
    for entry in &opts.dns_overrides {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (host, addr) = entry.split_once('=').ok_or_else(|| {
            generic_error(format!(
                "invalid DNS override (expected `host=ip:port`): {entry}"
            ))
        })?;
        let addr: SocketAddr = addr.parse().map_err(generic_error)?;
        builder = builder.resolve(host, addr);
    }
    builder.build().map_err(generic_error)
}

// Returns system roots only. has_ca_certificate permits an empty result because configure_mtls
// reads ca_path and appends that CA to the same builder before build().
#[cfg(feature = "rustls")]
fn hostname_bypass_roots(
    roots: rustls_native_certs::CertificateResult,
    has_ca_certificate: bool,
) -> ObjectStoreResult<Vec<Certificate>> {
    for error in &roots.errors {
        tracing::warn!(%error, "failed to load a system TLS root certificate");
    }
    if roots.certs.is_empty() && !has_ca_certificate {
        return Err(generic_error(format!(
            "cannot disable hostname verification without trusted CA certificates; \
             system root loading errors: {:?}",
            roots.errors
        )));
    }
    roots
        .certs
        .iter()
        .map(|cert| Certificate::from_der(cert.as_ref()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(generic_error)
}

#[cfg(all(test, feature = "rustls"))]
mod tests {
    use rstest::rstest;
    use rustls_native_certs::{CertificateResult, Error, ErrorKind};

    use super::hostname_bypass_roots;

    #[rstest]
    fn hostname_bypass_without_system_roots_requires_ca_certificate(
        #[values(false, true)] has_ca_certificate: bool,
        #[values(false, true)] load_error: bool,
    ) {
        let mut roots = CertificateResult::default();
        if load_error {
            roots.errors.push(Error {
                context: "cannot read test root store",
                kind: ErrorKind::Os(
                    std::io::Error::from(std::io::ErrorKind::PermissionDenied).into(),
                ),
            });
        }
        let result = hostname_bypass_roots(roots, has_ca_certificate);
        if has_ca_certificate {
            assert!(result.unwrap().is_empty());
        } else {
            let error = result.unwrap_err().to_string();
            assert!(error.contains("without trusted CA certificates"));
            if load_error {
                assert!(error.contains("cannot read test root store"));
                assert!(error.contains("PermissionDenied"));
            }
        }
    }
}
