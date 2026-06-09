/// This is a compilation test to ensure that the default-engine feature flags are working
/// correctly.
///
/// Run (from workspace root) with:
/// 1. `cargo b -p feature_tests --features default-engine-rustls`
/// 2. `cargo b -p feature_tests --features default-engine-native-tls`
///
/// These run in our build CI.
pub fn test_default_engine_feature_flags() {
    #[cfg(any(
        feature = "default-engine-native-tls",
        feature = "default-engine-rustls"
    ))]
    {
        #[allow(unused_imports)]
        use delta_kernel_default_engine::DefaultEngine;
    }
}

/// Regression tests for rustls crypto provider conflicts.
///
/// rustls 0.23 panics at runtime if both `aws-lc-rs` and `ring` features are active and no
/// provider is explicitly installed. object_store always brings `ring` transitively, so kernel
/// must avoid adding `aws-lc-rs` to the same rustls instance.
///
/// Two APIs are tested because they behave differently:
///  - `rustls::ClientConfig::builder()` relies on auto-detection and panics on dual providers.
///  - `reqwest::Client::new()` explicitly constructs its provider, so it always succeeds.
#[cfg(test)]
mod tests {
    // Verifies that `default-engine-native-tls` does not leak aws-lc-rs into the rustls
    // feature set. If this panics, kernel's reqwest is pulling in `default-tls` again.
    //
    // Excluded when `default-engine-rustls` is also active (e.g. --all-features) because
    // that feature inherently adds aws-lc-rs, making the dual-provider panic unavoidable.
    #[test]
    #[cfg(all(
        feature = "default-engine-native-tls",
        not(feature = "default-engine-rustls")
    ))]
    fn test_native_tls_rustls_builder_no_dual_provider_panic() {
        let _config = rustls::ClientConfig::builder();
    }

    #[test]
    #[cfg(feature = "default-engine-native-tls")]
    fn test_native_tls_reqwest_client_no_panic() {
        let _client = reqwest::Client::new();
    }

    // `default-engine-rustls` has an inherent dual-provider conflict: reqwest's `rustls`
    // feature brings aws-lc-rs while object_store brings ring. This is an upstream limitation.
    // If this test stops panicking, the upstream issue is fixed and the annotation can go.
    #[test]
    #[cfg(feature = "default-engine-rustls")]
    #[should_panic(expected = "Could not automatically determine the process-level CryptoProvider")]
    fn test_rustls_rustls_builder_has_dual_provider_panic() {
        let _config = rustls::ClientConfig::builder();
    }

    #[test]
    #[cfg(feature = "default-engine-rustls")]
    fn test_rustls_reqwest_client_no_panic() {
        let _client = reqwest::Client::new();
    }
}
