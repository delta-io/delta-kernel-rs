/// This is a compilation test to ensure that the default-engine feature flags are working
/// correctly.
///
/// Run (from workspace root) with:
/// 1. `cargo b -p feature_tests --features default-engine-rustls`
/// 2. `cargo b -p feature_tests --features default-engine`
///
/// These run in our build CI.
pub fn test_default_engine_feature_flags() {
    #[cfg(any(feature = "default-engine", feature = "default-engine-rustls"))]
    {
        #[allow(unused_imports)]
        use delta_kernel::engine::default::DefaultEngine;
    }
}
