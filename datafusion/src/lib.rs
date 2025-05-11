//! DataFusion extension for Delta Lake.
//!
//! This crate provides a seamless integration of Delta Lake backed tables into the Datafusion ecosystem.
//! The low level protocol is implemented in the [`delta_kernel`] crate while this crate provides
//! a DataFusion based [`Engine`](delta_kernel::Engine) to drive the work required by kernel.
//!
//! The main API to interact with the extension is provided via two extension traits.
//!
//! - [`KernelContextExt`] extends [`SessionContext`] to provide top level APIs for end users
//!   integrating Datafusion into their application.
//! - [`KernelSessionExt`] extends the [`Session`] trait to access kernel capabilities when writing custom
//!   extensions to Datafusion such as [`ExecutionPlan`]s.
//!
//! [`SessionContext`]: datafusion::prelude::SessionContext
//! [`Session`]: datafusion_session::Session
//! [`ExecutionPlan`]: datafusion_physical_plan::execution_plan::ExecutionPlan

mod engine;
mod error;
mod exec;
mod expressions;
mod log_table_provider;
mod session;
mod table_provider;
mod utils;

pub use engine::DataFusionEngine;
pub use log_table_provider::DeltaLogTableProvider;
pub use session::{KernelContextExt, KernelExtensionConfig, KernelSessionExt, ObjectStoreFactory};
pub use table_provider::DeltaTableProvider;

#[cfg(test)]
pub(crate) mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use acceptance::read_dat_case_info;
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::Engine;
    use rstest::*;

    use super::*;

    static SKIPPED_TESTS: &[&str; 5] = &[
        "iceberg_compat_v1",
        "no_stats",
        "no_replay",
        "stats_as_struct",
        "with_checkpoint",
    ];

    #[fixture]
    pub(crate) fn df_engine() -> (Arc<dyn Engine>, SessionContext) {
        let ctx = SessionContext::new();
        let engine = DataFusionEngine::<TokioBackgroundExecutor>::new_background(&ctx);
        (engine, ctx)
    }

    #[rstest]
    #[tokio::test]
    async fn read_dat_case(
        #[files("dat/out/reader_tests/generated/**/test_case_info.json")] path: PathBuf,
        df_engine: (Arc<dyn Engine>, SessionContext),
    ) {
        for skipped in SKIPPED_TESTS {
            if path.to_str().unwrap().contains(skipped) {
                println!("Skipping test: {}", skipped);
                return;
            }
        }
        let case = read_dat_case_info(path).unwrap();
        let (engine, _) = df_engine;
        case.assert_metadata(engine.clone()).await.unwrap();
        acceptance::data::assert_scan_metadata(engine.clone(), &case)
            .await
            .unwrap();
    }
}
