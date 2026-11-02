//! Development tasks for delta-kernel-rs, run with `cargo xtask <command>`.
//!
//! See <https://github.com/matklad/cargo-xtask> for the pattern. Commands:
//!
//! - `update-delta-error-catalog`: download the latest upstream catalog and source metadata.
//! - `generate-delta-error-conditions [--check]`: generate Rust from the local catalog and
//!   allowlist.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

mod codegen;
mod delta_error_catalog;

#[derive(Parser)]
struct Xtask {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download the latest Delta error catalog from delta-io/delta master.
    UpdateDeltaErrorCatalog,
    /// Regenerate selected `DeltaErrorCondition` variants from the pinned local catalog.
    GenerateDeltaErrorConditions(delta_error_catalog::GenerateArgs),
}

fn main() -> anyhow::Result<()> {
    match Xtask::parse().command {
        Command::UpdateDeltaErrorCatalog => delta_error_catalog::download(&project_root()),
        Command::GenerateDeltaErrorConditions(args) => {
            delta_error_catalog::generate(&project_root(), args)
        }
    }
}

/// The workspace root, which is the parent of this crate's directory.
fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in a workspace subdirectory")
        .to_path_buf()
}
