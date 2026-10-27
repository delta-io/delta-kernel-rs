//! Development tasks for delta-kernel-rs, run with `cargo xtask <command>`.
//!
//! See <https://github.com/matklad/cargo-xtask> for the pattern. Commands:
//!
//! - `update-delta-error-catalog [--offline] [--check]`: regenerate selected `DeltaErrorCondition`
//!   variants from the checked-in catalog and allowlist. All modes use local inputs.

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
    /// Regenerate selected `DeltaErrorCondition` variants from the pinned local catalog.
    UpdateDeltaErrorCatalog(delta_error_catalog::UpdateArgs),
}

fn main() -> anyhow::Result<()> {
    match Xtask::parse().command {
        Command::UpdateDeltaErrorCatalog(args) => {
            delta_error_catalog::update(&project_root(), args)
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
