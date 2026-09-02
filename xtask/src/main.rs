mod delta_error_catalog;

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};

const GENERATE_COMMAND: &str = "generate-delta-error-conditions";
const UPDATE_COMMAND: &str = "update-delta-error-catalog";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        bail!("missing xtask command");
    };
    let command = command
        .into_string()
        .map_err(|_| anyhow::anyhow!("xtask command must be valid UTF-8"))?;

    match command.as_str() {
        GENERATE_COMMAND => run_generate(args.collect()),
        UPDATE_COMMAND => run_update(args.collect()),
        "help" | "--help" | "-h" => {
            print_usage();
            Ok(())
        }
        _ => {
            print_usage();
            bail!("unknown xtask command {command:?}")
        }
    }
}

fn run_generate(args: Vec<OsString>) -> Result<()> {
    let check = match args.as_slice() {
        [] => false,
        [flag] if flag == "--check" => true,
        _ => bail!("usage: cargo xtask {GENERATE_COMMAND} [--check]"),
    };
    delta_error_catalog::generate(workspace_root()?, check)
}

fn run_update(args: Vec<OsString>) -> Result<()> {
    let mut delta_repo = None;
    let mut revision = "HEAD".to_string();
    let mut index = 0;
    while index < args.len() {
        let flag = args[index]
            .to_str()
            .context("xtask arguments must be valid UTF-8")?;
        match flag {
            "--delta-repo" => {
                index += 1;
                let value = args.get(index).context("--delta-repo requires a path")?;
                delta_repo = Some(PathBuf::from(value));
            }
            "--revision" => {
                index += 1;
                revision = args
                    .get(index)
                    .context("--revision requires a Git revision")?
                    .to_str()
                    .context("Git revision must be valid UTF-8")?
                    .to_string();
            }
            _ => bail!(
                "usage: cargo xtask {UPDATE_COMMAND} --delta-repo <path> [--revision <revision>]"
            ),
        }
        index += 1;
    }

    let delta_repo = delta_repo.context("--delta-repo is required")?;
    delta_error_catalog::update(workspace_root()?, &delta_repo, &revision)
}

fn workspace_root() -> Result<&'static Path> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("xtask must be located directly under the workspace root")
}

fn print_usage() {
    eprintln!(
        "\
Usage:
  cargo xtask {GENERATE_COMMAND} [--check]
  cargo xtask {UPDATE_COMMAND} --delta-repo <path> [--revision <revision>]"
    );
}
