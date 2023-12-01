extern crate cbindgen;

use cbindgen::{Config, Language};
use std::env;
use std::path::PathBuf;

fn get_target_dir(manifest_dir: &str) -> PathBuf {
    if let Ok(target) = env::var("CARGO_TARGET_DIR") {
        // allow a CARGO_TARGET_DIR var that could be set via e.g. cmake to override where we put
        // the header files
        PathBuf::from(target)
    } else {
        PathBuf::from(manifest_dir).join("target")
    }
}

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set");
    let package_name = env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME should be set");
    let target_dir = get_target_dir(crate_dir.as_str());

    // generate cxx bindings
    let output_file_hpp = target_dir
        .join(format!("{}.hpp", package_name))
        .display()
        .to_string();
    let mut config_hpp = Config::default();
    config_hpp.language = Language::Cxx;
    config_hpp.namespace = Some(String::from("ffi"));
    cbindgen::generate_with_config(&crate_dir, config_hpp)
        .expect("generate_with_config should have worked for Cxx")
        .write_to_file(output_file_hpp);

    // generate c bindings
    let output_file_h = target_dir
        .join(format!("{}.h", package_name))
        .display()
        .to_string();
    let mut config_h = Config::default();
    config_h.language = Language::C;
    cbindgen::generate_with_config(&crate_dir, config_h)
        .expect("generate_with_config should have worked for C")
        .write_to_file(output_file_h);
}

