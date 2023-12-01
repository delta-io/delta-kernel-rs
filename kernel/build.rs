extern crate cbindgen;

use cbindgen::{Config, Language};
use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let target_dir = PathBuf::from(crate_dir.clone());

    // generate cxx bindings
    let output_file_hpp = target_dir
        .join("target")
        .join(format!("{}.hpp", package_name))
        .display()
        .to_string();
    let mut config_hpp = Config::default();
    config_hpp.language = Language::Cxx;
    config_hpp.namespace = Some(String::from("ffi"));
    cbindgen::generate_with_config(&crate_dir, config_hpp)
        .unwrap()
        .write_to_file(output_file_hpp);

    // generate c bindings
    let output_file_h = target_dir
        .join("target")
        .join(format!("{}.h", package_name))
        .display()
        .to_string();
    let mut config_h = Config::default();
    config_h.language = Language::C;
    cbindgen::generate_with_config(&crate_dir, config_h)
        .unwrap()
        .write_to_file(output_file_h);
}
