use std::path::{Path, PathBuf};

use rustc_version::{version_meta, Channel};

#[path = "build/error_catalog.rs"]
mod error_catalog;

fn main() {
    if let Err(error) = generate_error_catalog() {
        eprintln!("failed to generate Delta error catalog: {error}");
        std::process::exit(1);
    }

    println!("cargo::rustc-check-cfg=cfg(NIGHTLY_CHANNEL)");
    // note if we're on the nightly channel so we can enable doc_auto_cfg if so
    if let Channel::Nightly = version_meta().unwrap().channel {
        println!("cargo:rustc-cfg=NIGHTLY_CHANNEL");
    }

    // Generate prost bindings for the declarative-plans proto schema only when the feature is
    // enabled. Off-by-default consumers don't pay the protoc / codegen cost and don't pull in
    // prost-build / protoc-bin-vendored.
    #[cfg(feature = "declarative-plans")]
    compile_proto_definitions();
}

fn generate_error_catalog() -> Result<(), String> {
    const OSS_CATALOG: &str = "src/error/catalog/delta-error-classes.json";
    const KERNEL_CATALOG: &str = "src/error/catalog/kernel-error-classes.json";
    const MANIFEST: &str = "src/error/catalog/catalog-manifest.json";

    for path in [OSS_CATALOG, KERNEL_CATALOG, MANIFEST] {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-changed=build/error_catalog.rs");

    let out_dir = std::env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| "Cargo did not provide OUT_DIR".to_string())?;

    error_catalog::generate(
        Path::new(OSS_CATALOG),
        Path::new(KERNEL_CATALOG),
        Path::new(MANIFEST),
        &out_dir.join("delta_error_codes.rs"),
    )
}

#[cfg(feature = "declarative-plans")]
fn compile_proto_definitions() {
    let proto_dir = "proto";
    let proto_files = [
        "schema.proto",
        "expressions.proto",
        "plan.proto",
        "operation.proto",
    ];

    for file in &proto_files {
        println!("cargo:rerun-if-changed={proto_dir}/{file}");
    }

    let files: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{proto_dir}/{f}"))
        .collect();

    // Point prost-build at a vendored `protoc` so the build doesn't require a system install.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    std::env::set_var("PROTOC", protoc);

    // Don't propagate `.proto` comments into the generated code as doc comments: they contain
    // angle-bracket generics (`Vec<...>`, `Option<...>`) that rustdoc would parse as unclosed
    // HTML tags. The `.proto` files stay the canonical reference for the wire format.
    prost_build::Config::new()
        .disable_comments(["."])
        .compile_protos(&files, &[proto_dir])
        .expect("failed to compile .proto files");
}
