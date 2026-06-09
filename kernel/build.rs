use rustc_version::{version_meta, Channel};

fn main() {
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

#[cfg(feature = "declarative-plans")]
fn compile_proto_definitions() {
    let proto_dir = "proto";
    let proto_files = ["schema.proto", "expressions.proto", "plan.proto"];

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
