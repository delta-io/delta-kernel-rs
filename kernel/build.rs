use rustc_version::{version_meta, Channel};

fn main() {
    println!("cargo::rustc-check-cfg=cfg(NIGHTLY_CHANNEL)");
    // note if we're on the nightly channel so we can enable doc_auto_cfg if so
    if let Channel::Nightly = version_meta().unwrap().channel {
        println!("cargo:rustc-cfg=NIGHTLY_CHANNEL");
    }
}
