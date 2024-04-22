use rustc_version::{version_meta, Channel};

fn main() {
    // note if we're on the nightly channel so we can enable doc_auto_cfg if so
    if let Channel::Nightly = version_meta().unwrap().channel {
        println!("cargo:rustc-cfg=NIGHTLY_CHANNEL");
    }
}
