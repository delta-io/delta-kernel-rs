# Delta Kernel Acceptance Testing

This directory contains acceptance tests for the Delta Kernel (Rust) project.
These tests have been implemented as a separate sub-project in the workspace to
ensure that they are acting as a "connector" for the kernel APIs, thereby
allowing us to test the exact same client interfaces that any connector
implemented on top of delta-kernel-rs would be using.
