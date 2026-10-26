# Changelog

## [v0.2.0_delta-kernel-unity-catalog](https://github.com/delta-io/delta-kernel-rs/tree/v0.2.0_delta-kernel-unity-catalog/) (2026-10-16)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.1.0_delta-kernel-unity-catalog...v0.2.0_delta-kernel-unity-catalog)

### 🏗️ Breaking changes

1. Upgrade to Kernel 0.30.0 ([#3650])
    - Upgrade your direct `delta_kernel` dependency to 0.30.0 so engines, snapshots, and committers
    use the same Kernel types.
    - Replace `DeltaResult`, `DeltaResultIterator`, and `DeltaResultIteratorStatic` with
    `KernelResult`, `KernelResultIterator`, and `KernelResultIteratorStatic`. Replace old `Error`
    imports and variant matches with `KernelError` ([#3420]), ([#3477]), ([#3479]).
    - Public helper functions and `UCCommitter` still return `KernelError` through `KernelResult`.
    Adapt direct Kernel transaction and table-creation calls using the Kernel 0.30.0 migration notes.
2. Default to Arrow 60 and remove Arrow 58 support ([#3403])
    - Remove `arrow-58` feature selections and align shared Arrow types with Arrow 60.
    - To use Arrow 59, disable default features and select `arrow-59`. Configure your engine to use
    Arrow 59 as well.

[#3420]: https://github.com/delta-io/delta-kernel-rs/pull/3420
[#3477]: https://github.com/delta-io/delta-kernel-rs/pull/3477
[#3479]: https://github.com/delta-io/delta-kernel-rs/pull/3479
[#3650]: https://github.com/delta-io/delta-kernel-rs/pull/3650
[#3403]: https://github.com/delta-io/delta-kernel-rs/pull/3403
