//! Snapshot-specific assembly of one borrowed callback payload.
//!
//! Shared backing storage and its lifetime rules live beside the FFI types in
//! [`crate::delta_types`].

use delta_kernel::snapshot::{SnapshotHint, SnapshotHintFreshness};
#[cfg(feature = "adaptive-metadata-in-dev")]
use delta_kernel::KernelError;
use delta_kernel::KernelResult;

use super::{
    FfiSnapshotHint, FfiSnapshotHintFreshness, FfiSnapshotHintV2, SnapshotHintVisitor,
    SnapshotHintVisitorV2,
};
use crate::delta_types::{CheckpointBacking, CrcBacking, MetadataBacking, ProtocolBacking};
use crate::log_path::FfiLogPath;
use crate::{kernel_string_slice, optional_pointer, FfiSlice, NullableCvoid};

pub(super) fn visit(
    hint: &SnapshotHint,
    context: NullableCvoid,
    visitor: SnapshotHintVisitor,
) -> KernelResult<()> {
    visit_impl(hint, false, |value, _| visitor(context, &value))
}

pub(super) fn visit_v2(
    hint: &SnapshotHint,
    context: NullableCvoid,
    visitor: SnapshotHintVisitorV2,
) -> KernelResult<()> {
    visit_impl(hint, true, |base, base_crc| {
        let (checkpoint_kind, crc_provenance) = hint.provenance().unwrap_or_default();
        visitor(
            context,
            &FfiSnapshotHintV2 {
                base,
                checkpoint_kind: checkpoint_kind.into(),
                crc_provenance: crc_provenance.into(),
                base_crc,
            },
        );
    })
}

fn visit_impl(
    hint: &SnapshotHint,
    include_stale_crc: bool,
    visitor: impl FnOnce(FfiSnapshotHint, *const crate::delta_types::FfiCrc),
) -> KernelResult<()> {
    let retained_crc = if include_stale_crc {
        hint.base_crc()
    } else {
        hint.crc()
    };
    #[cfg(feature = "adaptive-metadata-in-dev")]
    if retained_crc.is_some_and(|crc| {
        crc.last_manifest_commit().is_some()
            || crc
                .all_files()
                .is_some_and(|files| files.iter().any(|add| add.back_reference().is_some()))
    }) {
        return Err(KernelError::unsupported(
            "typed snapshot hint export cannot represent CRC lastManifestCommit or Add backReference",
        ));
    }
    let files = hint.log_segment_files();
    // Checkpoint adoption can retain the latest commit separately from replay files.
    let mut paths: Vec<_> = files
        .checkpoint_parts
        .iter()
        .chain(&files.ascending_commit_files)
        .chain(files.latest_commit_file.iter())
        .chain(files.latest_crc_file.iter())
        .collect();
    // Deduplication only removes adjacent entries; sorting groups copies of the same retained path.
    paths.sort_unstable_by_key(|path| path.location.location.as_str());
    paths.dedup_by_key(|path| path.location.location.as_str());
    let paths: Vec<_> = paths
        .into_iter()
        .map(|path| {
            let location = path.location.location.as_str();
            FfiLogPath::new(
                kernel_string_slice!(location),
                path.location.last_modified,
                path.location.size,
            )
        })
        .collect();
    let protocol = ProtocolBacking::new(hint.protocol());
    let metadata = MetadataBacking::new(hint.metadata());
    let checkpoint = hint
        .last_checkpoint_hint()
        .map(CheckpointBacking::try_new)
        .transpose()?;
    let crc = retained_crc.map(CrcBacking::new);
    let checkpoint_view = checkpoint.as_ref().map(CheckpointBacking::as_ffi);
    let crc_view = crc.as_ref().map(CrcBacking::as_ffi);
    let value = FfiSnapshotHint {
        version: hint.version(),
        freshness: match hint.freshness() {
            SnapshotHintFreshness::Latest => FfiSnapshotHintFreshness::Latest,
            SnapshotHintFreshness::Unverified => FfiSnapshotHintFreshness::Unverified,
        },
        log_paths: unsafe { FfiSlice::new_unsafe(&paths) },
        protocol: protocol.as_ffi(),
        metadata: metadata.as_ffi(),
        last_checkpoint: optional_pointer(checkpoint_view.as_ref()),
        crc: optional_pointer(crc_view.as_ref().filter(|_| hint.crc().is_some())),
        publication_watermark: hint.publication_watermark().into(),
    };
    visitor(value, optional_pointer(crc_view.as_ref()));
    Ok(())
}
