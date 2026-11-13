//! Connector-implemented declarative commit and publication services.

use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, PublishMetadata};
use delta_kernel::plans::proto::schema;
use delta_kernel::plans::{RelationRef, ScopedPlanExecutor};
use delta_kernel::{
    Engine, FileMeta, FilteredEngineData, KernelError, KernelResult, KernelResultIterator,
};
use prost::Message;
use url::Url;

use super::MutableCommitter;
use crate::commit_range::SharedCommitRange;
use crate::delta_types::{
    FfiDomainMetadata, FfiDomainMetadataArray, FfiMetadata, FfiProtocol, MetadataBacking,
    ProtocolBacking,
};
use crate::error::{EngineExecError, EngineExecResult};
use crate::handle::Handle;
use crate::{
    ExclusiveRustString, FfiSlice, KernelBytesSlice, KernelStringSlice, NullableCvoid,
    OptionalValue, kernel_bytes_slice,
};

/// Table operation passed to a connector's committer.
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum FfiCommitType {
    /// Create a filesystem table.
    PathBasedCreate,
    /// Create a catalog-managed table.
    CatalogManagedCreate,
    /// Update a filesystem table.
    PathBasedWrite,
    /// Update a catalog-managed table.
    CatalogManagedWrite,
    /// Enable catalog management.
    UpgradeToCatalogManaged,
    /// Disable catalog management.
    DowngradeToPathBased,
}

/// Borrowed view of one engine-held relation. Both slices are valid only during the callback.
#[repr(C)]
pub struct FfiCommitRelation {
    /// Engine identifier, resolved within the executor used for this transaction.
    pub id: KernelStringSlice,
    /// StructType protobuf describing the retained rows.
    pub schema: KernelBytesSlice,
}

/// Borrowed metadata for one physical commit attempt. All nested views expire after the callback.
#[repr(C)]
pub struct FfiCommitMetadata {
    /// Table URL.
    pub table_root: KernelStringSlice,
    /// Proposed version.
    pub version: u64,
    /// Operation and catalog-management state.
    pub commit_type: FfiCommitType,
    /// Attempt's in-commit timestamp, or filesystem timestamp if ICT is disabled.
    pub in_commit_timestamp: i64,
    /// Observed publication watermark, preserving absence.
    pub max_published_version: OptionalValue<u64>,
    /// Destination for a published commit.
    pub published_path: KernelStringSlice,
    /// Unique destination available for catalog ratification.
    pub staged_path: KernelStringSlice,
    /// Protocol from the read snapshot, absent for table creation.
    pub read_protocol: OptionalValue<FfiProtocol>,
    /// Metadata from the read snapshot, absent for table creation.
    pub read_metadata: OptionalValue<FfiMetadata>,
    /// Protocol written by this commit, absent when unchanged.
    pub new_protocol: OptionalValue<FfiProtocol>,
    /// Metadata written by this commit, absent when unchanged.
    pub new_metadata: OptionalValue<FfiMetadata>,
    /// Domain metadata additions and removals written by this commit.
    pub domain_metadata: FfiDomainMetadataArray,
}

/// Owned file metadata returned after a successful physical commit.
#[repr(C)]
pub struct FfiWrittenCommit {
    /// Absolute URL in an owned Kernel string; Kernel consumes the string handle.
    pub location: Handle<ExclusiveRustString>,
    /// File modification time in milliseconds.
    pub last_modified: i64,
    /// File size in bytes.
    pub size: u64,
}

/// Established physical outcome. Callback failures represent uncertain acceptance.
/// cbindgen:prefix-with-name=true
#[repr(C)]
pub enum FfiCommitResponse {
    /// The commit was accepted at the requested version.
    Committed(FfiWrittenCommit),
    /// A commit already occupies this rejected version. Kernel consumes the history or its error.
    Conflict {
        /// The version rejected by the catalog or filesystem.
        version: u64,
        /// Complete winning history, or a discovery failure preserving the confirmed conflict.
        commits: EngineExecResult<Handle<SharedCommitRange>>,
    },
    /// Confirmed temporary rejection without acceptance. Kernel consumes the error message.
    Retryable(EngineExecError),
}

/// Atomically writes/ratifies ordered action relations. All inputs are borrowed for the callback.
/// `metadata` is non-null, and it and every nested view are valid only until the callback returns.
/// Write an owning result to `out`; Kernel initializes it to Uninit and consumes all output
/// handles.
pub type CDeclarativeCommitFn = extern "C" fn(
    context: NullableCvoid,
    actions: FfiSlice<FfiCommitRelation>,
    metadata: *const FfiCommitMetadata,
    out: *mut EngineExecResult<FfiCommitResponse>,
);

/// Borrowed publication entry for a ratified catalog commit.
#[repr(C)]
pub struct FfiCommitToPublish {
    /// Commit version.
    pub version: u64,
    /// Ratified staged file URL.
    pub source: KernelStringSlice,
    /// Published destination URL.
    pub destination: KernelStringSlice,
}

/// Publishes ratified commits in order through `version`. All inputs are borrowed for the call.
/// Write Success(true) after publication; Success(false) is an invalid callback result.
pub type CPublishCommitsFn = extern "C" fn(
    context: NullableCvoid,
    version: u64,
    commits: FfiSlice<FfiCommitToPublish>,
    out: *mut EngineExecResult<bool>,
);

/// Connector committer callbacks. Every callback must initialize its result and must not unwind.
#[repr(C)]
pub struct DeclarativeCommitterCallbacks {
    /// Atomic submission service.
    pub commit: CDeclarativeCommitFn,
    /// Optional catalog publication service.
    pub publish: OptionalValue<CPublishCommitsFn>,
    /// Called exactly once when the committer's final owner is released.
    pub free: extern "C" fn(NullableCvoid),
}

/// Creates a connector-backed declarative committer, consumable by either declarative builder.
///
/// Imperative submission returns Unsupported. The commit callback bundles winning history or its
/// discovery error on a confirmed conflict; no history-recovery callback is retained.
///
/// # Safety
///
/// `context` may be null; otherwise it must be transferable between threads and valid until `free`
/// is called. Callbacks must remain valid, obey their ownership contracts, and not unwind.
/// Use `free_declarative_committer` if no builder consumes it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_declarative_committer(
    context: NullableCvoid,
    is_catalog: bool,
    callbacks: DeclarativeCommitterCallbacks,
) -> Handle<MutableCommitter> {
    let committer: Box<dyn Committer> = Box::new(FfiDeclarativeCommitter {
        context,
        is_catalog,
        callbacks,
    });
    committer.into()
}

/// Releases a committer that was not consumed by a builder or another owning API.
///
/// # Safety
/// `committer` must be valid and is consumed. Do not free a builder-consumed committer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_declarative_committer(committer: Handle<MutableCommitter>) {
    unsafe { committer.drop_handle() }
}

struct FfiDeclarativeCommitter {
    context: NullableCvoid,
    is_catalog: bool,
    callbacks: DeclarativeCommitterCallbacks,
}

// SAFETY: construction requires a context that can be transferred between threads.
unsafe impl Send for FfiDeclarativeCommitter {}

impl Committer for FfiDeclarativeCommitter {
    fn commit(
        &self,
        _engine: &dyn Engine,
        _actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        _metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        Err(KernelError::unsupported(
            "This FFI committer supports declarative submission only",
        ))
    }

    fn commit_declarative(
        &self,
        _executor: &dyn ScopedPlanExecutor,
        actions: Box<dyn Iterator<Item = RelationRef> + '_>,
        metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        let actions: Vec<_> = actions.collect();
        let backing = CommitMetadataBacking::try_new(&metadata)?;
        let view = backing.as_ffi();
        let mut out = EngineExecResult::Uninit;
        with_commit_relations(&actions, |actions| {
            (self.callbacks.commit)(self.context, actions, &view, &mut out);
        });
        match callback_result(out)? {
            FfiCommitResponse::Committed(file) => {
                let location = *unsafe { file.location.into_inner() };
                Ok(CommitResponse::Committed {
                    file_meta: FileMeta::new(Url::parse(&location)?, file.last_modified, file.size),
                })
            }
            FfiCommitResponse::Conflict { version, commits } => Ok(CommitResponse::Conflict {
                version,
                commits: callback_result(commits).map(|range| {
                    let range = unsafe { range.into_inner() };
                    std::sync::Arc::try_unwrap(range).unwrap_or_else(|range| range.as_ref().clone())
                }),
            }),
            FfiCommitResponse::Retryable(error) => Ok(CommitResponse::Retryable {
                error: error.into(),
            }),
        }
    }

    fn is_catalog_committer(&self) -> bool {
        self.is_catalog
    }

    fn publish(&self, _engine: &dyn Engine, metadata: PublishMetadata) -> KernelResult<()> {
        let callback = match self.callbacks.publish {
            OptionalValue::Some(callback) => callback,
            OptionalValue::None => {
                return Err(KernelError::unsupported(
                    "This committer has no publication callback",
                ));
            }
        };
        let views: Vec<_> = metadata
            .commits_to_publish()
            .iter()
            .map(|commit| FfiCommitToPublish {
                version: commit.version(),
                source: borrowed_string(commit.location().as_str()),
                destination: borrowed_string(commit.published_location().as_str()),
            })
            .collect();
        let mut out = EngineExecResult::Uninit;
        callback(
            self.context,
            metadata.publish_version(),
            unsafe { FfiSlice::new_unsafe(&views) },
            &mut out,
        );
        if callback_result(out)? {
            Ok(())
        } else {
            Err(KernelError::internal_error(
                "Publication callback returned false success",
            ))
        }
    }
}

impl Drop for FfiDeclarativeCommitter {
    fn drop(&mut self) {
        (self.callbacks.free)(self.context);
    }
}

/// Visits relation descriptors while their identifiers and encoded schemas remain alive.
///
/// The returned value must not retain any borrowed view passed to `visit`.
pub(super) fn with_commit_relations<T>(
    relations: &[RelationRef],
    visit: impl FnOnce(FfiSlice<FfiCommitRelation>) -> T,
) -> T {
    let schemas: Vec<_> = relations
        .iter()
        .map(|relation| schema::StructType::from(relation.schema().as_ref()).encode_to_vec())
        .collect();
    let views: Vec<_> = relations
        .iter()
        .zip(&schemas)
        .map(|(relation, schema)| FfiCommitRelation {
            id: borrowed_string(relation.id()),
            schema: kernel_bytes_slice!(schema),
        })
        .collect();
    visit(unsafe { FfiSlice::new_unsafe(&views) })
}

struct CommitMetadataBacking<'a> {
    source: &'a CommitMetadata,
    published: Url,
    staged: Url,
    read_protocol: Option<ProtocolBacking<'a>>,
    read_metadata: Option<MetadataBacking<'a>>,
    new_protocol: Option<ProtocolBacking<'a>>,
    new_metadata: Option<MetadataBacking<'a>>,
    domain_metadata: Vec<FfiDomainMetadata>,
}

impl<'a> CommitMetadataBacking<'a> {
    fn try_new(source: &'a CommitMetadata) -> KernelResult<Self> {
        let pm = source.protocol_metadata();
        Ok(Self {
            source,
            published: source.published_commit_path()?,
            staged: source.staged_commit_path()?,
            read_protocol: pm.read_protocol().map(ProtocolBacking::new),
            read_metadata: pm.read_metadata().map(MetadataBacking::new),
            new_protocol: pm.new_protocol().map(ProtocolBacking::new),
            new_metadata: pm.new_metadata().map(MetadataBacking::new),
            domain_metadata: source
                .domain_metadata_changes()
                .iter()
                .map(|domain| FfiDomainMetadata {
                    domain: borrowed_string(domain.domain()),
                    configuration: borrowed_string(domain.configuration()),
                    removed: domain.is_removed(),
                })
                .collect(),
        })
    }

    fn as_ffi(&self) -> FfiCommitMetadata {
        let commit_type = match self.source.commit_type() {
            delta_kernel::committer::CommitType::PathBasedCreate => FfiCommitType::PathBasedCreate,
            delta_kernel::committer::CommitType::CatalogManagedCreate => {
                FfiCommitType::CatalogManagedCreate
            }
            delta_kernel::committer::CommitType::PathBasedWrite => FfiCommitType::PathBasedWrite,
            delta_kernel::committer::CommitType::CatalogManagedWrite => {
                FfiCommitType::CatalogManagedWrite
            }
            delta_kernel::committer::CommitType::UpgradeToCatalogManaged => {
                FfiCommitType::UpgradeToCatalogManaged
            }
            delta_kernel::committer::CommitType::DowngradeToPathBased => {
                FfiCommitType::DowngradeToPathBased
            }
        };
        FfiCommitMetadata {
            table_root: borrowed_string(self.source.table_root().as_str()),
            version: self.source.version(),
            commit_type,
            in_commit_timestamp: self.source.in_commit_timestamp(),
            max_published_version: self.source.max_published_version().into(),
            published_path: borrowed_string(self.published.as_str()),
            staged_path: borrowed_string(self.staged.as_str()),
            read_protocol: self
                .read_protocol
                .as_ref()
                .map(ProtocolBacking::as_ffi)
                .into(),
            read_metadata: self
                .read_metadata
                .as_ref()
                .map(MetadataBacking::as_ffi)
                .into(),
            new_protocol: self
                .new_protocol
                .as_ref()
                .map(ProtocolBacking::as_ffi)
                .into(),
            new_metadata: self
                .new_metadata
                .as_ref()
                .map(MetadataBacking::as_ffi)
                .into(),
            domain_metadata: unsafe { FfiSlice::new_unsafe(&self.domain_metadata) },
        }
    }
}

fn callback_result<T>(out: EngineExecResult<T>) -> KernelResult<T> {
    match out {
        EngineExecResult::Success(value) => Ok(value),
        EngineExecResult::Failure(error) => Err(error.into()),
        EngineExecResult::Uninit => Err(KernelError::internal_error(
            "FFI committer did not write its callback result",
        )),
    }
}

fn borrowed_string(value: &str) -> KernelStringSlice {
    unsafe { KernelStringSlice::new_unsafe(value) }
}
