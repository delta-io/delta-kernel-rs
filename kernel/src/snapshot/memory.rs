//! Construction-time accounting for retained snapshot allocations.
//!
//! Schema nodes accumulate their children's summaries while being assembled. Snapshot finalization
//! reads those summaries and samples log paths; it does not traverse the schema or log-file list.
//! Metadata maps and JSON metadata payloads are counted at assembly. These costs are proportional
//! to those payloads. Container layouts and URL capacities remain models, not hard limits.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::iter::Sum;
use std::mem::{align_of, size_of};
use std::ops::{Add, AddAssign};
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use indexmap::IndexMap;
use serde_json::Value;
use url::Url;

use crate::crc::{DomainMetadataState, FileStatsState, SetTransactionState};
use crate::last_checkpoint_hint::HintAction;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, MetadataColumnSpec, MetadataValue, PrimitiveType};
use crate::table_features::TableFeature;

/// Why a complete retained-memory estimate could not be produced.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[internal_api]
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
pub(crate) enum Unavailable {
    /// An arithmetic operation exceeded the platform's addressable size.
    Overflow,
    /// A mutable schema was exposed after its construction summary was collected.
    MutatedSchema,
    /// Accounting for this schema type is not implemented.
    UnsupportedType,
    /// Accounting for adaptive metadata is not implemented.
    AdaptiveMetadata,
    /// The fixed-size shared-object accounting set was exhausted.
    TooManySharedObjects,
    /// A CRC retains per-file state not covered by this construction summary.
    CrcFileList,
}

/// Retained requested allocation bytes, excluding allocator overhead and resident-page effects.
///
/// `modeled_bytes` uses private container-layout assumptions and sampled log-path sizes. The total
/// can under- or overestimate actual allocations and must not be used as a hard memory limit.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
#[internal_api]
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
pub(crate) struct Memory {
    /// Storage with observable capacity, including owned String and Vec buffers.
    pub precisely_counted_bytes: usize,
    /// Storage inferred from container layouts, URL lengths, or path samples.
    pub modeled_bytes: usize,
    unavailable: Option<Unavailable>,
}

impl Memory {
    pub(crate) const ZERO: Self = Self {
        precisely_counted_bytes: 0,
        modeled_bytes: 0,
        unavailable: None,
    };
    pub(crate) const fn precise(bytes: usize) -> Self {
        Self {
            precisely_counted_bytes: bytes,
            ..Self::ZERO
        }
    }
    pub(crate) const fn modeled(bytes: usize) -> Self {
        Self {
            modeled_bytes: bytes,
            ..Self::ZERO
        }
    }
    pub(crate) const fn unavailable(reason: Unavailable) -> Self {
        Self {
            unavailable: Some(reason),
            ..Self::ZERO
        }
    }
    pub(crate) fn array<T>(capacity: usize) -> Self {
        capacity
            .checked_mul(size_of::<T>())
            .map_or(Self::unavailable(Unavailable::Overflow), Self::precise)
    }
    pub(crate) fn as_modeled(self) -> Self {
        self.total_bytes()
            .map_or_else(Self::unavailable, Self::modeled)
    }
    pub(crate) fn times(self, count: usize) -> Self {
        match (
            self.precisely_counted_bytes.checked_mul(count),
            self.modeled_bytes.checked_mul(count),
        ) {
            (Some(precisely_counted_bytes), Some(modeled_bytes)) => Self {
                precisely_counted_bytes,
                modeled_bytes,
                unavailable: self.unavailable,
            },
            _ => Self::unavailable(Unavailable::Overflow),
        }
    }
    /// Returns the total estimate, or the reason accounting is incomplete.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn total_bytes(self) -> Result<usize, Unavailable> {
        if let Some(reason) = self.unavailable {
            return Err(reason);
        }
        self.precisely_counted_bytes
            .checked_add(self.modeled_bytes)
            .ok_or(Unavailable::Overflow)
    }
}

impl Add for Memory {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        match (
            self.precisely_counted_bytes
                .checked_add(rhs.precisely_counted_bytes),
            self.modeled_bytes.checked_add(rhs.modeled_bytes),
        ) {
            (Some(precisely_counted_bytes), Some(modeled_bytes)) => Self {
                precisely_counted_bytes,
                modeled_bytes,
                unavailable: self.unavailable.or(rhs.unavailable),
            },
            _ => Self::unavailable(Unavailable::Overflow),
        }
    }
}
impl AddAssign for Memory {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}
impl Sum for Memory {
    fn sum<I: Iterator<Item = Self>>(values: I) -> Self {
        values.fold(Self::ZERO, |a, b| a + b)
    }
}

/// The retained graph reachable from one SnapshotRef when construction finishes.
///
/// Shared roots are counted once within this snapshot, regardless of external owners. Adding the
/// estimates of two snapshots can double count shared storage. Later lazy state, engine storage,
/// tracing subscribers, construction temporaries, allocator fragmentation, and RSS are excluded.
#[derive(Debug, Copy, Clone)]
#[internal_api]
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
pub(crate) struct SnapshotMemoryAtCreation {
    /// Snapshot's Arc allocation, including the accounting fields.
    pub root: Memory,
    /// Raw metadata, protocol, parsed properties, and cached schemas.
    pub configuration: Memory,
    /// Log-file vectors, sampled path sizes, and the retained checkpoint hint.
    pub log: Memory,
    /// The retained CRC, including a stale base CRC.
    pub crc: Memory,
}
impl SnapshotMemoryAtCreation {
    /// Returns the total estimate, or a reason the complete graph is unsupported.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn total_bytes(&self) -> Result<usize, Unavailable> {
        (self.root + self.configuration + self.log + self.crc).total_bytes()
    }
}

#[derive(Default)]
pub(crate) struct SizeContext {
    seen: [usize; 8],
    len: usize,
}
impl SizeContext {
    fn contains(&self, ptr: usize) -> bool {
        self.seen[..self.len].contains(&ptr)
    }
    fn insert(&mut self, ptr: usize) -> bool {
        let Some(slot) = self.seen.get_mut(self.len) else {
            return false;
        };
        *slot = ptr;
        self.len += 1;
        true
    }
}

pub(crate) trait HeapSize {
    fn heap_size(&self, context: &mut SizeContext) -> Memory;
}

impl HeapSize for String {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        Memory::precise(self.capacity())
    }
}

impl HeapSize for Url {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        // The Url API does not expose its backing String capacity.
        Memory::modeled(self.as_str().len())
    }
}

impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        self.as_ref().map_or(Memory::ZERO, |v| v.heap_size(context))
    }
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        Memory::array::<T>(self.capacity())
            + self.iter().map(|v| v.heap_size(context)).sum::<Memory>()
    }
}

impl<T: HeapSize> HeapSize for Box<T> {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        Memory::precise(size_of::<T>()) + self.as_ref().heap_size(context)
    }
}

impl<T: HeapSize> HeapSize for Arc<T> {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        if context.contains(Arc::as_ptr(self) as usize) {
            return Memory::ZERO;
        }
        if !context.insert(Arc::as_ptr(self) as usize) {
            return Memory::unavailable(Unavailable::TooManySharedObjects);
        }
        arc_allocation_bytes::<T>() + self.as_ref().heap_size(context)
    }
}

pub(crate) fn hash_table_bytes<T>(capacity: usize) -> Memory {
    if capacity == 0 {
        return Memory::ZERO;
    }
    let Some(buckets) = capacity
        .checked_add(1)
        .and_then(usize::checked_next_power_of_two)
    else {
        return Memory::unavailable(Unavailable::Overflow);
    };
    // Hashbrown's bucket/control layout is private. This is a model, including after removals.
    let group_width = if cfg!(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64")
    )) {
        16
    } else {
        size_of::<usize>()
    };
    let alignment = align_of::<T>().max(group_width);
    let bytes = buckets
        .checked_mul(size_of::<T>())
        .and_then(|n| n.checked_add(alignment - 1))
        .map(|n| n & !(alignment - 1))
        .and_then(|n| n.checked_add(buckets))
        .and_then(|n| n.checked_add(group_width));
    bytes.map_or(Memory::unavailable(Unavailable::Overflow), Memory::modeled)
}

pub(crate) fn index_map_bytes<K, V>(capacity: usize) -> Memory {
    hash_table_bytes::<usize>(capacity) + Memory::array::<(usize, K, V)>(capacity).as_modeled()
}

impl<K: HeapSize + Eq + Hash, V: HeapSize, S: BuildHasher> HeapSize for HashMap<K, V, S> {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        hash_table_bytes::<(K, V)>(self.capacity())
            + self
                .iter()
                .map(|(k, v)| k.heap_size(context) + v.heap_size(context))
                .sum::<Memory>()
    }
}

impl<K: HeapSize + Eq + Hash, V: HeapSize, S: BuildHasher> HeapSize for IndexMap<K, V, S> {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        // IndexMap exposes the minimum of its two capacities, not both actual capacities.
        index_map_bytes::<K, V>(self.capacity())
            + self
                .iter()
                .map(|(k, v)| k.heap_size(context) + v.heap_size(context))
                .sum::<Memory>()
    }
}

impl HeapSize for usize {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        Memory::ZERO
    }
}
impl HeapSize for i64 {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        Memory::ZERO
    }
}
impl HeapSize for MetadataColumnSpec {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        Memory::ZERO
    }
}

impl HeapSize for TableFeature {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::Unknown(s) => s.heap_size(context),
            _ => Memory::ZERO,
        }
    }
}

impl HeapSize for ParsedLogPath {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        Memory::precise(self.filename.capacity())
            + Memory::precise(self.extension.capacity())
            + Memory::modeled(self.location.location.as_str().len())
    }
}

impl HeapSize for DataType {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::Primitive(value) => value.heap_size(context),
            Self::Array(v) => v.heap_size(context),
            Self::Map(v) => v.heap_size(context),
            Self::Struct(v) | Self::Variant(v) | Self::File(v) => v.heap_size(context),
            #[cfg(feature = "udt-in-dev")]
            Self::UserDefined(_) => Memory::unavailable(Unavailable::UnsupportedType),
        }
    }
}

impl HeapSize for MetadataValue {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::Number(_) | Self::Boolean(_) => Memory::ZERO,
            Self::String(v) => v.heap_size(context),
            Self::Other(v) => v.heap_size(context),
        }
    }
}

impl HeapSize for Value {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::String(v) => v.heap_size(context),
            Self::Array(v) => v.heap_size(context),
            Self::Object(v) => {
                // serde_json uses BTreeMap without preserve_order. Node occupancy is private;
                // one leaf is exact for <= 11 entries, larger maps remain approximations.
                let leaves = v.len().div_ceil(11);
                let nodes = leaves + leaves.saturating_sub(1).div_ceil(12);
                Memory::array::<u8>(16 + 11 * (size_of::<String>() + size_of::<Value>()))
                    .times(nodes)
                    .as_modeled()
                    + v.iter()
                        .map(|(k, v)| Memory::precise(k.capacity()) + v.heap_size(context))
                        .sum::<Memory>()
            }
            _ => Memory::ZERO,
        }
    }
}

impl HeapSize for FileStatsState {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::Complete(v) => v.heap_size(context),
            Self::Indeterminate => Memory::ZERO,
        }
    }
}

pub(crate) fn arc_allocation_bytes<T>() -> Memory {
    let alignment = align_of::<T>().max(align_of::<usize>());
    let offset = (2 * size_of::<usize>() + align_of::<T>() - 1) & !(align_of::<T>() - 1);
    Memory::modeled((offset + size_of::<T>() + alignment - 1) & !(alignment - 1))
}

impl HeapSize for DomainMetadataState {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::Complete(v) | Self::Partial(v) => v.heap_size(context),
        }
    }
}

impl HeapSize for SetTransactionState {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::Complete(v) | Self::Partial(v) => v.heap_size(context),
        }
    }
}

impl HeapSize for HintAction {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        match self {
            Self::Metadata(v) => v.heap_size(context),
            Self::Protocol(v) => v.heap_size(context),
            Self::Txn(v) => v.heap_size(context),
            Self::DomainMetadata(v) => v.heap_size(context),
            Self::CheckpointMetadata(v) => v.heap_size(context),
        }
    }
}

impl HeapSize for PrimitiveType {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        match self {
            #[cfg(feature = "geo-type-in-dev")]
            Self::Geometry(_) | Self::Geography(_) => {
                Memory::unavailable(Unavailable::UnsupportedType)
            }
            _ => Memory::ZERO,
        }
    }
}

/// Exact vector backing storage, plus a model of the heap owned by its elements. Samples the
/// beginning, middle and end to avoid an additional pass over the log. This is not an upper bound.
pub(crate) fn sampled_paths(paths: &Vec<ParsedLogPath>, context: &mut SizeContext) -> Memory {
    let backing = Memory::array::<ParsedLogPath>(paths.capacity());
    if paths.is_empty() {
        return backing;
    }
    let sample = [0, paths.len() / 2, paths.len() - 1]
        .into_iter()
        .map(|i| paths[i].heap_size(context))
        .sum::<Memory>();
    let per_path = match sample.total_bytes() {
        Ok(n) => Memory::modeled(n.div_ceil(3)),
        Err(reason) => Memory::unavailable(reason),
    };
    backing + per_path.times(paths.len())
}

impl<T: HeapSize> HeapSize for &T {
    fn heap_size(&self, context: &mut SizeContext) -> Memory {
        (*self).heap_size(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::{Add, Metadata, Protocol};
    use crate::crc::Crc;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::schema::{StructField, StructType};
    use crate::snapshot::Snapshot;
    use crate::table_configuration::TableConfiguration;
    use crate::FileMeta;

    #[test]
    fn memory_overflow_is_unavailable_not_wrapped_or_zero() {
        let sizes = [
            Memory::precise(usize::MAX) + Memory::precise(1),
            Memory::precise(usize::MAX) + Memory::modeled(1),
            Memory::array::<u64>(usize::MAX),
            Memory::modeled(usize::MAX).times(2),
            hash_table_bytes::<String>(usize::MAX),
        ];
        for size in sizes {
            assert_eq!(size.total_bytes(), Err(Unavailable::Overflow));
        }
    }

    #[test]
    fn shared_schema_is_counted_once_regardless_of_other_owners() {
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "id",
            DataType::LONG,
        )]));
        let other_owner = schema.clone();
        let mut context = SizeContext::default();
        let first = schema.heap_size(&mut context);
        assert!(first.total_bytes().unwrap() > size_of::<StructType>());
        assert_eq!(other_owner.heap_size(&mut context), Memory::ZERO);
        assert_eq!(schema.heap_size(&mut SizeContext::default()), first);
    }

    #[test]
    fn strings_and_vectors_use_capacity_not_length() {
        let mut text = String::with_capacity(512);
        text.push('x');
        let mut strings = Vec::with_capacity(8);
        let text_capacity = text.capacity();
        strings.push(text);
        assert_eq!(
            strings.heap_size(&mut SizeContext::default()),
            Memory::precise(strings.capacity() * size_of::<String>() + text_capacity)
        );
    }

    #[test]
    fn fixed_shared_object_set_returns_unavailable_when_full() {
        let roots: Vec<_> = (0..9).map(|_| Arc::new(String::from("distinct"))).collect();
        let result = roots.heap_size(&mut SizeContext::default());
        assert_eq!(result.total_bytes(), Err(Unavailable::TooManySharedObjects));
    }
    #[test]
    fn snapshot_counts_stale_crc_and_preserves_estimate_with_extra_owners() {
        let metadata: Metadata = serde_json::from_value(serde_json::json!({
            "id":"test","format":{"provider":"parquet","options":{}},
            "schemaString":r#"{"type":"struct","fields":[]}"#,
            "partitionColumns":[],"configuration":{}
        }))
        .unwrap();
        let protocol: Protocol =
            serde_json::from_str(r#"{"minReaderVersion":1,"minWriterVersion":2}"#).unwrap();
        let crc = Arc::new(Crc {
            version: 0,
            metadata: metadata.clone(),
            protocol: protocol.clone(),
            ..Default::default()
        });
        let table_root = Url::parse("memory:///memory-accounting/").unwrap();
        let log_root = table_root.join("_delta_log/").unwrap();
        let paths: Vec<_> = (0..2)
            .map(|version| {
                ParsedLogPath::try_from(FileMeta {
                    location: log_root.join(&format!("{version:020}.json")).unwrap(),
                    last_modified: 0,
                    size: 1,
                })
                .unwrap()
                .unwrap()
            })
            .collect();
        let latest_commit_file = paths.last().cloned();
        let log = LogSegment::try_new(
            LogSegmentFiles {
                ascending_commit_files: paths,
                latest_commit_file,
                max_published_version: Some(1),
                ..Default::default()
            },
            log_root,
            Some(1),
            None,
        )
        .unwrap();
        let configuration = TableConfiguration::try_new(metadata, protocol, table_root, 1).unwrap();
        let snapshot = Arc::new(
            Snapshot::new_with_crc(log, configuration, Some(crc.clone()), false, false).unwrap(),
        );
        assert!(snapshot.crc_at_version().is_none());
        let memory = snapshot.retained_memory_at_creation();
        assert_eq!(memory.crc, crc.heap_size(&mut SizeContext::default()));
        assert!(memory.total_bytes().unwrap() > memory.crc.total_bytes().unwrap());
        let another_owner = snapshot.clone();
        assert!(std::ptr::eq(
            memory,
            another_owner.retained_memory_at_creation()
        ));
    }

    #[test]
    fn crc_with_file_list_is_explicitly_unavailable() {
        let crc = Crc {
            all_files: Some(vec![Add::default()]),
            ..Default::default()
        };
        assert_eq!(
            crc.heap_size(&mut SizeContext::default()).total_bytes(),
            Err(Unavailable::CrcFileList)
        );
    }
}
