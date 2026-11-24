# FFI design notes

Worked examples and benchmark evidence behind the conventions in `ffi/AGENTS.md`. Code snippets
illustrate ABI shapes; inspect the exported signatures before using an example name as an API.

## Schema metadata design example

Projected schemas can lose field metadata while round-tripping through the host schema
representation.
For a column-mapped table, the logical field name is not sufficient to read Parquet data. Kernel
needs typed field metadata such as:

* delta.columnMapping.physicalName as a string;
* delta.columnMapping.id as a number.

`scan_builder_with_schema` treats the supplied schema as the authoritative logical read schema.
During scan construction, Kernel converts each selected logical field into its physical form. If the
host reconstructs the projection from names, types, and nullability only, the mapping metadata is
missing.

### Existing Kernel-to-host path

The schema visitor passes `&CMetadataMap` with each field callback. `CMetadataMap` contains a Rust
`HashMap`, so the host cannot dereference it. The host must call `get_from_metadata_map` for known
keys or `visit_metadata_map` to enumerate it:

```c
host -> visit_schema
Rust -> host field callback with opaque CMetadataMap
host -> visit_metadata_map
Rust -> host callback once per entry
```

Passing the map pointer identifies the map; it does not expose a C-readable layout.

### Visitor-based alternative

A visitor-based alternative mirrors that indirection in reverse. Every non-null field metadata
descriptor causes another upcall, and each metadata value causes another downcall:

```c
host -> visit_field_*
Rust -> EngineMetadata visitor
host -> visit_metadata_value once per entry
Rust -> return to field construction
```

This is symmetric with the existing path, but it makes flat, materialized metadata re-entrant and
splits errors across nested calls.

### Preferred design order

1. For projected scans, determine whether Kernel can resolve selected logical paths against the
   snapshot schema and copy the canonical fields. If so, the host should not have to return Delta's
   own mapping metadata.
2. For genuinely host-authored schemas, such as table creation, pass metadata as a borrowed typed
   entry slice.
3. On the Kernel-to-host schema path, pass the same entry slice with each field callback instead of
   an opaque map that requires `visit_metadata_map`.
4. Preserve existing APIs as compatibility shims where necessary.

### Direct metadata ABI

Use the `CMetadataEntry` and `CMetadataEntries` layout from the error propagation example in
`ffi/AGENTS.md`.
Pass `CMetadataEntries` by value because it is a two-word borrowed view. The empty value is
`{ ptr: NULL, len: 0 }`. All entries and their key/value bytes remain valid until the enclosing
field call returns. Kernel validates UTF-8, parses values according to kind, rejects duplicate keys,
and copies the resulting metadata before returning. The kind tag is required: treating every value
as a string would change `delta.columnMapping.id = 7` into `delta.columnMapping.id = "7"`, which
fails Kernel's typed column mapping validation. A single JSON object is another valid
representation because Delta metadata is defined as JSON, but it trades the entry ABI for JSON
encoding, parsing, and escaping at every boundary. The same entry type can be used in both
directions. The producer owns the backing array regardless of whether the producer is Rust or the
host.

## Event callback examples and performance evidence

A Kernel Java logs, metrics, and frames benchmark compared three ways to deliver one event per
Rust-to-Java upcall. These abridged snippets show the difference.

### 1. Flatten by value, then build a Java record

Rust expands the event into callback arguments:

```rust
type EmitMetric = unsafe extern "C" fn(
    context: *mut c_void,
    kind: u32,
    value: i64,
    name: KernelStringSlice,
);

let event = FfiMetricEvent::from(metric);
emit(context, event.kind, event.value, event.name);
```

Java copies those arguments into its semantic type:

```java
public void invoke(
    Pointer context, int kind, long value, Pointer namePtr, long nameLen) {
  consumer.accept(new MetricEvent(kind, value, readUtf8(namePtr, nameLen)));
}
```

The Java consumer sees a safe record, but every field expands the callback ABI. Adding fields makes
the signature larger.

### 2. Borrow a native pointer, then build a Java record

Rust keeps the event alive for the callback:

```rust
type EmitMetric = unsafe extern "C" fn(
    context: *mut c_void,
    event: *const FfiMetricEvent,
);

let event = FfiMetricEvent::from(metric);
emit(context, &event);
// `event` may be dropped after the callback returns.
```

Java reads native memory and copies the data it keeps:

```java
public void invoke(Pointer context, Pointer eventPtr) {
  NativeMetricEvent event = NativeMetricEvent.view(eventPtr);
  consumer.accept(new MetricEvent(
      event.kind(), event.value(), event.nameAsJavaString()));
}
```

This keeps one small callback signature and avoids copying the outer event across the ABI. The
pointer and every nested slice expire when invoke returns; the Java record does not.

### 3. Pass the borrowed pointer to the final synchronous consumer

The Rust side has the same borrowed-pointer contract:

```rust
let event = FfiMetricEvent::from(metric);
emit(context, &event);
// Neither the callback nor its consumer may retain `&event`.
```

Java forwards the pointer-backed view without building a record:

```java
public void invoke(Pointer context, Pointer eventPtr) {
  consumer.accept(NativeMetricEvent.view(eventPtr));
  // The consumer must finish reading before this method returns.
}
```

This can save one Java allocation, but it exposes native layout and lifetime rules to every
consumer.

### Recommendation

Use approach 2 for general event APIs. It captured nearly all the performance of approach 3 while
keeping unsafe memory access in the binding layer. Storage metric decoding fell from 317.4 ns/event
with approach 1 to 95.0 ns/event with approach 2. With a Prometheus update, it fell from 433.7
ns/event to 208.1 ns/event. Approach 3 was effectively tied at 207.4 ns/event. For large strings,
decoding and allocation dominate. At 16 KiB, all three log paths took about 16.4-16.7 microseconds
per event. Use approach 3 only for a measured hot path where avoiding the Java record materially
helps. This experiment kept one upcall per event. It does not justify one callback per field or
collection entry. For a finite map or list, pass one borrowed record or a two-word `{ ptr, len }`
descriptor and read it during that call.

### Example: snapshot hint input

A snapshot hint is a complete value when its setter call begins, so one borrowed aggregate is the
appropriate input. The exported Rust API is:

```rust
#[repr(C)]
pub struct FfiSnapshotHint {
    pub version: Version,
    pub freshness: FfiSnapshotHintFreshness,
    pub log_paths: LogPathArray,
    pub protocol: FfiProtocol,
    pub metadata: FfiMetadata,
    pub last_checkpoint: *const FfiLastCheckpoint,
    pub crc: *const FfiCrc,
    pub publication_watermark: FfiPublicationWatermark,
    pub checkpoint_type: OptionalValue<FfiSelectedCheckpointType>,
    pub crc_source: OptionalValue<FfiCrcSource>,
}

pub unsafe extern "C" fn snapshot_builder_with_snapshot_hint(
    builder: Handle<ExclusiveSnapshotBuilder>,
    value: &FfiSnapshotHint,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>>;
```

The host keeps `hint` and all reachable memory alive for the call. Rust validates and copies the
full graph before changing the builder. One call avoids partial state and callback choreography.
The setter consumes the builder on both success and error, returning its replacement on success.
