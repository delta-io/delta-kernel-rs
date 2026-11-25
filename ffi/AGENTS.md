# FFI Layer

The `delta_kernel_ffi` crate exposes the kernel to C/C++ via a stable FFI boundary using
cbindgen-generated headers (`.h` and `.hpp`).

These files are not loaded automatically:

- `ffi/AGENTS/flows.md`: key files and call sequences for the snapshot, scan, commit-range,
  incremental-scan, write, and tracing exports. Read it before changing one of those exports.
- `ffi/AGENTS/design-notes.md`: worked examples and benchmark evidence behind the conventions
  below. Read it before designing a callback, metadata ABI, or event API.

## FFI boundary conventions

Memory ownership drives every choice below. Function inputs can usually borrow memory for the call.
A direct variable-length result needs an owner, a lifetime pinned by another object, or a named
release function.
These conventions govern the Delta Kernel C ABI used by C, C++, and Java/JNR connectors. They
cover three related choices:

1. whether data crosses the boundary as a C-compatible value or through callbacks;
2. whether a value is passed directly, through a pointer, or through a handle;
3. who owns referenced memory and how long it remains valid.

These rules apply to new APIs. Existing APIs do not all follow them and should be migrated only when
compatibility and value justify the change.
Code snippets in this section illustrate ABI shapes; inspect the exported signatures before using
an example name as an API.

### Core rule

Use C-compatible representations for data. Choose delivery from ownership, lifetime, and how the
producer creates the data. A callback can carry a `#[repr(C)]` record or slice when its purpose is
to bracket the data's lifetime.

Do not cross the boundary repeatedly merely to unpack a finite value that one side has already
materialized. For flat records and collections, avoid a downcall, upcall, repeated downcalls, and
repeated upcalls. That pattern complicates lifetimes, re-enters control flow, splits error handling,
hardens bindings, and enlarges the unsafe surface.

### Terminology

| Term | Meaning |
| :---- | :---- |
| Downcall | The host, such as Java or C, calls Rust. |
| Upcall | Rust calls a host-provided callback. |
| Owner | The side responsible for keeping memory alive and eventually releasing it. |
| Borrow | Temporary access to memory owned by the other side. A borrower never frees it. |
| View descriptor | A small C-compatible value such as `{ ptr, len }` that describes borrowed memory. |
| Handle | An opaque pointer-like value representing a Rust-owned or engine-owned object with explicit lifetime rules. |
| Materialized collection | A collection whose complete contents and length already exist when the call begins. |
| Lazy stream | A sequence whose items are produced over time, may be large, or may fail between items. |
| Visitor | A callback protocol used to translate a recursive or heterogeneous value into the receiver's native representation. |

### Choosing a representation

| Data or operation | Default representation | Reason |
| :---- | :---- | :---- |
| Scalar or small fixed record | `#[repr(C)]` value | It has a stable, directly consumable C layout. |
| Finite materialized collection | Borrowed `{ ptr, len }` slice | One call transfers the complete view without re-entrant enumeration. |
| Recursive heterogeneous tree | Visitor or stable serialized form | The receiver must reconstruct variants and parent-child relationships in its own type system. |
| Lazy or fallible sequence | Pull iterator with next | The producer need not materialize the sequence, and each failure has a clear call boundary. |
| Behavior invoked later or repeatedly | Callback or function-pointer table | The value crossing the boundary is behavior, not already-materialized data. |
| Object that outlives the call | Opaque handle | The concrete representation is not C-compatible or its lifetime spans calls. |
| Large tabular data | Arrow C Data Interface | It provides a standard ownership and release protocol for columnar batches. |

#### Materialized collections

When the producer already owns the complete collection, expose a borrowed entry slice. The owner
keeps the array and everything referenced by its entries alive until the call returns. The receiver
validates and copies anything it retains. A callback that receives one complete borrowed slice is
useful when it brackets the borrow's lifetime. Do not call a callback once per element of an eager
collection.

Borrowed record arrays in this crate use `FfiSlice<T>`: an empty slice may have a null or non-null
pointer, while a non-empty slice requires a non-null pointer. The pointed-to storage is never owned
by the view.

##### Collections inside handles

An opaque handle controls ownership; it does not decide how to expose the data inside it. If a
handle owns a materialized collection, expose one typed view of the complete collection. This
applies to nested collections as well as flat rows.
For example, a write context may own a list of column paths, where each path is a list of field
names:

```rust
#[repr(C)]
pub struct FfiColumnPath {
    pub parts: *const KernelStringSlice,
    pub len: usize,
}

#[repr(C)]
pub struct FfiColumnPaths {
    pub paths: *const FfiColumnPath,
    pub len: usize,
}

pub type VisitColumnPaths = unsafe extern "C" fn(
    context: *mut c_void,
    paths: FfiColumnPaths,
);
```

The accessor invokes `VisitColumnPaths` once. The outer slice, every `FfiColumnPath`, and every
string remain valid until that callback returns. The host copies any data it retains.
Avoid a callback per path:

```rust
// Avoid for an already-materialized collection.
for path in write_context.stats_columns() {
    visitor(context, path.parts.as_ptr(), path.parts.len());
}
```

Use a per-item visitor only when items are produced lazily, can fail independently, or cannot be
represented as a stable typed slice. The same rule applies when a handle owns rows: pass one slice
of row descriptors when all rows already exist; visit one row at a time only when the producer
streams them.

Compatibility examples include per-entry visitors for eager string maps, metadata maps, domain
metadata, clustering columns, and partition values. Their existence does not make per-entry
callbacks the default for new APIs. `CStringMap` and `CMetadataMap` wrap Rust maps, so their opaque
pointers identify the maps but do not expose C-readable entries. Owned array results such as
`KernelBoolSlice`, `KernelRowIndexArray`, and `KernelOwnedBytes` require explicit release. Existing
scan APIs also vary between callback delivery and direct Arrow results; choose from the producer's
laziness and the result's ownership rather than copying whichever signature is nearby.

#### Visitors

Visitors are appropriate for schemas and expressions because they are recursive tagged trees and the
host must construct its own native schema or expression representation. They are also useful when a
stable serialized representation would impose an undesirable dependency. Visitors are not justified
merely because the source collection is stored in a Rust HashMap or a host-native map. The boundary
representation can still be a temporary entry array.

#### Streams and iterators

Use a pull model when the producer is lazy, an item can fail independently, or materializing every
item would be expensive. Prefer returning or writing the next item directly through a tagged result
or out parameter. An additional item-delivery callback is unnecessary when ownership can be
expressed directly.

#### Service and event callbacks

Callbacks remain the correct representation for authentication refresh, plan execution, opaque
expression evaluation, catalog commit services, logging, metrics, and similar operations. These
callbacks represent behavior that Rust invokes later, potentially repeatedly or asynchronously.

### Passing values, pointers, and handles

`#[repr(C)]` defines layout. It does not decide whether a type should cross the ABI by value or by
pointer, and it does not define ownership.

#### Pass by value

Pass these by value:

* primitive scalars and `#[repr(C)]` enums;
* opaque handle wrappers;
* immutable records no larger than two pointer-sized words;
* small view descriptors such as `{ ptr, len }`;
* small direct results such as a pair of integers.

Plain by-value arguments cannot be null and do not add a lifetime for the outer value. Use
`OptionalValue<T>` when a small by-value value may be absent. These values also fit naturally in
registers on common ABIs. The callee receives a copy of the outer bytes.
Rust:

```rust
#[repr(C)]
pub struct KernelStringSlice {
    pub ptr: *const c_char,
    pub len: usize,
}
pub unsafe extern "C" fn open_table(path: KernelStringSlice) { /* ... */ }
```

C call site:

```c
KernelStringSlice path = {bytes, length};
open_table(path);
```

Only the two-word descriptor is copied. The string bytes remain borrowed from the caller for the
call.

#### Enums and choice sets

Prefer `#[repr(C)]` enums for closed FFI choice sets instead of integer aliases and constants.
Add `cbindgen:prefix-with-name=true` so generated variants remain unambiguous. Use an integer
discriminator only when unknown values are intentionally recoverable, and validate them at the
boundary. Invalid enum tags are undefined behavior, so unsafe FFI APIs must require valid tags.

#### Optional values

`OptionalValue<T>` is the tagged by-value representation for optional data:

```rust
#[repr(C)]
pub enum OptionalValue<T> {
    Some(T),
    None,
}
```

Use it when T is no larger than two pointer-sized words or when it is a field inside a larger
borrowed record. The tag distinguishes absence from a valid zero or empty value. For example,
`Some(KernelStringSlice { ptr: std::ptr::null(), len: 0 })` means a present empty string;
`None` means no string. An API using this pattern is:

```rust
pub unsafe extern "C" fn snapshot_file_stats(
    snapshot: Handle<SharedSnapshot>,
) -> OptionalValue<FfiFileStats> {
    /* ... */
}
```

For an optional large top-level input, prefer `*const T` with a documented NULL = absent contract.
`OptionalValue<T>` still carries the full T payload and may create a large tagged union that is
awkward to pass by value through JNR or libffi. `OptionalValue<T>` changes presence, not ownership.
`Some(KernelStringSlice)` is still a borrowed slice, and `Some(Handle<T>)` follows the handle's
existing ownership contract.

#### Pass read-only inputs as `*const T`

Use `*const T` for a read-only input when T is larger than two pointer-sized words or is a
callback/function table. A null pointer may represent an absent large record when the API documents
that contract. Use a pointer for an extensible configuration record only when the record also
carries an explicit size or version; a pointer by itself does not make an ABI extensible.
Rust:

```rust
#[repr(C)]
pub struct ReadOptions {
    pub struct_size: usize,
    pub predicate: KernelStringSlice,
    pub limit: i64,
}
pub unsafe extern "C" fn start_scan(options: *const ReadOptions) -> ExternResult<bool> {
    // Validate `options` before creating `&ReadOptions`.
    // Copy nested fields if Rust retains them beyond this call.
    /* ... */
}
```

C call site:

```c
ReadOptions options = {
    .struct_size = sizeof(ReadOptions),
    .predicate = predicate,
    .limit = 100,
};
start_scan(&options);
```

The pointer avoids requiring a large by-value marshalling contract and keeps the function signature
small. It is also easier for JNR and libffi than a large struct-by-value argument, whose register
and stack classification varies by platform ABI.

The event benchmark in `ffi/AGENTS/design-notes.md` supports a borrowed pointer for large
Rust-to-Java callback payloads. It does not measure arbitrary C/C++ downcalls; a native compiler
may already lower a large by-value argument to a hidden pointer. Small values remain better by
value. All FFI records must be C-compatible; "copy" here means copying outer bytes, not invoking a
C++ copy constructor or Rust Clone implementation.
The tradeoffs are real:

* Rust must validate nullability before creating a reference.
* The caller must provide aligned, initialized storage that stays valid for the call.
* The extra indirection can be slower than passing a tiny value in registers.
* Java may need to allocate or populate native memory for the record.
* Adding fields is still an ABI change unless a size/version protocol defines how old callers and
  new callees interoperate.

This only avoids a copy of the outer record. Rust must still copy nested strings or arrays if it
retains them after the call.

#### Use `*mut T` only for writes or ownership transfer

Use `*mut T` when the callee or callback writes through the pointer, borrows mutable state, or
consumes the pointed-to object. Out parameters and mutable builders are examples.
An out parameter lets Kernel allocate the result slot and the engine callback fill it:

```rust
pub type CExecuteOpFn = extern "C" fn(
    context: NullableCvoid,
    plan_proto: KernelBytesSlice,
    out: *mut EngineExecResult<CPlanResult>,
);

let mut out = EngineExecResult::Uninit;
callback(context, plan_proto, &mut out);
```

The REST authentication callback similarly fills a caller-provided record:

```rust
pub type CAuthHeaderCallback = extern "C" fn(
    context: NullableCvoid,
    out: *mut CAuthHeaders,
    error_allocators: *const ErrorAllocators,
);
```

Mutable builder APIs are another valid use. Some exports spell this as `&mut EngineBuilder`, which
appears as `EngineBuilder *` in C; new exports should use `*mut EngineBuilder` and validate it
before creating `&mut EngineBuilder` internally. `*mut T` can also mean ownership transfer rather
than a mutable borrow. For example, `builder_build(builder: *mut EngineBuilder)` consumes and frees
the builder. The API documentation must say which contract applies. Pointer mutability does not
express ownership. Do not use `*mut T` merely because the record contains an opaque context that a
callback may mutate. Mutability describes writes through this pointer, not logical mutation
elsewhere in the object graph.

#### Do not expose Rust references in new C APIs

New `extern "C"` signatures should not use `&T`, `&mut T`, or `Option<&T>`. Use `*const T` or
`*mut T` and convert to a Rust reference after validating the pointer.

Rust references impose non-null, alignment, initialization, lifetime, and aliasing requirements at
function entry. A C declaration only shows a pointer, so those stronger Rust invariants are easy for
a caller or binding generator to violate accidentally. Raw pointers make nullability and mutation
part of the documented ABI contract.

#### Handle rules

Exclusive handles (`mutable=true`) are `Box`-like owners and are neither `Copy` nor `Clone`.
Shared handles are `Arc`-like reference-counted owners. A borrowed handle is read with `as_ref()`
or `as_mut()`;
the caller keeps ownership and remains responsible for release. A consumed handle is converted with
`into_inner()` before any fallible parsing, decoding, or validation. Rust drops it on both success
and error, and the caller must not use or release it afterward. Do not conditionally consume only
after success.

* Pass a shared pointer-like handle by value.
* Pass an exclusive handle by value when the function consumes it.
* Borrow an exclusive handle through a raw pointer when the function does not consume it.
* Every owned handle has a named release operation unless another call explicitly consumes it.
* A derived handle may outlive its source only if it owns or retains everything it references.
  Otherwise, name the source handle that must remain alive.

Name accessors for the exact semantic view they return. For example, snapshot_table_physical_schema
and write_context_file_schema should remain distinct: the first may include partition or
non-materialized columns that must not be written into a data file.

### Mirror the Rust API lifecycle

FFI functions should map to the Rust API's meaningful objects and stages. The signatures will differ
because the FFI must use handles, C-compatible values, and explicit ownership. The lifecycle and
validation points should remain recognizable.
Given this Rust API:

```rust
let context = state
    .write_context_builder()
    .with_partition_values(values)?
    .build()?;
```

Expose the same stages at the boundary:

```c
WriteContextBuilder *builder = write_context_builder_new(state);
ExternResultBool set = write_context_builder_with_partition_values(
    builder, partition_entries);
ExternResultWriteContext context = write_context_builder_build(builder);
```

Document that write_context_builder_build consumes builder on both success and error, or leaves it
owned by the caller on both paths. Provide free_write_context_builder for every path where the
caller can retain ownership. Do not replace a builder with an ad hoc combined operation such as
write_state_bind(state, values) unless the Rust API provides the same atomic operation. A separate
FFI lifecycle keeps defaults, ordering constraints, validation, and future builder options aligned
with Rust. The mapping is semantic, not mechanical. The FFI may combine trivial getters, accept a
typed slice instead of a Rust iterator, or return a handle instead of a Rust value. It should not
invent a second object model.

### Ownership and lifetime

The direction of travel determines who must copy:

| Direction | Boundary contract | If the receiver keeps it |
| :---- | :---- | :---- |
| Host to Rust | Borrowed argument, valid for the downcall | Rust copies into String, Vec, or another owned type before returning. |
| Rust to host callback | Borrowed payload, valid for the upcall | The host copies before the callback returns. |
| Rust to host direct result | Owned value, or a borrow pinned by a named owner | The host calls the named release function, or keeps the owner alive. |

The allocator knows how to release memory; the owner decides when. Those can be different sides.
Every pointer-bearing API must state one of these lifetimes:

* borrowed for this downcall;
* borrowed for this upcall;
* borrowed until a named owner handle is released;
* ownership transferred to Rust or the host;
* owned result requiring a named `free_*` function.

Passing a structure by value copies only its outer bytes. It does not transfer anything referenced
by its pointers. Wrapping a value in `ExternResult<T>` or `OptionalValue<T>` describes status or
presence, not ownership.

For every `{ ptr, len }` view:

* ptr == NULL is valid only when len == 0, unless documented otherwise;
* non-empty arrays must be properly aligned and readable for the full call;
* nested slices in each entry follow the same lifetime;
* the receiver copies values before retaining them;
* the owner remains responsible for release.

### Opaque transport state

Transport an opaque encoded value as bytes, even if its encoding is JSON or UTF-8:

```rust
#[repr(C)]
pub struct KernelBytesSlice {
    pub ptr: *const u8,
    pub len: usize,
}
```

Use a borrowed `KernelBytesSlice` for an input or callback-scoped view. Use an owned byte handle or
an owned byte record with a named release function for a direct result. A string type promises text
semantics and needlessly prevents future compression or binary framing. Use the live owned handle
for calls in the same process. Encode only when state must cross a process or machine boundary. If
decoding requires compatible Kernel binaries, put a compatibility identifier in the envelope and
reject a mismatch before decoding the payload. A format version alone covers the encoding shape; it
does not identify builds that may interpret the same fields differently.

### String conventions

`KernelStringSlice` is a non-owning UTF-8 view:

```c
struct KernelStringSlice {
    const char *ptr;
    uintptr_t len;
};
```

It is length-delimited and need not be null-terminated.

| Need | Convention | Owner |
| :---- | :---- | :---- |
| Pass a host string into one Rust call | Host allocates scoped UTF-8 bytes and passes `KernelStringSlice` by value. | Host, after the downcall returns. |
| Give a temporary Rust string to the host | Rust invokes `AllocateStringFn`; the host copies during the callback. | Rust owns the source. The host owns its copy. |
| Rust must retain a host-provided string | Rust copies it or converts it into an explicitly owned Rust handle. | Rust after the copy or transfer. |
| Return a string tied to a long-lived owner | Return a borrowed slice only when a named live handle guarantees the backing lifetime. | The named owner. |
| Optional string | Use the ABI's optional representation without changing the slice's ownership. | Existing owner. |

Do not return a bare `KernelStringSlice` backed by a temporary Rust String. The temporary is dropped
before the host can safely consume the result. `AllocateStringFn` does not allocate by itself. Rust
supplies a temporary string slice; the host chooses the destination, copies the bytes, and returns
an opaque pointer when the API needs one.

### Error handling

Rust helpers return `KernelResult<T>` or `delta_kernel::Result<T>` and pass the result to
`IntoExternResult` to produce the C ABI's `ExternResult<T>`. The top-level error variant selects the
connector's allocation callback; a Delta error with a Kernel source still uses the Delta callback.

There are two error directions, with opposite message owners:

| Direction | Result envelope | Message construction | Final owner |
| :---- | :---- | :---- | :---- |
| Kernel to engine | `ExternResult<T>::Err(*mut c_void)` | Kernel calls the registered Kernel or Delta allocator with temporary error information. | The engine owns and frees the returned error object. |
| Engine to Kernel | `EngineExecResult<T>::Failure(EngineExecError)` | The engine calls `allocate_kernel_string` to copy its message into an `ExclusiveRustString` handle. | Kernel consumes and frees the string handle. |

#### Kernel and Delta errors returned to the engine

`ErrorAllocators` carries a connector context and both allocation callbacks. The error information
records in `src/error/allocation.rs` document the classification fields and borrowed diagnostics.
Callbacks copy any information they need after returning and return their own error object as
`void*`, or NULL if allocation fails. Kernel returns that pointer unchanged in `ExternResult::Err`
and never reads or frees it.

Engine constructors accept `*const ErrorAllocators` and copy the table into builders and engines.
The table pointer must be valid for the constructor call; callback code and context must remain
valid for every use and support concurrent calls when shared. Standalone exports borrow a table
for the call. Use the stored allocator when an engine or builder is already available:

```rust
unsafe { result.into_extern_result(&*error_allocators) }; // Borrowed table for this call.
unsafe { result.into_extern_result(&engine.as_ref()) }; // The engine stores the table.
```

Do not require both an engine and a separate allocator table for the same operation.

#### Engine errors returned to Kernel

An engine upcall cannot return an engine-owned message slice with an unspecified lifetime. It first
copies the message into a Rust-owned handle:

```rust
pub struct EngineExecError {
    pub etype: FFIKernelError,
    pub message: Handle<ExclusiveRustString>,
}
```

The engine creates message by calling allocate_kernel_string. Kernel takes ownership when it
receives EngineExecResult::Failure, consumes the handle exactly once, and frees it. The
`ErrorAllocators` argument to allocate_kernel_string is only for reporting a failure in that
downcall, such as invalid UTF-8; it does not own the returned string. `AllocateStringFn` is a
separate general-purpose conversion callback for temporary Kernel strings. It is not the error
allocator and should not be introduced as a third error-message ownership model.

#### Error propagation rules

When the caller passes a complete list or map, the callee validates and copies every entry before
changing any state. The top-level FFI function returns one ExternResult, so it can report the exact
failing key or value instead of a generic callback failure.
An illustrative one-shot metadata decoder:

```rust
#[repr(C)]
pub struct CMetadataEntry {
    pub key: KernelStringSlice,
    pub kind: CMetadataValueKind,
    pub value: KernelStringSlice,
}

#[repr(C)]
pub struct CMetadataEntries {
    pub ptr: *const CMetadataEntry,
    pub len: usize,
}

pub unsafe extern "C" fn set_metadata(
    builder: *mut FfiBuilder,
    entries: CMetadataEntries,
    error_allocators: *const ErrorAllocators,
) -> ExternResult<bool> {
    let result = crate::panic::catch_panic(|| unsafe {
        set_metadata_impl(builder, entries)
    });
    unsafe { result.into_extern_result(&*error_allocators) }
}

unsafe fn set_metadata_impl(
    builder: *mut FfiBuilder,
    entries: CMetadataEntries,
) -> KernelResult<bool> {
    let builder = unsafe { builder.as_mut() }
        .ok_or_else(|| KernelError::generic("builder must not be null"))?;
    let entries = match (entries.ptr.is_null(), entries.len) {
        (true, 0) => &[],
        (true, _) => return Err(KernelError::generic("entries is null with non-zero length")),
        (false, len) => unsafe { std::slice::from_raw_parts(entries.ptr, len) },
    };

    let mut decoded = HashMap::with_capacity(entries.len());
    for entry in entries {
        let key = unsafe { entry.key.try_to_string()? };
        let value = unsafe { parse_metadata_value(entry.kind, entry.value)? };
        if decoded.insert(key.clone(), value).is_some() {
            return Err(KernelError::generic(format!("duplicate metadata key: {key}")));
        }
    }

    builder.set_metadata(decoded); // Mutate only after the full collection is valid.
    Ok(true)
}
```

The decoder has one error channel. Invalid UTF-8, a bad value for kind, a null/length mismatch, or a
duplicate key becomes the same outer ExternResult::Err with its original message. The builder
remains unchanged if any entry fails. Avoid protocols where each entry performs another fallible
downcall and the enclosing callback can only return false. That splits one logical operation across
error channels and often reduces a precise failure to a generic "visitor failed" error. For lazy
iterators, each next call may return its own error. After a terminal error, the API must state
whether the iterator can be retried or is poisoned. No callback may unwind across the C ABI.

#### Panic boundaries

Every top-level `extern "C"` export returning `ExternResult` must wrap its entire operation in
`crate::panic::catch_panic`. Only error allocator acquisition and lifetime retention belong before
the catcher. Inside the inline closure, first convert consumed handles to Rust owners with
`into_inner()`, then perform all parsing, validation, allocation, kernel/engine calls, mutation,
serialization, and operation-local cleanup. Keep individual unsafe operations explicit.

The safe catcher returns a Rust `Result`; convert it through `IntoExternResult` in a separate
`unsafe` block afterward. Reuse existing tracing and error allocation. Do not add boundary macros,
panic hooks, or abort handling.

Catching a panic does not roll back state. Discard failed work and reconcile uncertain commit
outcomes before retrying.

### Review checklist for new FFI APIs

Before approving a new boundary type or function, answer:

1. Is this data already fully materialized? If yes, why is it not a direct record or slice?
2. If a handle owns a collection, does one call expose the complete typed view instead of visiting
   each item?
3. Does a callback represent behavior, recursion, laziness, or a lifetime bracket? If not, remove
   it.
4. Does the design introduce a downcall/upcall/downcall bounce for leaf data?
5. Does the FFI preserve the Rust API's meaningful objects, stages, and validation points?
6. Is every pointer's owner and exact lifetime documented?
7. Is NULL behavior specified independently for every pointer?
8. Does `*mut` correspond to an actual write through that pointer or ownership transfer?
9. Could a Rust reference in the exported signature be replaced by a raw pointer with explicit
   validation?
10. Is a by-value struct within the two-word guideline? If not, why is value passing preferable?
11. Are large outputs returned through an out pointer, owned handle, or standard interface such as
    Arrow?
12. Can one outer error channel preserve the precise failure?
13. Is there a named release operation for every transferred allocation or handle?
14. For consumed inputs, is ownership after an error explicit?
15. Does each accessor name and document the exact semantic view it returns?
16. Does opaque encoded state cross as bytes, with compatibility checked when required?
17. Does the C header communicate the same ownership, nullability, and mutation semantics as the
    Rust documentation?
18. Does each top-level C export returning `ExternResult` follow the
    [panic boundary policy](#panic-boundaries)?

## Source layout

Prefer mirroring the `kernel/src/` module hierarchy in `ffi/src/` for corresponding APIs.
Snapshot hints live under `src/snapshot/hint/`, matching `kernel/src/snapshot/hint.rs`.
Snapshot builders live in `src/snapshot/builder.rs`; snapshot accessors live in `src/lib.rs`.
FFI-specific ownership, conversion, and engine support code can use its own modules.

## Building

```bash
cargo build -p delta_kernel_ffi --release
# Headers written to target/ffi-headers/
```

Feature flags are defined in `ffi/Cargo.toml`.

## Testing under Miri

CI runs this crate's tests under Miri (the `miri` job in `build.yml`) to catch undefined
behavior in the `unsafe` FFI boundary: raw-pointer reads/writes, `unsafe impl Send/Sync`,
`Handle` conversions (`as_ref` / `clone_as_arc` / `into_inner`), and `free_*`. Miri is a MIR
interpreter, so it runs 10x-100x slower than native and is billed by the interpreted
instruction, not wall-clock work.

The consequence: a test is expensive under Miri in proportion to how much *code it executes*,
not how much it asserts. Tests that construct heavyweight but **safe** machinery -- a
reqwest/rustls client (crypto init), a multi-threaded tokio runtime, an Arrow/parquet write --
cost minutes under the interpreter while exercising none of our `unsafe`. That time buys no
UB detection.

Guidance for adding or triaging FFI tests:

- **Keep under Miri** any test that executes `unsafe` whose correctness Miri can check. This is
  the reason the job exists; do not skip these for speed.
- **Never add new `unsafe` to a Miri-ignored test.** An ignored test is invisible to Miri, so
  `unsafe` introduced in one is never checked for undefined behavior, and nothing in CI reports
  the gap. When a change needs new `unsafe` in an ignored test, either exercise that `unsafe`
  from a test that runs under Miri, or un-skip the test.
- **`#[cfg_attr(miri, ignore)]` is legitimate for two reasons, and only these two:**
  1. Miri cannot run it (e.g. an unsupported foreign function). Before accepting this, check
     whether the blocker is avoidable: local-filesystem storage reaches `std::fs::hard_link`
     (`linkat`), which Miri rejects, but in-memory storage does not. Prefer an in-memory store.
  2. The test executes no `unsafe`, OR only `unsafe` that a kept test already covers, AND it is
     expensive under Miri. Skip only with the coverage argument; skipping for cost alone drops
     UB coverage.
- When you skip under reason 2, **prove the coverage is preserved**: the kept tests' set of
  `unsafe` FFI functions must be a superset of the skipped test's. Name the covering test in the
  `ignore` reason or a nearby comment so a future reader can re-check it.
- Miri's leak check also flags handles a test itself forgot to free. Triage before assuming a bug
  in the code under test: a missing `free_*` in the test is a test fix, while an unjoined
  background thread at teardown can be an artifact of how the test ends.
- Prefer picking the **cheapest** test that crosses a given `unsafe` path over keeping several
  that cross the same path with more safe work each.
- `rest_engine` and the checkpoint tests in `lib.rs` document worked examples of this split.
- `-Zmiri-provenance-gc=1000000` on the Miri step is a pure speed knob (GC frequency) and does
  not weaken detection. Do NOT add `-Zmiri-disable-stacked-borrows`, `-disable-validation`,
  `-disable-data-race-detector`, or `-Zmiri-preemption-rate=0`: the first three are unsound, and
  the last reduces data-race schedule exploration.
