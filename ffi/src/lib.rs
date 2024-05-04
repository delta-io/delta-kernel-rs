/// FFI interface for the delta kernel
///
/// Exposes that an engine needs to call from C/C++ to interface with kernel
use std::collections::HashMap;
use std::default::Default;
use std::os::raw::{c_char, c_void};
use std::ptr::NonNull;
use std::sync::Arc;
use tracing::debug;
use url::Url;

use delta_kernel::expressions::{BinaryOperator, Expression, Scalar};
use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, Error};

mod handle;
use handle::{ArcHandle, BoxHandle, SizedArcHandle, Unconstructable};

pub mod scan;

pub(crate) type NullableCvoid = Option<NonNull<c_void>>;

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions. The engine retains ownership of the iterator.
#[repr(C)]
pub struct EngineIterator {
    // Opaque data that will be iterated over. This data will be passed to the get_next function
    // each time a next item is requested from the iterator
    data: NonNull<c_void>,
    /// A function that should advance the iterator and return the next time from the data
    /// If the iterator is complete, it should return null. It should be safe to
    /// call `get_next()` multiple times if it returns null.
    get_next: extern "C" fn(data: NonNull<c_void>) -> *const c_void,
}

/// test function to print for items. this assumes each item is an `int`
#[no_mangle]
extern "C" fn iterate(it: &mut EngineIterator) {
    for i in it {
        let i = i as *mut i32;
        let ii = unsafe { &*i };
        println!("Got an item: {:?}", ii);
    }
}

impl Iterator for EngineIterator {
    // Todo: Figure out item type
    type Item = *const c_void;

    fn next(&mut self) -> Option<Self::Item> {
        let next_item = (self.get_next)(self.data);
        if next_item.is_null() {
            None
        } else {
            Some(next_item)
        }
    }
}

/// A non-owned slice of a UTF8 string, intended for arg-passing between kernel and engine. The
/// slice is only valid until the function it was passed into returns, and should not be copied.
///
/// # Safety
///
/// Intentionally not Copy, Clone, Send, nor Sync.
///
/// Whoever instantiates the struct must ensure it does not outlive the data it points to. The
/// compiler cannot help us here, because raw pointers don't have lifetimes. To reduce the risk of
/// accidental misuse, it is recommended to only instantiate this struct as a function arg, by
/// converting a `&str` value `Into<KernelStringSlice>`, so the borrowed reference protects the
/// function call (callee must not retain any references to the slice after the call returns):
///
/// ```
/// fn wants_slice(slice: KernelStringSlice) { ... }
/// let msg = String::from(...);
/// wants_slice(msg.as_ref().into());
/// ```
#[repr(C)]
pub struct KernelStringSlice {
    ptr: *const c_char,
    len: usize,
}

// Intentionally not From, in order to reduce risk of accidental misuse. The main use is for callers
// to pass e.g. `my_str.as_str().into()` as a function arg.
#[allow(clippy::from_over_into)]
impl Into<KernelStringSlice> for &str {
    fn into(self) -> KernelStringSlice {
        KernelStringSlice {
            ptr: self.as_ptr().cast(),
            len: self.len(),
        }
    }
}

trait TryFromStringSlice: Sized {
    unsafe fn try_from_slice(slice: KernelStringSlice) -> Self;
}

impl TryFromStringSlice for String {
    /// Converts a slice back to a string
    ///
    /// # Safety
    ///
    /// The slice must be a valid (non-null) pointer, and must point to the indicated number of
    /// valid utf8 bytes.
    unsafe fn try_from_slice(slice: KernelStringSlice) -> String {
        let slice = unsafe { std::slice::from_raw_parts(slice.ptr.cast(), slice.len) };
        std::str::from_utf8(slice).unwrap().to_string()
    }
}

/// Allow engines to allocate strings of their own type. the contract of calling a passed allocate
/// function is that `kernel_str` is _only_ valid until the return from this function
pub type AllocateStringFn = extern "C" fn(kernel_str: KernelStringSlice) -> NullableCvoid;

/// TODO
#[repr(C)]
pub struct KernelBoolSlice {
    ptr: *mut bool,
    len: usize,
}

impl BoxHandle for KernelBoolSlice {}

impl From<Vec<bool>> for KernelBoolSlice {
    fn from(val: Vec<bool>) -> Self {
        let len = val.len();
        let boxed = val.into_boxed_slice();
        let ptr = Box::into_raw(boxed).cast();
        KernelBoolSlice { ptr, len }
    }
}

trait FromBoolSlice {
    unsafe fn from_slice(slice: KernelBoolSlice) -> Self;
}

impl FromBoolSlice for Vec<bool> {
    unsafe fn from_slice(slice: KernelBoolSlice) -> Self {
        Vec::from_raw_parts(slice.ptr, slice.len, slice.len)
    }
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn drop_bool_slice(slice: *mut KernelBoolSlice) {
    let vec = unsafe { Vec::from_raw_parts((*slice).ptr, (*slice).len, (*slice).len) };
    debug!("Dropping bool slice. It is {vec:#?}");
}

#[repr(C)]
#[derive(Debug)]
pub enum KernelError {
    UnknownError, // catch-all for unrecognized kernel Error types
    FFIError,     // errors encountered in the code layer that supports FFI
    ArrowError,
    EngineDataTypeError,
    ExtractError,
    GenericError,
    IOErrorError,
    ParquetError,
    #[cfg(feature = "default-engine")]
    ObjectStoreError,
    #[cfg(feature = "default-engine")]
    ObjectStorePathError,
    #[cfg(feature = "default-engine")]
    Reqwest,
    FileNotFoundError,
    MissingColumnError,
    UnexpectedColumnTypeError,
    MissingDataError,
    MissingVersionError,
    DeletionVectorError,
    InvalidUrlError,
    MalformedJsonError,
    MissingMetadataError,
    MissingProtocolError,
    MissingMetadataAndProtocolError,
    ParseError,
    JoinFailureError,
    Utf8Error,
    ParseIntError,
}

impl From<Error> for KernelError {
    fn from(e: Error) -> Self {
        match e {
            // NOTE: By definition, no kernel Error maps to FFIError
            Error::Arrow(_) => KernelError::ArrowError,
            Error::EngineDataType(_) => KernelError::EngineDataTypeError,
            Error::Extract(..) => KernelError::ExtractError,
            Error::Generic(_) => KernelError::GenericError,
            Error::GenericError { .. } => KernelError::GenericError,
            Error::IOError(_) => KernelError::IOErrorError,
            Error::Parquet(_) => KernelError::ParquetError,
            #[cfg(feature = "default-engine")]
            Error::ObjectStore(_) => KernelError::ObjectStoreError,
            #[cfg(feature = "default-engine")]
            Error::ObjectStorePath(_) => KernelError::ObjectStorePathError,
            #[cfg(feature = "default-engine")]
            Error::Reqwest(_) => KernelError::Reqwest,
            Error::FileNotFound(_) => KernelError::FileNotFoundError,
            Error::MissingColumn(_) => KernelError::MissingColumnError,
            Error::UnexpectedColumnType(_) => KernelError::UnexpectedColumnTypeError,
            Error::MissingData(_) => KernelError::MissingDataError,
            Error::MissingVersion => KernelError::MissingVersionError,
            Error::DeletionVector(_) => KernelError::DeletionVectorError,
            Error::InvalidUrl(_) => KernelError::InvalidUrlError,
            Error::MalformedJson(_) => KernelError::MalformedJsonError,
            Error::MissingMetadata => KernelError::MissingMetadataError,
            Error::MissingProtocol => KernelError::MissingProtocolError,
            Error::MissingMetadataAndProtocol => KernelError::MissingMetadataAndProtocolError,
            Error::ParseError(..) => KernelError::ParseError,
            Error::JoinFailure(_) => KernelError::JoinFailureError,
            Error::Utf8Error(_) => KernelError::Utf8Error,
            Error::ParseIntError(_) => KernelError::ParseIntError,
            Error::Backtraced {
                source,
                backtrace: _,
            } => Self::from(*source),
        }
    }
}

/// An error that can be returned to the engine. Engines that wish to associate additional
/// information can define and use any type that is [pointer
/// interconvertible](https://en.cppreference.com/w/cpp/language/static_cast#pointer-interconvertible)
/// with this one -- e.g. by subclassing this struct or by embedding this struct as the first member
/// of a [standard layout](https://en.cppreference.com/w/cpp/language/data_members#Standard-layout)
/// class.
#[repr(C)]
pub struct EngineError {
    etype: KernelError,
}

/// Semantics: Kernel will always immediately return the leaked engine error to the engine (if it
/// allocated one at all), and engine is responsible for freeing it.
#[repr(C)]
pub enum ExternResult<T> {
    Ok(T),
    Err(*mut EngineError),
}

pub type AllocateErrorFn =
    extern "C" fn(etype: KernelError, msg: KernelStringSlice) -> *mut EngineError;

// NOTE: We can't "just" impl From<DeltaResult<T>> because we require an error allocator.
impl<T> ExternResult<T> {
    pub fn is_ok(&self) -> bool {
        match self {
            Self::Ok(_) => true,
            Self::Err(_) => false,
        }
    }
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }
}

/// Represents an engine error allocator. Ultimately all implementations will fall back to an
/// [`AllocateErrorFn`] provided by the engine, but the trait allows us to conveniently access the
/// allocator in various types that may wrap it.
pub trait AllocateError {
    /// Allocates a new error in engine memory and returns the resulting pointer. The engine is
    /// expected to copy the passed-in message, which is only guaranteed to remain valid until the
    /// call returns. Kernel will always immediately return the result of this method to the engine.
    ///
    /// # Safety
    ///
    /// The string slice must be valid until the call returns, and the error allocator must also be
    /// valid.
    unsafe fn allocate_error(&self, etype: KernelError, msg: KernelStringSlice)
        -> *mut EngineError;
}

// TODO: Why is this even needed...
impl AllocateError for &dyn AllocateError {
    unsafe fn allocate_error(
        &self,
        etype: KernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError {
        (*self).allocate_error(etype, msg)
    }
}

impl AllocateError for AllocateErrorFn {
    unsafe fn allocate_error(
        &self,
        etype: KernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError {
        self(etype, msg)
    }
}
impl AllocateError for *const ExternEngineHandle {
    /// # Safety
    ///
    /// In addition to the usual requirements, the table client handle must be valid.
    unsafe fn allocate_error(
        &self,
        etype: KernelError,
        msg: KernelStringSlice,
    ) -> *mut EngineError {
        ArcHandle::clone_as_arc(*self)
            .error_allocator()
            .allocate_error(etype, msg)
    }
}

/// Converts a [DeltaResult] into an [ExternResult], using the engine's error allocator.
///
/// # Safety
///
/// The allocator must be valid.
trait IntoExternResult<T> {
    unsafe fn into_extern_result(self, allocate_error: impl AllocateError) -> ExternResult<T>;
}

impl<T> IntoExternResult<T> for DeltaResult<T> {
    unsafe fn into_extern_result(self, allocate_error: impl AllocateError) -> ExternResult<T> {
        match self {
            Ok(ok) => ExternResult::Ok(ok),
            Err(err) => {
                let msg = format!("{}", err);
                let err = unsafe { allocate_error.allocate_error(err.into(), msg.as_str().into()) };
                ExternResult::Err(err)
            }
        }
    }
}

// A wrapper for Engine which defines additional FFI-specific methods.
pub trait ExternEngine {
    fn engine(&self) -> Arc<dyn Engine>;
    fn error_allocator(&self) -> &dyn AllocateError;
}

pub struct ExternEngineHandle {
    _unconstructable: Unconstructable,
}

impl ArcHandle for ExternEngineHandle {
    type Target = dyn ExternEngine;
}

struct ExternEngineVtable {
    // Actual table client instance to use
    client: Arc<dyn Engine>,
    allocate_error: AllocateErrorFn,
}

/// # Safety
///
/// Kernel doesn't use any threading or concurrency. If engine chooses to do so, engine is
/// responsible for handling  any races that could result.
unsafe impl Send for ExternEngineVtable {}

/// # Safety
///
/// Kernel doesn't use any threading or concurrency. If engine chooses to do so, engine is
/// responsible for handling any races that could result.
///
/// These are needed because anything wrapped in Arc "should" implement it
/// Basically, by failing to implement these traits, we forbid the engine from being able to declare
/// its thread-safety (because rust assumes it is not threadsafe). By implementing them, we leave it
/// up to the engine to enforce thread safety if engine chooses to use threads at all.
unsafe impl Sync for ExternEngineVtable {}

impl ExternEngine for ExternEngineVtable {
    fn engine(&self) -> Arc<dyn Engine> {
        self.client.clone()
    }
    fn error_allocator(&self) -> &dyn AllocateError {
        &self.allocate_error
    }
}

/// # Safety
///
/// Caller is responsible for passing a valid path pointer.
unsafe fn unwrap_and_parse_path_as_url(path: KernelStringSlice) -> DeltaResult<Url> {
    let path = unsafe { String::try_from_slice(path) };
    Ok(Url::parse(&path)?)
}

// A client builder allows setting options before building an actual client
pub struct EngineBuilder {
    url: Url,
    allocate_fn: AllocateErrorFn,
    options: HashMap<String, String>,
}

impl EngineBuilder {
    fn set_option(&mut self, key: String, val: String) {
        self.options.insert(key, val);
    }
}

/// Get a "builder" that can be used to construct an engine client. The function
/// [`set_builder_option`] can be used to set options on the builder prior to constructing the
/// actual client
///
/// # Safety
/// Caller is responsible for passing a valid path pointer.
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn get_engine_builder(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*mut EngineBuilder> {
    get_engine_builder_impl(path, allocate_error).into_extern_result(allocate_error)
}

#[cfg(feature = "default-engine")]
unsafe fn get_engine_builder_impl(
    path: KernelStringSlice,
    allocate_fn: AllocateErrorFn,
) -> DeltaResult<*mut EngineBuilder> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) }?;
    let builder = Box::new(EngineBuilder {
        url,
        allocate_fn,
        options: HashMap::default(),
    });
    Ok(Box::into_raw(builder))
}

/// Set an option on the builder
///
/// # Safety
///
/// Client must pass a valid ClientBuilder pointer, and valid slices for key and value
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn set_builder_option(
    builder: &mut EngineBuilder,
    key: KernelStringSlice,
    value: KernelStringSlice,
) {
    let key = unsafe { String::try_from_slice(key) };
    let value = unsafe { String::try_from_slice(value) };
    builder.set_option(key, value);
}

/// Consume the builder and return an engine. After calling, the passed pointer is _no
/// longer valid_.
///
/// # Safety
///
/// Caller is responsible to pass a valid ClientBuilder pointer, and to not use it again afterwards
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn builder_build(
    builder: *mut EngineBuilder,
) -> ExternResult<*const ExternEngineHandle> {
    let builder_box = unsafe { Box::from_raw(builder) };
    get_default_client_impl(
        builder_box.url,
        builder_box.options,
        builder_box.allocate_fn,
    )
    .into_extern_result(builder_box.allocate_fn)
}

/// # Safety
///
/// Caller is responsible for passing a valid path pointer.
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn get_default_client(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*const ExternEngineHandle> {
    get_default_default_client_impl(path, allocate_error).into_extern_result(allocate_error)
}

// get the default version of the default client :)
#[cfg(feature = "default-engine")]
unsafe fn get_default_default_client_impl(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<*const ExternEngineHandle> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) }?;
    get_default_client_impl(url, Default::default(), allocate_error)
}

#[cfg(feature = "default-engine")]
unsafe fn get_default_client_impl(
    url: Url,
    options: HashMap<String, String>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<*const ExternEngineHandle> {
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    let engine = DefaultEngine::<TokioBackgroundExecutor>::try_new(
        &url,
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let client = Arc::new(engine.map_err(Error::generic)?);
    let client: Arc<dyn ExternEngine> = Arc::new(ExternEngineVtable {
        client,
        allocate_error,
    });
    Ok(ArcHandle::into_handle(client))
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn drop_engine(engine: *const ExternEngineHandle) {
    ArcHandle::drop_handle(engine);
}

pub struct SnapshotHandle {
    _unconstructable: Unconstructable,
}

impl SizedArcHandle for SnapshotHandle {
    type Target = Snapshot;
}

/// Get the latest snapshot from the specified table
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
#[no_mangle]
pub unsafe extern "C" fn snapshot(
    path: KernelStringSlice,
    engine: *const ExternEngineHandle,
) -> ExternResult<*const SnapshotHandle> {
    snapshot_impl(path, engine).into_extern_result(engine)
}

unsafe fn snapshot_impl(
    path: KernelStringSlice,
    extern_engine: *const ExternEngineHandle,
) -> DeltaResult<*const SnapshotHandle> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) }?;
    let extern_engine = unsafe { ArcHandle::clone_as_arc(extern_engine) };
    let snapshot = Snapshot::try_new(url, extern_engine.engine().as_ref(), None)?;
    Ok(ArcHandle::into_handle(snapshot))
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn drop_snapshot(snapshot: *const SnapshotHandle) {
    ArcHandle::drop_handle(snapshot);
}

/// Get the version of the specified snapshot
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn version(snapshot: *const SnapshotHandle) -> u64 {
    let snapshot = unsafe { ArcHandle::clone_as_arc(snapshot) };
    snapshot.version()
}

// WARNING: the visitor MUST NOT retain internal references to the string slices passed to visitor methods
// TODO: other types, nullability
#[repr(C)]
pub struct EngineSchemaVisitor {
    // opaque state pointer
    data: *mut c_void,
    // Creates a new field list, optionally reserving capacity up front
    make_field_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,
    // visitor methods that should instantiate and append the appropriate type to the field list
    visit_struct: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        child_list_id: usize,
    ),
    visit_string: extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
    visit_integer:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
    visit_long: extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: KernelStringSlice),
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn visit_schema(
    snapshot: *const SnapshotHandle,
    visitor: &mut EngineSchemaVisitor,
) -> usize {
    let snapshot = unsafe { ArcHandle::clone_as_arc(snapshot) };
    // Visit all the fields of a struct and return the list of children
    fn visit_struct_fields(visitor: &EngineSchemaVisitor, s: &StructType) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, s.fields.len());
        for field in s.fields() {
            visit_field(visitor, child_list_id, field);
        }
        child_list_id
    }

    // Visit a struct field (recursively) and add the result to the list of siblings.
    fn visit_field(visitor: &EngineSchemaVisitor, sibling_list_id: usize, field: &StructField) {
        let name: &str = field.name.as_ref();
        match &field.data_type {
            DataType::Primitive(PrimitiveType::Integer) => {
                (visitor.visit_integer)(visitor.data, sibling_list_id, name.into())
            }
            DataType::Primitive(PrimitiveType::Long) => {
                (visitor.visit_long)(visitor.data, sibling_list_id, name.into())
            }
            DataType::Primitive(PrimitiveType::String) => {
                (visitor.visit_string)(visitor.data, sibling_list_id, name.into())
            }
            DataType::Struct(s) => {
                let child_list_id = visit_struct_fields(visitor, s);
                (visitor.visit_struct)(visitor.data, sibling_list_id, name.into(), child_list_id);
            }
            other => println!("Unsupported data type: {}", other),
        }
    }

    visit_struct_fields(visitor, snapshot.schema())
}

// A set that can identify its contents by address
pub struct ReferenceSet<T> {
    map: std::collections::HashMap<usize, T>,
    next_id: usize,
}

impl<T> ReferenceSet<T> {
    pub fn new() -> Self {
        Default::default()
    }

    // Inserts a new value into the set. This always creates a new entry
    // because the new value cannot have the same address as any existing value.
    // Returns a raw pointer to the value. This pointer serves as a key that
    // can be used later to take() from the set, and should NOT be dereferenced.
    pub fn insert(&mut self, value: T) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.map.insert(id, value);
        id
    }

    // Attempts to remove a value from the set, if present.
    pub fn take(&mut self, i: usize) -> Option<T> {
        self.map.remove(&i)
    }

    // True if the set contains an object whose address matches the pointer.
    pub fn contains(&self, id: usize) -> bool {
        self.map.contains_key(&id)
    }

    // The current size of the set.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<T> Default for ReferenceSet<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            // NOTE: 0 is interpreted as None
            next_id: 1,
        }
    }
}

#[derive(Default)]
pub struct KernelExpressionVisitorState {
    // TODO: ReferenceSet<Box<dyn MetadataFilterFn>> instead?
    inflight_expressions: ReferenceSet<Expression>,
}
impl KernelExpressionVisitorState {
    fn new() -> Self {
        Self {
            inflight_expressions: Default::default(),
        }
    }
}

// When invoking [[get_scan_files]], The engine provides a pointer to the (engine's native)
// predicate, along with a visitor function that can be invoked to recursively visit the
// predicate. This engine state is valid until the call to [[get_scan_files]] returns. Inside that
// method, the kernel allocates visitor state, which becomes the second argument to the predicate
// visitor invocation along with the engine-provided predicate pointer. The visitor state is valid
// for the lifetime of the predicate visitor invocation. Thanks to this double indirection, engine
// and kernel each retain ownership of their respective objects, with no need to coordinate memory
// lifetimes with the other.
#[repr(C)]
pub struct EnginePredicate {
    predicate: *mut c_void,
    visitor:
        extern "C" fn(predicate: *mut c_void, state: &mut KernelExpressionVisitorState) -> usize,
}

fn wrap_expression(state: &mut KernelExpressionVisitorState, expr: Expression) -> usize {
    state.inflight_expressions.insert(expr)
}

fn unwrap_kernel_expression(
    state: &mut KernelExpressionVisitorState,
    exprid: usize,
) -> Option<Expression> {
    state.inflight_expressions.take(exprid)
}

// TODO move visitors to separate module
fn visit_expression_binary(
    state: &mut KernelExpressionVisitorState,
    op: BinaryOperator,
    a: usize,
    b: usize,
) -> usize {
    let left = unwrap_kernel_expression(state, a).map(Box::new);
    let right = unwrap_kernel_expression(state, b).map(Box::new);
    match left.zip(right) {
        Some((left, right)) => {
            wrap_expression(state, Expression::BinaryOperation { op, left, right })
        }
        None => 0, // invalid child => invalid node
    }
}

// The EngineIterator is not thread safe, not reentrant, not owned by callee, not freed by callee.
#[no_mangle]
pub extern "C" fn visit_expression_and(
    state: &mut KernelExpressionVisitorState,
    children: &mut EngineIterator,
) -> usize {
    let result = Expression::and_from(
        children.flat_map(|child| unwrap_kernel_expression(state, child as usize)),
    );
    wrap_expression(state, result)
}

#[no_mangle]
pub extern "C" fn visit_expression_lt(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::LessThan, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_le(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::LessThanOrEqual, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_gt(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::GreaterThan, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_ge(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::GreaterThanOrEqual, a, b)
}

#[no_mangle]
pub extern "C" fn visit_expression_eq(
    state: &mut KernelExpressionVisitorState,
    a: usize,
    b: usize,
) -> usize {
    visit_expression_binary(state, BinaryOperator::Equal, a, b)
}

/// # Safety
/// The string slice must be valid
#[no_mangle]
pub unsafe extern "C" fn visit_expression_column(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    visit_expression_column_impl(state, name).into_extern_result(allocate_error)
}
unsafe fn visit_expression_column_impl(
    state: &mut KernelExpressionVisitorState,
    name: KernelStringSlice,
) -> DeltaResult<usize> {
    let name = unsafe { String::try_from_slice(name) };
    Ok(wrap_expression(state, Expression::Column(name)))
}

/// # Safety
/// The string slice must be valid
#[no_mangle]
pub unsafe extern "C" fn visit_expression_literal_string(
    state: &mut KernelExpressionVisitorState,
    value: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<usize> {
    visit_expression_literal_string_impl(state, value).into_extern_result(allocate_error)
}
unsafe fn visit_expression_literal_string_impl(
    state: &mut KernelExpressionVisitorState,
    value: KernelStringSlice,
) -> DeltaResult<usize> {
    let value = unsafe { String::try_from_slice(value) };
    Ok(wrap_expression(
        state,
        Expression::Literal(Scalar::from(value)),
    ))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_long(
    state: &mut KernelExpressionVisitorState,
    value: i64,
) -> usize {
    wrap_expression(state, Expression::Literal(Scalar::from(value)))
}
