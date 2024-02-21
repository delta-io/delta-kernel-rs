/// FFI interface for the delta kernel
///
/// Exposes that an engine needs to call from C/C++ to interface with kernel
use std::collections::HashMap;
use std::default::Default;
use std::ffi::{CStr, CString};
use std::fmt::{Display, Formatter};
use std::os::raw::{c_char, c_void};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use url::Url;

use deltakernel::expressions::{BinaryOperator, Expression, Scalar};
use deltakernel::scan::ScanBuilder;
use deltakernel::schema::{DataType, PrimitiveType, StructField, StructType};
use deltakernel::snapshot::Snapshot;
use deltakernel::{DeltaResult, Error, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler, TableClient};

mod handle;
use handle::{ArcHandle, SizedArcHandle, Uncreate};

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions. The engine retains ownership of the iterator.
#[repr(C)]
pub struct EngineIterator {
    // Opaque data that will be iterated over. This data will be passed to the get_next function
    // each time a next item is requested from the iterator
    data: *mut c_void,
    /// A function that should advance the iterator and return the next time from the data
    /// If the iterator is complete, it should return null. It should be safe to
    /// call `get_next()` multiple times if it is null.
    get_next: extern "C" fn(data: *mut c_void) -> *const c_void,
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

#[repr(C)]
pub enum KernelError {
    UnknownError, // catch-all for unrecognized kernel Error types
    FFIError, // errors encountered in the code layer that supports FFI
    ArrowError,
    GenericError,
    ParquetError,
    ObjectStoreError,
    FileNotFoundError,
    MissingColumnError,
    UnexpectedColumnTypeError,
    MissingDataError,
    MissingVersionError,
    DeletionVectorError,
    InvalidUrlError,
    MalformedJsonError,
    MissingMetadataError,
}

impl Display for KernelError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KernelError::UnknownError => write!(f, "UnknownError"),
            KernelError::FFIError => write!(f, "FFIError"),
            KernelError::ArrowError => write!(f, "ArrowError"),
            KernelError::GenericError => write!(f, "GenericError"),
            KernelError::ParquetError => write!(f, "ParquetError"),
            KernelError::ObjectStoreError => write!(f, "ObjectStoreError"),
            KernelError::FileNotFoundError => write!(f, "FileNotFoundError"),
            KernelError::MissingColumnError => write!(f, "MissingColumnError"),
            KernelError::UnexpectedColumnTypeError => write!(f, "UnexpectedColumnTypeError"),
            KernelError::MissingDataError => write!(f, "MissingDataError"),
            KernelError::MissingVersionError => write!(f, "MissingVersionError"),
            KernelError::DeletionVectorError => write!(f, "DeletionVectorError"),
            KernelError::InvalidUrlError => write!(f, "InvalidUrlError"),
            KernelError::MalformedJsonError => write!(f, "MalformedJsonError"),
            KernelError::MissingMetadataError => write!(f, "MissingMetadataError"),
        }
    }
}

impl From<Error> for KernelError {
    fn from(e: Error) -> Self {
        match e {
            // NOTE: By definition, no kernel Error maps to FFIError
            Error::Arrow(_) => KernelError::ArrowError,
            Error::Generic(_) => KernelError::GenericError,
            Error::GenericError { .. } => KernelError::GenericError,
            Error::Parquet(_) => KernelError::ParquetError,
            Error::ObjectStore(_) => KernelError::ObjectStoreError,
            Error::FileNotFound(_) => KernelError::FileNotFoundError,
            Error::MissingColumn(_) => KernelError::MissingColumnError,
            Error::UnexpectedColumnType(_) => KernelError::UnexpectedColumnTypeError,
            Error::MissingData(_) => KernelError::MissingDataError,
            Error::MissingVersion => KernelError::MissingVersionError,
            Error::DeletionVector(_) => KernelError::DeletionVectorError,
            Error::InvalidUrl(_) => KernelError::InvalidUrlError,
            Error::MalformedJson(_) => KernelError::MalformedJsonError,
            Error::MissingMetadata => KernelError::MissingMetadataError,
        }
    }
}

/// An error that can be returned to the engine. Engines can define additional struct fields on
/// their side, by e.g. embedding this struct as the first member of a larger struct.
#[repr(C)]
pub struct EngineError {
    etype: KernelError,
}

#[repr(C)]
pub enum ExternResult<T> {
    Ok(T),
    Err(*mut EngineError),
}

type AllocateErrorFn = extern "C" fn(
    etype: KernelError,
    msg_ptr: *const c_char,
    msg_len: usize,
) -> *mut EngineError;

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


trait IntoExternResult<T> {
    fn into_extern_result(
        self,
        allocate_error: AllocateErrorFn,
    ) -> ExternResult<T>;
}

impl<T> IntoExternResult<T> for DeltaResult<T> {
    fn into_extern_result(
        self,
        allocate_error: AllocateErrorFn,
    ) -> ExternResult<T> {
        match self {
            Ok(ok) => ExternResult::Ok(ok),
            Err(err) => {
                let msg = format!("{}", err);
                let err = allocate_error(err.into(), msg.as_ptr().cast(), msg.len());
                ExternResult::Err(err)
            }
        }
    }
}

// An extended version of TableClient which defines additional FFI-specific methods.
pub trait ExternTableClient {
    /// The underlying table client instance to use
    fn table_client(&self) -> Arc<dyn TableClient>;

    /// Allocates a new error in engine memory and returns the resulting pointer. The engine is
    /// expected to copy the passed-in message, which is only guaranteed to remain valid until the
    /// call returns. Kernel will always immediately return the result of this method to the engine.
    fn allocate_error(&self, error: Error) -> *mut EngineError;
}

pub struct ExternTableClientHandle {
    _uncreate: Uncreate,
}

impl ArcHandle for ExternTableClientHandle {
    type Target = dyn ExternTableClient;
}

struct ExternTableClientVtable {
    // Actual table client instance to use
    client: Arc<dyn TableClient>,
    allocate_error: AllocateErrorFn,
}

impl ExternTableClient for ExternTableClientVtable {
    fn table_client(&self) -> Arc<dyn TableClient> {
        self.client.clone()
    }
    fn allocate_error(&self, error: Error) -> *mut EngineError {
        let msg = format!("{}", error);
        (self.allocate_error)(error.into(), msg.as_ptr().cast(), msg.len())
    }
}

/// # Safety
///
/// Caller is responsible to pass a valid path pointer.
unsafe fn unwrap_and_parse_path_as_url(path: *const c_char) -> DeltaResult<Url> {
    let path = unsafe { CStr::from_ptr(path) };
    let path = path.to_str().map_err(Error::generic)?;
    let path = std::fs::canonicalize(PathBuf::from(path)).map_err(Error::generic)?;
    Url::from_directory_path(path).map_err(|_| Error::generic("Invalid url"))
}

#[no_mangle]
pub unsafe extern "C" fn get_default_client(
    path: *const c_char,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*const ExternTableClientHandle> {
    get_default_client_impl(path, allocate_error).into_extern_result(allocate_error)
}

unsafe fn get_default_client_impl(
    path: *const c_char,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<*const ExternTableClientHandle> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) }?;
    use deltakernel::client::executor::tokio::TokioBackgroundExecutor;
    let table_client = deltakernel::DefaultTableClient::<TokioBackgroundExecutor>::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let client = Arc::new(table_client.map_err(Error::generic)?);
    let client: Arc<dyn ExternTableClient> = Arc::new(ExternTableClientVtable {
        client,
        allocate_error,
    });
    Ok(ArcHandle::into_handle(client))
}

#[no_mangle]
pub unsafe extern "C" fn drop_table_client(table_client: *const ExternTableClientHandle) {
    ArcHandle::drop_handle(table_client);
}

pub struct SnapshotHandle {
    _uncreate: Uncreate,
}

impl SizedArcHandle for SnapshotHandle {
    type Target = Snapshot;
}

/// Get the latest snapshot from the specified table
#[no_mangle]
pub unsafe extern "C" fn snapshot(
    path: *const c_char,
    table_client: *const ExternTableClientHandle,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*const SnapshotHandle> {
    snapshot_impl(path, table_client).into_extern_result(allocate_error)
}

unsafe fn snapshot_impl(
    path: *const c_char,
    table_client: *const ExternTableClientHandle,
) -> DeltaResult<*const SnapshotHandle> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) }?;
    let table_client = unsafe { ArcHandle::clone_as_arc(table_client) };
    let snapshot = Snapshot::try_new(url, table_client.table_client().as_ref(), None)?;
    Ok(ArcHandle::into_handle(snapshot))
}

#[no_mangle]
pub unsafe extern "C" fn drop_snapshot(snapshot: *const SnapshotHandle) {
    ArcHandle::drop_handle(snapshot);
}

/// Get the version of the specified snapshot
#[no_mangle]
pub unsafe extern "C" fn version(snapshot: *const SnapshotHandle) -> u64 {
    let snapshot = unsafe { ArcHandle::clone_as_arc(snapshot) };
    snapshot.version()
}

// WARNING: the visitor MUST NOT retain internal references to the c_char names passed to visitor methods
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
        name: *const c_char,
        child_list_id: usize,
    ) -> (),
    visit_string:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: *const c_char) -> (),
    visit_integer:
        extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: *const c_char) -> (),
    visit_long: extern "C" fn(data: *mut c_void, sibling_list_id: usize, name: *const c_char) -> (),
}

#[no_mangle]
pub extern "C" fn visit_schema(snapshot: *const SnapshotHandle, visitor: &mut EngineSchemaVisitor) -> usize {
    let snapshot = unsafe { ArcHandle::clone_as_arc(snapshot) };
    // Visit all the fields of a struct and return the list of children
    fn visit_struct_fields(visitor: &EngineSchemaVisitor, s: &StructType) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, s.fields.len());
        for field in s.fields.iter() {
            visit_field(visitor, child_list_id, field);
        }
        child_list_id
    }

    // Visit a struct field (recursively) and add the result to the list of siblings.
    fn visit_field(visitor: &EngineSchemaVisitor, sibling_list_id: usize, field: &StructField) {
        let name = CString::new(field.name.as_bytes()).unwrap();
        match &field.data_type {
            DataType::Primitive(PrimitiveType::Integer) => {
                (visitor.visit_integer)(visitor.data, sibling_list_id, name.as_ptr())
            }
            DataType::Primitive(PrimitiveType::Long) => {
                (visitor.visit_long)(visitor.data, sibling_list_id, name.as_ptr())
            }
            DataType::Primitive(PrimitiveType::String) => {
                (visitor.visit_string)(visitor.data, sibling_list_id, name.as_ptr())
            }
            DataType::Struct(s) => {
                let child_list_id = visit_struct_fields(visitor, s);
                (visitor.visit_struct)(visitor.data, sibling_list_id, name.as_ptr(), child_list_id);
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

fn unwrap_c_string(s: *const c_char) -> String {
    let s = unsafe { CStr::from_ptr(s) };
    s.to_str().unwrap().to_string()
}

fn unwrap_kernel_expression(
    state: &mut KernelExpressionVisitorState,
    exprid: usize,
) -> Option<Expression> {
    state.inflight_expressions.take(exprid)
}

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

#[no_mangle]
pub extern "C" fn visit_expression_column(
    state: &mut KernelExpressionVisitorState,
    name: *const c_char,
) -> usize {
    wrap_expression(state, Expression::Column(unwrap_c_string(name)))
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_string(
    state: &mut KernelExpressionVisitorState,
    value: *const c_char,
) -> usize {
    wrap_expression(
        state,
        Expression::Literal(Scalar::from(unwrap_c_string(value))),
    )
}

#[no_mangle]
pub extern "C" fn visit_expression_literal_long(
    state: &mut KernelExpressionVisitorState,
    value: i64,
) -> usize {
    wrap_expression(state, Expression::Literal(Scalar::from(value)))
}

pub struct KernelScanFileIterator {
    // Box -> Wrap its unsized content this struct is fixed-size with thin pointers.
    // Mutex -> We need to protect the iterator against multi-threaded engine access.
    // Item = String -> Owned items because rust can't correctly express lifetimes for borrowed items
    // (we would need a way to assert that item lifetimes are bounded by the iterator's lifetime).
    files: Box<Mutex<dyn Iterator<Item = String>>>,

    // The iterator has an internal borrowed reference to the Snapshot it came from. Rust can't
    // track that across the FFI boundary, so it's up to us to keep the Snapshot alive.
    _snapshot: Arc<Snapshot>,

    // The iterator also has an internal borrowed reference to the table client it came from.
    _table_client: Arc<dyn TableClient>,
}

impl Drop for KernelScanFileIterator {
    fn drop(&mut self) {
        println!("dropping KernelScanFileIterator");
    }
}

/// Get a FileList for all the files that need to be read from the table.
/// # Safety
///
/// Caller is responsible to pass a valid snapshot pointer.
#[no_mangle]
pub extern "C" fn kernel_scan_files_init(
    snapshot: *const SnapshotHandle,
    table_client: *const ExternTableClientHandle,
    predicate: Option<&mut EnginePredicate>,
) -> *mut KernelScanFileIterator {
    let snapshot = unsafe { ArcHandle::clone_as_arc(snapshot) };
    let table_client = unsafe { ArcHandle::clone_as_arc(table_client) };
    let mut scan_builder = ScanBuilder::new(snapshot.clone());
    if let Some(predicate) = predicate {
        // TODO: There is a lot of redundancy between the various visit_expression_XXX methods here,
        // vs. ProvidesMetadataFilter trait and the class hierarchy that supports it. Can we justify
        // combining the two, so that native rust kernel code also uses the visitor idiom? Doing so
        // might mean kernel no longer needs to define an expression class hierarchy of its own (at
        // least, not for data skipping). Things may also look different after we remove arrow code
        // from the kernel proper and make it one of the sensible default engine clients instead.
        let mut visitor_state = KernelExpressionVisitorState::new();
        let exprid = (predicate.visitor)(predicate.predicate, &mut visitor_state);
        if let Some(predicate) = unwrap_kernel_expression(&mut visitor_state, exprid) {
            println!("Got predicate: {}", predicate);
            scan_builder = scan_builder.with_predicate(predicate);
        }
    }
    let real_table_client = table_client.table_client();
    let scan_adds = scan_builder.build().files(real_table_client.as_ref()).unwrap();
    let files = scan_adds.map(|add| add.unwrap().path);
    let files = KernelScanFileIterator {
        files: Box::new(Mutex::new(files)),
        _snapshot: snapshot,
        _table_client: real_table_client,
    };
    Box::into_raw(Box::new(files))
}

#[no_mangle]
pub extern "C" fn kernel_scan_files_next(
    files: &mut KernelScanFileIterator,
    engine_context: *mut c_void,
    engine_visitor: extern "C" fn(engine_context: *mut c_void, ptr: *const c_char, len: usize),
) {
    let mut files = files.files.lock().unwrap();
    if let Some(file) = files.next() {
        println!("Got file: {}", file);
        (engine_visitor)(engine_context, file.as_ptr().cast(), file.len());
    }
}

#[no_mangle]
pub extern "C" fn kernel_scan_files_free(files: *mut KernelScanFileIterator) {
    let _ = unsafe { Box::from_raw(files) };
}
