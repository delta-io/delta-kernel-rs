/// FFI interface for the delta kernel
///
/// Exposes that an engine needs to call from C/C++ to interface with kernel
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

use deltakernel::expressions::{scalars::Scalar, BinaryOperator, Expression};
use deltakernel::scan::ScanBuilder;
use deltakernel::schema::{DataType, PrimitiveType, StructField, StructType};
use deltakernel::snapshot::Snapshot;

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

// TODO: We REALLY don't want to expose the real type of default client. Doing so forces every kernel
// API call to take either a default client or a generic client, because those are incompatible types.
use deltakernel::client::executor::tokio::TokioBackgroundExecutor;
type KernelDefaultTableClient = deltakernel::DefaultTableClient<TokioBackgroundExecutor>;

/// # Safety
///
/// Caller is responsible to pass a valid path pointer.
unsafe fn unwrap_and_parse_path_as_url(path: *const c_char) -> Option<Url> {
    let path = unsafe { CStr::from_ptr(path) };
    let path = path.to_str().unwrap();
    let path = std::fs::canonicalize(PathBuf::from(path));
    let Ok(path) = path else {
        println!("Couldn't open table: {}", path.err().unwrap());
        return None;
    };
    let Ok(url) = Url::from_directory_path(path) else {
        println!("Invalid url");
        return None;
    };
    Some(url)
}

#[no_mangle]
pub unsafe extern "C" fn get_default_client(
    path: *const c_char,
) -> *const KernelDefaultTableClient {
    let path = unsafe { unwrap_and_parse_path_as_url(path) };
    let Some(url) = path else {
        return std::ptr::null_mut();
    };
    let table_client = KernelDefaultTableClient::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let Ok(table_client) = table_client else {
        println!(
            "Error creating table client: {}",
            table_client.err().unwrap()
        );
        return std::ptr::null_mut();
    };
    Arc::into_raw(Arc::new(table_client))
}

/// Get the latest snapshot from the specified table
#[no_mangle]
pub unsafe extern "C" fn snapshot(
    path: *const c_char,
    table_client: &KernelDefaultTableClient,
) -> *const Snapshot {
    let path = unsafe { unwrap_and_parse_path_as_url(path) };
    let Some(url) = path else {
        return std::ptr::null_mut();
    };
    let snapshot = Snapshot::try_new(url, table_client, None).unwrap();
    Arc::into_raw(snapshot)
}

/// Get the version of the specified snapshot
#[no_mangle]
pub extern "C" fn version(snapshot: &Snapshot) -> u64 {
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
pub extern "C" fn visit_schema(
    snapshot: &Snapshot,
    visitor: &mut EngineSchemaVisitor,
) -> usize {
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

    visit_struct_fields(visitor, &snapshot.schema())
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
        children.flat_map(|child| unwrap_kernel_expression(state, child as usize))
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

#[repr(C)]
pub struct FileList {
    files: *mut *mut c_char,
    file_count: i32,
}

/// Get a FileList for all the files that need to be read from the table. NB: This _consumes_ the
/// snapshot, it is no longer valid after making this call (TODO: We should probably fix this?)
///
/// # Safety
///
/// Caller is responsible to pass a valid snapshot pointer.
#[no_mangle]
pub unsafe extern "C" fn get_scan_files(
    snapshot: *const Snapshot,
    table_client: *const KernelDefaultTableClient,
    predicate: Option<&mut EnginePredicate>,
) -> FileList {
    let snapshot: Arc<Snapshot> = unsafe { Arc::from_raw(snapshot) };
    let table_client = unsafe { Arc::from_raw(table_client) };
    let mut scan_builder = ScanBuilder::new(snapshot);
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
    let scan_adds = scan_builder
        .build()
        .files(table_client.as_ref())
        .unwrap();
    let mut file_count = 0;
    let mut files: Vec<*mut i8> = scan_adds
        .into_iter()
        .map(|add| {
            file_count += 1;
            CString::new(add.unwrap().path).unwrap().into_raw()
        })
        .collect();
    let ptr = files.as_mut_ptr();
    std::mem::forget(files);
    println!("{} files survived pruning", file_count);
    FileList {
        files: ptr,
        file_count,
    }
}
