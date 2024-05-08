//! A set of traits and types to help safely and ergonically pass rust objects across the FFI
//! boundary as thin pointer "handles". A handle is an opaque type that aids type safety by uniquely
//! identifying some Rust type that may not even be representable (such as traits). There are three
//! kinds of handles:
//!
//! * [BoxHandle] represents the content of a `Box<T>`, where `T` is a sized type.
//! * [ArcHandle] represents the content of an `Arc<T>`, where `T` may not be sized.
//! * [SizedArcHandle] specializes [ArcHandle] to handle sized types efficiently.
//!
//! Box handles are useful for passing owned sized data across the FFI boundary, while Arc handles
//! are more suitable for shared and/or unsized data.
use std::sync::Arc;

/// Helper trait that simplifies passing an instance of a Sized type across the FFI boundary as a
/// leaked thin pointer. Does not include any reference-counting capability, so engine is
/// responsible to do whatever reference counting the engine may need. Engine is also responsible
/// not to drop an in-use handle, so kernel code can safely dereference the pointer.
pub trait BoxHandle: Sized + Send + Sync {
    fn into_handle(self) -> *mut Self {
        Box::into_raw(Box::new(self))
    }
    /// # Safety
    ///
    /// The `handle` was returned by a call to `into_handle`, has not already been passed to
    /// `drop_handle`, and has no other live references.
    unsafe fn drop_handle(handle: *mut Self) {
        let _ = unsafe { Box::from_raw(handle) };
    }
}

mod unconstructable {
    /// A struct that cannot be instantiated by any rust code because this module exposes no public
    /// constructor for it. Intentionally _NOT_ a zero-sized type, to avoid weirdness in C/C++ land.
    pub struct Unconstructable {
        _private: usize,
    }
}

// Make it easy to use (the module was only used to enforce member privacy).
pub type Unconstructable = unconstructable::Unconstructable;

/// Helper trait that allows passing `Arc<Target>` across the FFI boundary as a thin-pointer handle
/// type. The pointer remains valid until freed by a call to [ArcHandle::drop_handle]. The handle is
/// strongly typed, in the sense that each handle type maps to a single `Target` type.
///
/// Typically, the handle (`Self`) is an opaque struct (_not_ `repr(C)`) with an FFI-friendly name
/// name, containing an [Unconstructable] member so rust code cannot legally instantiate it.
///
/// # Examples
///
/// To define and use a handle for a trait or other unsized type:
/// ```
/// // The (unsized) trait to pass across the FFI boundary
/// trait MyTrait { ... }
///
/// // The handle that will represent `MyStruct` externally
/// struct MyTraitHandle {
///     _unconstructable: Unconstructable,
/// }
///
/// // Connect the two
/// impl ArcHandle for MyTraitHandle {
///     type Target = dyn MyTrait;
/// }
///
/// fn unsized_handle_example(val: Arc<dyn MyTrait>) {
///     let handle: *const MyTraitHandle = ArcHandle::into_handle(val);
///     let val2 = unsafe { ArcHandle::clone_as_arc(handle) };
///     unsafe { ArcHandle::drop_handle(handle) };
///     // `handle` is no longer safe to use, but `val2` (a cloned Arc) is still valid
/// }
/// ```
///
/// To define and use a handle optimized for a sized type, just impl [SizedArcHandle] (which
/// automatically impl [ArcHandle]), and use the [ArcHandle] API in the same way as for unsized:
///
/// ```
/// // The (sized) struct to pass across the FFI boundary struct
/// MyStruct { ... }
///
/// // The handle that will represent `MyStruct` externally
/// struct MyStructHandle {
///     _unconstructable: Unconstructable,
/// }
///
/// // Connect the two
/// impl SizedArcHandle for MyStructHandle {
///     type Target = MyStruct;
/// }
///
/// fn sized_handle_example(val: Arc<MyStruct>) {
///     let handle: *const MyStructHandle = ArcHandle::into_handle(val);
///     let val2 = unsafe { ArcHandle::clone_as_arc(handle) };
///     unsafe { ArcHandle::drop_handle(handle) };
///     // `handle` is no longer safe to use, but `val2` (a cloned Arc) is still valid
/// }
/// ```
///
/// # Safety
///
/// In addition to being a raw pointer, a handle always points to an underlying allocation of some
/// other type. Thus, it is _ALWAYS_ incorrect to dereference (`*`) a handle or cast it to any other
/// type, (including `Target`). The only safe operations on a handle are to copy the pointer itself
/// by value and to invoke associated functions of `ArcHandle`.
///
/// Additionally, this trait intentionally does _NOT_ expose anything like an `as_ref()` function,
/// because the resulting reference would have an arbitrary (= unnecessarily dangerous) lifetime
/// that cannot be safely enforced, even if we aggressively bound its lifetime. For example:
/// ```
/// let handle: *const MyHandle = ArcHandle::into_handle(...);
/// let handle_copy = handle; // does NOT increment the Arc refcount
/// let my_ref = ArcHandle::as_ref(&handle);
/// ArcHandle::drop_handle(handle_copy); // still-borrowed `handle` can't prevent this
/// my_ref.some_access(); // Illegal access to dangling reference
/// ```
///
/// If a borrowed reference is needed, call [ArcHandle::clone_as_arc] and invoke [Arc::as_ref] on
/// the result.
/// ```
/// let handle: *const MyHandle = ArcHandle::into_handle(...);
/// let handle_copy = handle; // does NOT increment the Arc refcount
/// let my_ref = ArcHandle::clone_as_arc(handle); // increments refcount
/// ArcHandle::drop_handle(handle);
/// my_ref.some_access(); // No problem.
/// ```
///
/// # Synchronization
///
/// The handle (a raw pointer) is neither [Send] nor [Sync] by default:
///
/// It is ONLY safe to manually implement [Send] for a handle (or a type that embeds it) if all of
/// the following conditions hold:
///
/// * The handle's `Target` type is `Send`
///
/// * Calls to [ArcHandle::clone_as_arc] and [ArcHandle::drop_handle] always obey their respective
///   safety requirements. In particular, this means proving (or at least affirming) that no handle
///   is ever accessed after passing it to [ArcHandle::drop_handle].
///
/// It is ONLY safe to implement [Sync] for a handle (or type embedding it) if all of the following
/// conditions hold:
///
/// * The `Target` type is `Sync`
///
/// * The handle is (correctly) `Send`
pub trait ArcHandle: Sized {
    /// The target type this handle represents.
    type Target: ?Sized + Send + Sync;

    /// Converts the target Arc into a (leaked) "handle" that can cross the FFI boundary. The Arc
    /// refcount does not change. The handle remains valid until passed to [Self::drop_handle].
    fn into_handle(target: Arc<Self::Target>) -> *const Self {
        Box::into_raw(Box::new(target)) as _
    }

    /// Clones an Arc from an FFI handle for kernel use, without dropping the handle. The Arc
    /// refcount increases by one.
    ///
    /// # Safety
    ///
    /// Caller of this method asserts that `handle` satisfies all of the following conditions:
    ///
    /// * Obtained by calling [into_handle]
    /// * Never cast to any other type nor dereferenced
    /// * Not previously passed to [drop_handle]
    unsafe fn clone_as_arc(handle: *const Self) -> Arc<Self::Target> {
        let ptr = handle as *mut Arc<Self::Target>;
        let arc = unsafe { &*ptr };
        arc.clone()
    }

    /// Drops the handle, invalidating it. The Arc refcount decreases by one, which may drop the
    /// underlying trait instance as well.
    ///
    /// # Safety
    ///
    /// Caller of this method asserts that `handle` satisfies all of the following conditions:
    ///
    /// * Obtained by calling [into_handle]
    /// * Never cast to any other type nor dereferenced
    /// * Not previously passed to [drop_handle]
    /// * Has no other live references
    unsafe fn drop_handle(handle: *const Self) {
        let ptr = handle as *mut Arc<Self::Target>;
        let _ = unsafe { Box::from_raw(ptr) };
    }
}

/// A special kind of [ArcHandle] which is optimized for [Sized] types. Handles for sized types are
/// more efficient if they implement this trait instead of [ArcHandle].
pub trait SizedArcHandle: Sized {
    type Target: Sized + Send + Sync;
}

// A blanket implementation of `ArcHandle` for all types satisfying `SizedArcHandle`.
//
// Unlike the default (unsized) implementation, which must wrap the Arc in a Box in order to obtain
// a thin pointer, the sized implementation can directly return a pointer to the Arc's underlying
// type. This blanket implementation applies automatically to all types that implement
// SizedArcHandle, so a type that implements SizedArcHandle cannot directly implement ArcHandle
// (which is a Good Thing, because it preserves the important type safety invariant that every
// handle has exactly one `Target` type):
//
// ```
// error[E0119]: conflicting implementations of trait `ArcHandle` for type `FooHandle`
//   --> src/main.rs:55:1
//    |
// 28 | impl<H, T> ArcHandle for H where H: SizedArcHandle<Target = T> {
//    | -------------------------------------------------------------- first implementation here
// ...
// 55 | impl ArcHandle for FooHandle {
//    | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ conflicting implementation for `FooHandle`
// ```
impl<H, T> ArcHandle for H
where
    H: SizedArcHandle<Target = T>,
    T: Send + Sync,
{
    type Target = T;

    fn into_handle(target: Arc<Self::Target>) -> *const Self {
        Arc::into_raw(target) as _
    }

    unsafe fn clone_as_arc(handle: *const Self) -> Arc<Self::Target> {
        let ptr = handle as *const Self::Target;
        unsafe { Arc::increment_strong_count(ptr) };
        unsafe { Arc::from_raw(ptr) }
    }

    unsafe fn drop_handle(handle: *const Self) {
        let ptr = handle as *const Self::Target;
        let _ = unsafe { Arc::from_raw(ptr) };
    }
}
