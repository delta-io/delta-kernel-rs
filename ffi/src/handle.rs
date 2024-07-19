//! [`Handle`] is an idiom for visibly transferring ownership of opaque rust objects across the FFI
//! boundary.
//!
//! Creating a [`Handle<T>`] always implies some kind of ownership transfer. A mutable handle takes
//! ownership of the object itself (analagous to [`Box<T>`]), while a non-mutable (shared) handle
//! takes ownership of a shared reference to the object (analagous to [`std::sync::Arc<T>`]). Thus, a created
//! handle remains [valid][Handle#Validity], and its underlying object remains accessible, until the
//! handle is explicitly dropped or consumed. Dropping a mutable handle always drops the underlying
//! object as well; dropping a shared handle only drops the underlying object if the handle was the
//! last reference to that object.
//!
//! Because handles carry ownership semantics, and lifetime information is not preserved across the
//! FFI boundary, handles are always opaque types to avoid confusiong them with normal references
//! and pointers (`&Foo`, `*const Foo`, etc) that are possibly only valid until the FFI call
//! returns. For the same reason, mutable handles implement neither [`Copy`] nor [`Clone`]. However,
//! this only helps on the Rust side, because handles appear as simple pointers in the FFI and are
//! thus easily duplicated there. In order to improve safety, external (non-Rust) code is strongly
//! advised to maintain "unique pointer" semantics for mutable handles.
//!
//! NOTE: While shared handles could conceptually impl [`Clone`], cloning would require unsafe code
//! and so we can't actually implement the trait. Use [`Handle::clone_handle`] instead.

/// Describes the kind of handle a given opaque pointer type represents.
///
/// It is not normally necessary to implement this trait directly; instead, use the provided
/// attribute macro [`#[handle_descriptor]`][delta_kernel_ffi_macros::handle_descriptor] to generate
/// the implementation automatically, e.g.
/// ```ignore
/// # use delta_kernel_ffi_macros::handle_descriptor;
/// # use handle::Handle;
/// pub struct Foo {
///     x: usize,
/// }
///
/// #[handle_descriptor(target = Foo, mutable = true, sized = true)]
/// pub struct MutableFoo;
///
/// pub trait Bar: Send {}
///
/// #[handle_descriptor(target = dyn Bar, mutable = false)]
/// pub struct SharedBar;
///
/// ```
///
/// NOTE: For obscure rust generic type constraint reasons, a `HandleDescriptor` must specifically
/// state whether the target type is [`Sized`]. However, two arguments suffice for trait targets,
/// because `sized=true` is implied.
pub trait HandleDescriptor {
    /// The true type of the handle's underlying object
    type Target: ?Sized + Send;

    /// If `True`, the handle owns the underlying object (`Box`-like); otherwise, the handle owns a
    /// shared reference to the underlying object (`Arc`-like).
    type Mutable: Boolean;

    /// `True` iff the handle's `Target` is [`Sized`].
    type Sized: Boolean;
}

mod private {

    use std::mem::ManuallyDrop;
    use std::ptr::NonNull;
    use std::sync::Arc;

    use super::*;

    /// Represents an object that crosses the FFI boundary and which outlives the scope that created
    /// it. It can be passed freely between rust code and external code. The
    ///
    /// An accompanying [`HandleDescriptor`] trait defines the behavior of each handle type:
    ///
    /// * The true underlying ("target") type the handle represents. For safety reasons, target type
    /// must always be [`Send`].
    ///
    /// * Mutable (`Box`-like) vs. shared (`Arc`-like). For safety reasons, the target type of a
    /// shared handle must always be [`Send`]+[`Sync`].
    ///
    /// * Sized vs. unsized. Sized types allow handle operations to be implemented more efficiently.
    ///
    /// # Validity
    ///
    /// A `Handle` is _valid_ if all of the following hold:
    ///
    /// * It was created by a call to [`Handle::from`]
    /// * Not yet dropped by a call to [`Handle::drop_handle`]
    /// * Not yet consumed by a call to [`Handle::into_inner`]
    ///
    /// Additionally, in keeping with the [`Send`] contract, multi-threaded external code must
    /// enforce mutual exclusion -- no mutable handle should ever be passed to more than one kernel
    /// API call at a time. If thread races are possible, the handle should be protected with a
    /// mutex. Due to Rust [reference
    /// rules](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html#the-rules-of-references),
    /// this requirement applies even for API calls that appear to be read-only (because Rust code
    /// always receives the handle as mutable).
    ///
    /// NOTE: Because the underlying type is always [`Sync`], multi-threaded external code can
    /// freely access shared (non-mutable) handles.
    ///
    /// cbindgen:transparent-typedef
    #[repr(transparent)]
    pub struct Handle<H: HandleDescriptor> {
        ptr: NonNull<H>,
    }

    /// # Safety
    ///
    /// The underlying type of a `Handle` is always `Send`, so Rust code will handle it correctly
    /// even if the external code calls into kernel from a different thread than the one that
    /// created the handle. If external code allows multiple threads to access a given mutable
    /// handle, the external code must use appropriate mutual exclusion to uphold the [`Send`
    /// contract](https://doc.rust-lang.org/book/ch16-04-extensible-concurrency-sync-and-send.html#allowing-transference-of-ownership-between-threads-with-send).
    unsafe impl<H: HandleDescriptor> Send for Handle<H> {}

    /// # Safety
    ///
    /// The underlying type of a shared handle is always `Sync`, so Rust code will handle it
    /// correctly even if external code calls into kernel from a different thread than the one
    /// that created the handle.
    ///
    /// NOTE: Mutable handles are not `Sync`, but they do not need to be. Kernel does not employ
    /// threading internally, and multi-threaded external code is responsible to have already
    /// enforced `Send` semantics before passing a mutable handle into kernel. Thus, the receiving
    /// Rust code only has to worry about normal reference rules.
    unsafe impl<H: HandleDescriptor + SharedHandle> Sync for Handle<H> {}

    // [`Handle`] operations applicable to all handles, with implementations forwarded to the
    // appropriate specialization of `HandleOps`.
    impl<T, M, S, H> Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = M, Sized = S> + HandleOps<T, M, S>,
    {
        /// Obtains a shared reference to this handle's underlying object. Unsafe equivalent to
        /// [`AsRef::as_ref`].
        ///
        /// # Safety
        ///
        /// Caller asserts that this handle obeys [shared reference
        /// semantics](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html#the-rules-of-references),
        /// namely:
        ///
        /// * The handle is [valid][Handle#Validity].
        ///
        /// * No mutable references can overlap with the returned reference.
        pub unsafe fn as_ref(&self) -> &H::Target {
            H::as_ref(self.ptr.cast().as_ptr())
        }

        /// Consumes this handle and returns the resource it was originally created `From`: A
        /// mutable handle returns [`Box<T>`], and a shared handle returns [`Arc<T>`].
        ///
        /// # Safety
        ///
        /// Caller asserts that the handle is [valid][Handle#Validity].
        pub unsafe fn into_inner(self) -> H::From {
            H::into_inner(self.ptr.cast().as_ptr())
        }
        /// Drops this handle. Dropping a mutable handle always drops the underlying object as well;
        /// dropping a shared handle only drops the underlying object if the handle was the last
        /// reference to that object.
        ///
        /// # Safety
        ///
        /// Caller asserts that the handle is [valid][Handle#Validity].
        pub unsafe fn drop_handle(self) {
            drop(self.into_inner())
        }
    }

    // [`Handle`] operations applicable only to mutable handles, with implementations forwarded to
    // the appropriate specialization of `MutableHandleOps`.
    impl<T, S, H> Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = S> + MutableHandleOps<T, S>,
    {
        /// Obtains a mutable reference to the handle's underlying object. Unsafe equivalent to
        /// [`AsMut::as_mut`].

        ///
        /// # Safety
        ///
        /// Caller asserts that this handle obeys [mutable reference
        /// semantics](https://doc.rust-lang.org/book/ch04-02-references-and-borrowing.html#the-rules-of-references),
        /// namely:
        ///
        /// * The handle is [valid][Handle#Validity].
        ///
        /// * No other references (neither shared nor mutable) can overlap with the returned
        ///   reference. In particular, this means the caller must ensure no other thread can access this
        ///   handle while the returned reference is still alive.
        pub unsafe fn as_mut(&mut self) -> &mut T {
            H::as_mut(self.ptr.cast().as_ptr())
        }
    }

    // [`Handle`] operations applicable only to shared handles, with implementations forwarded to
    // the appropriate specialization of `MutableHandleOps`.
    impl<T, S, H> Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = S>
            + SharedHandleOps<T, S, From = Arc<T>>,
    {
        /// Clones this shared handle, increasing the underlying object's refcount by one. Unsafe
        /// equivalent to [`Clone::clone`]. The new handle is independent of the source handle, and
        /// should be dropped when it is no longer needed. This is equivalent to calling
        /// `clone_as_arc().into_handle()`.
        ///
        /// # Safety
        ///
        /// Caller asserts that the handle is [valid][Handle#Validity].
        pub unsafe fn clone_handle(&self) -> Self {
            self.clone_as_arc().into()
        }

        /// Clones a new `Arc<T>` out of this handle, increasing the underlying object's refcount by
        /// one. This is equivalent to calling `clone_handle().into_inner()`.
        ///
        /// # Safety
        ///
        /// Caller asserts that the handle is [valid][Handle#Validity].
        pub unsafe fn clone_as_arc(&self) -> Arc<T> {
            H::clone_arc(self.ptr.cast().as_ptr())
        }
    }

    // NOTE: While it would be really convenient to `impl From<T> for Handle<H>`, that definition is
    // not disjoint and conflicts with the standard `impl From<T> for T` (since there's no way to
    // prove categorically that T != H). Ditto for `impl<X: Into<Box<T>>> From<X> for Handle<H>`.
    impl<T, S, H> From<Box<T>> for Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = S>
            + MutableHandleOps<T, S, From = Box<T>>,
    {
        fn from(val: Box<T>) -> Handle<H> {
            let ptr = H::into_handle_ptr(val).cast();
            Handle { ptr }
        }
    }

    impl<T, S, H> From<Arc<T>> for Handle<H>
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = S>
            + SharedHandleOps<T, S, From = Arc<T>>,
    {
        fn from(val: Arc<T>) -> Handle<H> {
            let ptr = H::into_handle_ptr(val).cast();
            Handle { ptr }
        }
    }

    pub trait MutableHandle {}
    impl<T, S, H> MutableHandle for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = S>,
    {
    }

    pub trait SharedHandle {}
    impl<T, S, H> SharedHandle for H
    where
        T: ?Sized + Sync,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = S>,
    {
    }

    // Helper for [`Handle`] that encapsulates operations applicable to all handles. By taking `M`
    // (mutable) and `S` (sized) as generic parameters, we can define disjoint generic impl for each
    // of the four combinations of `M` and `S`, without risk of ambiguity.
    //
    // NOTE: This trait is only public because otherwise `Handle` cannot impl it. It is not part of
    // stable public API.
    #[doc(hidden)]
    pub trait HandleOps<T: ?Sized, M, S> {
        type From: Sized;
        type Raw: Sized;

        fn into_handle_ptr(val: Self::From) -> NonNull<Self::Raw>;

        // # Safety
        //
        // This method is VERY unsafe to call directly, because the returned reference receives an
        // arbitrary lifetime provided by its caller. Assuming the safety requirements of
        // [`Handle::as_ref`] are met, that method will ensure safety by assigning a lifetime that
        // does not outlive the source `Handle` itself, and can thus safely call this method..
        unsafe fn as_ref<'a>(ptr: *const Self::Raw) -> &'a T;

        // # Safety
        //
        // Assuming the safety requirements of [`Handle::into_inner`] are met, that method can
        // safely call this method.
        unsafe fn into_inner(ptr: *mut Self::Raw) -> Self::From;
    }

    // Helper for [`Handle`] that encapsulates operations applicable only to mutable handles. By
    // taking `S` (sized) as a generic parameter, we can define disjoint generic impl for sized and
    // unsized types.
    //
    // NOTE: This trait is only public because otherwise `Handle` cannot impl it. It is not part of
    // stable public API.
    #[doc(hidden)]
    pub trait MutableHandleOps<T: ?Sized, S>: HandleOps<T, True, S> + MutableHandle {
        // # Safety
        //
        // This method is VERY unsafe to call directly, because the returned reference receives an
        // arbitrary lifetime provided by its caller. Assuming the safety requirements of
        // [`HandleAsMut::as_mut`] are met, that method will ensure safety by assigning a lifetime
        // that does not outlive the source `Handle` itself, and can thus safely call this method.
        unsafe fn as_mut<'a>(ptr: *mut Self::Raw) -> &'a mut T;
    }

    // Helper for [`Handle`] that encapsulates operations applicable only to shared handles. By
    // taking `S` (sized) as a generic parameter, we can define disjoint generic impl for sized and
    // unsized types.
    //
    // NOTE: This trait is only public because otherwise `Handle` cannot impl it. It is not part of
    // stable public API.
    #[doc(hidden)]
    pub trait SharedHandleOps<T: ?Sized, S>: HandleOps<T, False, S> + SharedHandle {
        // # Safety
        //
        // Assuming the safety requirements of [`CloneHandle::clone_as_arc`] are met, that method
        // can safely call this method.
        unsafe fn clone_arc(ptr: *const Self::Raw) -> Arc<T>;
    }

    // Acts like `Box<T: Sized>` -- we can directly use the input `Box`
    impl<T, H> HandleOps<T, True, True> for H
    where
        H: HandleDescriptor<Target = T, Mutable = True, Sized = True>,
    {
        type From = Box<T>;
        type Raw = T;

        fn into_handle_ptr(val: Box<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const T) -> &'a T {
            &*ptr
        }
        unsafe fn into_inner(ptr: *mut T) -> Box<T> {
            Box::from_raw(ptr)
        }
    }

    impl<T, H> MutableHandleOps<T, True> for H
    where
        H: HandleDescriptor<Target = T, Mutable = True, Sized = True>,
    {
        unsafe fn as_mut<'a>(ptr: *mut T) -> &'a mut T {
            &mut *ptr
        }
    }

    // Acts like `Arc<T: Sized>` -- we can directly use the input `Arc`
    impl<T, H> HandleOps<T, False, True> for H
    where
        H: HandleDescriptor<Target = T, Mutable = False, Sized = True>,
    {
        type From = Arc<T>;
        type Raw = T;

        fn into_handle_ptr(val: Arc<T>) -> NonNull<T> {
            let val = ManuallyDrop::new(val);
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const T) -> &'a T {
            &*ptr
        }
        unsafe fn into_inner(ptr: *mut T) -> Arc<T> {
            Arc::from_raw(ptr)
        }
    }

    impl<T, H> SharedHandleOps<T, True> for H
    where
        T: Sync,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = True>,
    {
        unsafe fn clone_arc(ptr: *const T) -> Arc<T> {
            Arc::increment_strong_count(ptr);
            Arc::from_raw(ptr)
        }
    }

    // Acts like `Box<T: ?Sized>` -- could be a fat pointer, so we have to box it up
    impl<T, H> HandleOps<T, True, False> for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = False>,
    {
        type From = Box<T>;
        type Raw = Box<T>;

        fn into_handle_ptr(val: Box<T>) -> NonNull<Box<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const Box<T>) -> &'a T {
            let boxed = unsafe { &*ptr };
            boxed.as_ref()
        }
        unsafe fn into_inner(ptr: *mut Box<T>) -> Box<T> {
            *Box::from_raw(ptr)
        }
    }

    impl<T, H> MutableHandleOps<T, False> for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = True, Sized = False>,
    {
        unsafe fn as_mut<'a>(ptr: *mut Box<T>) -> &'a mut T {
            let boxed = unsafe { &mut *ptr };
            boxed.as_mut()
        }
    }

    // Acts like `Arc<T: ?Sized>` -- could be a fat pointer, so we have to box it up
    impl<T, H> HandleOps<T, False, False> for H
    where
        T: ?Sized,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = False>,
    {
        type From = Arc<T>;
        type Raw = Arc<T>;

        fn into_handle_ptr(val: Arc<T>) -> NonNull<Arc<T>> {
            // Double-boxing needed in order to obtain a thin pointer
            let val = ManuallyDrop::new(Box::new(val));
            val.as_ref().into()
        }
        unsafe fn as_ref<'a>(ptr: *const Arc<T>) -> &'a T {
            let arc = unsafe { &*ptr };
            arc.as_ref()
        }
        unsafe fn into_inner(ptr: *mut Arc<T>) -> Arc<T> {
            *Box::from_raw(ptr)
        }
    }

    impl<T, H> SharedHandleOps<T, False> for H
    where
        T: ?Sized + Sync,
        H: HandleDescriptor<Target = T, Mutable = False, Sized = False>,
    {
        unsafe fn clone_arc(ptr: *const Arc<T>) -> Arc<T> {
            let arc = unsafe { &*ptr };
            arc.clone()
        }
    }

    // A NZST struct that cannot be instantiated by any Rust code because this module exposes no
    // public constructor for it. Intentionally _NOT_ a zero-sized type, to avoid FFI weirdness.
    #[doc(hidden)]
    pub struct Unconstructable {
        _private: usize,
    }

    // This trait is a workaround until
    // [`#![feature(associated_const_equality)]`](https://github.com/rust-lang/rust/labels/F-associated_const_equality)
    // stabilizes. Ideally, the `M` and `S` parameters would be actual constant boolean values, but
    // we cannot specialize generics based on constant values. So instead we define this sealed
    // `Boolean` trait along with a couple implementing structs that represent true and false.
    #[doc(hidden)]
    pub trait Boolean: Sealed {}
    #[doc(hidden)]
    pub trait Sealed {}

    #[doc(hidden)]
    pub struct True {}
    #[doc(hidden)]
    pub struct False {}

    impl Boolean for True {}
    impl Boolean for False {}

    impl Sealed for True {}
    impl Sealed for False {}
}

pub use private::{Boolean, False, Handle, True, Unconstructable};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use delta_kernel_ffi_macros::handle_descriptor;

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct Foo {
        pub x: usize,
        pub y: String,
    }

    #[handle_descriptor(target=Foo, mutable=true, sized=true)]
    pub struct MutableFoo;

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct Bar {
        pub x: usize,
        pub y: String,
    }

    #[handle_descriptor(target=Bar, mutable=false, sized=true)]
    pub struct SharedBar;

    pub trait Baz: Send + Sync {
        fn squawk(&self) -> String;
    }

    impl Baz for Bar {
        fn squawk(&self) -> String {
            format!("{self:?}")
        }
    }

    #[handle_descriptor(target=dyn Baz, mutable=true)]
    pub struct MutableBaz;

    #[handle_descriptor(target=dyn Baz, mutable=false)]
    pub struct SharedBaz;

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct NotSync {
        pub ptr: *mut u32,
    }
    unsafe impl Send for NotSync {}

    #[handle_descriptor(target=NotSync, mutable=true, sized=true)]
    pub struct MutNotSync;

    // Because tests compile as binaries against packages, this test can only run correctly if we
    // use developer-visibility to make mod handle public. Otherwise it's inaccessible for testing.
    #[test]
    #[cfg(feature = "developer-visibility")]
    fn invalid_handle_code() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/invalid-handle-code/*.rs");
    }

    #[test]
    fn test_handle_use_cases_compile() {
        let s = NotSync {
            ptr: std::ptr::null_mut(),
        };
        let h: Handle<MutNotSync> = Box::new(s).into();
        unsafe { h.drop_handle() };

        let f = Foo {
            x: rand::random::<usize>(),
            y: rand::random::<usize>().to_string(),
        };
        let s = format!("{f:?}");
        let mut h: Handle<MutableFoo> = Box::new(f).into();
        let r = unsafe { h.as_mut() };
        assert_eq!(format!("{r:?}"), s);

        unsafe { h.drop_handle() };

        let b = Bar {
            x: rand::random::<usize>(),
            y: rand::random::<usize>().to_string(),
        };
        let s = format!("{b:?}");
        let h: Handle<SharedBar> = Arc::new(b).into();
        let r = unsafe { h.as_ref() };
        assert_eq!(format!("{r:?}"), s);

        let r = unsafe { h.clone_as_arc() };
        assert_eq!(format!("{r:?}"), s);
        unsafe { h.drop_handle() };

        let b = Bar {
            x: rand::random::<usize>(),
            y: rand::random::<usize>().to_string(),
        };
        let s = b.squawk();
        let t: Arc<dyn Baz> = Arc::new(b);
        let h: Handle<SharedBaz> = t.into();
        let r = unsafe { h.as_ref() };
        assert_eq!(s, r.squawk());
        let r = unsafe { h.clone_as_arc() };
        assert_eq!(s, r.squawk());

        let h2 = unsafe { h.clone_handle() };
        let s2 = s;
        unsafe { h.drop_handle() };

        let randstr = rand::random::<usize>().to_string();
        let randint = rand::random::<usize>();

        let b = Bar {
            x: randint,
            y: randstr.clone(),
        };
        let s = b.squawk();
        let t: Box<dyn Baz> = Box::new(b);
        let mut h: Handle<MutableBaz> = t.into();
        let r = unsafe { h.as_mut() };
        assert_eq!(s, r.squawk());
        let r = unsafe { h.as_ref() };
        assert_eq!(s, r.squawk());

        unsafe { h.drop_handle() };

        let r = unsafe { h2.as_ref() };
        assert_eq!(r.squawk(), s2);
        unsafe { h2.drop_handle() };
    }
}
