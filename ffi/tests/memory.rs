//! Exercises the process allocator and the C configuration/release boundary.
#![cfg(feature = "global-allocator")]

use std::ffi::c_void;

use delta_kernel::memory::{KERNEL_ALLOCATOR, MemoryError, MetadataBytes};
use delta_kernel_ffi::OptionalValue;
use delta_kernel_ffi::error::allocation::{DeltaErrorInfo, ErrorAllocators, KernelErrorInfo};
use delta_kernel_ffi::memory::{FfiMemoryUsage, kernel_memory_set_limit, kernel_memory_usage};

fn usage() -> FfiMemoryUsage {
    let mut out = std::mem::MaybeUninit::uninit();
    assert!(unsafe { kernel_memory_usage(out.as_mut_ptr()) });
    unsafe { out.assume_init() }
}

unsafe extern "C" fn kernel_error(_: *mut c_void, info: *const KernelErrorInfo) -> *mut c_void {
    // SAFETY: Kernel calls this function with its live descriptor for the callback's duration.
    let info = unsafe { &*info };
    assert_eq!(info.code as usize, 59);
    // The descriptor and its static message are borrowed for this call.
    60usize as *mut c_void
}

unsafe extern "C" fn delta_error(_: *mut c_void, _: *const DeltaErrorInfo) -> *mut c_void {
    std::ptr::null_mut()
}

#[test]
fn global_allocator_control_and_ffi_lifetimes() {
    assert!(kernel_memory_set_limit(OptionalValue::None));
    std::thread::spawn(|| {}).join().unwrap();
    let baseline = usage().reserved;
    assert!(!unsafe { kernel_memory_usage(std::ptr::null_mut()) });
    let mut bytes = Vec::<u8>::new();
    bytes.try_reserve_exact(1024).unwrap();
    bytes.resize(1024, 7);
    let retained = usage().reserved;
    assert_eq!(retained, baseline + bytes.capacity());
    assert!(!kernel_memory_set_limit(OptionalValue::Some(retained - 1)));
    assert!(kernel_memory_set_limit(OptionalValue::Some(retained)));
    assert!(MetadataBytes::try_copy(b"x").is_err());
    assert!(bytes.try_reserve_exact(1024).is_err());
    assert_eq!(usage().reserved, retained);
    let allocators = ErrorAllocators {
        context: std::ptr::null_mut(),
        allocate_kernel_error: kernel_error,
        allocate_delta_error: delta_error,
    };
    let error =
        delta_kernel::KernelError::Memory(MemoryError::AllocationFailed { limit: retained });
    // SAFETY: These callbacks borrow only the supplied descriptor and return opaque sentinels.
    let error = unsafe { allocators.allocate_kernel_error(error) };
    assert_eq!(error as usize, 60);
    assert!(kernel_memory_set_limit(OptionalValue::None));
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    assert_eq!(usage().reserved, baseline);

    #[cfg(feature = "declarative-plans")]
    {
        let bytes = bytes::Bytes::from_owner(MetadataBytes::try_copy(&[7; 1024]).unwrap());
        let retained = bytes.clone();
        let ffi_bytes = delta_kernel_ffi::KernelOwnedBytes::from(bytes);
        let before_release = usage().reserved;
        std::thread::spawn(move || unsafe { delta_kernel_ffi::free_kernel_bytes(ffi_bytes) })
            .join()
            .unwrap();
        assert!(usage().reserved < before_release);
        assert!(usage().reserved > baseline);
        drop(retained);
        assert_eq!(usage().reserved, baseline);
    }
    assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
}
