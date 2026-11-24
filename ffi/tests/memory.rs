//! The global limit is isolated in a test binary so this also runs under Miri.

use delta_kernel::memory::{KERNEL_ALLOCATOR, MetadataBytes};
use delta_kernel_ffi::OptionalValue;
use delta_kernel_ffi::memory::{FfiMemoryUsage, kernel_memory_set_limit, kernel_memory_usage};

fn usage() -> FfiMemoryUsage {
    let mut out = std::mem::MaybeUninit::uninit();
    assert!(unsafe { kernel_memory_usage(out.as_mut_ptr()) });
    unsafe { out.assume_init() }
}

#[test]
fn global_allocator_control_and_ffi_lifetimes() {
    assert!(kernel_memory_set_limit(OptionalValue::None));
    let baseline = usage().reserved;
    assert!(!unsafe { kernel_memory_usage(std::ptr::null_mut()) });
    assert!(matches!(usage().limit, OptionalValue::None));
    let bytes = MetadataBytes::try_copy(&[7; 1024]).unwrap();
    assert_eq!(usage().reserved, baseline + 1024);
    assert!(!kernel_memory_set_limit(OptionalValue::Some(
        baseline + 1023
    )));
    assert!(matches!(usage().limit, OptionalValue::None));
    assert!(kernel_memory_set_limit(OptionalValue::Some(
        baseline + 1024
    )));
    assert!(MetadataBytes::try_copy(b"x").is_err());
    assert_eq!(usage().reserved, baseline + 1024);
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    assert_eq!(usage().reserved, baseline);
    assert!(kernel_memory_set_limit(OptionalValue::None));
    let bytes = MetadataBytes::try_copy(&[7; 2048]).unwrap();
    assert_eq!(usage().reserved, baseline + 2048);
    drop(bytes);
    assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);

    #[cfg(feature = "declarative-plans")]
    {
        let bytes = bytes::Bytes::from_owner(MetadataBytes::try_copy(&[7; 1024]).unwrap());
        let retained = bytes.clone();
        let ffi_bytes = delta_kernel_ffi::KernelOwnedBytes::from(bytes);
        std::thread::spawn(move || unsafe { delta_kernel_ffi::free_kernel_bytes(ffi_bytes) })
            .join()
            .unwrap();
        assert_eq!(usage().reserved, baseline + 1024);
        assert!(kernel_memory_set_limit(OptionalValue::Some(
            baseline + 1024
        )));
        drop(retained);
        assert_eq!(usage().reserved, baseline);
        let ffi_bytes = delta_kernel_ffi::KernelOwnedBytes::from(bytes::Bytes::from_owner(
            MetadataBytes::try_copy(&[8; 512]).unwrap(),
        ));
        std::thread::spawn(move || unsafe { delta_kernel_ffi::free_kernel_bytes(ffi_bytes) })
            .join()
            .unwrap();
        assert_eq!(usage().reserved, baseline);
        assert!(kernel_memory_set_limit(OptionalValue::None));
    }
}
