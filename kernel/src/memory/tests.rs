use std::alloc::{AllocError, Allocator, Layout};
use std::process::Command;
use std::sync::{Arc, Barrier};

use super::*;
use crate::schema::{DataType, StructType};

// Each cap-changing test runs alone even under cargo test's shared-process harness.
fn isolated(run: impl FnOnce()) {
    let current = std::thread::current();
    let name = current.name().unwrap();
    if std::env::var("KERNEL_MEMORY_TEST").as_deref() == Ok(name) {
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        run();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env("KERNEL_MEMORY_TEST", name)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn singleton_allocator_tracks_unlimited_then_enforces_and_reconfigures() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        let allocator = KERNEL_ALLOCATOR;
        assert_eq!(allocator.usage().limit, None);
        assert_eq!(allocator.usage().reserved, baseline);
        let mut bytes = MetadataBytes::try_copy(b"abcdefgh").unwrap();
        assert_eq!(allocator.usage().reserved, baseline + bytes.capacity());
        assert_eq!(
            allocator.set_limit(Some(baseline + 7)),
            Err(MemoryError::LimitBelowUsage)
        );
        assert_eq!(allocator.usage().limit, None);
        allocator.set_limit(Some(baseline + 8)).unwrap();
        assert!(bytes.try_push(b'i').is_err());
        assert_eq!(&*bytes, b"abcdefgh");
        assert_eq!(allocator.usage().reserved, baseline + 8);
        assert_eq!(allocator.usage().rejected, 1);
        allocator.set_limit(None).unwrap();
        bytes.try_push(b'i').unwrap();
        let peak = allocator.usage().peak;
        drop(bytes);
        assert_eq!(allocator.usage().reserved, baseline);
        allocator.set_limit(Some(baseline)).unwrap();
        assert!(MetadataBytes::try_copy(b"x").is_err());
        assert!(MetadataBytes::try_copy(b"").is_ok());
        assert_eq!(allocator.usage().peak, peak);
        allocator.set_limit(None).unwrap();
    });
}

#[test]
fn allocator_zeroed_growth_shrink_alignment_and_backing_failure() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        let allocator = KERNEL_ALLOCATOR;
        let a = Layout::from_size_align(64, 64).unwrap();
        let b = Layout::from_size_align(128, 64).unwrap();
        let allocation = allocator.allocate_zeroed(a).unwrap();
        // SAFETY: The allocator returned a live block of the requested size.
        unsafe {
            assert!(allocation.as_ref().iter().all(|byte| *byte == 0));
            allocation.cast::<u8>().as_ptr().write(23);
            allocator.set_limit(Some(baseline + 128)).unwrap();
            assert!(allocator.grow(allocation.cast(), a, b).is_err());
            assert_eq!(allocation.cast::<u8>().as_ptr().read(), 23);
            assert_eq!(allocator.usage().reserved, baseline + 64);
            allocator.set_limit(Some(baseline + 192)).unwrap();
            let grown = allocator.grow_zeroed(allocation.cast(), a, b).unwrap();
            assert_eq!(grown.cast::<u8>().as_ptr().read(), 23);
            assert!(grown.as_ref()[64..].iter().all(|byte| *byte == 0));
            let shrunk = allocator.shrink(grown.cast(), b, a).unwrap();
            assert_eq!(shrunk.cast::<u8>().as_ptr().read(), 23);
            assert_eq!(allocator.usage().reserved, baseline + 64);
            allocator.deallocate(shrunk.cast(), a);
        }
        allocator.set_limit(None).unwrap();
        let rejected = allocator.usage().rejected;
        assert!(allocator.allocate_with(a, |_| Err(AllocError)).is_err());
        assert_eq!(allocator.usage().reserved, baseline);
        assert_eq!(allocator.usage().rejected, rejected);
        let aligned = Layout::from_size_align(256, 4096).unwrap();
        let allocation = allocator.allocate(aligned).unwrap();
        assert_eq!(allocation.cast::<u8>().as_ptr().addr() % 4096, 0);
        unsafe {
            KERNEL_ALLOCATOR.deallocate(allocation.cast(), aligned);
        }
        assert_eq!(allocator.usage().reserved, baseline);
        assert_eq!(
            MetadataVec::<u64>::new().try_reserve_exact(usize::MAX),
            Err(MemoryError::CapacityOverflow)
        );
    });
}

#[test]
fn one_cap_is_shared_by_concurrent_allocations_and_cross_thread_frees() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        KERNEL_ALLOCATOR.set_limit(Some(baseline + 4096)).unwrap();
        let initial_peak = KERNEL_ALLOCATOR.usage().peak;
        let gate = Arc::new(Barrier::new(17));
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let gate = gate.clone();
                std::thread::spawn(move || {
                    let allocation = MetadataBytes::try_copy(&[7; 1024]).ok();
                    gate.wait();
                    gate.wait();
                    allocation
                })
            })
            .collect();
        gate.wait();
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline + 4096);
        assert_eq!(KERNEL_ALLOCATOR.usage().rejected, 12);
        assert_eq!(
            KERNEL_ALLOCATOR.usage().peak,
            initial_peak.max(baseline + 4096)
        );
        gate.wait();
        let allocations: Vec<_> = workers
            .into_iter()
            .filter_map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(allocations.len(), 4);
        drop(allocations);
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
    });
}

#[test]
fn retained_strings_and_nested_schema_use_the_same_allocator_without_scopes() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        let json = r#"{"type":"struct","fields":[{"name":"nested","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"leaf","type":"long","nullable":true,"metadata":{"comment":"retained annotation"}}]},"containsNull":true},"nullable":true,"metadata":{}}]}"#;
        let schema: StructType = decode_json(json.as_bytes()).unwrap();
        assert!(matches!(
            schema.field("nested").unwrap().data_type(),
            DataType::Array(_)
        ));
        let used = KERNEL_ALLOCATOR.usage().reserved;
        assert!(used > 0);
        KERNEL_ALLOCATOR.set_limit(Some(used)).unwrap();
        assert!(decode_json::<StructType>(json.as_bytes()).is_err());
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, used);
        drop(schema);
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        let text = SharedString::try_copy("schema payload").unwrap();
        let copy = text.clone();
        let retained = KERNEL_ALLOCATOR.usage().reserved;
        drop(text);
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, retained);
        std::thread::spawn(move || drop(copy)).join().unwrap();
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    });
}

#[test]
fn file_and_variant_schema_decoding_is_fallible_at_each_small_limit() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        for json in [br#""file""#.as_slice(), br#""variant""#.as_slice()] {
            for headroom in (0..2048).step_by(32) {
                KERNEL_ALLOCATOR
                    .set_limit(Some(baseline + headroom))
                    .unwrap();
                drop(decode_json::<DataType>(json));
                assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
            }
        }
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
    });
}
