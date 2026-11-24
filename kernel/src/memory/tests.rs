use std::alloc::{GlobalAlloc, Layout};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;
use crate::schema::StructType;

#[global_allocator]
static TEST_ALLOCATOR: KernelAllocator = KernelAllocator::new();

// Cap-changing tests use child processes even with cargo test's shared-process harness.
fn isolated(run: impl FnOnce()) {
    let current = std::thread::current();
    let name = current.name().unwrap();
    if std::env::var("KERNEL_MEMORY_TEST").as_deref() == Ok(name) {
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        run();
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
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
fn standard_containers_return_errors_at_the_shared_global_limit() {
    isolated(|| {
        let mut bytes = Vec::<u8>::new();
        let mut text = String::new();
        let mut map = HashMap::<u64, u64, BuildHasherDefault<DefaultHasher>>::default();
        let baseline = KERNEL_ALLOCATOR.usage();
        KERNEL_ALLOCATOR.set_limit(Some(baseline.reserved)).unwrap();
        assert!(bytes.try_reserve(1024).is_err());
        assert!(text.try_reserve(1024).is_err());
        assert!(map.try_reserve(128).is_err());
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline.reserved);
        assert_eq!(KERNEL_ALLOCATOR.usage().rejected, baseline.rejected + 3);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        bytes.try_reserve(1024).unwrap();
        text.try_reserve(1024).unwrap();
        map.try_reserve(128).unwrap();
    });
}

#[test]
fn standard_vector_failed_reallocation_preserves_contents_and_accounting() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(64).unwrap();
        bytes.extend_from_slice(&[23u8; 64]);
        let before = KERNEL_ALLOCATOR.usage().reserved;
        assert_eq!(before, baseline + bytes.capacity());
        KERNEL_ALLOCATOR.set_limit(Some(before)).unwrap();
        assert!(bytes.try_reserve_exact(64).is_err());
        assert_eq!(bytes.as_slice(), &[23u8; 64]);
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, before);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        bytes.try_reserve_exact(64).unwrap();
        drop(bytes);
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    });
}

#[test]
fn global_allocator_zeroed_alignment_and_backing_failure() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage();
        let layout = Layout::from_size_align(256, 4096).unwrap();
        assert!(
            KERNEL_ALLOCATOR
                .allocate_with(layout, |_| std::ptr::null_mut())
                .is_null()
        );
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline.reserved);
        assert_eq!(KERNEL_ALLOCATOR.usage().rejected, baseline.rejected);
        // SAFETY: The allocation is released with the same nonzero layout.
        unsafe {
            let pointer = KERNEL_ALLOCATOR.alloc_zeroed(layout);
            assert!(!pointer.is_null());
            assert_eq!(pointer.addr() % 4096, 0);
            assert!(
                std::slice::from_raw_parts(pointer, 256)
                    .iter()
                    .all(|b| *b == 0)
            );
            KERNEL_ALLOCATOR.dealloc(pointer, layout);
        }
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline.reserved);
    });
}

#[test]
fn fallible_box_and_vector_helpers_use_standard_global_storage() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        KERNEL_ALLOCATOR.set_limit(Some(baseline)).unwrap();
        assert!(Owned::try_new([0u8; 128]).is_err());
        assert!(MetadataBytes::try_copy(b"payload").is_err());
        assert_eq!(*Owned::try_new(()).unwrap(), ());
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
        assert_eq!(
            MetadataVec::<u64>::new().try_reserve_exact(usize::MAX),
            Err(MemoryError::CapacityOverflow)
        );
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        let payload = Owned::try_new([23u8; 128]).unwrap();
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline + 128);
        assert_eq!(payload.into_inner(), [23u8; 128]);
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    });
}

#[test]
fn global_limit_and_frees_follow_ownership_across_threads() {
    isolated(|| {
        let bytes = MetadataBytes::try_copy(&[7; 4096]).unwrap();
        let capacity = bytes.capacity();
        let flags = Arc::new([
            AtomicBool::new(false),
            AtomicBool::new(false),
            AtomicBool::new(false),
            AtomicBool::new(false),
        ]);
        let worker_flags = flags.clone();
        let worker = std::thread::spawn(move || {
            worker_flags[0].store(true, Ordering::Release);
            while !worker_flags[1].load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            let mut blocked = Vec::<u8>::new();
            assert!(blocked.try_reserve_exact(8192).is_err());
            drop(bytes);
            worker_flags[2].store(true, Ordering::Release);
            while !worker_flags[3].load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
        });
        while !flags[0].load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        KERNEL_ALLOCATOR.set_limit(Some(baseline)).unwrap();
        flags[1].store(true, Ordering::Release);
        while !flags[2].load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline - capacity);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        flags[3].store(true, Ordering::Release);
        worker.join().unwrap();
    });
}

#[test]
fn schema_strings_and_dependency_objects_are_in_the_global_counters() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        let json = r#"{"type":"struct","fields":[{"name":"nested","type":{"type":"struct","fields":[{"name":"leaf","type":"long","nullable":true,"metadata":{"comment":"retained annotation"}}]},"nullable":true,"metadata":{}}]}"#;
        let schema: StructType = decode_json(json.as_bytes()).unwrap();
        let text = SharedString::try_copy("shared payload").unwrap();
        assert!(KERNEL_ALLOCATOR.usage().reserved > baseline);
        drop((schema, text));
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    });
}

#[test]
fn json_output_reservation_failure_does_not_allocate_an_error() {
    isolated(|| {
        let baseline = KERNEL_ALLOCATOR.usage();
        KERNEL_ALLOCATOR.set_limit(Some(baseline.reserved)).unwrap();
        assert_eq!(
            try_to_json([23u64; 128].as_slice()),
            Err(MemoryError::AllocationFailed {
                limit: baseline.reserved
            })
        );
        assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline.reserved);
        assert_eq!(KERNEL_ALLOCATOR.usage().rejected, baseline.rejected + 1);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        assert!(!try_to_json([23u64; 128].as_slice()).unwrap().is_empty());
    });
}

#[test]
fn concurrent_standard_vectors_cannot_oversubscribe_the_limit() {
    isolated(|| {
        let flags = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
        let ready = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let successes = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(16);
        for _ in 0..16 {
            let flags = flags.clone();
            let ready = ready.clone();
            let finished = finished.clone();
            let successes = successes.clone();
            workers.push(std::thread::spawn(move || {
                let mut bytes = Vec::<u8>::new();
                ready.fetch_add(1, Ordering::Release);
                while !flags[0].load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                if bytes.try_reserve_exact(1024).is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
                finished.fetch_add(1, Ordering::Release);
                while !flags[1].load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                drop(bytes);
            }));
        }
        while ready.load(Ordering::Acquire) != 16 {
            std::hint::spin_loop();
        }
        let baseline = KERNEL_ALLOCATOR.usage();
        let limit = baseline.reserved + 4096;
        KERNEL_ALLOCATOR.set_limit(Some(limit)).unwrap();
        flags[0].store(true, Ordering::Release);
        while finished.load(Ordering::Acquire) != 16 {
            std::hint::spin_loop();
        }
        assert_eq!(successes.load(Ordering::Relaxed), 4);
        let used = KERNEL_ALLOCATOR.usage();
        assert_eq!(used.reserved, limit);
        assert_eq!(used.rejected, baseline.rejected + 12);
        assert_eq!(
            KERNEL_ALLOCATOR.set_limit(Some(limit - 1)),
            Err(MemoryError::LimitBelowUsage)
        );
        assert_eq!(KERNEL_ALLOCATOR.usage().limit, Some(limit));
        flags[1].store(true, Ordering::Release);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        for worker in workers {
            worker.join().unwrap();
        }
    });
}

#[test]
fn concurrent_limit_changes_preserve_the_reservation_bound() {
    isolated(|| {
        let start = Arc::new(AtomicBool::new(false));
        let exit = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(4);
        for _ in 0..4 {
            let start = start.clone();
            let exit = exit.clone();
            let ready = ready.clone();
            workers.push(std::thread::spawn(move || {
                ready.fetch_add(1, Ordering::Release);
                while !start.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                while !exit.load(Ordering::Acquire) {
                    let mut bytes = Vec::<u8>::new();
                    let _ = bytes.try_reserve_exact(1024);
                    std::hint::black_box(&bytes);
                    drop(bytes);
                }
            }));
        }
        while ready.load(Ordering::Acquire) != 4 {
            std::hint::spin_loop();
        }
        let baseline = KERNEL_ALLOCATOR.usage().reserved;
        KERNEL_ALLOCATOR.set_limit(Some(baseline + 8192)).unwrap();
        start.store(true, Ordering::Release);
        for iteration in 0..2000 {
            let headroom = if iteration % 2 == 0 { 1024 } else { 8192 };
            let _ = KERNEL_ALLOCATOR.set_limit(Some(baseline + headroom));
            let observed = KERNEL_ALLOCATOR.usage();
            assert!(observed.reserved <= observed.limit.unwrap());
        }
        exit.store(true, Ordering::Release);
        KERNEL_ALLOCATOR.set_limit(None).unwrap();
        for worker in workers {
            worker.join().unwrap();
        }
    });
}
