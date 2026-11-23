use std::alloc::{GlobalAlloc, Layout};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::memory::KERNEL_ALLOCATOR;

#[path = "../examples/declarative/mod.rs"]
mod declarative;

static CALLS: AtomicUsize = AtomicUsize::new(0);
static FAIL_AT: AtomicUsize = AtomicUsize::new(0);
static REJECTED: AtomicUsize = AtomicUsize::new(0);
static PROBE: Probe = Probe;

struct Probe;

// SAFETY: Successful storage and layouts are forwarded unchanged to Kernel's allocator.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let call = CALLS.fetch_add(1, Ordering::Relaxed) + 1;
        if call == FAIL_AT.load(Ordering::Relaxed) {
            REJECTED.fetch_add(1, Ordering::Relaxed);
            return std::ptr::null_mut();
        }
        unsafe { KERNEL_ALLOCATOR.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { KERNEL_ALLOCATOR.dealloc(ptr, layout) }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let phase = args.first().ok_or("missing phase")?.as_str();
    let shape = args.get(1).ok_or("missing shape")?.as_str();
    let fail_at: usize = args.get(2).ok_or("missing failure ordinal")?.parse()?;
    arrow_buffer::alloc::install_default_allocator(&PROBE)?;
    KERNEL_ALLOCATOR.set_limit(None)?;
    if phase == "lifecycle" {
        return lifecycle_probe(shape, fail_at);
    }
    let baseline = KERNEL_ALLOCATOR.usage().reserved;
    let (data_type, value) = match shape {
        "string" => (DataType::Utf8, serde_json::json!("x".repeat(4096))),
        "primitive" => (DataType::Int64, serde_json::json!(42)),
        "boolean" => (DataType::Boolean, serde_json::json!(true)),
        "nested" => (
            DataType::Struct(
                vec![
                    Arc::new(Field::new("text", DataType::Utf8, true)),
                    Arc::new(Field::new("count", DataType::Int64, true)),
                ]
                .into(),
            ),
            serde_json::json!({"text":"nested value", "count":42}),
        ),
        "list" => (
            DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
            serde_json::json!([1, null, 3, 4, 5, 6, 7, 8, 9, 10]),
        ),
        "map" => (
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new("value", DataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            serde_json::json!({"first":"value", "second":null}),
        ),
        _ => return Err("unknown shape".into()),
    };
    let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
    let rows = [
        serde_json::json!({"value":value}),
        serde_json::json!({"value":null}),
    ];
    let input = rows
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let run = || -> Result<(), ArrowError> {
        if phase == "initialize" {
            arm(fail_at);
        }
        let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
        if phase == "decode" {
            arm(fail_at);
        }
        if phase == "serialize" {
            arm(fail_at);
            decoder.serialize(&rows)?;
        } else {
            assert_eq!(decoder.decode(input.as_bytes())?, input.len());
        }
        if phase == "flush" {
            arm(fail_at);
        }
        let output = decoder
            .flush()?
            .ok_or_else(|| ArrowError::JsonError("missing batch".into()))?;
        assert_eq!(output.num_rows(), 2);
        Ok(())
    };
    let result = run();
    let calls = CALLS.load(Ordering::Relaxed);
    let rejected = REJECTED.load(Ordering::Relaxed);
    FAIL_AT.store(0, Ordering::Relaxed);
    if rejected != 0 {
        assert!(
            matches!(result, Err(ArrowError::MemoryError(_))),
            "{result:?}"
        );
    } else {
        result?;
    }
    assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    println!(
        "{}",
        serde_json::json!({"phase":phase,"shape":shape,"ordinal":fail_at,"calls":calls,"injected":rejected == 1,"ok":true})
    );
    Ok(())
}

fn arm(ordinal: usize) {
    CALLS.store(0, Ordering::Relaxed);
    FAIL_AT.store(ordinal, Ordering::Relaxed);
}

fn lifecycle_probe(root: &str, fail_at: usize) -> Result<(), Box<dyn Error>> {
    drop(declarative::run(root, &mut |_| {})?);
    let baseline = KERNEL_ALLOCATOR.usage().reserved;
    arm(fail_at);
    let result = declarative::run(root, &mut |_| {});
    let calls = CALLS.load(Ordering::Relaxed);
    let rejected = REJECTED.load(Ordering::Relaxed);
    FAIL_AT.store(0, Ordering::Relaxed);
    if rejected != 0 {
        assert!(
            matches!(&result, Err(delta_kernel::Error::Kernel(error))
            if matches!(error.without_backtrace(), delta_kernel::KernelError::Memory(_)
                | delta_kernel::KernelError::Arrow(ArrowError::MemoryError(_)))),
            "lifecycle did not propagate a typed allocation error: {}",
            result
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default()
        );
    } else {
        drop(result?);
    }
    assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    drop(declarative::run(root, &mut |_| {})?);
    assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    println!(
        "{}",
        serde_json::json!({"phase":"lifecycle", "shape":"checkpoint", "ordinal":fail_at,
        "calls":calls, "injected":rejected == 1, "recovered":true, "ok":true})
    );
    Ok(())
}
