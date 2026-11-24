use std::error::Error;
use std::sync::Arc;

use arrow_buffer::alloc::{TryVecExt, try_vec};
use arrow_buffer::{Buffer, MutableBuffer};
use delta_kernel::arrow::array::{Array, Int64Array};
use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
use delta_kernel::arrow::error::ArrowError;
use delta_kernel::arrow::ffi::FFI_ArrowArray;
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::memory::KERNEL_ALLOCATOR;

#[global_allocator]
static ALLOCATOR: delta_kernel::memory::KernelAllocator = delta_kernel::memory::KernelAllocator::new();

type CheckResult = Result<(), Box<dyn Error>>;

fn main() -> CheckResult {
    std::thread::spawn(|| {}).join().unwrap();
    KERNEL_ALLOCATOR.set_limit(None)?;
    let case = std::env::args().nth(1).ok_or("missing check name")?;
    let baseline = KERNEL_ALLOCATOR.usage().reserved;
    match case.as_str() {
        "buffer_growth_and_release" => {
            let mut buffer = MutableBuffer::try_with_capacity(100)?;
            buffer.try_extend_from_slice(b"preserved after failed growth")?;
            assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline + 128);
            let ptr = buffer.as_ptr();
            KERNEL_ALLOCATOR.set_limit(Some(KERNEL_ALLOCATOR.usage().reserved))?;
            assert!(buffer.try_reserve(1024).is_err());
            assert_eq!(buffer.as_ptr(), ptr);
            assert_eq!(buffer.as_slice(), b"preserved after failed growth");
            KERNEL_ALLOCATOR.set_limit(None)?;
            let buffer = Buffer::from(buffer);
            let shared = buffer.slice(10);
            drop(buffer);
            assert!(KERNEL_ALLOCATOR.usage().reserved >= baseline + 128);
            KERNEL_ALLOCATOR.set_limit(None)?;
            std::thread::spawn(move || drop(shared))
                .join()
                .map_err(|_| "cross-thread release panicked")?;
        }
        "buffer_shrink" => {
            let mut buffer = MutableBuffer::try_with_capacity(1024)?;
            buffer.try_extend_from_slice(b"content")?;
            KERNEL_ALLOCATOR.set_limit(Some(baseline + 2048))?;
            buffer.try_shrink_to_fit()?;
            assert_eq!(buffer.as_slice(), b"content");
            assert_eq!(
                KERNEL_ALLOCATOR.usage().reserved,
                baseline + buffer.capacity()
            );
        }
        "imported_vector" => {
            let values = vec![1u64, 2, 3];
            let ptr = values.as_ptr();
            let buffer = Buffer::from_vec(values);
            let values = buffer
                .into_vec::<u64>()
                .map_err(|_| "vector import lost ownership")?;
            assert_eq!(values.as_ptr(), ptr);
            assert_eq!(values, [1, 2, 3]);
        }
        "allocator_vector_release" => {
            let mut values = try_vec::<u64>(8)?;
            values.try_extend_from_slice(&[1, 2, 3])?;
            let pointer = values.as_ptr();
            assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline + 64);
            let buffer = Buffer::from_vec(values);
            assert_eq!(buffer.as_ptr(), pointer.cast());
            let buffer = buffer
                .into_vec::<u64>()
                .expect("standard global vector lost ownership");
            assert_eq!(buffer.as_slice(), &[1, 2, 3]);
            assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline + 64);
            std::thread::spawn(move || drop(buffer))
                .join()
                .map_err(|_| "cross-thread vector release panicked")?;
        }
        "ffi_array_release" => {
            let mut buffer = MutableBuffer::try_with_capacity(128)?;
            buffer.try_extend_from_slice(&[1_i64, 2, 3])?;
            let array = Int64Array::new(Buffer::from(buffer).into(), None);
            let mut exported = FFI_ArrowArray::new(&array.to_data());
            drop(array);
            let retained = KERNEL_ALLOCATOR.usage().reserved;
            assert!(retained >= baseline + 128);
            KERNEL_ALLOCATOR.set_limit(Some(retained))?;
            assert!(MutableBuffer::try_with_capacity(64).is_err());
            KERNEL_ALLOCATOR.set_limit(None)?;
            std::thread::spawn(move || {
                let release = exported.release().expect("missing C release callback");
                // SAFETY: exported owns this live Arrow C Data array and is released exactly once.
                unsafe { release(&mut exported) };
                assert!(exported.is_released());
            })
            .join()
            .map_err(|_| "FFI release panicked")?;
            assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
            let buffer = MutableBuffer::try_with_capacity(128)?;
            drop(buffer);
        }
        "json_decoder_initialization" => {
            let builder = ReaderBuilder::new(schema());
            KERNEL_ALLOCATOR.set_limit(Some(KERNEL_ALLOCATOR.usage().reserved))?;
            let result = builder.build_decoder();
            KERNEL_ALLOCATOR.set_limit(None)?;
            let error = result
                .err()
                .ok_or("decoder unexpectedly fit")?;
            assert!(matches!(error, ArrowError::MemoryError(_)), "{error}");
        }
        "json_tape_growth" => {
            let input = format!("{{\"value\":\"{}\"}}", "x".repeat(128 * 1024));
            let mut decoder = ReaderBuilder::new(schema()).build_decoder()?;
            let live = KERNEL_ALLOCATOR.usage().reserved;
            KERNEL_ALLOCATOR.set_limit(Some(live))?;
            let result = decoder.decode(input.as_bytes());
            KERNEL_ALLOCATOR.set_limit(None)?;
            let error = result.expect_err("growth unexpectedly fit");
            assert!(matches!(error, ArrowError::MemoryError(_)), "{error}");
            drop(decoder);
            assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline + input.capacity());
            KERNEL_ALLOCATOR.set_limit(None)?;
            let mut decoder = ReaderBuilder::new(schema()).build_decoder()?;
            assert_eq!(decoder.decode(input.as_bytes())?, input.len());
            assert_eq!(decoder.flush()?.ok_or("missing decoded row")?.num_rows(), 1);
        }
        "json_flush" => {
            let mut decoder = ReaderBuilder::new(schema()).build_decoder()?;
            decoder.decode(br#"{"value":"data"}"#)?;
            KERNEL_ALLOCATOR.set_limit(Some(KERNEL_ALLOCATOR.usage().reserved))?;
            let result = decoder.flush();
            KERNEL_ALLOCATOR.set_limit(None)?;
            let error = result.expect_err("output unexpectedly fit");
            assert!(matches!(error, ArrowError::MemoryError(_)), "{error}");
        }
        "json_serialize" => {
            let rows = [serde_json::json!({"value": "x".repeat(128 * 1024)})];
            let mut decoder = ReaderBuilder::new(schema()).build_decoder()?;
            KERNEL_ALLOCATOR.set_limit(Some(KERNEL_ALLOCATOR.usage().reserved))?;
            let result = decoder.serialize(&rows);
            KERNEL_ALLOCATOR.set_limit(None)?;
            let error = result.expect_err("serialization unexpectedly fit");
            assert!(matches!(error, ArrowError::MemoryError(_)), "{error}");
        }
        _ => return Err("unknown check".into()),
    }
    KERNEL_ALLOCATOR.set_limit(None)?;
    assert_eq!(KERNEL_ALLOCATOR.usage().reserved, baseline);
    println!(
        "{}",
        serde_json::json!({"case": case, "ok": true, "usage": KERNEL_ALLOCATOR.usage().reserved})
    );
    Ok(())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
}
