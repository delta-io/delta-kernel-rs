//! Benchmark for expression evaluation performance with the default engine.
//!
//! You can run this benchmark with `cargo bench --bench expression_bench`.
//!
//! To compare your changes vs. latest main, you can:
//! ```bash
//! # checkout baseline branch (upstream/main) and save as baseline
//! git checkout main # or upstream/main, another branch, etc.
//! cargo bench --bench expression_bench -- --save-baseline main
//!
//! # switch back to your changes, and compare against baseline
//! git checkout your-branch
//! cargo bench --bench expression_bench -- --baseline main
//! ```

use std::hint::black_box;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, StringBuilder, StructArray,
};
use delta_kernel::arrow::datatypes::{DataType, Field, Fields};
use delta_kernel::engine::arrow_expression::evaluate_expression::to_json;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Creates a test struct array with realistic data for benchmarking.
fn create_test_struct_array(num_rows: usize) -> StructArray {
    let mut id_builder = Int32Builder::with_capacity(num_rows);
    let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
    let mut score_builder = Float64Builder::with_capacity(num_rows);
    let mut active_builder = BooleanBuilder::with_capacity(num_rows);

    for i in 0..num_rows {
        id_builder.append_value(i as i32);
        name_builder.append_value(format!("user_{i}"));
        score_builder.append_value((i as f64) * 0.1 + 100.0);
        active_builder.append_value(i % 3 != 0);
    }

    let fields = Fields::from(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("score", DataType::Float64, false)),
        Arc::new(Field::new("active", DataType::Boolean, false)),
    ]);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(name_builder.finish()),
        Arc::new(score_builder.finish()),
        Arc::new(active_builder.finish()),
    ];

    StructArray::new(fields, arrays, None)
}

/// Creates a simple struct array with fewer fields for lightweight benchmarking.
fn create_simple_struct_array(num_rows: usize) -> StructArray {
    let mut id_builder = Int32Builder::with_capacity(num_rows);
    let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 10);

    for i in 0..num_rows {
        id_builder.append_value(i as i32);
        name_builder.append_value(format!("item_{i}"));
    }

    let fields = Fields::from(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
    ]);

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(name_builder.finish()),
    ];

    StructArray::new(fields, arrays, None)
}

/// Creates a nested struct array for complex JSON benchmarking.
fn create_nested_struct_array(num_rows: usize) -> StructArray {
    // Create inner struct
    let mut inner_int_builder = Int32Builder::with_capacity(num_rows);
    let mut inner_string_builder = StringBuilder::with_capacity(num_rows, num_rows * 15);

    for i in 0..num_rows {
        inner_int_builder.append_value(i as i32 * 10);
        inner_string_builder.append_value(format!("inner_{i}"));
    }

    let inner_fields = Fields::from(vec![
        Arc::new(Field::new("inner_int", DataType::Int32, true)),
        Arc::new(Field::new("inner_string", DataType::Utf8, true)),
    ]);

    let inner_arrays: Vec<ArrayRef> = vec![
        Arc::new(inner_int_builder.finish()),
        Arc::new(inner_string_builder.finish()),
    ];

    let inner_struct = Arc::new(StructArray::new(inner_fields.clone(), inner_arrays, None));

    // Create outer struct
    let mut outer_id_builder = Int32Builder::with_capacity(num_rows);
    for i in 0..num_rows {
        outer_id_builder.append_value(i as i32);
    }

    let fields = Fields::from(vec![
        Arc::new(Field::new("outer_id", DataType::Int32, false)),
        Arc::new(Field::new(
            "nested_struct",
            DataType::Struct(inner_fields),
            true,
        )),
    ]);

    let arrays: Vec<ArrayRef> = vec![Arc::new(outer_id_builder.finish()), inner_struct];

    StructArray::new(fields, arrays, None)
}

fn to_json_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("to_json");

    // Test different sizes for scalability analysis
    let test_sizes = [100, 1_000, 10_000, 100_000, 1_000_000];

    for &size in &test_sizes {
        group.throughput(Throughput::Elements(size as u64));

        // Benchmark simple struct array
        let simple_struct = create_simple_struct_array(size);
        group.bench_with_input(
            BenchmarkId::new("simple_struct", size),
            &simple_struct,
            |b, struct_array| {
                b.iter(|| {
                    let result = to_json(black_box(struct_array));
                    black_box(result).unwrap()
                })
            },
        );

        // Benchmark complex struct array
        let complex_struct = create_test_struct_array(size);
        group.bench_with_input(
            BenchmarkId::new("complex_struct", size),
            &complex_struct,
            |b, struct_array| {
                b.iter(|| {
                    let result = to_json(black_box(struct_array));
                    black_box(result).unwrap()
                })
            },
        );

        // Benchmark nested struct array
        let nested_struct = create_nested_struct_array(size);
        group.bench_with_input(
            BenchmarkId::new("nested_struct", size),
            &nested_struct,
            |b, struct_array| {
                b.iter(|| {
                    let result = to_json(black_box(struct_array));
                    black_box(result).unwrap()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, to_json_benchmark);
criterion_main!(benches);
