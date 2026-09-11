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
//!
//! Filter with `float_predicates` for signed-zero comparisons or `dictionary_nullable` to compare
//! dictionary decoding with plain arrays containing the same logical values.

use std::hint::black_box;
use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use delta_kernel::arrow::array::{
    ArrayRef, BooleanBuilder, DictionaryArray, Float32Array, Float64Array, Float64Builder,
    Int32Array, Int32Builder, RecordBatch, StringBuilder, StructArray,
};
use delta_kernel::arrow::compute::cast;
use delta_kernel::arrow::datatypes::{DataType, Field, Fields, Int32Type};
use delta_kernel::engine::arrow_expression::evaluate_expression::{evaluate_predicate, to_json};
use delta_kernel::expressions::{col, lit, Expression};

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

macro_rules! float_predicate_benchmark {
    ($name:ident, $float:ty, $array:ty) => {
        fn $name(c: &mut Criterion) {
            let mut group = c.benchmark_group(format!("float_predicates/{}", stringify!($float)));
            let special_values: [$float; 17] = [
                -0.0,
                0.0,
                -1.0,
                1.0,
                <$float>::INFINITY,
                <$float>::NEG_INFINITY,
                <$float>::MIN,
                <$float>::MAX,
                <$float>::MIN_POSITIVE,
                -<$float>::MIN_POSITIVE,
                <$float>::from_bits(1),
                -<$float>::from_bits(1),
                <$float>::NAN,
                -<$float>::NAN,
                <$float>::from_bits(<$float>::NAN.to_bits() + 1),
                <$float>::from_bits(<$float>::INFINITY.to_bits() + 1),
                -<$float>::from_bits(<$float>::INFINITY.to_bits() + 1),
            ];
            for special in [false, true] {
                let dataset = if special {
                    "nullable_special"
                } else {
                    "finite"
                };
                for num_rows in [4_096, 65_536] {
                    let mut seed = 0x3141592653589793_u64;
                    let mut make_array = |offset: usize, null_every: usize| -> ArrayRef {
                        let values = (0..num_rows)
                            .map(|index| {
                                let value = if special && index % 4 == 0 {
                                    special_values[(index / 4 + offset) % special_values.len()]
                                } else {
                                    seed ^= seed << 13;
                                    seed ^= seed >> 7;
                                    seed ^= seed << 17;
                                    (seed % 200_001) as $float - 100_000.0
                                };
                                if special && index % null_every == 0 {
                                    None
                                } else {
                                    Some(value)
                                }
                            })
                            .collect::<Vec<_>>();
                        Arc::new(<$array>::from(values))
                    };
                    let batch = RecordBatch::try_from_iter([
                        ("left", make_array(0, 7)),
                        ("right", make_array(1, 11)),
                    ])
                    .unwrap();
                    bench_float_predicates(&mut group, dataset, &batch, lit(0.0 as $float));
                }
            }

            let values = special_values
                .into_iter()
                .map(Some)
                .chain([None])
                .collect::<Vec<_>>();
            let (plain, dictionary) = float_dictionary_batches(Arc::new(<$array>::from(values)));
            for (representation, batch) in [("plain", plain), ("encoded", dictionary)] {
                bench_float_predicates(
                    &mut group,
                    &format!("dictionary_nullable/{representation}"),
                    &batch,
                    lit(0.0 as $float),
                );
            }
            group.finish();
        }
    };
}

float_predicate_benchmark!(float32_predicate_benchmark, f32, Float32Array);
float_predicate_benchmark!(float64_predicate_benchmark, f64, Float64Array);

fn bench_float_predicates(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    batch: &RecordBatch,
    literal: Expression,
) {
    group.throughput(Throughput::Elements(batch.num_rows() as u64));
    for (shape, right) in [("literal", literal), ("column", col!("right"))] {
        for (operation, predicate) in [
            ("eq", col!("left").eq(right.clone())),
            ("lt", col!("left").lt(right.clone())),
            ("distinct", col!("left").distinct(right)),
        ] {
            group.bench_function(
                BenchmarkId::new(format!("{name}/{shape}/{operation}"), batch.num_rows()),
                |b| {
                    b.iter(|| {
                        evaluate_predicate(black_box(&predicate), black_box(batch), false).unwrap()
                    })
                },
            );
        }
    }
}

fn float_dictionary_batches(values: ArrayRef) -> (RecordBatch, RecordBatch) {
    let make_dictionary = |offset: usize, null_every: usize| -> ArrayRef {
        let keys = Int32Array::from_iter((0..65_536).map(|index| {
            (index % null_every != 0).then_some(((index + offset) % values.len()) as i32)
        }));
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values.clone()).unwrap())
    };
    let left = make_dictionary(0, 7);
    let right = make_dictionary(1, 11);
    // Decode outside timing so plain and encoded cases differ only in their input representation.
    let plain = RecordBatch::try_from_iter([
        ("left", cast(&left, values.data_type()).unwrap()),
        ("right", cast(&right, values.data_type()).unwrap()),
    ])
    .unwrap();
    let dictionary = RecordBatch::try_from_iter([("left", left), ("right", right)]).unwrap();
    (plain, dictionary)
}

criterion_group!(
    benches,
    to_json_benchmark,
    float32_predicate_benchmark,
    float64_predicate_benchmark
);
criterion_main!(benches);
