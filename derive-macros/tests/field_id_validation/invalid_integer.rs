use delta_kernel_derive::ToSchema;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = 18446744073709551616]  // This is u64::MAX + 1, too large for i64
    field: String,
}

fn main() {}
