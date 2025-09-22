use delta_kernel_derive::ToSchema;

// Dummy types to avoid dependency issues in test
pub trait ToSchema {
    fn to_schema() -> StructType;
}

pub struct StructType;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = 18446744073709551616]  // This is u64::MAX + 1, too large for i64
    field: String,
}

fn main() {}
