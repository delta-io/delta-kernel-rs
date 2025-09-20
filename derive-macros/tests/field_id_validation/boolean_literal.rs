use delta_kernel_derive::ToSchema;

// Dummy types to avoid dependency issues in test
pub trait ToSchema {
    fn to_schema() -> StructType;
}

pub struct StructType;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = true]  // Should be integer, not boolean
    field: String,
}

fn main() {}
