use delta_kernel_derive::ToSchema;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = 123.45]  // Should be integer, not float
    field: String,
}

fn main() {}
