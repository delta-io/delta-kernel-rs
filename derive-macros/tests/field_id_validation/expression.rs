use delta_kernel_derive::ToSchema;

const MY_ID: i64 = 123;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = MY_ID]  // Should be literal, not expression
    field: String,
}

fn main() {}
