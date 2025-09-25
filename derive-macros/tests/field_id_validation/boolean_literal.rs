use delta_kernel_derive::ToSchema;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = true]  // Should be integer, not boolean
    field: String,
}

fn main() {}
