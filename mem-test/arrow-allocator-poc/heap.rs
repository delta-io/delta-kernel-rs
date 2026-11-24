#[path = "../examples/kernel_lifecycle.rs"]
mod lifecycle;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    arrow_buffer::alloc::install_default_allocator(&delta_kernel::memory::KERNEL_ALLOCATOR)?;
    lifecycle::main()
}
