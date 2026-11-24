#[path = "../examples/kernel_lifecycle.rs"]
mod lifecycle;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    lifecycle::main()
}
