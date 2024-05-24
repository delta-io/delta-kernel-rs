use delta_kernel_ffi_macros::handle_descriptor;

pub struct NotSend(*mut u32);

#[handle_descriptor(target=NotSend, mutable=false, sized=true)]
pub struct SharedNotSend;

#[handle_descriptor(target=NotSend, mutable=true, sized=true)]
pub struct MutNotSend;

fn main() {}
