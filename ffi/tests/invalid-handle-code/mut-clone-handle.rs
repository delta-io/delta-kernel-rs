use delta_kernel_ffi_macros::handle_descriptor;
use delta_kernel_ffi::handle::Handle;
use std::sync::Arc;

pub struct Foo(u32);

#[handle_descriptor(target=Foo, mutable=true, sized=true)]
pub struct MutFoo;

fn main() {
    let s = Foo(0);
    let h: Handle<MutFoo> = Arc::new(s).into();
    let r = h.clone_as_arc();
    let h = h.clone_handle();
}
