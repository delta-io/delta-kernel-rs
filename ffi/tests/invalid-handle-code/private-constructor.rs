use delta_kernel_ffi_macros::handle_descriptor;
use delta_kernel_ffi::handle::Handle;

pub struct Foo(u32);

#[handle_descriptor(target=Foo, mutable=false, sized=true)]
pub struct SharedFoo;

fn main() {
    let _: Handle<SharedFoo> = Handle { ptr: std::ptr::NonNull::dangling() };
}
