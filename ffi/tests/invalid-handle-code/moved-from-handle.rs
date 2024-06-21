use delta_kernel_ffi_macros::handle_descriptor;
use delta_kernel_ffi::handle::Handle;

pub struct Foo(u32);

#[handle_descriptor(target=Foo, mutable=true, sized=true)]
pub struct MutFoo;

fn main() {
    let s = Foo(0);
    let mut h: Handle<MutFoo> = Box::new(s).into();
    unsafe { h.drop_handle() };
    let _ = unsafe { h.into_inner() };
    let _ = unsafe { h.as_mut() };
}
