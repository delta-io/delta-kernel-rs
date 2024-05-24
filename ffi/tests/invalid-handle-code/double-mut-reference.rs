use delta_kernel_ffi_macros::handle_descriptor;
use delta_kernel_ffi::handle::Handle;

pub struct Foo(u32);

#[handle_descriptor(target=Foo, mutable=true, sized=true)]
pub struct MutFoo;

fn main() {
    let s = Foo(0);
    let mut h: Handle<MutFoo> = Box::new(s).into();
    let r = unsafe { h.as_mut() };
    let _ = unsafe { h.as_mut() };
    let _ = unsafe { h.as_ref() };
    r.0 = 1;
}
