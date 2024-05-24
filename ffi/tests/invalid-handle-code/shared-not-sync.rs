use delta_kernel_ffi_macros::handle_descriptor;
use delta_kernel_ffi::handle::Handle;
use std::sync::Arc;

pub struct NotSync(*mut u32);

unsafe impl Send for NotSync {}

#[handle_descriptor(target=NotSync, mutable=false, sized=true)]
pub struct SharedNotSync;

fn main() {
    let s = NotSync(std::ptr::null_mut());
    let h: Handle<SharedNotSync> = Arc::new(s).into();
    let r = h.clone_as_arc();
    let h = h.clone_handle();
}
