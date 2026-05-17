//! Macros for KDF implementations.
//!
//! The trivial `Kdf` supertrait methods (`kdf_id` + `finish`) are identical
//! across every concrete KDF — just a distinct string and `Box::new(*self)`.
//! [`impl_kdf!`] collapses them to a single line, leaving each KDF file to
//! contain only the real per-KDF logic (the `apply` body, `KdfOutput`
//! reducer, and `RowVisitor` column list + row loop).

/// Generate `impl Kdf for $Type` with the given `$kdf_id` enum value and the
/// canonical `finish(self) = Box::new(*self)`.
///
/// ```ignore
/// use delta_kernel::plans::kdf::token::ConsumerKdfId;
/// impl_kdf!(CheckpointHintReader, ConsumerKdfId::CheckpointHint);
/// ```
#[macro_export]
macro_rules! impl_kdf {
    ($ty:ty, $id:expr) => {
        impl $crate::plans::kdf::Kdf for $ty {
            fn kdf_id(&self) -> $crate::plans::kdf::token::ConsumerKdfId {
                $id
            }
            fn finish(self: Box<Self>) -> Box<dyn ::std::any::Any + ::std::marker::Send> {
                Box::new(*self)
            }
        }
    };
}
