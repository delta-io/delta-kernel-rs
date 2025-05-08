/// A [`std::result::Result`] that has the engine [`Error`] as the error variant
pub type EngineResult<T, E = Error> = std::result::Result<T, E>;

#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {}
