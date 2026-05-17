//! `SidecarCollector` — consumer KDF that collects sidecar file references
//! from a V2 checkpoint manifest scan.
//!
//! Delegates row visiting to the existing
//! [`SidecarVisitor`](crate::actions::visitors::SidecarVisitor) and
//! resolves the captured sidecars against `log_root` into `Vec<FileMeta>`
//! via [`Sidecar::to_filemeta`](crate::actions::Sidecar::to_filemeta) in
//! [`KdfOutput::into_output`].

use url::Url;

use crate::actions::visitors::SidecarVisitor;
use crate::engine_data::RowVisitor;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::kdf::{ConsumerKdf, ConsumerKdfId, KdfControl, KdfOutput};
use crate::{delta_error, DeltaResult, EngineData, FileMeta};

/// Accumulates sidecars as batches stream in.
#[derive(Debug, Clone)]
pub struct SidecarCollector {
    log_root: Url,
    visitor: SidecarVisitor,
}

impl SidecarCollector {
    pub fn new(log_root: Url) -> Self {
        Self {
            log_root,
            visitor: SidecarVisitor::default(),
        }
    }
}

impl ConsumerKdf for SidecarCollector {
    fn kdf_id(&self) -> ConsumerKdfId {
        ConsumerKdfId::SidecarCollector
    }

    fn finish(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        Box::new(*self)
    }

    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        self.visitor.visit_rows_of(batch)?;
        Ok(KdfControl::Continue)
    }
}

impl KdfOutput for SidecarCollector {
    type Output = Vec<FileMeta>;

    fn into_output(self) -> Result<Self::Output, DeltaError> {
        let log_root = self.log_root;
        self.visitor
            .sidecars
            .into_iter()
            .map(|sidecar| {
                sidecar.to_filemeta(&log_root).map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "sidecar_collector.into_output: to_filemeta: {e}",
                    )
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::Sidecar;

    fn log_root() -> Url {
        Url::parse("file:///test/_delta_log/").unwrap()
    }

    #[test]
    fn kdf_id_is_stable() {
        let s = SidecarCollector::new(log_root());
        assert_eq!(
            crate::plans::kdf::ConsumerKdf::kdf_id(&s),
            ConsumerKdfId::SidecarCollector
        );
    }

    #[test]
    fn empty_partition_produces_empty_vec() {
        let out = SidecarCollector::new(log_root()).into_output().unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn into_output_resolves_sidecar_paths() {
        let mut s = SidecarCollector::new(log_root());
        s.visitor.sidecars.push(Sidecar {
            path: "sidecar1.parquet".to_string(),
            size_in_bytes: 1000,
            modification_time: 42,
            tags: None,
        });
        let out = s.into_output().unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0]
            .location
            .as_str()
            .ends_with("/_sidecars/sidecar1.parquet"));
        assert_eq!(out[0].size, 1000);
        assert_eq!(out[0].last_modified, 42);
    }
}
