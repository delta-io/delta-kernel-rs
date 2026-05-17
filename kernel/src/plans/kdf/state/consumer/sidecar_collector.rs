//! `SidecarCollector` — consumer KDF that collects sidecar file references
//! from a V2 checkpoint manifest scan.
//!
//! Shares [`Sidecar`]'s schema via [`Sidecar::to_schema`] + the `sidecar.*`
//! dotted column layout. Rows are kept as `(path, size, mtime)` triples
//! internally; [`KdfOutput::into_output`] resolves them against `log_root`
//! into `Vec<FileMeta>` via [`Sidecar::to_filemeta`].

use std::sync::LazyLock;

use url::Url;

use crate::actions::{Sidecar, SIDECAR_NAME};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::plans::errors::DeltaError;
use crate::plans::kdf::{
    take_single, ConsumerKdf, ConsumerKdfId, KdfControl, KdfOutput, KdfStateToken,
};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, ToSchema};
use crate::{DeltaResult, EngineData, FileMeta};

/// Raw row captured from a sidecar manifest.
#[derive(Debug, Clone)]
struct SidecarRow {
    path: String,
    size: i64,
    modification_time: i64,
}

/// Accumulates sidecar rows as batches stream in.
#[derive(Debug, Clone)]
pub struct SidecarCollector {
    log_root: Url,
    sidecars: Vec<SidecarRow>,
}

impl SidecarCollector {
    pub fn new(log_root: Url) -> Self {
        Self {
            log_root,
            sidecars: Vec::new(),
        }
    }
}

impl_kdf!(SidecarCollector, ConsumerKdfId::SidecarCollector);

impl ConsumerKdf for SidecarCollector {
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        self.visit_rows_of(batch)?;
        Ok(KdfControl::Continue)
    }
}

impl KdfOutput for SidecarCollector {
    type Output = Vec<FileMeta>;

    fn into_output(parts: Vec<Self>) -> Result<Self::Output, DeltaError> {
        let token = KdfStateToken::new(ConsumerKdfId::SidecarCollector);
        let single = take_single(parts, &token)?;
        let log_root = single.log_root;
        single
            .sidecars
            .into_iter()
            .map(|row| {
                let sidecar = Sidecar {
                    path: row.path,
                    size_in_bytes: row.size,
                    modification_time: row.modification_time,
                    tags: None,
                };
                sidecar.to_filemeta(&log_root).map_err(|e| {
                    crate::delta_error!(
                        crate::plans::errors::DeltaErrorCode::DeltaCommandInvariantViolation,
                        "sidecar_collector.into_output: to_filemeta: {e}",
                    )
                })
            })
            .collect()
    }
}

impl RowVisitor for SidecarCollector {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Sidecar::to_schema().leaves(SIDECAR_NAME));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            let Some(path) = getters[0].get_opt(i, "sidecar.path")? else {
                continue;
            };
            self.sidecars.push(SidecarRow {
                path,
                size: getters[1].get(i, "sidecar.sizeInBytes")?,
                modification_time: getters[2].get(i, "sidecar.modificationTime")?,
            });
            // `tags` (column index 3) is declared in the schema for the row
            // visitor but unused by FileMeta construction — tags do not
            // affect sidecar file resolution.
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_root() -> Url {
        Url::parse("file:///test/_delta_log/").unwrap()
    }

    #[test]
    fn kdf_id_is_stable() {
        let s = SidecarCollector::new(log_root());
        assert_eq!(
            crate::plans::kdf::Kdf::kdf_id(&s),
            ConsumerKdfId::SidecarCollector
        );
    }

    #[test]
    fn empty_partition_produces_empty_vec() {
        let out = SidecarCollector::into_output(vec![SidecarCollector::new(log_root())]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn into_output_resolves_sidecar_paths() {
        let mut s = SidecarCollector::new(log_root());
        s.sidecars.push(SidecarRow {
            path: "sidecar1.parquet".to_string(),
            size: 1000,
            modification_time: 42,
        });
        let out = SidecarCollector::into_output(vec![s]).unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0]
            .location
            .as_str()
            .ends_with("/_sidecars/sidecar1.parquet"));
        assert_eq!(out[0].size, 1000);
        assert_eq!(out[0].last_modified, 42);
    }
}
