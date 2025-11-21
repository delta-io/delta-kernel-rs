use url::Url;

use super::scan_file::CdfScanFileType;
use crate::actions::deletion_vector::{deletion_treemap_to_bools, selection_treemap_to_bools};
use crate::table_changes::scan_file::CdfScanFile;
use crate::{DeltaResult, Engine, Error};

/// A [`CdfScanFile`] with its associated `selection_vector`. The `scan_type` is resolved to
/// match the `_change_type` that its rows will have in the change data feed.
pub(crate) struct ResolvedCdfScanFile {
    /// The scan file that holds the path the data file to be read. The `scan_type` field is
    /// resolved to the `_change_type` of the rows for this data file.
    pub(crate) scan_file: CdfScanFile,
    /// Optional vector of bools. If `selection_vector[i] = true`, then that row must be included
    /// in the CDF output. Otherwise the row must be filtered out. The vector may be shorter than
    /// the data file. In this case, all the remaining rows are *not* selected. If `selection_vector`
    /// is `None`, then all rows are selected.
    pub(crate) selection_vector: Option<Vec<bool>>,
}

/// Resolves the deletion vectors for a [`CdfScanFile`]. This function handles two
/// types of `CdfScanFile`s:
/// 1. The first case is a [`CdfScanFile`] paired with a remove deletion vector. The `scan_type`
///    must be [`CdfScanFileType::Add`]. In this case, both the add and remove deletion vectors are
///    read if they exist. Then, we find the set of rows that have been added and rows that
///    have been removed. The set of removed rows (if any) will be represented by a
///    [`ResolvedCdfScanFile`] with `scan_type` = [`CdfScanFileType::Remove`]. The set of added rows
///    (if any) will be  represented by a [`ResolvedCdfScanFile`] with `scan_type` = [`CdfScanFileType::Add`].
///
///    Note: We allow the possibility for there to be both added rows and removed rows for a
///    single add/remove pair.
/// 2. The second case handles all other add, remove, and cdc [`CdfScanFile`]s. These will simply
///    read the deletion vector (if present), and each is converted into a [`ResolvedCdfScanFile`].
///    No changes are made to the `scan_type`.
pub(crate) fn resolve_scan_file_dv(
    engine: &dyn Engine,
    table_root: &Url,
    scan_file: CdfScanFile,
) -> DeltaResult<impl Iterator<Item = ResolvedCdfScanFile>> {
    let add_dv = scan_file.dv_info.get_treemap(engine, table_root)?;
    let rm_dv = scan_file
        .remove_dv
        .as_ref()
        .map(|rm_dv| rm_dv.get_treemap(engine, table_root))
        .transpose()?;
    let (add_dv, rm_dv) = match (add_dv, rm_dv, &scan_file.scan_type) {
        (_, Some(_), CdfScanFileType::Remove) => {
            return Err(Error::generic(
                "CdfScanFile with type remove cannot have a remove deletion vector",
            ));
        }
        (_, Some(_), CdfScanFileType::Cdc) => {
            return Err(Error::generic(
                "CdfScanFile with type cdc cannot have a remove deletion vector",
            ));
        }
        (add_dv, Some(rm_dv), CdfScanFileType::Add) => {
            let add_dv = add_dv.unwrap_or_else(Default::default);
            let rm_dv = rm_dv.unwrap_or_else(Default::default);
            // Here we show how deletion vectors are resolved. Note that logically the `rm_dv` is the
            // beginning state of the commit, and `add_dv` is the final state of the commit. In
            // other words the dv went from being `rm_dv` to `add_dv`.
            //
            // ===== IMPORTANT =====
            // Both `rm_dv` and `add_dv` are deletion treemaps. We define two types of treemaps:
            //   - _Deletion_ treemaps  (denoted `Treemap_d`) store the indices of deleted rows.
            //     For instance, `Treemap_d(0, 2)` means that rows 0 and 2 are _deleted_. When
            //     converted to a vector of bools, it is equivalent to a deletion vector [1, 0, 1].
            //   - _Selection_ treemaps (denoted `Treemap_s`) store the indices of selected rows.
            //     `Treemap_s(0, 1)` means that rows 0 and 1 are _selected_. This is equivalent
            //     to the selection vector [1, 1, 0] when converted into a boolean vector.
            // In ordinary scans, only deletion treemaps are used. However in the case of deletion
            // vector pairs, we generate selection treemaps.
            //
            // We use a motivating example to explain the deletion vector resolution. We read
            // `rm_dv` and `add_dv`, and they are initialized to the empty map by default
            // if no deletion vector is given.
            //  rm_dv  = Treemap_d(0, 1)
            //  add_dv = Treemap_d(1, 2)
            //
            // The result of this commit is:
            // - row 0 is restored
            // - row 1 is unchanged (previously deleted)
            // - row 2 is newly deleted
            // Thus for this commit we must generate `Treemap_s(0)` for the added rows, and
            // `Treemap_s(2)` for deleted rows.
            //
            //  # Insertion Selection Treemap
            //  The selection vector of added rows is calculated using set subtraction over deletion
            //  treemaps `rm_dv - add_dv`. These rows went from set (deleted) in `rm_dv` to unset
            //  (restored) in the `add_dv`. All other rows are either cancelled in set subtraction,
            //  or were not selected in either treemap. Hence, they are not selected to
            //  be in the set of changed rows. Applying this to our deletion treemaps:
            //  rm_dv - add_dv =
            //      Treemap_d(0, 1)
            //    - Treemap_d(1, 2)
            //    = Treemap_s(0)
            //  The selection treemap shows that row 0 was inserted
            //
            //  # Deletion Selection Treemap
            //  The selection vector of deleted rows is calculated using `add_dv - rm_dv`. These rows went
            //  from unset (present) in `rm_dv` to set (deleted) in the `add_dv`. Once again, all
            //  other rows are either cancelled in set subtraction, or were not set in either treemap.
            //  Applying this to our deletion vectors:
            //  add_dv - rm_dv =
            //      Treemap_d(1, 2)
            //    - Treemap_d(0, 1)
            //    = Treemap_s(2)
            //  The selection treemap shows that row 2 was deleted
            //
            //  # Conversion to Selection Vector
            //  The selection treemap is converted to a selection vector by setting the bits:
            //      Treemap_s(0) => [true]
            //      Treemap_s(2) => [false, false, true]
            //  All other rows are unselected (false).
            let adds = &rm_dv - &add_dv;
            let removes = add_dv - rm_dv;

            let adds = (!adds.is_empty()).then_some(adds);
            let removes = (!removes.is_empty()).then_some(removes);
            (adds, removes)
        }
        (add_dv, None, CdfScanFileType::Add | CdfScanFileType::Cdc) => {
            (Some(add_dv.unwrap_or_else(Default::default)), None)
        }
        (rm_dv, None, CdfScanFileType::Remove) => {
            (None, Some(rm_dv.unwrap_or_else(Default::default)))
        }
    };
    let treemap_to_bools = if scan_file.remove_dv.is_some() {
        selection_treemap_to_bools
    } else {
        deletion_treemap_to_bools
    };

    let resolve = |scan_file, sv: Vec<bool>| ResolvedCdfScanFile {
        scan_file,
        selection_vector: (!sv.is_empty()).then_some(sv),
    };

    let removes = rm_dv.map(treemap_to_bools).map(|sv| {
        let scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Remove,
            ..scan_file.clone()
        };
        resolve(scan_file, sv)
    });
    let adds = add_dv
        .map(treemap_to_bools)
        .map(|sv| resolve(scan_file, sv));
    Ok([removes, adds].into_iter().flatten())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write, path::PathBuf};

    use bytes::BufMut;
    use itertools::Itertools;
    use roaring::RoaringTreemap;

    use crate::{
        actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType},
        engine::sync::SyncEngine,
        scan::state::DvInfo,
        table_changes::scan_file::{CdfScanFile, CdfScanFileType},
        Error,
    };

    use super::resolve_scan_file_dv;

    fn treemap_to_dv_descriptor(map: RoaringTreemap) -> DeletionVectorDescriptor {
        let buf = Vec::new();
        let mut writer = buf.writer();
        let magic: u32 = 1681511377;
        writer.write_all(&magic.to_le_bytes()).unwrap();
        map.serialize_into(&mut writer).unwrap();
        let buf = writer.into_inner();
        let inline_dv = z85::encode(&buf);
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::Inline,
            path_or_inline_dv: inline_dv,
            offset: None,
            size_in_bytes: buf.len().try_into().unwrap(),
            cardinality: map.len().try_into().unwrap(),
        }
    }

    fn get_scan_file(
        scan_type: CdfScanFileType,
        dv_info: DvInfo,
        remove_dv: Option<DvInfo>,
    ) -> CdfScanFile {
        CdfScanFile {
            scan_type,
            path: "fake_path".to_string(),
            dv_info,
            remove_dv,
            partition_values: HashMap::new(),
            commit_version: 42,
            commit_timestamp: 1234,
        }
    }

    #[test]
    fn add_with_dv() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        });
        let dv_info = DvInfo { deletion_vector };
        let remove_dv = Some(Default::default());
        let scan_file = get_scan_file(CdfScanFileType::Add, dv_info, remove_dv);

        // Remove: None deleted
        // Add: DV with 0th and 9th bit set (ie deleted)
        let mut expected_sv = vec![false; 10];
        expected_sv[0] = true;
        expected_sv[9] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Remove, Some(expected_sv))]);
    }

    #[test]
    fn rm_with_dv() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        });

        let dv_info = Default::default();
        let remove_dv = Some(DvInfo { deletion_vector });
        let scan_file = get_scan_file(CdfScanFileType::Add, dv_info, remove_dv);

        // Remove: DV with 0th and 9th bit set (ie deleted)
        // Add: No rows deleted
        let mut expected_sv = vec![false; 10];
        expected_sv[0] = true;
        expected_sv[9] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Add, Some(expected_sv))]);
    }

    #[test]
    fn restore_subset() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let rm_dv = treemap_to_dv_descriptor(RoaringTreemap::from([0, 1, 4, 5]));
        let add_dv = treemap_to_dv_descriptor(RoaringTreemap::from([0, 5]));

        let dv_info = DvInfo::from(add_dv);
        let remove_dv = Some(DvInfo::from(rm_dv));
        let scan_file = get_scan_file(CdfScanFileType::Add, dv_info, remove_dv);

        let mut expected_sv = vec![false; 5];
        expected_sv[1] = true;
        expected_sv[4] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Add, Some(expected_sv))]);
    }
    #[test]
    fn delete_subset() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let rm_dv = treemap_to_dv_descriptor(RoaringTreemap::from([0, 5]));
        let add_dv = treemap_to_dv_descriptor(RoaringTreemap::from([0, 1, 4, 5]));

        let dv_info = DvInfo::from(add_dv);
        let remove_dv = Some(DvInfo::from(rm_dv));
        let scan_file = get_scan_file(CdfScanFileType::Add, dv_info, remove_dv);

        let mut expected_sv = vec![false; 5];
        expected_sv[1] = true;
        expected_sv[4] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Remove, Some(expected_sv))]);
    }

    #[test]
    fn adds_and_removes() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let rm_dv = treemap_to_dv_descriptor(RoaringTreemap::from([0, 2]));
        let add_dv = treemap_to_dv_descriptor(RoaringTreemap::from([0, 1]));

        let dv_info = DvInfo::from(add_dv);
        let remove_dv = Some(DvInfo::from(rm_dv));
        let scan_file = get_scan_file(CdfScanFileType::Add, dv_info, remove_dv);

        let mut rm_sv = vec![false; 2];
        rm_sv[1] = true;
        let mut add_sv = vec![false; 3];
        add_sv[2] = true;

        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(
            resolved,
            vec![
                (CdfScanFileType::Remove, Some(rm_sv)),
                (CdfScanFileType::Add, Some(add_sv))
            ]
        );
    }

    #[test]
    fn cdc_and_remove_with_remove_dv_fails() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let rm_dv = treemap_to_dv_descriptor(RoaringTreemap::from([0, 2]));

        let remove_dv = Some(DvInfo::from(rm_dv));
        let mut scan_file = get_scan_file(CdfScanFileType::Cdc, Default::default(), remove_dv);

        let expected_err =
            Error::generic("CdfScanFile with type cdc cannot have a remove deletion vector");

        let res = resolve_scan_file_dv(&engine, &table_root, scan_file.clone())
            .err()
            .unwrap();
        assert_eq!(res.to_string(), expected_err.to_string());

        scan_file.scan_type = CdfScanFileType::Remove;
        let expected_err =
            Error::generic("CdfScanFile with type remove cannot have a remove deletion vector");
        let res = resolve_scan_file_dv(&engine, &table_root, scan_file)
            .err()
            .unwrap();
        assert_eq!(res.to_string(), expected_err.to_string());
    }

    #[test]
    fn cdc_file_resolution() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let scan_file = get_scan_file(CdfScanFileType::Cdc, Default::default(), None);

        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file.clone())
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Cdc, None)]);
    }

    #[test]
    fn remove_file_resolution() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let scan_file = get_scan_file(CdfScanFileType::Remove, Default::default(), None);

        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file.clone())
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Remove, None)]);
    }
    #[test]
    fn add_file_no_dv_resolution() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let scan_file = get_scan_file(CdfScanFileType::Add, Default::default(), None);

        let resolved = resolve_scan_file_dv(&engine, &table_root, scan_file.clone())
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Add, None)]);
    }
}
