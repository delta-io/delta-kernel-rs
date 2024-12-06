use std::iter;

use itertools::Either;
use roaring::RoaringTreemap;
use url::Url;

use super::scan_file::{CdfScanFileType, ResolvedCdfScanFile, UnresolvedCdfScanFile};
use crate::actions::deletion_vector::selection_treemap_to_bools;
use crate::table_changes::scan_file::CdfScanFile;
use crate::{DeltaResult, Engine, Error};

/// Resolves the deletion vectors for an [`UnresolvedCdfScanFile`]. This function handles two
/// types of `CdfScanFile`s:
/// 1. The first case is a [`CdfScanFile`] paired with a remove deletion vector. The `scan_type`
///    must be [`CdfScanFileType::Add`]. In this case, both the add and remove deletion vectors are
///    read if they exist. Then, we find the set of rows in the scan file that have been added, and
///    the set of rows that have been removed. The set of removed rows (if any) will be represented
///    by a [`ResolvedCdfScanFile`] with `scan_type` = [`CdfScanFileType::Remove`]. The set of
///    added rows (if any) will be represented by a [`ResolvedCdfScanFile`] with `scan_type` =
///    [`CdfScanFileType::Add`].
///
///    Note: We allow the possibility for there to be both added rows, and removed rows for a
///    single add/remove pair.
/// 2. The second case handles all other add, remove, and cdc [`CdfScanFile`]s. These will simply
///    have the deletion vector read (if present), and each is converted into a [`ResolvedCdfScanFile`].
///    No changes are made to the `scan_type`.
#[allow(unused)]
pub(crate) fn resolve_scan_file_dv(
    engine: &dyn Engine,
    table_root: &Url,
    scan_file: UnresolvedCdfScanFile,
) -> DeltaResult<impl Iterator<Item = ResolvedCdfScanFile>> {
    let UnresolvedCdfScanFile {
        scan_file,
        remove_dvs,
    } = scan_file;
    let paired_rm_dv = remove_dvs.get(&scan_file.path);
    match (&scan_file.scan_type, paired_rm_dv) {
        (CdfScanFileType::Add, Some(rm_dv)) => {
            // Helper function to convert a treemap to a [`ResolvedCdfScanFile`]. The `scan_type`
            // of the [`ResolvedCdfScanFile`] is set to `out_type` This returns an empty iterator
            // if nothing is selected.
            fn treemap_to_iter(
                selection_treemap: RoaringTreemap,
                mut scan_file: CdfScanFile,
                out_type: CdfScanFileType,
            ) -> impl Iterator<Item = ResolvedCdfScanFile> {
                if selection_treemap.is_empty() {
                    // Nothing has been selected, we do not read this data file
                    Either::Left(iter::empty())
                } else {
                    let added_dv = selection_treemap_to_bools(selection_treemap);
                    scan_file.scan_type = out_type;

                    Either::Right(iter::once(ResolvedCdfScanFile {
                        scan_file,
                        selection_vector: Some(added_dv),
                    }))
                }
            }

            // Retrieve the deletion vector from the add action and remove action
            let add_dv = scan_file
                .dv_info
                .get_treemap(engine, table_root)?
                .unwrap_or_else(Default::default);
            let rm_dv = rm_dv
                .get_treemap(engine, table_root)?
                .unwrap_or_else(Default::default);

            // Here we show how deletion vectors are resolved. Note that logically the `rm_dv` is the
            // beginning state of the commit, and `add_dv` is the final state of the commit. In
            // other words the dv went from being `rm_dv` to become `add_dv`. We use a motivating
            // example to explain the cases:
            //  rm_dv  = [1, 1, 0]
            // - add_dv = [0, 1, 1]
            //
            // The result of this commit is:
            // - row 0 is restored
            // - row 1 is unchanged
            // - row 2 is deleted
            //
            //  # Insertion Selection Vector
            //  The selection vector of add rows is calculated using `rm_dv - add_dv`. These rows went
            //  from 1 (deleted) in `rm_dv` to 0 (restored) in the `add_dv`. All unchanged rows will remain 0.
            //  Applying this to our deletion vectors:
            //  rm_dv - add_dv =
            //      [1, 1, 0]
            //    - [0, 1, 1]
            //    = [1, 0, 0]
            //  The selection vector shows that row 0 was inserted
            //
            //  # Deletion Selection Vector
            //  The selection vector of deleted rows is calculated using `add_dv - rm_dv`. These rows went
            //  from 0 (present) in `rm_dv` to 1 (deleted) in the `add_dv`. All unchanged rows will remain 0.
            //  Applying this to our deletion vectors:
            //  add_dv - rm_dv =
            //      [0, 1, 1]
            //    - [1, 1, 0]
            //    = [0, 0, 1]
            //  The selection vector shows that row 2 was deleted
            let added_selection_treemap = &rm_dv - &add_dv;
            let removed_selection_treemap = add_dv - rm_dv;

            // Generate the [`ResolvedCdfScanFile`] for remove if there are rows selected
            let removed = treemap_to_iter(
                removed_selection_treemap,
                scan_file.clone(),
                CdfScanFileType::Remove,
            );

            // Generate the [`ResolvedCdfScanFile`] for add if there are rows selected
            let added = treemap_to_iter(added_selection_treemap, scan_file, CdfScanFileType::Add);

            Ok(Either::Right(added.chain(removed)))
        }
        (_, Some(_)) => Err(Error::generic(
            "Remove DV should only match to an add action!",
        )),
        (_, None) => {
            let selection_vector = scan_file.dv_info.get_selection_vector(engine, table_root)?;
            Ok(Either::Left(iter::once(ResolvedCdfScanFile {
                scan_file,
                selection_vector,
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use itertools::Itertools;

    use crate::{
        actions::deletion_vector::DeletionVectorDescriptor,
        engine::sync::SyncEngine,
        scan::state::DvInfo,
        table_changes::scan_file::{CdfScanFile, CdfScanFileType, UnresolvedCdfScanFile},
    };

    use super::resolve_scan_file_dv;

    #[test]
    fn add_with_dv() {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let table_root = url::Url::from_directory_path(path).unwrap();

        let commit_version = 42_i64;
        let commit_timestamp = 1234_i64;
        let deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        });
        let path = "fake_path".to_string();
        let dv_info = DvInfo { deletion_vector };
        let scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Add,
            path: path.clone(),
            dv_info,
            partition_values: HashMap::new(),
            commit_version,
            commit_timestamp,
        };

        let rm_dv = DvInfo {
            deletion_vector: None,
        };
        let input = UnresolvedCdfScanFile {
            scan_file,
            remove_dvs: Arc::new(HashMap::from([(path.clone(), rm_dv)])),
        };

        // Remove: None deleted
        // Add: DV with 0th and 9th bit set (ie deleted)
        let mut expected_sv = vec![false; 10];
        expected_sv[0] = true;
        expected_sv[9] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, input)
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

        let commit_version = 42_i64;
        let commit_timestamp = 1234_i64;

        let path = "fake_path".to_string();
        let dv_info = DvInfo {
            deletion_vector: None,
        };
        let scan_file = CdfScanFile {
            scan_type: CdfScanFileType::Add,
            path: path.clone(),
            dv_info,
            partition_values: HashMap::new(),
            commit_version,
            commit_timestamp,
        };

        let deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        });
        let rm_dv = DvInfo { deletion_vector };
        let input = UnresolvedCdfScanFile {
            scan_file,
            remove_dvs: Arc::new(HashMap::from([(path.clone(), rm_dv)])),
        };

        // Remove: DV with 0th and 9th bit set (ie deleted)
        // Add: No rows deleted
        let mut expected_sv = vec![false; 10];
        expected_sv[0] = true;
        expected_sv[9] = true;
        let resolved = resolve_scan_file_dv(&engine, &table_root, input)
            .unwrap()
            .map(|file| (file.scan_file.scan_type, file.selection_vector))
            .collect_vec();
        assert_eq!(resolved, vec![(CdfScanFileType::Add, Some(expected_sv))]);
    }
}
