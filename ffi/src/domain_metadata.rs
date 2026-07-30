use delta_kernel::snapshot::Snapshot;
use delta_kernel::DeltaResult;

use crate::error::{ExternResult, IntoExternResult};
use crate::expressions::kernel_visitor::NullTypeTag;
use crate::handle::Handle;
use crate::{
    kernel_string_slice, AllocateStringFn, ExternEngine, KernelStringSlice, NullableCvoid,
    OptionalValue, SharedExternEngine, SharedSnapshot, TryFromStringSlice,
};

/// Get the domain metadata as an optional string allocated by `AllocatedStringFn` for a specific
/// domain in this snapshot
///
/// # Safety
///
/// Caller is responsible for passing in a valid handle
#[no_mangle]
pub unsafe extern "C" fn get_domain_metadata(
    snapshot: Handle<SharedSnapshot>,
    domain: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
    allocate_fn: AllocateStringFn,
) -> ExternResult<NullableCvoid> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let domain = unsafe { String::try_from_slice(&domain) };

    get_domain_metadata_impl(snapshot, domain, engine, allocate_fn).into_extern_result(&engine)
}

fn get_domain_metadata_impl(
    snapshot: &Snapshot,
    domain: DeltaResult<String>,
    extern_engine: &dyn ExternEngine,
    allocate_fn: AllocateStringFn,
) -> DeltaResult<NullableCvoid> {
    Ok(snapshot
        .get_domain_metadata(&domain?, extern_engine.engine().as_ref())?
        .and_then(|config| allocate_fn(kernel_string_slice!(config))))
}

/// Signature of the callback invoked once per clustering column by
/// [`visit_clustering_columns`], in the order the columns appear in the `delta.clustering`
/// domain. Each invocation describes one column:
///   - `logical_column`: the column path resolved against the table schema. When column mapping is
///     enabled this is the logical name, not the physical identifier stored in the domain.
///   - `physical_column`: the column path as stored in the domain. Use this to correlate a
///     clustering column with per-file statistics, which are keyed on physical names.
///   - `type_tag`: the column's data type, encoded with the same tag values as
///     `visit_expression_literal_null`. Complex types (struct, array, map, variant) and void report
///     the non-primitive sentinel (255); use a schema visitor for their full details.
///   - `precision` / `scale`: meaningful only when `type_tag` is the `decimal` tag (12); zero
///     otherwise.
///
/// Both column paths are dotted strings with individual fields backtick-escaped when they
/// contain special characters, losslessly parseable back into a multi-part column reference.
/// Note that column-mapping physical identifiers (e.g. `col-<uuid>`) contain hyphens and so
/// arrive backtick-quoted; parse the path rather than comparing the raw string.
///
/// Resolving a domain-listed column against the schema matches names case-sensitively, so a
/// domain entry whose casing differs from the schema's is reported as an error rather than
/// resolved. Java kernel matches case-insensitively here.
pub type ClusteringColumnVisitor = extern "C" fn(
    engine_context: NullableCvoid,
    logical_column: KernelStringSlice,
    physical_column: KernelStringSlice,
    type_tag: u8,
    precision: u8,
    scale: u8,
);

/// Visit the table's clustering columns, invoking `visitor` once per column. See
/// [`ClusteringColumnVisitor`] for what each invocation reports.
///
/// Returns the number of columns visited when the table has the `clustering` feature with a
/// current `delta.clustering` domain entry -- `Some(0)` means the table is clustered but the
/// entry lists no columns. Returns `None` when the feature is absent or the domain has no
/// current entry; the visitor does not run in that case. This distinction mirrors kernel's
/// `Option<Vec<_>>`: `None` is "not clustered", `Some(0)` is "clustered on nothing".
///
/// # Safety
///
/// Caller is responsible for passing in a valid snapshot and engine handle, a valid
/// `engine_context` opaque pointer forwarded to each `visitor` invocation, and a valid
/// `visitor` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_clustering_columns(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    engine_context: NullableCvoid,
    visitor: ClusteringColumnVisitor,
) -> ExternResult<OptionalValue<usize>> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    visit_clustering_columns_impl(snapshot, engine, engine_context, visitor)
        .into_extern_result(&engine)
}

fn visit_clustering_columns_impl(
    snapshot: &Snapshot,
    extern_engine: &dyn ExternEngine,
    engine_context: NullableCvoid,
    visitor: ClusteringColumnVisitor,
) -> DeltaResult<OptionalValue<usize>> {
    let Some(infos) = snapshot.get_clustering_column_infos(extern_engine.engine().as_ref())? else {
        return Ok(OptionalValue::None);
    };
    for info in &infos {
        let logical = info.logical_column().to_string();
        let physical = info.physical_column().to_string();
        let (tag, precision, scale) = NullTypeTag::from_data_type(info.data_type());
        visitor(
            engine_context,
            kernel_string_slice!(logical),
            kernel_string_slice!(physical),
            tag as u8,
            precision,
            scale,
        );
    }
    Ok(OptionalValue::Some(infos.len()))
}

/// Get the domain metadata as an optional string allocated by `AllocatedStringFn` for a specific
/// domain in this snapshot
///
/// # Safety
///
/// Caller is responsible for passing in a valid handle
#[no_mangle]
pub unsafe extern "C" fn visit_domain_metadata(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    engine_context: NullableCvoid,
    visitor: extern "C" fn(
        engine_context: NullableCvoid,
        domain: KernelStringSlice,
        configuration: KernelStringSlice,
    ),
) -> ExternResult<bool> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };

    visit_domain_metadata_impl(snapshot, engine, engine_context, visitor)
        .into_extern_result(&engine)
}

fn visit_domain_metadata_impl(
    snapshot: &Snapshot,
    extern_engine: &dyn ExternEngine,
    engine_context: NullableCvoid,
    visitor: extern "C" fn(
        engine_context: NullableCvoid,
        key: KernelStringSlice,
        value: KernelStringSlice,
    ),
) -> DeltaResult<bool> {
    let res = snapshot.get_all_domain_metadata(extern_engine.engine().as_ref())?;
    res.iter().for_each(|metadata| {
        let domain = &metadata.domain();
        let configuration = &metadata.configuration();
        visitor(
            engine_context,
            kernel_string_slice!(domain),
            kernel_string_slice!(configuration),
        );
    });

    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ptr::NonNull;
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::DeltaResult;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use serde_json::json;
    use test_utils::add_commit;

    use super::*;
    use crate::error::KernelError;
    use crate::ffi_test_utils::{
        allocate_err, allocate_str, assert_extern_result_error_with_message, build_snapshot,
        ok_or_panic, recover_string,
    };
    use crate::{engine_to_handle, free_engine, free_snapshot, kernel_string_slice};

    #[tokio::test]
    async fn test_domain_metadata() -> DeltaResult<()> {
        let storage = Arc::new(InMemory::new());

        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        let table_root = "memory:///test_table/";

        // commit0
        // - domain1: not removed
        // - domain2: not removed
        let commit = join_actions(&[
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 1
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit0",
                    "removed": false
                }
            }),
        ]);

        add_commit(table_root, storage.as_ref(), 0, commit)
            .await
            .unwrap();

        // commit1
        // - domain1: removed
        // - domain2: not-removed
        // - internal domain
        let commit = join_actions(&[
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit1",
                    "removed": true
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit1",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "delta.domain3",
                    "configuration": "domain3_commit1",
                    "removed": false
                }
            }),
        ]);

        add_commit(table_root, storage.as_ref(), 1, commit)
            .await
            .unwrap();

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

        let get_domain_metadata_helper = |domain: &str| unsafe {
            get_domain_metadata(
                snapshot.shallow_copy(),
                kernel_string_slice!(domain),
                engine.shallow_copy(),
                allocate_str,
            )
        };

        // First, we test fetching the domain metadata one-by-one

        let domain1 = "domain1";
        let res = ok_or_panic(get_domain_metadata_helper(domain1));
        assert!(res.is_none());

        let domain2 = "domain2";
        let res = ok_or_panic(get_domain_metadata_helper(domain2));
        assert_eq!(recover_string(res.unwrap()), "domain2_commit1");

        let domain3 = "delta.domain3";
        let res = get_domain_metadata_helper(domain3);
        assert_extern_result_error_with_message(res, KernelError::GenericError, Some("Generic delta kernel error: User DomainMetadata are not allowed to use system-controlled 'delta.*' domain"));

        // Secondly, we visit the entire domain metadata

        // Create visitor state
        let visitor_state: Box<HashMap<String, String>> = Box::default();
        let visitor_state_ptr = Box::into_raw(visitor_state);

        // Test visitor function
        extern "C" fn visitor(
            state: NullableCvoid,
            key: KernelStringSlice,
            value: KernelStringSlice,
        ) {
            let mut collected_metadata = unsafe {
                Box::from_raw(
                    state.unwrap().as_ptr() as *mut std::collections::HashMap<String, String>
                )
            };
            let key: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&key) };
            let value: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&value) };
            collected_metadata.insert(key.unwrap(), value.unwrap());
            Box::leak(collected_metadata);
        }

        // Visit all (user) domain metadata
        let res = unsafe {
            ok_or_panic(visit_domain_metadata(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::new_unchecked(visitor_state_ptr).cast()),
                visitor,
            ))
        };

        // Confirm visitor picked up all entries in map
        let collected_metadata = unsafe { Box::from_raw(visitor_state_ptr) };
        assert!(res);
        assert!(collected_metadata.get("domain1").is_none());
        assert!(collected_metadata.get("delta.domain3").is_none());
        assert_eq!(
            collected_metadata.get("domain2").unwrap(),
            "domain2_commit1"
        );

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }

        Ok(())
    }

    // === visit_clustering_columns tests ===

    fn protocol_action(clustering: bool) -> serde_json::Value {
        if clustering {
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 7,
                    "writerFeatures": ["domainMetadata", "clustering"]
                }
            })
        } else {
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 1
                }
            })
        }
    }

    fn metadata_action(schema_json: &str) -> serde_json::Value {
        json!({
            "metaData": {
                "id": "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                "format": { "provider": "parquet", "options": {} },
                "schemaString": schema_json,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }
        })
    }

    // Schema with two top-level columns (`id: integer`, `val: string`) plus a nested
    // `addr: struct<city: string>` so clustering on `["addr", "city"]` resolves.
    fn clustering_test_schema_json() -> String {
        let addr = json!({
            "type": "struct",
            "fields": [
                {"name": "city", "type": "string", "nullable": true, "metadata": {}}
            ]
        });
        json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": true, "metadata": {}},
                {"name": "val", "type": "string", "nullable": true, "metadata": {}},
                {"name": "addr", "type": addr, "nullable": true, "metadata": {}}
            ]
        })
        .to_string()
    }

    fn clustering_domain_action(config: &str, removed: bool) -> serde_json::Value {
        json!({
            "domainMetadata": {
                "domain": "delta.clustering",
                "configuration": config,
                "removed": removed
            }
        })
    }

    fn join_actions(actions: &[serde_json::Value]) -> String {
        actions
            .iter()
            .map(|j| j.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// One visited clustering column: `(logical, physical, type_tag, precision, scale)`.
    type VisitedColumn = (String, String, u8, u8, u8);

    /// Drive `visit_clustering_columns`, collecting every visited column. Returns the count the
    /// FFI symbol reported alongside the visited descriptors.
    unsafe fn collect_clustering_columns(
        snapshot: &crate::Handle<crate::SharedSnapshot>,
        engine: &crate::Handle<crate::SharedExternEngine>,
    ) -> (Option<usize>, Vec<VisitedColumn>) {
        let collected: Box<Vec<VisitedColumn>> = Box::default();
        let collected_ptr = Box::into_raw(collected);

        extern "C" fn visitor(
            state: NullableCvoid,
            logical_column: KernelStringSlice,
            physical_column: KernelStringSlice,
            type_tag: u8,
            precision: u8,
            scale: u8,
        ) {
            let mut columns =
                unsafe { Box::from_raw(state.unwrap().as_ptr() as *mut Vec<VisitedColumn>) };
            let logical: DeltaResult<String> =
                unsafe { TryFromStringSlice::try_from_slice(&logical_column) };
            let physical: DeltaResult<String> =
                unsafe { TryFromStringSlice::try_from_slice(&physical_column) };
            columns.push((
                logical.unwrap(),
                physical.unwrap(),
                type_tag,
                precision,
                scale,
            ));
            Box::leak(columns);
        }

        let count: Option<usize> = ok_or_panic(unsafe {
            visit_clustering_columns(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::new_unchecked(collected_ptr).cast()),
                visitor,
            )
        })
        .into();

        let columns = *unsafe { Box::from_raw(collected_ptr) };
        (count, columns)
    }

    async fn build_clustering_snapshot(
        table_root: &str,
        clustering_feature: bool,
        domain_config: Option<&str>,
        follow_up_commits: &[serde_json::Value],
    ) -> (
        crate::Handle<crate::SharedExternEngine>,
        crate::Handle<crate::SharedSnapshot>,
    ) {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let mut actions = vec![
            protocol_action(clustering_feature),
            metadata_action(&clustering_test_schema_json()),
        ];
        if let Some(config) = domain_config {
            actions.push(clustering_domain_action(config, false));
        }
        add_commit(table_root, storage.as_ref(), 0, join_actions(&actions))
            .await
            .unwrap();

        for (version, commit_actions) in follow_up_commits.iter().enumerate() {
            add_commit(
                table_root,
                storage.as_ref(),
                version as u64 + 1,
                commit_actions.to_string(),
            )
            .await
            .unwrap();
        }

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };
        (engine, snapshot)
    }

    #[tokio::test]
    async fn test_get_clustering_domain_metadata_user_facing_path_still_rejects() -> DeltaResult<()>
    {
        let table_root = "memory:///test_clustering_user_facing_rejected/";
        let (engine, snapshot) = build_clustering_snapshot(
            table_root,
            true,
            Some(r#"{"clusteringColumns":[["id"]]}"#),
            &[],
        )
        .await;

        let domain = "delta.clustering";
        let rejected = unsafe {
            get_domain_metadata(
                snapshot.shallow_copy(),
                kernel_string_slice!(domain),
                engine.shallow_copy(),
                allocate_str,
            )
        };
        assert_extern_result_error_with_message(
            rejected,
            KernelError::GenericError,
            Some(
                "Generic delta kernel error: User DomainMetadata are not allowed to use \
                 system-controlled 'delta.*' domain",
            ),
        );

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }

        Ok(())
    }

    // Type tags mirror `visit_expression_literal_null`'s encoding.
    const TAG_INTEGER: u8 = 3;
    const TAG_STRING: u8 = 7;

    // `follow_up` is an optional second `delta.clustering` write `(config, removed)`, letting
    // one case exercise tombstone and latest-write-wins reconciliation. Expectations are
    // `(logical, physical, type_tag)`; without column mapping logical == physical.
    #[rstest]
    #[case::clustered_single_column(
        true,
        Some(r#"{"clusteringColumns":[["id"]]}"#),
        None,
        Some(vec![("id", "id", TAG_INTEGER)]),
        "memory:///test_clustering_single/"
    )]
    #[case::clustered_multi_column(
        true,
        Some(r#"{"clusteringColumns":[["id"],["val"]]}"#),
        None,
        Some(vec![("id", "id", TAG_INTEGER), ("val", "val", TAG_STRING)]),
        "memory:///test_clustering_multi/"
    )]
    // Nested paths render as dotted, backtick-escaped `ColumnName` strings.
    #[case::clustered_nested_path(
        true,
        Some(r#"{"clusteringColumns":[["addr","city"]]}"#),
        None,
        Some(vec![("addr.city", "addr.city", TAG_STRING)]),
        "memory:///test_clustering_nested/"
    )]
    // Domain entry present but lists no columns: clustered on nothing -- Some(0), no visits.
    #[case::clustered_empty_column_list(
        true,
        Some(r#"{"clusteringColumns":[]}"#),
        None,
        Some(vec![]),
        "memory:///test_clustering_empty_list/"
    )]
    // Not clustered: None, and the visitor never runs.
    #[case::unclustered_returns_none(
        false,
        None,
        None,
        None,
        "memory:///test_clustering_unclustered/"
    )]
    #[case::clustered_feature_with_no_domain_entry(
        true,
        None,
        None,
        None,
        "memory:///test_clustering_feature_no_entry/"
    )]
    #[case::no_clustering_feature_but_domain_present(
        false,
        Some(r#"{"clusteringColumns":[["id"]]}"#),
        None,
        None,
        "memory:///test_clustering_no_feature_domain_present/"
    )]
    // A tombstone (`removed: true`) clears the domain: None, and the visitor never runs.
    #[case::tombstoned_returns_none(
        true,
        Some(r#"{"clusteringColumns":[["id"]]}"#),
        Some((r#"{"clusteringColumns":[["id"]]}"#, true)),
        None,
        "memory:///test_clustering_tombstoned/"
    )]
    // A later non-removed write wins over the earlier one.
    #[case::latest_write_wins(
        true,
        Some(r#"{"clusteringColumns":[["id"]]}"#),
        Some((r#"{"clusteringColumns":[["val"]]}"#, false)),
        Some(vec![("val", "val", TAG_STRING)]),
        "memory:///test_clustering_latest_wins/"
    )]
    #[tokio::test]
    async fn test_visit_clustering_columns(
        #[case] clustering_feature: bool,
        #[case] domain_config: Option<&str>,
        #[case] follow_up: Option<(&str, bool)>,
        #[case] expected: Option<Vec<(&str, &str, u8)>>,
        #[case] table_root: &str,
    ) -> DeltaResult<()> {
        let follow_up_commits: Vec<serde_json::Value> = follow_up
            .into_iter()
            .map(|(config, removed)| clustering_domain_action(config, removed))
            .collect();
        let (engine, snapshot) = build_clustering_snapshot(
            table_root,
            clustering_feature,
            domain_config,
            &follow_up_commits,
        )
        .await;

        let (count, columns) = unsafe { collect_clustering_columns(&snapshot, &engine) };

        match expected {
            Some(want) => {
                // Non-decimal types report zero precision/scale.
                let want: Vec<VisitedColumn> = want
                    .into_iter()
                    .map(|(logical, physical, tag)| {
                        (logical.to_string(), physical.to_string(), tag, 0, 0)
                    })
                    .collect();
                assert_eq!(count, Some(want.len()));
                assert_eq!(columns, want);
            }
            None => {
                assert_eq!(count, None);
                assert!(columns.is_empty());
            }
        }

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }

        Ok(())
    }

    // Decimal is the only tag carrying a payload, so pin precision/scale explicitly.
    #[tokio::test]
    async fn test_visit_clustering_columns_decimal_reports_precision_and_scale() -> DeltaResult<()>
    {
        const TAG_DECIMAL: u8 = 12;
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        let table_root = "memory:///test_clustering_decimal/";

        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "amt", "type": "decimal(12,3)", "nullable": true, "metadata": {}}
            ]
        })
        .to_string();
        let commit = join_actions(&[
            protocol_action(true),
            metadata_action(&schema),
            clustering_domain_action(r#"{"clusteringColumns":[["amt"]]}"#, false),
        ]);
        add_commit(table_root, storage.as_ref(), 0, commit)
            .await
            .unwrap();

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

        let (count, columns) = unsafe { collect_clustering_columns(&snapshot, &engine) };
        assert_eq!(count, Some(1));
        assert_eq!(
            columns,
            vec![("amt".to_string(), "amt".to_string(), TAG_DECIMAL, 12, 3)]
        );

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }

        Ok(())
    }

    // With column mapping the domain stores physical identifiers, so the visitor must report the
    // schema-resolved logical name alongside the physical one the domain (and per-file stats) use.
    #[tokio::test]
    async fn test_visit_clustering_columns_column_mapping_reports_both_names() -> DeltaResult<()> {
        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        let table_root = "memory:///test_clustering_column_mapping/";

        // `region` is stored physically as `col-region-phys`; clustering names the physical id.
        let schema = json!({
            "type": "struct",
            "fields": [{
                "name": "region",
                "type": "string",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": "col-region-phys"
                }
            }]
        })
        .to_string();
        let commit = join_actions(&[
            json!({
                "protocol": {
                    "minReaderVersion": 2,
                    "minWriterVersion": 7,
                    "writerFeatures": ["domainMetadata", "clustering", "columnMapping"]
                }
            }),
            json!({
                "metaData": {
                    "id": "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": schema,
                    "partitionColumns": [],
                    "configuration": { "delta.columnMapping.mode": "name" },
                    "createdTime": 1587968585495i64
                }
            }),
            clustering_domain_action(r#"{"clusteringColumns":[["col-region-phys"]]}"#, false),
        ]);
        add_commit(table_root, storage.as_ref(), 0, commit)
            .await
            .unwrap();

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

        let (count, columns) = unsafe { collect_clustering_columns(&snapshot, &engine) };
        assert_eq!(count, Some(1));
        // Physical identifiers contain hyphens, which `ColumnName` renders backtick-quoted.
        assert_eq!(
            columns,
            vec![(
                "region".to_string(),
                "`col-region-phys`".to_string(),
                TAG_STRING,
                0,
                0
            )]
        );

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }

        Ok(())
    }

    // A clustering column absent from the schema cannot resolve to a logical name, so the
    // symbol surfaces an `ExternResult::Err` rather than reporting the table as unclustered.
    #[tokio::test]
    async fn test_visit_clustering_columns_column_not_in_schema_errors() -> DeltaResult<()> {
        let table_root = "memory:///test_clustering_col_missing/";
        // "ghost" is not one of id/val/addr in clustering_test_schema_json().
        let (engine, snapshot) = build_clustering_snapshot(
            table_root,
            true,
            Some(r#"{"clusteringColumns":[["ghost"]]}"#),
            &[],
        )
        .await;

        extern "C" fn visitor(
            _: NullableCvoid,
            _: KernelStringSlice,
            _: KernelStringSlice,
            _: u8,
            _: u8,
            _: u8,
        ) {
        }
        let res = unsafe {
            visit_clustering_columns(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                None,
                visitor,
            )
        };
        assert_extern_result_error_with_message(res, KernelError::GenericError, None);

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }

        Ok(())
    }
}
