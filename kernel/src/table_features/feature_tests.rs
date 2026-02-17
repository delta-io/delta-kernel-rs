//! Spec-derived test battery for all table features.
//!
//! Each feature has a [`FeatureFixture`] that captures its properties from the Delta protocol
//! specification (not from [`FeatureInfo`]). The shared [`run_battery`] function exercises a
//! standard set of scenarios against each fixture, including:
//!
//! - Property-based construction tests (enabled/disabled/error)
//! - Auto-generated scenarios (empty props, orphan detection, legacy inference)
//! - Kernel capability tests (exhaustive over [`Operation`] variants)
//!
//! Valid property combinations (ICT auxiliary props, iceberg compat cross-feature deps) are
//! exercised here via `prop_cases`. This test framework is able to exercise many, but not all,
//! feature-specific validation edge cases, leaving less work for feature-specific test modules.

use std::collections::HashMap;

use strum::IntoEnumIterator;
use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::schema::{DataType, MetadataValue, StructField, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{FeatureType, IntoTableFeature as _, Operation, TableFeature};

// ================================================================================================
// Core types
// ================================================================================================

/// Minimum (reader, writer) protocol version pair for legacy features.
#[derive(Debug, Clone, Copy)]
struct MinReaderWriterVersion(i32, i32);

/// Expected outcome for a single operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpExpect {
    Ok,
    Err,
}

/// Expected kernel capability outcomes when the feature is supported+enabled.
/// Fields are exhaustive over [`Operation`] variants -- adding a variant forces a compile error
/// here and in all fixture definitions until they are updated.
#[derive(Debug, Clone)]
struct CapabilityExpect {
    scan: OpExpect,
    cdf: OpExpect,
    write: OpExpect,
}

/// A property-based test case. The variant encodes the expected outcome when the feature
/// IS listed in a modern (3,7) protocol.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Error variant will be used by features with validation (ICT pairing, etc.)
enum PropCase {
    /// TC construction succeeds, feature is enabled.
    Enabled(&'static [&'static str]),
    /// TC construction succeeds, feature is NOT enabled.
    Disabled(&'static [&'static str]),
    /// TC construction fails.
    Error(&'static [&'static str]),
}

impl PropCase {
    fn props(&self) -> &[&'static str] {
        match self {
            PropCase::Enabled(p) | PropCase::Disabled(p) | PropCase::Error(p) => p,
        }
    }
}

/// Spec-derived fixture for a single table feature. Drives the shared test battery.
#[derive(Debug, Clone)]
struct FeatureFixture {
    /// Feature name as it appears in the protocol (e.g. "appendOnly", "allowColumnDefaults").
    name: &'static str,
    /// From the Delta spec: Writer or ReaderWriter. NOT from FeatureInfo.
    spec_type: FeatureType,
    /// Legacy protocol versions if this is a legacy feature.
    /// `None` for modern-only features. Controls whether legacy battery tests apply.
    spec_legacy_versions: Option<MinReaderWriterVersion>,
    /// Whether legacy enablement depends on metadata presence (schema annotations or properties).
    /// `false`: feature has no presence checker yet, so enablement cannot be tested at legacy version.
    /// `true`: presence checker exists; legacy version + no presence = supported but NOT enabled.
    /// Only meaningful when `spec_legacy_versions` is `Some`. Delete once all legacy features
    /// have presence checkers (at which point all would be `true`).
    legacy_enablement_by_presence: bool,
    /// Schema with annotations that make the feature "present" (for schema-based features).
    /// The harness combines this with Enabled prop_cases when constructing TCs.
    presence_schema: Option<StructType>,
    /// Feature names that must also be in the protocol for requirements to pass.
    required_deps: &'static [&'static str],
    /// Feature names that must NOT be in the protocol.
    anti_deps: &'static [&'static str],
    /// Property-based test cases. Convention: first `Enabled` variant is the "full presence" case,
    /// used for orphan tests and capability setup.
    prop_cases: &'static [PropCase],
    /// Expected outcome when presence metadata exists but the feature is NOT in the protocol.
    /// `OpExpect::Err` for features with active orphan detection.
    /// `OpExpect::Ok` for features without orphan detection yet.
    expected_orphan: OpExpect,
    /// Expected kernel capability when the feature is supported+enabled.
    capability: CapabilityExpect,
}

impl FeatureFixture {
    /// Find the first `Enabled` case's props, or `None` if there are no `Enabled` cases.
    fn find_enabled(&self) -> Option<&'static [&'static str]> {
        self.prop_cases.iter().find_map(|c| match c {
            PropCase::Enabled(p) => Some(*p),
            _ => None,
        })
    }

    /// Find the first `Disabled` case's props, or `None` if there are no `Disabled` cases.
    fn find_disabled(&self) -> Option<&'static [&'static str]> {
        self.prop_cases.iter().find_map(|c| match c {
            PropCase::Disabled(p) => Some(*p),
            _ => None,
        })
    }

    /// Whether this feature has any metadata footprint (props or schema).
    fn has_metadata_footprint(&self) -> bool {
        !self.prop_cases.is_empty() || self.presence_schema.is_some()
    }

    /// Parse the feature name to a TableFeature. Unknown features become TableFeature::Unknown.
    fn table_feature(&self) -> TableFeature {
        self.name.into_table_feature()
    }

    /// Is this feature known to the kernel (has a non-Unknown enum variant)?
    fn is_feature_known(&self) -> bool {
        !matches!(self.table_feature(), TableFeature::Unknown(_))
    }
}

// ================================================================================================
// Test helper: construct a TableConfiguration from string feature names + arbitrary props
// ================================================================================================

/// Parse "key=value" strings into (key, value) pairs.
fn parse_props<'a>(raw: &[&'a str]) -> Vec<(&'a str, &'a str)> {
    raw.iter()
        .map(|s| {
            s.split_once('=')
                .unwrap_or_else(|| panic!("PropCase entry must be 'key=value', got: {s:?}"))
        })
        .collect()
}

/// Attempt to construct a [`TableConfiguration`] from string feature names and "key=value" props.
///
/// Features are placed on the appropriate protocol lists based on their parsed type:
/// - ReaderWriter features go on both reader and writer lists
/// - Writer/Unknown features go on writer list only
///
/// If `min_reader_version >= 3`, reader features are included. If `min_writer_version >= 7`,
/// writer features are included. Otherwise feature lists are omitted (legacy mode).
fn try_create_table_config(
    feature_names: &[&str],
    raw_props: &[&str],
    schema: Option<&StructType>,
    min_reader_version: i32,
    min_writer_version: i32,
) -> crate::DeltaResult<TableConfiguration> {
    let schema = schema.cloned().unwrap_or_else(|| {
        StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)])
    });

    let props = parse_props(raw_props);
    let config = HashMap::from_iter(props.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let metadata = Metadata::try_new(None, None, std::sync::Arc::new(schema), vec![], 0, config)?;

    // Parse feature names and determine list placement
    let features: Vec<TableFeature> = feature_names
        .iter()
        .map(|name| name.into_table_feature())
        .collect();

    let reader_features: Vec<&str> = feature_names
        .iter()
        .zip(features.iter())
        .filter(|(_, f)| f.feature_type() == FeatureType::ReaderWriter)
        .map(|(name, _)| *name)
        .collect();

    let writer_features: Vec<&str> = feature_names.to_vec();

    let reader_opt = (min_reader_version >= 3).then_some(reader_features);
    let writer_opt = (min_writer_version >= 7).then_some(writer_features);

    let protocol = Protocol::try_new(
        min_reader_version,
        min_writer_version,
        reader_opt,
        writer_opt,
    )?;

    let table_root = Url::try_from("file:///").unwrap();
    TableConfiguration::try_new(metadata, protocol, table_root, 0)
}

/// Convenience: create TC that must succeed, panicking on failure.
fn create_table_config(
    feature_names: &[&str],
    raw_props: &[&str],
    schema: Option<&StructType>,
    min_reader_version: i32,
    min_writer_version: i32,
) -> TableConfiguration {
    try_create_table_config(
        feature_names,
        raw_props,
        schema,
        min_reader_version,
        min_writer_version,
    )
    .unwrap_or_else(|e| {
        panic!(
            "Expected TC construction to succeed for features {:?} with props {:?}, but got: {e}",
            feature_names, raw_props
        )
    })
}

// ================================================================================================
// Test battery
// ================================================================================================

/// Run the full test battery for a feature fixture.
fn run_battery(fixture: &FeatureFixture) {
    let name = fixture.name;
    let feature = fixture.table_feature();

    // Collect all dep feature names (required_deps + anti_deps excluded) for protocol construction
    let all_feature_names: Vec<&str> = std::iter::once(name)
        .chain(fixture.required_deps.iter().copied())
        .collect();

    let schema_ref = fixture.presence_schema.as_ref();
    let enabled_props = fixture.find_enabled().unwrap_or(&[]);

    // ============================================================================================
    // A. Prop cases: feature IS listed in modern (3,7) protocol
    // ============================================================================================
    for (i, case) in fixture.prop_cases.iter().enumerate() {
        let result = try_create_table_config(&all_feature_names, case.props(), schema_ref, 3, 7);

        match case {
            PropCase::Enabled(_) => {
                let tc = result.unwrap_or_else(|e| {
                    panic!("{name}: prop_case[{i}] Enabled: expected TC construction to succeed, got: {e}")
                });
                assert!(
                    tc.is_feature_supported(&feature),
                    "{name}: prop_case[{i}] Enabled: expected is_feature_supported = true"
                );
                assert!(
                    tc.is_feature_enabled(&feature),
                    "{name}: prop_case[{i}] Enabled: expected is_feature_enabled = true"
                );
            }
            PropCase::Disabled(_) => {
                let tc = result.unwrap_or_else(|e| {
                    panic!("{name}: prop_case[{i}] Disabled: expected TC construction to succeed, got: {e}")
                });
                assert!(
                    tc.is_feature_supported(&feature),
                    "{name}: prop_case[{i}] Disabled: expected is_feature_supported = true"
                );
                assert!(
                    !tc.is_feature_enabled(&feature),
                    "{name}: prop_case[{i}] Disabled: expected is_feature_enabled = false"
                );
            }
            PropCase::Error(_) => {
                assert!(
                    result.is_err(),
                    "{name}: prop_case[{i}] Error: expected TC construction to fail, but it succeeded"
                );
            }
        }
    }

    // ============================================================================================
    // B. Auto-generated: empty props + listed (modern 3,7)
    // ============================================================================================
    if fixture.is_feature_known() {
        let tc = create_table_config(&all_feature_names, enabled_props, schema_ref, 3, 7);
        assert!(
            tc.is_feature_supported(&feature),
            "{name}: listed: expected is_feature_supported = true"
        );
    }

    // ============================================================================================
    // C. Auto-generated: empty props + NOT listed (modern 3,7)
    // ============================================================================================
    if fixture.is_feature_known() {
        // Only test for known features -- unknown features have no FeatureInfo and
        // is_feature_supported always returns false anyway
        let tc = create_table_config(&[], &[], None, 3, 7);
        assert!(
            !tc.is_feature_supported(&feature),
            "{name}: empty props + not listed: expected is_feature_supported = false"
        );
        assert!(
            !tc.is_feature_enabled(&feature),
            "{name}: empty props + not listed: expected is_feature_enabled = false"
        );
    }

    // ============================================================================================
    // D. Orphan test: presence metadata + feature NOT in protocol
    // ============================================================================================
    if fixture.has_metadata_footprint() {
        // Use Enabled props if available (for property-based presence), otherwise empty
        // (for schema-only presence like timestampNtz, variantType).

        // Feature NOT in protocol, but its metadata IS present
        let result = try_create_table_config(&[], enabled_props, schema_ref, 3, 7);

        match fixture.expected_orphan {
            OpExpect::Err => {
                assert!(
                    result.is_err(),
                    "{name}: orphan: expected TC construction to fail (orphan detection), but it succeeded"
                );
            }
            OpExpect::Ok => {
                result.unwrap_or_else(|e| {
                    panic!("{name}: orphan: expected TC construction to succeed (no orphan detection yet), got: {e}")
                });
            }
        }
    }

    // ============================================================================================
    // E. Legacy inference (only for features with spec_legacy_versions)
    // ============================================================================================
    if let Some(MinReaderWriterVersion(legacy_reader, legacy_writer)) = fixture.spec_legacy_versions
    {
        // E1: Legacy version with no metadata presence -- version alone is sufficient to infer
        // support, but enablement is determined by presence or toggle properties.
        let tc = create_table_config(&[], &[], None, legacy_reader, legacy_writer);
        assert!(
            tc.is_feature_supported(&feature),
            "{name}: legacy version only ({legacy_reader},{legacy_writer}): \
             expected is_feature_supported = true"
        );
        assert!(
            !fixture.legacy_enablement_by_presence || !tc.is_feature_enabled(&feature),
            "{name}: legacy version only ({legacy_reader},{legacy_writer}): \
             expected is_feature_enabled = false (no presence)"
        );

        // E2: Legacy version insufficient -> feature not supported
        // Use one version below the minimum writer version
        if legacy_writer > 1 {
            let tc = create_table_config(&[], &[], None, 1, legacy_writer - 1);
            assert!(
                !tc.is_feature_supported(&feature),
                "{name}: legacy insufficient version (1,{}): expected is_feature_supported = false",
                legacy_writer - 1
            );
        }

        // E3: ReaderWriter feature with writer sufficient but reader insufficient
        if legacy_reader > 1 && fixture.spec_type == FeatureType::ReaderWriter {
            let tc = create_table_config(&[], &[], None, legacy_reader - 1, legacy_writer);
            assert!(
                !tc.is_feature_supported(&feature),
                "{name}: legacy asymmetric ({},{legacy_writer}): expected (reader insufficient)",
                legacy_reader - 1
            );
        }

        // E4/E5: Legacy enablement for toggle features.
        //
        // Toggle features have both Enabled and Disabled prop_cases (a boolean property
        // like delta.appendOnly that can be true or false). We verify that legacy version
        // inference correctly distinguishes supported-but-not-enabled from supported-and-enabled.
        //
        // Non-toggle features are excluded: AlwaysIfSupported features (no prop_cases) are
        // always enabled when supported, and columnMapping uses a multi-valued mode property
        // whose disabled state (mode=none) is tested in the column_mapping module.
        if let Some(disabled_props) = fixture.find_disabled() {
            assert!(
                !enabled_props.is_empty(),
                "{name}: has Disabled prop_case but no Enabled prop_case"
            );

            // E4: Legacy version + enabled props -> supported AND enabled
            let tc = create_table_config(&[], enabled_props, None, legacy_reader, legacy_writer);
            assert!(
                tc.is_feature_supported(&feature),
                "{name}: legacy + enabled props: expected is_feature_supported = true"
            );
            assert!(
                tc.is_feature_enabled(&feature),
                "{name}: legacy + enabled props: expected is_feature_enabled = true"
            );

            // E5: Legacy version + disabled props -> supported but NOT enabled
            let tc = create_table_config(&[], disabled_props, None, legacy_reader, legacy_writer);
            assert!(
                tc.is_feature_supported(&feature),
                "{name}: legacy + disabled props: expected is_feature_supported = true"
            );
            assert!(
                !tc.is_feature_enabled(&feature),
                "{name}: legacy + disabled props: expected is_feature_enabled = false"
            );
        }
    }

    // ============================================================================================
    // F. Capability tests: exhaustive over Operation variants
    // ============================================================================================
    {
        // Build a TC where the feature is supported+enabled.
        let tc = create_table_config(&all_feature_names, enabled_props, schema_ref, 3, 7);

        // Operation::iter() automatically enumerates every variant even as new ones are added, and
        // the exhaustive match on CapabilityExpect fields ensures every variant has an
        // expectation -- adding a new Operation variant forces a compile error here.
        for op in Operation::iter() {
            let expected = match op {
                Operation::Scan => fixture.capability.scan,
                Operation::Cdf => fixture.capability.cdf,
                Operation::Write => fixture.capability.write,
            };

            let result = tc.ensure_operation_supported(op);
            match expected {
                OpExpect::Ok => {
                    assert!(
                        result.is_ok(),
                        "{name}: capability {op:?}: expected Ok, got: {result:?}"
                    );
                }
                OpExpect::Err => {
                    assert!(
                        result.is_err(),
                        "{name}: capability {op:?}: expected Err, got Ok"
                    );
                }
            }
        }
    }

    // ============================================================================================
    // G. Dependency violations: missing required dep or present anti-dep
    // ============================================================================================
    // A dependency violation is a protocol-level error, so construction is allowed to reject it
    // regardless of the operation to be performed. When construction doesn't catch it, we verify
    // that operations which exercise the feature are rejected, based on feature type.
    {
        // G1: Missing required dep -> should be rejected
        for missing_dep in fixture.required_deps.iter() {
            let without_dep: Vec<&str> = all_feature_names
                .iter()
                .copied()
                .filter(|d| d != missing_dep)
                .collect();

            assert_dep_violation(
                &format!("{name}: missing dep '{missing_dep}'"),
                fixture.spec_type,
                try_create_table_config(&without_dep, enabled_props, schema_ref, 3, 7),
            );
        }

        // G2: Present anti-dep -> should be rejected
        for anti_dep in fixture.anti_deps.iter() {
            let with_anti: Vec<&str> = all_feature_names
                .iter()
                .copied()
                .chain(std::iter::once(*anti_dep))
                .collect();

            assert_dep_violation(
                &format!("{name}: present anti-dep '{anti_dep}'"),
                fixture.spec_type,
                try_create_table_config(&with_anti, enabled_props, schema_ref, 3, 7),
            );
        }
    }
}

/// Assert that a dependency violation is rejected. If construction succeeds, verify that
/// operations exercising the feature fail, based on feature type.
#[track_caller]
fn assert_dep_violation(
    label: &str,
    spec_type: FeatureType,
    result: crate::DeltaResult<TableConfiguration>,
) {
    let Ok(tc) = result else {
        return; // Construction rejected -- protocol violation caught
    };
    for op in Operation::iter() {
        let checks_feature = match op {
            Operation::Scan | Operation::Cdf => spec_type == FeatureType::ReaderWriter,
            Operation::Write => true,
        };
        if checks_feature {
            assert!(
                tc.ensure_operation_supported(op).is_err(),
                "{label}: expected {op:?} to fail"
            );
        }
    }
}

// ================================================================================================
// Schema helpers
// ================================================================================================

/// Creates a simple schema with column mapping annotations on all fields.
fn column_mapping_schema() -> StructType {
    StructType::new_unchecked([
        StructField::nullable("value", DataType::INTEGER).with_metadata([
            (
                "delta.columnMapping.id".to_string(),
                MetadataValue::Number(1),
            ),
            (
                "delta.columnMapping.physicalName".to_string(),
                MetadataValue::String("value".to_string()),
            ),
        ]),
    ])
}

/// Creates a schema with a type-widened column (integer -> long).
fn type_widened_schema() -> StructType {
    StructType::new_unchecked([
        StructField::nullable("value", DataType::LONG).with_metadata([(
            "delta.typeWidening.typeChanges",
            r#"[{"fieldPath":"","fromType":"integer","toType":"long"}]"#,
        )]),
    ])
}

/// Creates a schema with a Variant column.
fn variant_schema() -> StructType {
    StructType::new_unchecked([
        StructField::nullable("value", DataType::INTEGER),
        StructField::nullable("v", DataType::unshredded_variant()),
    ])
}

// ================================================================================================
// Shorthand constructors for fixtures
// ================================================================================================

/// All operations succeed (reads and writes supported).
const ALL_SUPPORTED: CapabilityExpect = CapabilityExpect {
    scan: OpExpect::Ok,
    cdf: OpExpect::Ok,
    write: OpExpect::Ok,
};

/// Reads succeed, writes fail (unsupported writer-only feature).
const READS_ONLY: CapabilityExpect = CapabilityExpect {
    scan: OpExpect::Ok,
    cdf: OpExpect::Ok,
    write: OpExpect::Err,
};

/// All operations fail (unsupported reader-writer feature).
// Currently unused because catalog-managed feature flag is enabled in tests.
#[allow(dead_code)]
const ALL_UNSUPPORTED: CapabilityExpect = CapabilityExpect {
    scan: OpExpect::Err,
    cdf: OpExpect::Err,
    write: OpExpect::Err,
};

// ================================================================================================
// Legacy feature fixtures
// ================================================================================================

#[test]
fn test_append_only() {
    run_battery(&FeatureFixture {
        name: "appendOnly",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: Some(MinReaderWriterVersion(1, 2)),
        // TODO: infer enablement by metadata presence, not just protocol version
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&["delta.appendOnly=true"]),
            PropCase::Disabled(&["delta.appendOnly=false"]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_invariants() {
    let invariants_schema =
        StructType::new_unchecked([StructField::nullable("value", DataType::INTEGER)
            .with_metadata([(
                "delta.invariants".to_string(),
                MetadataValue::String("value > 0".to_string()),
            )])]);
    run_battery(&FeatureFixture {
        name: "invariants",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: Some(MinReaderWriterVersion(1, 2)),
        // TODO: infer enablement by metadata presence, not just protocol version
        legacy_enablement_by_presence: false,
        presence_schema: Some(invariants_schema),
        required_deps: &[],
        anti_deps: &[],
        // No toggle property. TODO: Enabled when delta.invariants column metadata is present in schema.
        prop_cases: &[],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: READS_ONLY,
    });
}

#[test]
fn test_check_constraints() {
    run_battery(&FeatureFixture {
        name: "checkConstraints",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: Some(MinReaderWriterVersion(1, 3)),
        // TODO: infer enablement by metadata presence, not just protocol version
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // No toggle property. Enabled when delta.constraints.* table properties are present.
        prop_cases: &[
            PropCase::Enabled(&["delta.constraints.valueInRange=value > 0"]),
            PropCase::Enabled(&[]), // TODO: Should be Disabled
        ],
        expected_orphan: OpExpect::Ok,
        capability: READS_ONLY,
    });
}

#[test]
fn test_change_data_feed() {
    run_battery(&FeatureFixture {
        name: "changeDataFeed",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: Some(MinReaderWriterVersion(1, 4)),
        // TODO: infer enablement by metadata presence, not just protocol version
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&["delta.enableChangeDataFeed=true"]),
            PropCase::Disabled(&["delta.enableChangeDataFeed=false"]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_generated_columns() {
    let gen_schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::INTEGER).with_metadata([(
            "delta.generationExpression".to_string(),
            MetadataValue::String("id + 1".to_string()),
        )]),
    ]);
    run_battery(&FeatureFixture {
        name: "generatedColumns",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: Some(MinReaderWriterVersion(1, 4)),
        // TODO: infer enablement by metadata presence, not just protocol version
        legacy_enablement_by_presence: false,
        presence_schema: Some(gen_schema),
        required_deps: &[],
        anti_deps: &[],
        // No toggle property. Enabled when delta.generationExpression column metadata is present.
        prop_cases: &[],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: READS_ONLY,
    });
}

#[test]
fn test_column_mapping() {
    run_battery(&FeatureFixture {
        name: "columnMapping",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: Some(MinReaderWriterVersion(2, 5)),
        // TODO: infer enablement by metadata presence, not just protocol version
        legacy_enablement_by_presence: false,
        presence_schema: Some(column_mapping_schema()),
        required_deps: &[],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&["delta.columnMapping.mode=name"]),
            PropCase::Enabled(&["delta.columnMapping.mode=id"]),
            // mode=none is a feature-specific edge case -- see column_mapping module tests.
        ],
        // CM schema annotations without CM in protocol are rejected.
        expected_orphan: OpExpect::Err,
        // Column mapping: reads supported (scan+cdf), writes not supported
        capability: READS_ONLY,
    });
}

#[test]
fn test_identity_columns() {
    let identity_schema = StructType::new_unchecked([StructField::nullable("id", DataType::LONG)
        .with_metadata([
            ("delta.identity.start".to_string(), MetadataValue::Number(1)),
            ("delta.identity.step".to_string(), MetadataValue::Number(1)),
        ])]);
    run_battery(&FeatureFixture {
        name: "identityColumns",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: Some(MinReaderWriterVersion(1, 6)),
        // TODO: infer enablement by metadata presence, not just protocol version
        legacy_enablement_by_presence: false,
        presence_schema: Some(identity_schema),
        required_deps: &[],
        anti_deps: &[],
        // No toggle property. Enabled when delta.identity.* column metadata is present in schema.
        prop_cases: &[],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: READS_ONLY,
    });
}

// ================================================================================================
// Modern feature fixtures
// ================================================================================================

#[test]
fn test_deletion_vectors() {
    run_battery(&FeatureFixture {
        name: "deletionVectors",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&["delta.enableDeletionVectors=true"]),
            PropCase::Disabled(&["delta.enableDeletionVectors=false"]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_row_tracking() {
    run_battery(&FeatureFixture {
        name: "rowTracking",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &["domainMetadata"],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&[
                "delta.enableRowTracking=true",
                "delta.rowTracking.materializedRowIdColumnName=_row-id",
                "delta.rowTracking.materializedRowCommitVersionColumnName=_row-cv",
            ]),
            PropCase::Disabled(&[
                "delta.enableRowTracking=false",
                "delta.rowTracking.materializedRowIdColumnName=_row-id",
                "delta.rowTracking.materializedRowCommitVersionColumnName=_row-cv",
            ]),
            // Suspended with enabled=false: valid, not enabled.
            PropCase::Disabled(&[
                "delta.enableRowTracking=false",
                "delta.rowTrackingSuspended=true",
                "delta.rowTracking.materializedRowIdColumnName=_row-id",
                "delta.rowTracking.materializedRowCommitVersionColumnName=_row-cv",
            ]),
            // TODO: should be Error (spec forbids both enabled and suspended = true),
            // but kernel does not currently reject this combination at construction time.
            PropCase::Disabled(&[
                "delta.enableRowTracking=true",
                "delta.rowTrackingSuspended=true",
                "delta.rowTracking.materializedRowIdColumnName=_row-id",
                "delta.rowTracking.materializedRowCommitVersionColumnName=_row-cv",
            ]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_timestamp_ntz() {
    // timestampNtz is schema-based: presence is a column of type TimestampNtz in the schema.
    let ntz_schema = StructType::new_unchecked([
        StructField::nullable("value", DataType::INTEGER),
        StructField::nullable("ts", DataType::TIMESTAMP_NTZ),
    ]);
    run_battery(&FeatureFixture {
        name: "timestampNtz",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(ntz_schema),
        required_deps: &[],
        anti_deps: &[],
        // No toggle property. Presence is a TimestampNtz column in the schema.
        prop_cases: &[],
        // TimestampNtz columns without the feature in protocol are rejected.
        expected_orphan: OpExpect::Err,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_domain_metadata() {
    run_battery(&FeatureFixture {
        name: "domainMetadata",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // Protocol-only, no metadata footprint.
        prop_cases: &[],
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_v2_checkpoint() {
    run_battery(&FeatureFixture {
        name: "v2Checkpoint",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // delta.checkpointPolicy is an optional config (not a toggle)
        prop_cases: &[
            PropCase::Enabled(&["delta.checkpointPolicy=v2"]),
            PropCase::Enabled(&[]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_iceberg_compat_v1() {
    run_battery(&FeatureFixture {
        name: "icebergCompatV1",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(column_mapping_schema()),
        // IcebergCompatV1 requires column mapping enabled in name or id mode.
        // Also requires DeletionVectors NOT supported.
        required_deps: &["columnMapping"],
        anti_deps: &["deletionVectors"],
        prop_cases: &[
            PropCase::Enabled(&[
                "delta.enableIcebergCompatV1=true",
                "delta.columnMapping.mode=name",
            ]),
            PropCase::Disabled(&[
                "delta.enableIcebergCompatV1=false",
                "delta.columnMapping.mode=name",
            ]),
        ],
        // CM schema annotations without CM in protocol are rejected.
        expected_orphan: OpExpect::Err,
        capability: READS_ONLY,
    });
}

#[test]
fn test_iceberg_compat_v2() {
    run_battery(&FeatureFixture {
        name: "icebergCompatV2",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(column_mapping_schema()),
        // IcebergCompatV2 requires column mapping enabled in name or id mode.
        // Requires DeletionVectors NOT supported, IcebergCompatV1 NOT enabled.
        required_deps: &["columnMapping"],
        anti_deps: &["icebergCompatV1", "deletionVectors"],
        prop_cases: &[
            PropCase::Enabled(&[
                "delta.enableIcebergCompatV2=true",
                "delta.columnMapping.mode=name",
            ]),
            PropCase::Disabled(&[
                "delta.enableIcebergCompatV2=false",
                "delta.columnMapping.mode=name",
            ]),
        ],
        // CM schema annotations without CM in protocol are rejected.
        expected_orphan: OpExpect::Err,
        capability: READS_ONLY,
    });
}

#[test]
fn test_clustered_table() {
    run_battery(&FeatureFixture {
        name: "clustering",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &["domainMetadata"],
        anti_deps: &[],
        // Protocol-only, no metadata footprint.
        prop_cases: &[],
        expected_orphan: OpExpect::Ok,
        #[cfg(feature = "clustered-table")]
        capability: ALL_SUPPORTED,
        #[cfg(not(feature = "clustered-table"))]
        capability: READS_ONLY,
    });
}

#[test]
fn test_vacuum_protocol_check() {
    run_battery(&FeatureFixture {
        name: "vacuumProtocolCheck",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // Protocol-only, no metadata footprint.
        prop_cases: &[],
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_in_commit_timestamp() {
    run_battery(&FeatureFixture {
        name: "inCommitTimestamp",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&["delta.enableInCommitTimestamps=true"]),
            PropCase::Disabled(&["delta.enableInCommitTimestamps=false"]),
            // Valid: both auxiliary properties present.
            PropCase::Enabled(&[
                "delta.enableInCommitTimestamps=true",
                "delta.inCommitTimestampEnablementVersion=1",
                "delta.inCommitTimestampEnablementTimestamp=12345",
            ]),
            // TODO: should be Error (unpaired aux property), but pairing is not
            // currently validated at construction time -- only at query time.
            PropCase::Enabled(&[
                "delta.enableInCommitTimestamps=true",
                "delta.inCommitTimestampEnablementVersion=1",
            ]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_variant_type() {
    run_battery(&FeatureFixture {
        name: "variantType",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(variant_schema()),
        required_deps: &[],
        anti_deps: &[],
        // No toggle property. Presence is a Variant column in the schema.
        prop_cases: &[],
        // Variant columns without the feature in protocol are rejected.
        expected_orphan: OpExpect::Err,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_variant_type_preview() {
    run_battery(&FeatureFixture {
        name: "variantType-preview",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(variant_schema()),
        required_deps: &[],
        anti_deps: &[],
        // No toggle property. Presence is a Variant column in the schema.
        prop_cases: &[],
        // Variant columns without the feature in protocol are rejected.
        expected_orphan: OpExpect::Err,
        capability: ALL_SUPPORTED,
    });
}

/// Variant shredding requires variantType OR variantType-preview in the protocol.
/// We test each alternative as a separate fixture since the harness only supports AND deps.
#[test]
fn test_variant_shredding_preview_with_variant_type() {
    run_battery(&FeatureFixture {
        name: "variantShredding-preview",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(variant_schema()),
        required_deps: &["variantType"],
        anti_deps: &[],
        // No toggle property. Presence is a Variant column in the schema.
        prop_cases: &[],
        expected_orphan: OpExpect::Err,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_variant_shredding_preview_with_variant_type_preview() {
    run_battery(&FeatureFixture {
        name: "variantShredding-preview",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(variant_schema()),
        required_deps: &["variantType-preview"],
        anti_deps: &[],
        // No toggle property. Presence is a Variant column in the schema.
        prop_cases: &[],
        expected_orphan: OpExpect::Err,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_type_widening() {
    run_battery(&FeatureFixture {
        name: "typeWidening",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(type_widened_schema()),
        required_deps: &[],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&["delta.enableTypeWidening=true"]),
            PropCase::Disabled(&["delta.enableTypeWidening=false"]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: READS_ONLY,
    });
}

#[test]
fn test_type_widening_preview() {
    run_battery(&FeatureFixture {
        name: "typeWidening-preview",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: Some(type_widened_schema()),
        required_deps: &[],
        anti_deps: &[],
        prop_cases: &[
            PropCase::Enabled(&["delta.enableTypeWidening=true"]),
            PropCase::Disabled(&["delta.enableTypeWidening=false"]),
        ],
        // TODO: reject metadata presence when feature not in protocol
        expected_orphan: OpExpect::Ok,
        capability: READS_ONLY,
    });
}

#[test]
fn test_materialize_partition_columns() {
    run_battery(&FeatureFixture {
        name: "materializePartitionColumns",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // Protocol-only, no metadata footprint.
        prop_cases: &[],
        expected_orphan: OpExpect::Ok,
        capability: ALL_SUPPORTED,
    });
}

#[test]
fn test_catalog_managed() {
    run_battery(&FeatureFixture {
        name: "catalogManaged",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // Protocol-only, no metadata footprint.
        prop_cases: &[],
        expected_orphan: OpExpect::Ok,
        // CDF not supported for catalog-managed tables.
        capability: CapabilityExpect {
            scan: OpExpect::Ok,
            cdf: OpExpect::Err,
            write: OpExpect::Ok,
        },
    });
}

#[test]
fn test_catalog_owned_preview() {
    run_battery(&FeatureFixture {
        name: "catalogOwned-preview",
        spec_type: FeatureType::ReaderWriter,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // Protocol-only, no metadata footprint.
        prop_cases: &[],
        expected_orphan: OpExpect::Ok,
        // CDF not supported for catalog-managed tables (same as catalogManaged).
        capability: CapabilityExpect {
            scan: OpExpect::Ok,
            cdf: OpExpect::Err,
            write: OpExpect::Ok,
        },
    });
}

#[test]
fn test_allow_column_defaults() {
    // allowColumnDefaults: spec says writer-only, writer version 7.
    // Not in kernel's TableFeature enum -> parses as Unknown.
    // Kernel should reject operations on tables with unknown features.
    run_battery(&FeatureFixture {
        name: "allowColumnDefaults",
        spec_type: FeatureType::Writer,
        spec_legacy_versions: None,
        legacy_enablement_by_presence: false,
        presence_schema: None,
        required_deps: &[],
        anti_deps: &[],
        // Unknown to kernel -- no toggle property can be modeled.
        prop_cases: &[],
        expected_orphan: OpExpect::Ok,
        // Unknown writer feature: reads should succeed (writer-only gate skips), writes fail.
        capability: READS_ONLY,
    });
}
