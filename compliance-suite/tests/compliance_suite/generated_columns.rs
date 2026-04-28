//! generated columns compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static GENERATED_COLUMNS: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("generated-columns.json"));

compliance_case_inexpressible!(GENERATED_COLUMNS, 1); // fixture uses protocol (1,3); create_table() always produces (3,7)
compliance_case_success!(GENERATED_COLUMNS, 2);
compliance_case_success!(GENERATED_COLUMNS, 3);
compliance_case_inexpressible!(GENERATED_COLUMNS, 4); // fixture uses protocol (1,4); create_table() always produces (3,7)
compliance_case_success!(GENERATED_COLUMNS, 5);
compliance_case_success!(GENERATED_COLUMNS, 6, "Feature 'generatedColumns' is not supported");
compliance_case_success!(GENERATED_COLUMNS, 7, "Feature 'generatedColumns' is not supported");
compliance_case_success!(GENERATED_COLUMNS, 8, "'delta.feature.generatedColumns' is not supported during CREATE TABLE");
compliance_case_success!(GENERATED_COLUMNS, 9);
compliance_case_success!(GENERATED_COLUMNS, 10);
compliance_case_success!(GENERATED_COLUMNS, 11);
compliance_case_success!(GENERATED_COLUMNS, 12, "Feature 'checkConstraints' is not supported");
compliance_case_failure!(GENERATED_COLUMNS, 13, "'delta.feature.generatedColumns' is not supported during CREATE TABLE");
compliance_case_success!(GENERATED_COLUMNS, 14, "'delta.feature.generatedColumns' is not supported during CREATE TABLE");
compliance_case_success!(GENERATED_COLUMNS, 15, "'delta.feature.generatedColumns' is not supported during CREATE TABLE");
compliance_case_success!(GENERATED_COLUMNS, 16, "Feature 'generatedColumns' is not supported");
compliance_case_success!(GENERATED_COLUMNS, 17, "is not supported during CREATE TABLE"); // generatedColumns + checkConstraints both unsupported; which fires first is non-deterministic
compliance_case_sentinel!(GENERATED_COLUMNS, 18);
