//! allow column defaults compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static ALLOW_COLUMN_DEFAULTS: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("allow-column-defaults.json"));

compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 1, "'delta.feature.allowColumnDefaults' is not supported during CREATE TABLE");
compliance_case_failure!(ALLOW_COLUMN_DEFAULTS, 2, diverges: "kernel does not require allowColumnDefaults listed when property is present");
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 3);
compliance_case_failure!(ALLOW_COLUMN_DEFAULTS, 4, diverges: "kernel does not require allowColumnDefaults listed when property is present");
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 5, "Feature 'allowColumnDefaults' is not supported");
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 6);
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 7, "Feature 'allowColumnDefaults' is not supported");
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 8);
compliance_case_inexpressible!(ALLOW_COLUMN_DEFAULTS, 9); // fixture uses protocol (1,4); create_table() always produces (3,7)
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 10, "is not supported during CREATE TABLE"); // allowColumnDefaults + checkConstraints both unsupported; which fires first is non-deterministic
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 11, "'delta.feature.allowColumnDefaults' is not supported during CREATE TABLE");
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 12, "Feature 'allowColumnDefaults' is not supported");
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 13);
compliance_case_success!(ALLOW_COLUMN_DEFAULTS, 14, "Feature 'allowColumnDefaults' is not supported");
compliance_case_sentinel!(ALLOW_COLUMN_DEFAULTS, 15);
