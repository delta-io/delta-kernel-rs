//! column mapping compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static COLUMN_MAPPING: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("column-mapping.json"));

compliance_case_inexpressible!(COLUMN_MAPPING, 1); // fixture uses protocol (1,5); create_table() always produces (3,7)
compliance_case_inexpressible!(COLUMN_MAPPING, 2); // fixture uses protocol (2,5); create_table() always produces (3,7)
compliance_case_inexpressible!(COLUMN_MAPPING, 3); // fixture uses protocol (2,5); create_table() always produces (3,7)
compliance_case_failure!(COLUMN_MAPPING, 4, "Duplicate column mapping ID");
compliance_case_failure!(COLUMN_MAPPING, 5, "annotation on field 'id' must be a number");
compliance_case_success!(COLUMN_MAPPING, 6, "already has column mapping metadata");
compliance_case_success!(COLUMN_MAPPING, 7, "already has column mapping metadata");
compliance_case_success!(COLUMN_MAPPING, 8, "is annotated with delta.columnMapping.physicalName");
compliance_case_success!(COLUMN_MAPPING, 9, "is annotated with delta.columnMapping.physicalName");
compliance_case_success!(COLUMN_MAPPING, 10);
compliance_case_success!(COLUMN_MAPPING, 11, "Writer features must be Writer-only or also listed in reader features");
compliance_case_success!(COLUMN_MAPPING, 12);
compliance_case_success!(COLUMN_MAPPING, 13);
compliance_case_success!(COLUMN_MAPPING, 14);
compliance_case_success!(COLUMN_MAPPING, 15);
compliance_case_success!(COLUMN_MAPPING, 16);
compliance_case_success!(COLUMN_MAPPING, 17);
compliance_case_success!(COLUMN_MAPPING, 18);
compliance_case_success!(COLUMN_MAPPING, 19, "Feature 'checkConstraints' is not supported");
compliance_case_success!(COLUMN_MAPPING, 20, "Feature 'checkConstraints' is not supported");
compliance_case_success!(COLUMN_MAPPING, 21);
compliance_case_success!(COLUMN_MAPPING, 22);
compliance_case_failure!(COLUMN_MAPPING, 23, diverges: "kernel auto-annotates missing CM fields (physicalName+id) when mode=name is active instead of rejecting");
compliance_case_failure!(COLUMN_MAPPING, 24, diverges: "kernel auto-annotates missing CM fields (physicalName+id) when mode=id is active instead of rejecting");
compliance_case_failure!(COLUMN_MAPPING, 25, "lacks the delta.columnMapping.physicalName annotation");
compliance_case_failure!(COLUMN_MAPPING, 26, "lacks the delta.columnMapping.physicalName annotation");
compliance_case_sentinel!(COLUMN_MAPPING, 27);
