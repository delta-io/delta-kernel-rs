//! row tracking compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static ROW_TRACKING: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("row-tracking.json"));

compliance_case_inexpressible!(ROW_TRACKING, 1); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ROW_TRACKING, 2); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ROW_TRACKING, 3); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ROW_TRACKING, 4); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ROW_TRACKING, 5); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ROW_TRACKING, 6); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ROW_TRACKING, 7); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(ROW_TRACKING, 8);
compliance_case_success!(ROW_TRACKING, 9);
compliance_case_success!(ROW_TRACKING, 10);
compliance_case_success!(ROW_TRACKING, 11);
compliance_case_failure!(ROW_TRACKING, 12, diverges: "kernel does not reject rowTracking simultaneously suspended and enabled");
compliance_case_failure!(ROW_TRACKING, 13, diverges: "kernel does not reject missing rowTracking materialized column prerequisites during empty_commit");
compliance_case_sentinel!(ROW_TRACKING, 14);
