//! append only compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static APPEND_ONLY: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("append-only.json"));

compliance_case_inexpressible!(APPEND_ONLY, 1); // fixture uses protocol (1,1); create_table() always produces (3,7)
compliance_case_inexpressible!(APPEND_ONLY, 2); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_inexpressible!(APPEND_ONLY, 3); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_success!(APPEND_ONLY, 4);
compliance_case_success!(APPEND_ONLY, 5);
compliance_case_success!(APPEND_ONLY, 6);
compliance_case_success!(APPEND_ONLY, 7);
compliance_case_success!(APPEND_ONLY, 8);
compliance_case_success!(APPEND_ONLY, 9);
compliance_case_success!(APPEND_ONLY, 10);
compliance_case_success!(APPEND_ONLY, 11);
compliance_case_success!(APPEND_ONLY, 12);
compliance_case_success!(APPEND_ONLY, 13);
compliance_case_success!(APPEND_ONLY, 14);
compliance_case_success!(APPEND_ONLY, 15);
compliance_case_sentinel!(APPEND_ONLY, 16);
