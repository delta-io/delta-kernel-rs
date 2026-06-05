//! invariants compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static INVARIANTS: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("invariants.json"));

compliance_case_failure!(INVARIANTS, 1, diverges: "kernel does not require invariants listed when schema has invariant expressions");
compliance_case_failure!(INVARIANTS, 2, diverges: "kernel does not require invariants listed when schema has invariant expressions");
compliance_case_inexpressible!(INVARIANTS, 3); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_success!(INVARIANTS, 4, "Column invariants are not yet supported");
compliance_case_success!(INVARIANTS, 5);
compliance_case_success!(INVARIANTS, 6);
compliance_case_success!(INVARIANTS, 7, "Column invariants are not yet supported");
compliance_case_inexpressible!(INVARIANTS, 8); // fixture uses protocol (1,1); create_table() always produces (3,7)
compliance_case_success!(INVARIANTS, 9, "'delta.feature.invariants' is not supported during CREATE TABLE");
compliance_case_success!(INVARIANTS, 10);
compliance_case_success!(INVARIANTS, 11);
compliance_case_success!(INVARIANTS, 12);
compliance_case_success!(INVARIANTS, 13, "'delta.feature.invariants' is not supported during CREATE TABLE");
compliance_case_success!(INVARIANTS, 14, "Column invariants are not yet supported");
compliance_case_sentinel!(INVARIANTS, 15);
