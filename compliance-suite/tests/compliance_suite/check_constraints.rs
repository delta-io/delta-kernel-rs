//! check constraints compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static CHECK_CONSTRAINTS: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("check-constraints.json"));

compliance_case_inexpressible!(CHECK_CONSTRAINTS, 1); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_failure!(CHECK_CONSTRAINTS, 2, "'delta.constraints.id_positive' is not supported during CREATE TABLE");
compliance_case_failure!(CHECK_CONSTRAINTS, 3, diverges: "kernel does not reject empty_commit on tables that already carry orphan checkConstraints metadata");
compliance_case_inexpressible!(CHECK_CONSTRAINTS, 4); // fixture uses protocol (1,3); create_table() always produces (3,7)
compliance_case_success!(CHECK_CONSTRAINTS, 5);
compliance_case_success!(CHECK_CONSTRAINTS, 6, "Feature 'checkConstraints' is not supported");
compliance_case_success!(CHECK_CONSTRAINTS, 7, "Feature 'checkConstraints' is not supported");
compliance_case_success!(CHECK_CONSTRAINTS, 8, "Feature 'checkConstraints' is not supported");
compliance_case_failure!(CHECK_CONSTRAINTS, 9, "'delta.feature.checkConstraints' is not supported during CREATE TABLE");
compliance_case_success!(CHECK_CONSTRAINTS, 10, "'delta.feature.checkConstraints' is not supported during CREATE TABLE");
compliance_case_success!(CHECK_CONSTRAINTS, 11, "'delta.feature.checkConstraints' is not supported during CREATE TABLE");
compliance_case_success!(CHECK_CONSTRAINTS, 12, "Feature 'checkConstraints' is not supported");
compliance_case_success!(CHECK_CONSTRAINTS, 13, "'delta.feature.checkConstraints' is not supported during CREATE TABLE");
compliance_case_success!(CHECK_CONSTRAINTS, 14);
compliance_case_success!(CHECK_CONSTRAINTS, 15);
compliance_case_success!(CHECK_CONSTRAINTS, 16);
compliance_case_sentinel!(CHECK_CONSTRAINTS, 17);
