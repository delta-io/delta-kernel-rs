//! in commit timestamp compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static IN_COMMIT_TIMESTAMP: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("in-commit-timestamp.json"));

compliance_case_inexpressible!(IN_COMMIT_TIMESTAMP, 1); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(IN_COMMIT_TIMESTAMP, 2); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(IN_COMMIT_TIMESTAMP, 3);
compliance_case_success!(IN_COMMIT_TIMESTAMP, 4);
compliance_case_success!(IN_COMMIT_TIMESTAMP, 5);
compliance_case_failure!(IN_COMMIT_TIMESTAMP, 6, diverges: "kernel does not require inCommitTimestamp listed when delta.enableInCommitTimestamps=true");
compliance_case_inexpressible!(IN_COMMIT_TIMESTAMP, 7); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(IN_COMMIT_TIMESTAMP, 8);
compliance_case_success!(IN_COMMIT_TIMESTAMP, 9);
compliance_case_sentinel!(IN_COMMIT_TIMESTAMP, 10);
