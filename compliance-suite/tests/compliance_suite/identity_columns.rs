//! identity columns compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static IDENTITY_COLUMNS: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("identity-columns.json"));

compliance_case_inexpressible!(IDENTITY_COLUMNS, 1); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(IDENTITY_COLUMNS, 2); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(IDENTITY_COLUMNS, 3);
compliance_case_success!(IDENTITY_COLUMNS, 4, "Feature 'identityColumns' is not supported");
compliance_case_inexpressible!(IDENTITY_COLUMNS, 5); // fixture uses protocol (1,6); create_table() always produces (3,7)
compliance_case_success!(IDENTITY_COLUMNS, 6);
compliance_case_success!(IDENTITY_COLUMNS, 7, "Feature 'checkConstraints' is not supported");
compliance_case_inexpressible!(IDENTITY_COLUMNS, 8); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(IDENTITY_COLUMNS, 9); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(IDENTITY_COLUMNS, 10); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(IDENTITY_COLUMNS, 11, "'delta.feature.identityColumns' is not supported during CREATE TABLE");
compliance_case_success!(IDENTITY_COLUMNS, 12);
compliance_case_success!(IDENTITY_COLUMNS, 13, "Feature 'identityColumns' is not supported");
compliance_case_failure!(IDENTITY_COLUMNS, 14, diverges: "kernel allows identity metadata in schema during create_table without identityColumns in writerFeatures");
compliance_case_failure!(IDENTITY_COLUMNS, 15, diverges: "kernel allows empty_commit on tables with identity metadata without identityColumns in writerFeatures");
compliance_case_sentinel!(IDENTITY_COLUMNS, 16);
