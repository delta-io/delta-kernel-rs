//! iceberg compat v1 compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static ICEBERG_COMPAT_V1: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("iceberg-compat-v1.json"));

compliance_case_inexpressible!(ICEBERG_COMPAT_V1, 1); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_success!(ICEBERG_COMPAT_V1, 2, "'delta.feature.icebergCompatV1' is not supported during CREATE TABLE");
compliance_case_inexpressible!(ICEBERG_COMPAT_V1, 3); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V1, 4); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V1, 5); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V1, 6); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V1, 7); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V1, 8); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_success!(ICEBERG_COMPAT_V1, 9);
compliance_case_success!(ICEBERG_COMPAT_V1, 10, "Feature 'icebergCompatV1' is not supported");
compliance_case_success!(ICEBERG_COMPAT_V1, 11);
compliance_case_failure!(ICEBERG_COMPAT_V1, 12, "Feature 'icebergCompatV1' is not supported");
compliance_case_sentinel!(ICEBERG_COMPAT_V1, 13);
