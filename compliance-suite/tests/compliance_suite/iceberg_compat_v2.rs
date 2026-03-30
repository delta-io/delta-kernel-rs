//! iceberg compat v2 compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static ICEBERG_COMPAT_V2: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("iceberg-compat-v2.json"));

compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 1); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 2); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_success!(ICEBERG_COMPAT_V2, 3);
compliance_case_success!(ICEBERG_COMPAT_V2, 4, "Feature 'icebergCompatV2' is not supported");
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 5); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_success!(ICEBERG_COMPAT_V2, 6, "'delta.feature.icebergCompatV2' is not supported during CREATE TABLE");
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 7); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 8); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 9); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 10); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 11); // fixture requires ICv1+ICv2 combination; inexpressible shape
compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 12); // fixture requires ICv1+ICv2 combination at (3,7); inexpressible shape
compliance_case_success!(ICEBERG_COMPAT_V2, 13);
compliance_case_failure!(ICEBERG_COMPAT_V2, 14, "Feature 'icebergCompatV2' is not supported");
compliance_case_sentinel!(ICEBERG_COMPAT_V2, 15);
