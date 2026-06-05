//! iceberg compat v3 preview compliance tests.
//!
//! All 8 cases use protocol shapes that `create_table()` cannot produce. The fixture uses
//! protocol (2,7) with asymmetric feature combinations, while `create_table()` always
//! produces (3,7) with symmetric feature arrays.

use super::Fixture;
use std::sync::LazyLock;

static ICEBERG_COMPAT_V3_PREVIEW: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("iceberg-compat-v3-preview.json"));

compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 1); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 2); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 3); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 4); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 5); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 6); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 7); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_COMPAT_V3_PREVIEW, 8); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_sentinel!(ICEBERG_COMPAT_V3_PREVIEW, 9);
