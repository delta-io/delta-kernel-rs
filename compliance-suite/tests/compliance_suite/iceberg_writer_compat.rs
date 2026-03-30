//! iceberg writer compat compliance tests.
//!
//! All 6 cases use protocol shapes that `create_table()` cannot produce. The fixture uses
//! protocol (2,7) with asymmetric feature combinations, while `create_table()` always
//! produces (3,7) with symmetric feature arrays.

use super::Fixture;
use std::sync::LazyLock;

static ICEBERG_WRITER_COMPAT: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("iceberg-writer-compat.json"));

compliance_case_inexpressible!(ICEBERG_WRITER_COMPAT, 1); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_WRITER_COMPAT, 2); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_WRITER_COMPAT, 3); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_WRITER_COMPAT, 4); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_WRITER_COMPAT, 5); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(ICEBERG_WRITER_COMPAT, 6); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_sentinel!(ICEBERG_WRITER_COMPAT, 7);
