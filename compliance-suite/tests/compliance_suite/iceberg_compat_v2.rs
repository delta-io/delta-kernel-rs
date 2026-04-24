//! iceberg compat v2 compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static ICEBERG_COMPAT_V2: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("iceberg-compat-v2.json"));

compliance_case_inexpressible!(ICEBERG_COMPAT_V2, 1); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_success!(ICEBERG_COMPAT_V2, 2, "delta.enableIcebergCompatV2' is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 3);
compliance_case_success!(ICEBERG_COMPAT_V2, 4, "Feature 'icebergCompatV2' is not supported");
compliance_case_success!(ICEBERG_COMPAT_V2, 5, "is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 6, "'delta.feature.icebergCompatV2' is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 7, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 8, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 9, "is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 10, "is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 11, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 12, "is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 13);
compliance_case_failure!(ICEBERG_COMPAT_V2, 14, "is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 15, "delta.enableIcebergCompatV2' is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 16, "is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 17, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 18, "Unsupported Delta table type: 'void'");
compliance_case_failure!(ICEBERG_COMPAT_V2, 19, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 20, "is not supported during CREATE TABLE");
compliance_case_success!(ICEBERG_COMPAT_V2, 21, "Unsupported Delta table type: 'void'");
compliance_case_failure!(ICEBERG_COMPAT_V2, 22, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 23, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 24, "is not supported during CREATE TABLE");
compliance_case_failure!(ICEBERG_COMPAT_V2, 25, "is not supported during CREATE TABLE");
compliance_case_sentinel!(ICEBERG_COMPAT_V2, 26);
