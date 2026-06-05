//! variant compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static VARIANT: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("variant.json"));

compliance_case_success!(VARIANT, 1, "'delta.feature.variantType' is not supported during CREATE TABLE");
compliance_case_inexpressible!(VARIANT, 2); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_failure!(VARIANT, 3, diverges: "kernel does not require variantType listed when schema has variant columns");
compliance_case_success!(VARIANT, 4);
compliance_case_success!(VARIANT, 5, "Unknown complex type: 'variant'");
compliance_case_success!(VARIANT, 6);
compliance_case_failure!(VARIANT, 7, "VARIANT columns but does not have the required 'variantType' feature");
compliance_case_success!(VARIANT, 8);
compliance_case_success!(VARIANT, 9);
compliance_case_failure!(VARIANT, 10, "VARIANT columns but does not have the required 'variantType' feature");
compliance_case_success!(VARIANT, 11, "'delta.feature.variantType-preview' is not supported during CREATE TABLE");
compliance_case_success!(VARIANT, 12);
compliance_case_failure!(VARIANT, 13, "VARIANT columns but does not have the required 'variantType' feature");
compliance_case_success!(VARIANT, 14);
compliance_case_sentinel!(VARIANT, 15);
