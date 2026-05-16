//! variant shredding preview compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static VARIANT_SHREDDING_PREVIEW: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("variant-shredding-preview.json"));

compliance_case_failure!(VARIANT_SHREDDING_PREVIEW, 1, "'delta.feature.variantShredding-preview' is not supported during CREATE TABLE");
compliance_case_success!(VARIANT_SHREDDING_PREVIEW, 2, "is not supported during CREATE TABLE"); // variantType + variantShredding-preview both unsupported; which fires first is non-deterministic
compliance_case_success!(VARIANT_SHREDDING_PREVIEW, 3);
compliance_case_success!(VARIANT_SHREDDING_PREVIEW, 4, "is not supported during CREATE TABLE"); // variantType-preview + variantShredding-preview both unsupported; which fires first is non-deterministic
compliance_case_sentinel!(VARIANT_SHREDDING_PREVIEW, 5);
