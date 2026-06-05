//! type widening compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static TYPE_WIDENING: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("type-widening.json"));

compliance_case_success!(TYPE_WIDENING, 1);
compliance_case_success!(TYPE_WIDENING, 2);
compliance_case_failure!(TYPE_WIDENING, 3, diverges: "kernel does not reject typeWidening listed only in writerFeatures without a matching readerFeatures entry");
compliance_case_success!(TYPE_WIDENING, 4);
compliance_case_success!(TYPE_WIDENING, 5);
compliance_case_success!(TYPE_WIDENING, 6);
compliance_case_success!(TYPE_WIDENING, 7);
compliance_case_failure!(TYPE_WIDENING, 8, diverges: "kernel does not reject unsupported type-change entries in the schema");
compliance_case_success!(TYPE_WIDENING, 9);
compliance_case_success!(TYPE_WIDENING, 10, "Writer features must be Writer-only or also listed in reader features");
compliance_case_success!(TYPE_WIDENING, 11, "Feature 'typeWidening' is not supported for writes");
compliance_case_success!(TYPE_WIDENING, 12, "Feature 'typeWidening' is not supported for writes");
compliance_case_success!(TYPE_WIDENING, 13);
compliance_case_success!(TYPE_WIDENING, 14, "'delta.feature.typeWidening-preview' is not supported during CREATE TABLE");
compliance_case_success!(TYPE_WIDENING, 15);
compliance_case_sentinel!(TYPE_WIDENING, 16);
