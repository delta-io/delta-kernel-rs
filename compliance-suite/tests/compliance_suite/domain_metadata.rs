//! domain metadata compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static DOMAIN_METADATA: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("domain-metadata.json"));

compliance_case_inexpressible!(DOMAIN_METADATA, 1); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(DOMAIN_METADATA, 2);
compliance_case_success!(DOMAIN_METADATA, 3);
compliance_case_success!(DOMAIN_METADATA, 4);
compliance_case_success!(DOMAIN_METADATA, 5);
compliance_case_sentinel!(DOMAIN_METADATA, 6);
