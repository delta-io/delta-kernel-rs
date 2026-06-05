//! vacuum protocol check compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static VACUUM_PROTOCOL_CHECK: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("vacuum-protocol-check.json"));

compliance_case_success!(VACUUM_PROTOCOL_CHECK, 1);
compliance_case_success!(VACUUM_PROTOCOL_CHECK, 2);
compliance_case_success!(VACUUM_PROTOCOL_CHECK, 3);
compliance_case_inexpressible!(VACUUM_PROTOCOL_CHECK, 4); // fixture uses reader-only feature placement; create_table() enforces symmetric placement
compliance_case_failure!(VACUUM_PROTOCOL_CHECK, 5, diverges: "kernel does not enforce vacuumProtocolCheck cannot appear only in writerFeatures");
compliance_case_success!(VACUUM_PROTOCOL_CHECK, 6);
compliance_case_failure!(VACUUM_PROTOCOL_CHECK, 7, "Reader features must contain only ReaderWriter features that are also listed in writer features");
compliance_case_failure!(VACUUM_PROTOCOL_CHECK, 8, "Writer features must be Writer-only or also listed in reader features");
compliance_case_sentinel!(VACUUM_PROTOCOL_CHECK, 9);
