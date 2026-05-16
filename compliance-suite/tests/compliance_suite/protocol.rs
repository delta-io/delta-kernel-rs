//! Delta protocol compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static PROTOCOL_VALIDITY: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("protocol-validity.json"));

compliance_case_success!(PROTOCOL_VALIDITY, 1);
compliance_case_success!(PROTOCOL_VALIDITY, 2);
compliance_case_failure!(PROTOCOL_VALIDITY, 3, "Reader features must be present when minimum reader version = 3");
compliance_case_failure!(PROTOCOL_VALIDITY, 4, "Reader features should be present in writer features");
compliance_case_failure!(PROTOCOL_VALIDITY, 5, "Writer features must be present when minimum writer version = 7");
compliance_case_failure!(PROTOCOL_VALIDITY, 6, "Reader features must not be present when minimum reader version != 3");
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 7); // fixture uses protocol (3,5); create_table() always produces (3,7)
compliance_case_failure!(PROTOCOL_VALIDITY, 8, "Feature 'unknownFeature' is not supported");
compliance_case_success!(PROTOCOL_VALIDITY, 9);
compliance_case_failure!(PROTOCOL_VALIDITY, 10, "Feature 'unknownFeature' is not supported");
compliance_case_failure!(PROTOCOL_VALIDITY, 11, "Reader features must contain only ReaderWriter features that are also listed in writer features");
compliance_case_failure!(PROTOCOL_VALIDITY, 12, "Writer features must be Writer-only or also listed in reader features");
compliance_case_failure!(PROTOCOL_VALIDITY, 13, "Reader features must not be present when minimum reader version != 3");
compliance_case_success!(PROTOCOL_VALIDITY, 14, "Writer features must not be present when minimum writer version != 7");
compliance_case_failure!(PROTOCOL_VALIDITY, 15, "Writer features must not be present when minimum writer version != 7");
compliance_case_success!(PROTOCOL_VALIDITY, 16);
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 17); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 18); // fixture uses protocol (2,999); create_table() always produces (3,7)
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 19); // fixture uses protocol (999,999); create_table() always produces (3,7)
compliance_case_success!(PROTOCOL_VALIDITY, 20);
compliance_case_success!(PROTOCOL_VALIDITY, 21);
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 22); // create_table() always produces both feature arrays; fixture omits writerFeatures
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 23); // create_table() always produces both feature arrays; fixture omits readerFeatures
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 24); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_failure!(PROTOCOL_VALIDITY, 25, "Writer features must be present when minimum writer version = 7");
compliance_case_failure!(PROTOCOL_VALIDITY, 26, "Reader features must not be present when minimum reader version != 3");
compliance_case_failure!(PROTOCOL_VALIDITY, 27, "Reader features should be present in writer features");
compliance_case_failure!(PROTOCOL_VALIDITY, 28, "'delta.feature.unknownFeature' is not supported during CREATE TABLE");
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 29); // fixture uses protocol (2,7); create_table() always produces (3,7)
compliance_case_failure!(PROTOCOL_VALIDITY, 30, "Feature 'unknownFeature' is not supported");
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 31); // create_table() enforces symmetric placement; fixture uses reader-only feature
compliance_case_failure!(PROTOCOL_VALIDITY, 32, diverges: "kernel does not reject reader-writer features listed only in writerFeatures during create_table");
compliance_case_failure!(PROTOCOL_VALIDITY, 33, "Reader features must contain only ReaderWriter features that are also listed in writer features");
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 34); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_failure!(PROTOCOL_VALIDITY, 35, "'delta.feature.unknownFeature' is not supported during CREATE TABLE");
compliance_case_failure!(PROTOCOL_VALIDITY, 36, "Writer features must be Writer-only or also listed in reader features");
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 37); // fixture uses protocol (1,1); create_table() always produces (3,7)
compliance_case_failure!(PROTOCOL_VALIDITY, 38, diverges: "kernel does not validate nullable=false fields require invariants or checkConstraints");
compliance_case_success!(PROTOCOL_VALIDITY, 39);
compliance_case_failure!(PROTOCOL_VALIDITY, 40, "Reader features must be present when minimum reader version = 3");
compliance_case_failure!(PROTOCOL_VALIDITY, 41, "Reader features must be present when minimum reader version = 3");
compliance_case_inexpressible!(PROTOCOL_VALIDITY, 42); // create_table() always produces both feature arrays; fixture omits writerFeatures
compliance_case_failure!(PROTOCOL_VALIDITY, 43, "Writer features must be present when minimum writer version = 7");
compliance_case_failure!(PROTOCOL_VALIDITY, 44, "Writer features must be present when minimum writer version = 7");
compliance_case_success!(PROTOCOL_VALIDITY, 45);
compliance_case_success!(PROTOCOL_VALIDITY, 46);
compliance_case_sentinel!(PROTOCOL_VALIDITY, 47);
