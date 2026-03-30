//! v2 checkpoint compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static V2_CHECKPOINT: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("v2-checkpoint.json"));

compliance_case_success!(V2_CHECKPOINT, 1);
compliance_case_success!(V2_CHECKPOINT, 2);
compliance_case_success!(V2_CHECKPOINT, 3);
compliance_case_success!(V2_CHECKPOINT, 4);
compliance_case_failure!(V2_CHECKPOINT, 5, diverges: "kernel does not enforce v2Checkpoint cannot appear only in writerFeatures");
compliance_case_inexpressible!(V2_CHECKPOINT, 6); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(V2_CHECKPOINT, 7, "'delta.checkpointPolicy' is not supported during CREATE TABLE");
compliance_case_failure!(V2_CHECKPOINT, 8, "Writer features must be Writer-only or also listed in reader features");
compliance_case_failure!(V2_CHECKPOINT, 9, "Reader features must contain only ReaderWriter features that are also listed in writer features");
compliance_case_sentinel!(V2_CHECKPOINT, 10);
