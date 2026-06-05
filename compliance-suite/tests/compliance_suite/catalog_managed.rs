//! catalog managed compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static CATALOG_MANAGED: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("catalog-managed.json"));

compliance_case_success!(CATALOG_MANAGED, 1, "requires a catalog committer");
compliance_case_success!(CATALOG_MANAGED, 2);
compliance_case_failure!(CATALOG_MANAGED, 3, diverges: "kernel does not enforce that catalogManaged tables require inCommitTimestamp");
compliance_case_failure!(CATALOG_MANAGED, 4, "In-Commit Timestamp not found in commit file");
compliance_case_failure!(CATALOG_MANAGED, 5, "requires a catalog committer");
compliance_case_success!(CATALOG_MANAGED, 6, "'delta.feature.catalogOwned-preview' is not supported during CREATE TABLE");
compliance_case_success!(CATALOG_MANAGED, 7);
compliance_case_success!(CATALOG_MANAGED, 8, "requires a catalog committer");
compliance_case_failure!(CATALOG_MANAGED, 9, "Reader features must contain only ReaderWriter features that are also listed in writer features");
compliance_case_sentinel!(CATALOG_MANAGED, 10);
