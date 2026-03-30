//! change data feed compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static CHANGE_DATA_FEED: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("change-data-feed.json"));

compliance_case_inexpressible!(CHANGE_DATA_FEED, 1); // fixture uses protocol (1,4); create_table() always produces (3,7)
compliance_case_inexpressible!(CHANGE_DATA_FEED, 2); // fixture uses protocol (1,3); create_table() always produces (3,7)
compliance_case_success!(CHANGE_DATA_FEED, 3);
compliance_case_success!(CHANGE_DATA_FEED, 4, "Feature 'checkConstraints' is not supported");
compliance_case_failure!(CHANGE_DATA_FEED, 5, diverges: "kernel does not require changeDataFeed listed when delta.enableChangeDataFeed=true");
compliance_case_success!(CHANGE_DATA_FEED, 6);
compliance_case_success!(CHANGE_DATA_FEED, 7);
compliance_case_success!(CHANGE_DATA_FEED, 8);
compliance_case_success!(CHANGE_DATA_FEED, 9);
compliance_case_success!(CHANGE_DATA_FEED, 10);
compliance_case_success!(CHANGE_DATA_FEED, 11);
compliance_case_success!(CHANGE_DATA_FEED, 12);
compliance_case_success!(CHANGE_DATA_FEED, 13, "'delta.columnMapping.maxColumnId' is not supported during CREATE TABLE");
compliance_case_success!(CHANGE_DATA_FEED, 14);
compliance_case_success!(CHANGE_DATA_FEED, 15);
compliance_case_success!(CHANGE_DATA_FEED, 16);
compliance_case_sentinel!(CHANGE_DATA_FEED, 17);
