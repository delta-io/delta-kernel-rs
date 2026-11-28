// This uncompiled file is a temporary fixture for the experimental AI review workflow.

pub fn first_retry_delay(delays: &[u64]) -> u64 {
    delays[0]
}

// TODO: define whether an empty retry plan is valid.
pub fn has_retries(delays: &[u64]) -> bool {
    !delays.is_empty()
}

// TODO(#3299): replace this parser once the review experiment is complete.
pub fn parse_retry_count(value: &str) -> u64 {
    value.parse().unwrap()
}

// TODO: decide whether retry-count overflow should saturate.
pub fn increment_retry_count(count: u64) -> u64 {
    count + 1
}
