//! A collator for tests of collated comparisons.

use std::cmp::Ordering;

use super::collation::ArrowCollator;
use crate::arrow::array::{BooleanArray, StringArray};
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// Test collator: ASCII case-insensitive under the one collation it names, an error for any
/// other. Returns FALSE for null values, like a selection mask.
#[doc(hidden)]
#[derive(Debug)]
pub struct AsciiCaseInsensitive(pub &'static str);

impl ArrowCollator for AsciiCaseInsensitive {
    fn compare(
        &self,
        collation: &str,
        ordering: Ordering,
        values: &StringArray,
        literal: &str,
    ) -> KernelResult<BooleanArray> {
        require!(
            collation == self.0,
            KernelError::unsupported(format!("Unknown collation '{collation}'"))
        );
        let literal = literal.to_ascii_lowercase();
        let matches = |value: &str| value.to_ascii_lowercase().cmp(&literal) == ordering;
        Ok(values
            .iter()
            .map(|value| Some(value.is_some_and(matches)))
            .collect())
    }
}
