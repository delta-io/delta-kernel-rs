use std::cmp::Ordering;
use std::error::Error;
use std::fmt::Debug;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Bound {
    LeastUpper,
    GreatestLower,
}

#[derive(Debug)]
pub(crate) enum SearchError<T: Error> {
    OutOfRange,
    KeyFunctionError(T),
}

pub(crate) fn binary_search_by_key_with_bounds<'a, T, K: Ord + Debug, E: Error>(
    values: &'a [T],
    key: K,
    key_fn: impl Fn(&'a T) -> Result<K, E>,
    bound: Bound,
) -> Result<usize, SearchError<E>> {
    let (mut lo, mut hi) = (0, values.len());
    while lo != hi {
        let mid = lo + (hi - lo) / 2;
        debug_assert!(lo <= mid && mid < hi);

        // Use the key function to get a key. Return an error otherwise.
        let mid_key = key_fn(&values[mid]).map_err(SearchError::KeyFunctionError)?;
        match key.cmp(&mid_key) {
            Ordering::Equal => match bound {
                Bound::LeastUpper => hi = mid,
                Bound::GreatestLower => lo = mid + 1,
            },
            Ordering::Less => hi = mid,
            Ordering::Greater => lo = mid + 1,
        }
    }

    // No exact match. We have a valid LUB (GLB) only if the upper (lower)
    // bound actually moved during the search.
    match bound {
        Bound::LeastUpper if hi < values.len() => Ok(hi),
        Bound::GreatestLower if lo > 0 => Ok(lo - 1),
        _ => Err(SearchError::OutOfRange),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeltaResult, Error};

    // Simple key extraction function
    fn get_val(x: &i32) -> DeltaResult<i32> {
        Ok(*x)
    }

    #[test]
    fn test_exact_match() {
        let values = vec![1, 3, 5, 7, 9];

        // LeastUpper bound with exact match
        let result =
            binary_search_by_key_with_bounds(&values, 5, get_val, Bound::LeastUpper).unwrap();
        assert_eq!(result, 2);

        // GreatestLower bound with exact match
        let result =
            binary_search_by_key_with_bounds(&values, 5, get_val, Bound::GreatestLower).unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn test_no_exact_match() {
        let values = vec![1, 3, 5, 7, 9];

        // LeastUpper bound (find element >= key)
        let result =
            binary_search_by_key_with_bounds(&values, 4, get_val, Bound::LeastUpper).unwrap();
        assert_eq!(result, 2); // Index of 5

        // GreatestLower bound (find element <= key)
        let result =
            binary_search_by_key_with_bounds(&values, 6, get_val, Bound::GreatestLower).unwrap();
        assert_eq!(result, 2); // Index of 5
    }

    #[test]
    fn test_duplicate_values() {
        let values = vec![1, 3, 5, 5, 5, 7, 9];

        // LeastUpper should find first occurrence
        let result =
            binary_search_by_key_with_bounds(&values, 5, get_val, Bound::LeastUpper).unwrap();
        assert_eq!(result, 2); // First index of 5

        // GreatestLower should find last occurrence
        let result =
            binary_search_by_key_with_bounds(&values, 5, get_val, Bound::GreatestLower).unwrap();
        assert_eq!(result, 4); // Last index of 5
    }

    #[test]
    fn test_edge_cases() {
        // Empty array
        let empty: Vec<i32> = vec![];
        let result = binary_search_by_key_with_bounds(&empty, 5, get_val, Bound::LeastUpper);
        assert!(result.is_err());

        // Value less than all elements (LeastUpper)
        let values = vec![5, 7, 9];
        let result =
            binary_search_by_key_with_bounds(&values, 3, get_val, Bound::LeastUpper).unwrap();
        assert_eq!(result, 0); // Should return index of first element

        // Value less than all elements (GreatestLower)
        let result = binary_search_by_key_with_bounds(&values, 3, get_val, Bound::GreatestLower);
        assert!(matches!(result, Err(SearchError::OutOfRange))); // No lower bound exists

        // Value greater than all elements (LeastUpper)
        let result = binary_search_by_key_with_bounds(&values, 10, get_val, Bound::LeastUpper);
        assert!(matches!(result, Err(SearchError::OutOfRange))); // No upper bound exists

        // Value greater than all elements (GreatestLower)
        let result =
            binary_search_by_key_with_bounds(&values, 10, get_val, Bound::GreatestLower).unwrap();
        assert_eq!(result, 2); // Should return index of last element
    }
    #[test]
    fn test_error_propagation() {
        let values = vec![1, 3, 5, 7, 9];

        let failing_key_fn = |x: &i32| -> DeltaResult<i32> {
            if *x == 5 {
                Err(Error::generic("Error extracting key"))
            } else {
                Ok(*x)
            }
        };

        let result =
            binary_search_by_key_with_bounds(&values, 7, failing_key_fn, Bound::LeastUpper);
        assert!(matches!(
            result,
            Err(SearchError::KeyFunctionError(crate::Error::Generic(msg))) if msg.contains("Error extracting key")
        ));
    }
}
