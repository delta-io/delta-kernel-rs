//! This module defines the [`binary_search_by_key_with_bounds`] utility method. This can be used to
//! search over sorted slices of values to find the greatest lower or least upper bounds.
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::Debug;

/// Defines the type of bound to return from a binary search operation.
///
/// For a search operation over `values` using key `key`:
/// * [`Bound::LeastUpper`] - Finds the smallest index `i` such that `values[i] >= key`.
///   This represents the first element greater than or equal to the search key.
///
/// * [`Bound::GreatestLower`] - Finds the largest index `i` such that `values[i] <= key`.
///   This represents the last element less than or equal to the search key.
#[allow(unused)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum Bound {
    LeastUpper,
    GreatestLower,
}

/// Represents the errors that can occur when performing binary search using
/// [`binary_search_by_key_with_bounds`].
#[allow(unused)]
#[derive(Debug)]
pub(crate) enum SearchError<T: Error> {
    /// Error that occurs when a search goes out of range. The meaning of "out of range" depends on
    /// the search [`Bound`]:
    /// - If the search bound is [`Bound::LeastUpper`], then no element was greater than or equal
    ///   to the provided key.
    /// - If the search bound is [`Bound::GreatestLower`], then no element was less than or equal
    ///   to the provided key.
    OutOfRange,
    /// Error that occurs when the `key_fn` fails when retrieving the key in
    /// [`binary_search_by_key_with_bounds`]. The error that occurred in the `key_fn` is returned in
    /// this error variant.
    KeyFunctionError(T),
}

/// Performs a binary search on a slice to find the greatest lower or least upper bound.
/// This function performs a binary search with the following properties:
///
/// 1. For [`Bound::LeastUpper`], find the smallest index `i` such that `values[i] >= key`
/// 2. For [`Bound::GreatestLower`], find the largest index `i` such that `values[i] <= key`
///
/// # Arguments
///
/// * values - The slice to search in
/// * key - The key value to search for
/// * key_fn - A fallible function that extracts a comparable key from slice elements
/// * bound - The type of bound to search for (least upper bound or greatest lower bound)
///
/// # Returns
///
/// * Ok(usize) - The index of the found element
/// * [`SearchError::OutOfRange`] - If no suitable bound exists in the slice
/// * [`SearchError::KeyFunctionError`] - If the key function fails. This contains the error
///   emitted by the `key_fn`.
///
/// #Safety
/// Assumes that the provided input is sorted by key using the `key_fn`. If the input is not
/// sorted by key, then the output is unspecified and meaningless.
///
/// # Examples
///
/// ## Finding the Least Upper Bound
/// ```rust,ignore
/// # use delta_kernel::history_manager::search::*;
/// # use delta_kernel::DeltaResult;
/// let values = [10, 20, 30, 40, 50];
/// // Simple key function that just returns the value
/// let key_fn = |&val| -> DeltaResult<_> { Ok(val) };
///
/// // Find the least upper bound for 25 (element that is ≥ 25)
/// let result = binary_search_by_key_with_bounds(
///     &values,
///     25,
///     key_fn,
///     Bound::LeastUpper
/// );
/// assert_eq!(result.unwrap(), 2); // Index of 30
/// ```
///
/// ## Finding the Greatest Lower Bound
/// ```rust,ignore
/// # use delta_kernel::history_manager::search::*;
/// # use delta_kernel::DeltaResult;
/// let values = [10, 20, 30, 40, 50];
/// let key_fn = |&val| -> DeltaResult<_> { Ok(val) };
///
/// // Find the greatest lower bound for 25 (element that is ≤ 25)
/// let result = binary_search_by_key_with_bounds(
///     &values,
///     25,
///     key_fn,
///     Bound::GreatestLower
/// );
/// assert_eq!(result.unwrap(), 1); // Index of 20
/// ```
///
/// ## Handling Out of Range Values
/// ```rust,ignore
/// # use delta_kernel::history_manager::search::*;
/// # use delta_kernel::DeltaResult;
/// let values = [10, 20, 30, 40, 50];
/// let key_fn = |&val| -> DeltaResult<_> { Ok(val) };
///
/// // Finding a bound that's out of range
/// let result = binary_search_by_key_with_bounds(
///     &values,
///     5,
///     key_fn,
///     Bound::GreatestLower
/// );
/// assert!(matches!(result, Err(SearchError::OutOfRange)));
/// ```
///
/// ## Using a Fallible Key Function
/// ```rust,ignore
/// # use delta_kernel::history_manager::search::*;
/// # use delta_kernel::DeltaResult;
/// // Using a fallible key function
/// let values = ["10", "20", "thirty", "40"];
/// let result = binary_search_by_key_with_bounds(
///     &values,
///     25,
///     |&s| s.parse::<i32>(),
///     Bound::LeastUpper
/// );
/// assert!(matches!(result, Err(SearchError::KeyFunctionError(_))));
/// ```
#[allow(unused)]
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
        match (key.cmp(&mid_key), bound) {
            (Ordering::Less, _) => hi = mid,
            (Ordering::Equal, Bound::LeastUpper) => hi = mid,
            (Ordering::Equal, Bound::GreatestLower) => lo = mid + 1,
            (Ordering::Greater, _) => lo = mid + 1,
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
