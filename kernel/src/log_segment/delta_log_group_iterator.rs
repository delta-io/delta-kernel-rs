//! An iterator that groups [`ParsedLogPath`]s by version.

use tracing::warn;

use crate::log_segment::ParsedLogPath;
use crate::DeltaResult;
use crate::Version;
use std::iter::Peekable;

/**
 * The [`DeltaLogGroupingIterator`] is a utility iterator that groups log paths.
 * It takes an iterator of [`ParsedLogPath`]s and groups them by version.
 * For example for an input iterator:
 * - 11.checkpoint.0.1.parquet
 * - 11.checkpoint.1.1.parquet
 * - 11.json
 * - 12.checkpoint.parquet
 * - 12.json
 * - 13.json
 * - 14.json
 * - 15.checkpoint.0.1.parquet
 * - 15.checkpoint.1.1.parquet
 * - 15.checkpoint.<uuid>.parquet
 * - 15.json
 *  This will return:
 *  - (11, Vec[11.checkpoint.0.1.parquet, 11.checkpoint.1.1.parquet, 11.json])
 *  - (12, Vec[12.checkpoint.parquet, 12.json])
 *  - (13, Vec[13.json])
 *  - (14, Vec[14.json])
 *  - (15, Vec[15.checkpoint.0.1.parquet, 15.checkpoint.1.1.parquet, 15.checkpoint.<uuid>.parquet,
 *             15.json])
 */
pub(super) struct DeltaLogGroupingIterator<I>
where
    I: Iterator<Item = DeltaResult<ParsedLogPath>>,
{
    iter: Peekable<I>,
}

impl<I> DeltaLogGroupingIterator<I>
where
    I: Iterator<Item = DeltaResult<ParsedLogPath>>,
{
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter: iter.peekable(),
        }
    }
}

impl<I> Iterator for DeltaLogGroupingIterator<I>
where
    I: Iterator<Item = DeltaResult<ParsedLogPath>>,
{
    type Item = (Version, Vec<ParsedLogPath>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut paths = Vec::new();
        let mut version = None;

        while let Some(next) = self.iter.peek() {
            match next {
                Ok(next_path) => {
                    if let Some(v) = version {
                        if next_path.version != v {
                            break;
                        }
                    } else {
                        version = Some(next_path.version);
                    }

                    if let Ok(path) = self.iter.next().unwrap() {
                        paths.push(path);
                    }
                }
                Err(e) => {
                    warn!("Error processing path: {:?}", e);
                    self.iter.next(); // Skip the error
                }
            }
        }

        version.map(|v| (v, paths))
    }
}
