//! Parsing utilities for TripAdvisor XML feed.

use futures::stream::Stream;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tracing::debug;

/// Expected string at start of a <Property /> item.
const START_TOKEN: &[u8] = b"<Property ";

/// Expected string at the end of a <Property /> item.
const END_TOKEN: &[u8] = b"</Property>\n";

/// Split each <Property /> item into a buffer that can be deserialized independently.
///
/// There are a few assumptions that are made over the input data, mostly for
/// performance reasons:
///  - each item starts with a line containing "<Property ", the beginning of
///    the line will be ignored.
///  - each item ends with a line "</Property>"
pub fn split_raw_properties(input: impl AsyncBufRead + Unpin) -> impl Stream<Item = Vec<u8>> {
    futures::stream::unfold(input, |mut input| async {
        let mut buffer = Vec::new();

        while input
            .read_until(b'\n', &mut buffer)
            .await
            .expect("failed to read line from XML")
            > 0
        {
            if buffer.ends_with(END_TOKEN) {
                // The first buffer may contain some extra information
                let token_start = {
                    if let Some(start) = find_naive(&buffer, START_TOKEN) {
                        start
                    } else {
                        panic!(
                            "Found a property which didn't start with pattern `{}`",
                            std::str::from_utf8(START_TOKEN).unwrap()
                        );
                    }
                };

                if token_start > 0 {
                    debug!(
                        "Ignored beginning of buffer: {}",
                        String::from_utf8_lossy(&buffer[..token_start])
                    );

                    buffer = buffer[token_start..].to_vec();
                }

                return Some((buffer, input));
            }
        }

        if !buffer.is_empty() {
            debug!("Ignored end of file: {}", String::from_utf8_lossy(&buffer));
        }

        None
    })
}

/// Search for first occurrence of `needle` in `haystack`.
///
/// This is a naïve approach that runs in `O(haystack.len() * needle.len())`
/// but which is still very efficient with small instances of `needle` which
/// is the case here.
///
/// # Example
///
/// ```
/// # use fafnir::sources::tripadvisor::parse::find_naive;
/// assert_eq!(find_naive(b"barbarbare", b"barbare"), Some(3));
/// assert_eq!(find_naive(b"yes", b"no"), None);
/// ```
pub fn find_naive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|win| win == needle)
}
