//! Parsing utilities for TripAdvisor XML feed.

use futures::stream::Stream;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tracing::debug;

/// Expected string at start of a <Property /> item.
const START_TOKEN: &str = "<Property ";

/// Expected string at the end of a <Property /> item.
const END_TOKEN: &str = "</Property>\n";

/// Split each <Property /> item into a buffer that can be deserialized independantly.
///
/// There are a few assumptions that are made over the input data, mostly for
/// performance reasons:
///  - each item starts with a line containing "<Property ", the begining of
///    the line will be ignoreed.
///  - each item ends with a line "</Property>"
pub fn split_raw_properties(input: impl AsyncBufRead + Unpin) -> impl Stream<Item = String> {
    futures::stream::unfold(input, |mut input| async {
        let mut first_line = true;
        let mut buffer = String::new();

        while input
            .read_line(&mut buffer)
            .await
            .expect("failed to read line from XML")
            > 0
        {
            // The first line may contain some extra XML informations
            if first_line {
                first_line = false;

                let token_start = {
                    if let Some(start) = buffer.find(START_TOKEN) {
                        start
                    } else {
                        break;
                    }
                };

                if token_start > 0 {
                    debug!("Ignored begining of file: {}", &buffer[..token_start]);
                    buffer = buffer[token_start..].to_string();
                }
            }

            if buffer.ends_with(END_TOKEN) {
                return Some((buffer, input));
            }
        }

        debug!("Ignored end of file: {}", buffer);
        None
    })
}
