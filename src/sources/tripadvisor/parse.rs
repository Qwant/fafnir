//! Parsing utilities for TripAdvisor XML feed.

use futures::stream::{Stream, StreamExt};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;
use tracing::debug;

/// Expected string at start of a <Property /> item.
const START_TOKEN: &str = "<Property ";

/// Expected string at the end of a <Property /> item.
const END_TOKEN: &str = "</Property>";

/// Split each <Property /> item into a buffer that can be deserialized independantly.
///
/// There are a few assumptions that are made over the input data, mostly for
/// performance reasons:
///  - each item starts with a line containing "<Property ", the begining of
///    the line will be ignoreed.
///  - each item ends with a line "</Property>"
pub fn split_raw_properties(input: impl AsyncBufRead + Unpin) -> impl Stream<Item = String> {
    let lines = LinesStream::new(input.lines());

    futures::stream::unfold(lines, move |mut lines| {
        let mut buffer = String::new();

        async move {
            while let Some(line) = lines.next().await {
                let line = line.expect("could not read raw line from XML");

                // The first line may contain some extra XML informations
                let line = {
                    if buffer.is_empty() {
                        let token_start = line.find(START_TOKEN)?;

                        if token_start > 0 {
                            debug!("Ignored begining of file: {}", &line[..token_start]);
                        }

                        &line[token_start..]
                    } else {
                        &line
                    }
                };

                buffer.push_str(line);

                if line.trim() == END_TOKEN {
                    return Some((buffer, lines));
                }
            }

            if !buffer.is_empty() {
                debug!("Ignored end of file: {}", buffer);
            }

            None
        }
    })
}
