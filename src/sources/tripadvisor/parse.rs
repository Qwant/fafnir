//! Parsing utilities for TripAdvisor XML feed.

use futures::stream::Stream;
use serde::de::DeserializeOwned;
use serde_json::Deserializer;
use std::io;
use std::io::Read;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tracing::debug;

/// Expected string at start of a { item.
const START_TOKEN: &[u8] = b"{";

/// Expected string at the end of a } item.
const END_TOKEN: &[u8] = b"\n},\n";
const FINAL_END_TOKEN: &[u8] = b"\n}\n";

pub fn split_raw_properties(input: impl AsyncBufRead + Unpin) -> impl Stream<Item = Vec<u8>> {
    futures::stream::unfold(input, |mut input| async {
        let mut buffer = Vec::new();

        while input
            .read_until(b'\n', &mut buffer)
            .await
            .expect("failed to read line from JSON")
            > 0
        {
            if buffer.ends_with(END_TOKEN) {
                // The first buffer may contain some extra information
                let token_start = find_naive(&buffer, START_TOKEN)
                    .expect("found a property which didn't start with expected pattern");

                buffer = buffer[token_start..buffer.len() - 2].to_vec();

                return Some((buffer, input));
            }
            if buffer.ends_with(FINAL_END_TOKEN) {
                // The first buffer may contain some extra information
                let token_start = find_naive(&buffer, START_TOKEN)
                    .expect("found a property which didn't start with expected pattern");

                buffer = buffer[token_start..buffer.len() - 1].to_vec();

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

fn read_skipping_ws(mut reader: impl Read) -> io::Result<u8> {
    loop {
        let mut byte = 0u8;
        reader.read_exact(std::slice::from_mut(&mut byte))?;
        if !byte.is_ascii_whitespace() {
            return Ok(byte);
        }
    }
}

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn deserialize_single<T: DeserializeOwned, R: Read>(reader: R) -> io::Result<T> {
    let next_obj = Deserializer::from_reader(reader).into_iter::<T>().next();
    match next_obj {
        Some(result) => result.map_err(Into::into),
        None => Err(invalid_data("premature EOF")),
    }
}

fn yield_next_obj<T: DeserializeOwned, R: Read>(
    mut reader: R,
    at_start: &mut bool,
) -> io::Result<Option<T>> {
    if !*at_start {
        *at_start = true;
        if read_skipping_ws(&mut reader)? == b'[' {
            // read the next char to see if the array is empty
            let peek = read_skipping_ws(&mut reader)?;
            if peek == b']' {
                Ok(None)
            } else {
                deserialize_single(io::Cursor::new([peek]).chain(reader)).map(Some)
            }
        } else {
            Err(invalid_data("`[` not found"))
        }
    } else {
        match read_skipping_ws(&mut reader)? {
            b',' => deserialize_single(reader).map(Some),
            b']' => Ok(None),
            _ => Err(invalid_data("`,` or `]` not found")),
        }
    }
}

pub fn iter_json_array<T: DeserializeOwned, R: Read>(
    mut reader: R,
) -> impl Iterator<Item = Result<T, io::Error>> {
    let mut at_start = false;
    std::iter::from_fn(move || yield_next_obj(&mut reader, &mut at_start).transpose())
}
