pub mod convert;
pub mod models;
pub mod parse;

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::StreamExt;
use futures::Stream;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use places::poi::Poi;
use tokio::io::AsyncBufRead;
use tracing::log::warn;

/// Number of tokio's blocking thread that can be spawned to parse XML. Keeping
/// a rather low constant value is fine as the input will be provided by a GZip
/// decoder, which only runs on a single thread.
const PARSER_THREADS: usize = 8;

/// Number of <Property /> items that are sent to spawned threads for parsing.
const PARSER_CHUNK_SIZE: usize = 1000;

pub fn read_pois(
    input: impl AsyncBufRead + Unpin,
    geofinder: Arc<AdminGeoFinder>,
) -> impl Stream<Item = Poi> {
    parse::split_raw_properties(input)
        .chunks(PARSER_CHUNK_SIZE)
        .map(move |chunk| {
            let geofinder = geofinder.clone();

            async move {
                let chunk_parsed: Vec<_> = tokio::task::spawn_blocking(move || {
                    let mut local_errors = HashMap::new();
                    let res = chunk
                        .into_iter()
                        .filter_map(|raw| {
                            let property =
                                quick_xml::de::from_str(&raw).expect("failed to parse properties");

                            convert::build_poi(property, geofinder.as_ref())
                                .map_err(|err| *local_errors.entry(err).or_insert(0) += 1)
                                .ok()
                        })
                        .collect();

                    if !local_errors.is_empty() {
                        warn!("parsion errors for current block: {:?}", local_errors);
                    }

                    res
                })
                .await
                .expect("blocking task panicked");

                futures::stream::iter(chunk_parsed)
            }
        })
        .buffered(PARSER_THREADS)
        .flatten()
}
