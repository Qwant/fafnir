pub mod convert;
pub mod models;
pub mod parse;

use std::sync::Arc;

use futures::stream::StreamExt;
use futures::Stream;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use places::poi::Poi;
use tokio::io::AsyncBufRead;

/// Number of tokio's blocking thread that can be spawned to parse XML. Keeping
/// a rather low constant value is fine as the input will be provided by a GZip
/// decoder, which only runs on a single thread.
const PARSER_THREADS: usize = 1;

/// Number of <Property /> items that are sent to spawned threads for parsing.
const PARSER_CHUNK_SIZE: usize = 1000;

#[derive(Debug)]
pub struct Photos {
    pub id: u32,
    pub urls: Vec<String>,
}

pub fn read_pois(
    input: impl AsyncBufRead + Unpin,
    geofinder: Arc<AdminGeoFinder>,
) -> impl Stream<Item = Result<Poi, convert::pois::BuildError>> {
    parse::split_raw_properties(input)
        .chunks(PARSER_CHUNK_SIZE)
        .map(move |chunk| {
            let geofinder = geofinder.clone();

            async move {
                let chunk_parsed: Vec<_> = tokio::task::spawn_blocking(move || {
                    chunk
                        .into_iter()
                        .map(|raw| {
                            let property = quick_xml::de::from_reader(raw.as_slice())
                                .expect("failed to poi property");

                            convert::pois::build_poi(property, geofinder.as_ref())
                        })
                        .collect()
                })
                .await
                .expect("blocking task panicked");

                futures::stream::iter(chunk_parsed)
            }
        })
        .buffered(PARSER_THREADS)
        .flatten()
}

pub fn read_photos(
    input: impl AsyncBufRead + Unpin,
) -> impl Stream<Item = Result<Photos, convert::photos::BuildError>> {
    parse::split_raw_properties(input)
        .chunks(PARSER_CHUNK_SIZE)
        .map(|chunk| async move {
            let chunk_parsed: Vec<_> = tokio::task::spawn_blocking(move || {
                chunk
                    .into_iter()
                    .map(|raw| {
                        let property = quick_xml::de::from_reader(raw.as_slice())
                            .expect("failed to photos property");

                        convert::photos::build_photo(property)
                    })
                    .collect()
            })
            .await
            .expect("blocking task panicked");

            futures::stream::iter(chunk_parsed)
        })
        .buffered(PARSER_THREADS)
        .flatten()
}
