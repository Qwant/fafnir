pub mod parse;
pub mod photos;
pub mod pois;
pub mod reviews;

use futures::stream::StreamExt;
use futures::{FutureExt, Stream};
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use places::poi::Poi;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::AsyncBufRead;

/// Number of tokio's blocking thread that can be spawned to parse XML. Keeping
/// a rather low constant value is fine as the input will be provided by a GZip
/// decoder, which only runs on a single thread.
const PARSER_THREADS: usize = 8;

/// Number of <Property /> items that are sent to spawned threads for parsing.
const PARSER_CHUNK_SIZE: usize = 1000;

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct TripAdvisorWeightSettings {
    pub high_review_count: f64,
    pub boost: f64,
}

/// Compute the actual Elasticsearch ID of a document given TripAdvisor's
/// property ID.
pub fn build_id(ta_id: u32) -> String {
    format!("ta:poi:{ta_id}")
}

fn parse_properties<P, R>(
    input: impl AsyncBufRead + Unpin,
    convert: impl Fn(P) -> R + Sync + Send + 'static,
) -> impl Stream<Item = R>
where
    P: DeserializeOwned,
    R: Send + 'static,
{
    let parse = Arc::new(convert);

    parse::split_raw_properties(input)
        .chunks(PARSER_CHUNK_SIZE)
        .map(move |chunk| {
            let parse = parse.clone();

            let task = async move {
                let chunk_parsed: Vec<_> = chunk
                    .into_iter()
                    .map(|raw| {
                        let property = quick_xml::de::from_reader(raw.as_slice())
                            .expect("failed parse to poi property");

                        parse(property)
                    })
                    .collect();

                futures::stream::iter(chunk_parsed)
            };

            tokio::spawn(task).map(|res| res.expect("blocking task panicked"))
        })
        .buffered(PARSER_THREADS)
        .flatten()
}

pub fn read_pois(
    input: impl AsyncBufRead + Unpin,
    geofinder: AdminGeoFinder,
    weight_settings: TripAdvisorWeightSettings,
) -> impl Stream<Item = Result<(u32, Poi), pois::convert::BuildError>> {
    parse_properties(input, move |property| {
        pois::convert::build_poi(property, &geofinder, weight_settings)
    })
}

pub fn read_photos(
    input: impl AsyncBufRead + Unpin,
) -> impl Stream<Item = Result<(u32, String), photos::convert::BuildError>> {
    parse_properties(input, photos::convert::build_photo)
}

pub fn read_reviews(
    input: impl AsyncBufRead + Unpin,
) -> impl Stream<Item = Result<(u32, Vec<String>), reviews::convert::BuildError>> {
    parse_properties(input, reviews::convert::build_reviews)
}
