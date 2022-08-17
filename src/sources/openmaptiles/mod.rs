//! Utilities to fetch list of POIs from a postgres databased indexed with the openmaptiles schema.
//! See https://github.com/Qwant/openmaptiles/
pub mod pois;
pub mod postgres;


use elasticsearch::Elasticsearch;
use futures::stream::{Stream, StreamExt};
use futures::{future, stream};

use crate::lazy_es::LazyEs;
use crate::settings::FafnirSettings;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use pois::IndexedPoi;
use postgres::fetch_all_pois_query;

/// Iter over all POIs from postgres.
// Clippy most probably gives a false positive here:
// https://github.com/rust-lang/rust-clippy/issues/7271
#[allow(clippy::needless_lifetimes)]
pub async fn fetch_pois<'a>(
    pg: &tokio_postgres::Client,
    bbox: Option<[f64; 4]>,
    langs: &'a [String],
) -> impl Stream<Item = IndexedPoi> + 'a {
    let query = fetch_all_pois_query(bbox);

    let stmt = pg
        .prepare(&query.build())
        .await
        .expect("failed to prepare query");

    pg.query_raw::<_, i32, _>(&stmt, [])
        .await
        .expect("could not query postgres")
        .map(|row| {
            row.unwrap_or_else(|err| panic!("error while fetching row from postgres: {err}"))
        })
        .filter_map(move |row| {
            let poi = IndexedPoi::from_row(row, langs);
            future::ready(poi)
        })
}

/// Iter over all POIs from postgres and search for its admin/address.
pub async fn fetch_and_locate_pois<'a>(
    pg: &tokio_postgres::Client,
    es: Elasticsearch,
    admin_geofinder: AdminGeoFinder,
    poi_index_name: &'a str,
    poi_index_nosearch_name: &'a str,
    try_skip_reverse: bool,
    settings: &'a FafnirSettings,
) -> impl Stream<Item = IndexedPoi> + 'a {
    let admin_geofinder = Arc::new(admin_geofinder);
    let es = Arc::new(es);

    // Keeping chunks big enough compared to the batch size will ensure that most of the requests
    // will have exactly `max_query_batch_size` elements to be sent to ES.
    let chunks_size = 10 * settings.max_query_batch_size;

    fetch_pois(pg, settings.bounding_box, &settings.langs)
        .await
        .chunks(chunks_size)
        .map(move |pois| {
            let admin_geofinder = admin_geofinder.clone();
            let es = es.clone();
            let poi_index_name = poi_index_name.to_string();
            let poi_index_nosearch_name = poi_index_nosearch_name.to_string();
            let langs = settings.langs.clone();
            let max_query_batch_size = settings.max_query_batch_size;

            tokio::spawn(async move {
                // Build POIs from postgres
                let pois: Vec<_> = pois
                    .iter()
                    .map(|indexed_poi| {
                        indexed_poi.locate_poi(
                            &admin_geofinder,
                            &langs,
                            &poi_index_name,
                            &poi_index_nosearch_name,
                            try_skip_reverse,
                        )
                    })
                    .collect();

                // Run ES queries until all POIs are fully built
                let pois: Vec<_> =
                    LazyEs::batch_make_progress_until_value(&es, pois, max_query_batch_size)
                        .await
                        .into_iter()
                        .flatten()
                        .collect();

                stream::iter(pois)
            })
        })
        .buffer_unordered(settings.concurrent_blocks)
        .map(|res| res.expect("task panicked"))
        .flatten()
}
