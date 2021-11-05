pub mod pois;
pub mod postgres;

use elasticsearch::Elasticsearch;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use futures::{future, stream};

use crate::lazy_es::LazyEs;
use crate::openmaptiles2mimir::Settings;
use crate::Error;
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
            row.unwrap_or_else(|err| panic!("error while fetching row from postgres: {}", err))
        })
        .filter_map(move |row| {
            let poi = IndexedPoi::from_row(row, langs);
            future::ready(poi)
        })
}

/// Iter over all POIs from postgres and search for its admin/address.
pub async fn fetch_and_locate_pois<'a>(
    pg: &tokio_postgres::Client,
    es: &'a Elasticsearch,
    admins_geofinder: &'a AdminGeoFinder,
    poi_index_name: &'a str,
    poi_index_nosearch_name: &'a str,
    try_skip_reverse: bool,
    settings: &'a Settings,
) -> impl Stream<Item = Result<IndexedPoi, Error>> + 'a {
    fetch_pois(pg, settings.bounding_box, &settings.langs)
        .await
        .chunks(1500) // TODO
        .map(move |pois| async move {
            // Build POIs from postgres
            let pois: Vec<_> = pois
                .iter()
                .map(|indexed_poi| {
                    Ok(indexed_poi.locate_poi(
                        admins_geofinder,
                        &settings.langs,
                        poi_index_name,
                        poi_index_nosearch_name,
                        try_skip_reverse,
                    ))
                })
                .collect::<Result<_, Error>>()?;

            // Run ES queries until all POIs are fully built
            let pois: Vec<_> =
                LazyEs::batch_make_progress_until_value(es, pois, settings.max_query_batch_size)
                    .await
                    .into_iter()
                    .flatten()
                    .map(Ok)
                    .collect();

            Ok::<_, Error>(stream::iter(pois))
        })
        .buffer_unordered(settings.concurrent_blocks)
        .try_flatten()
}
