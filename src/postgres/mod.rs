mod poi_query;

use crate::pois::IndexedPoi;
use futures::future;
use futures::stream::{Stream, StreamExt};
use poi_query::fetch_all_pois_query;

/// Iter over all POIs from postgres.
// Clippy most probably gives a false positive here:
// https://github.com/rust-lang/rust-clippy/issues/7271
#[allow(clippy::needless_lifetimes)]
pub async fn fetch_all_pois<'a>(
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
