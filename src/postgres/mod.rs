mod poi_query;

use crate::pois::IndexedPoi;
use futures::future;
use futures::stream::{Stream, StreamExt};
use poi_query::{PoisQuery, TableQuery};

/// Iter over all POIs from postgres.
pub async fn fetch_all_pois<'a>(
    pg: &tokio_postgres::Client,
    bbox: Option<&str>,
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
            let poi = IndexedPoi::from_row(row, &langs);
            future::ready(poi)
        })
}

fn fetch_all_pois_query(bbox: Option<&str>) -> PoisQuery {
    let mut query = PoisQuery::new()
        .with_table(TableQuery::new("all_pois(14)").id_column("global_id"))
        .with_table(
            TableQuery::new("osm_aerodrome_label_point")
                .override_class("'aerodrome'")
                .override_subclass("'airport'"),
        )
        .with_table(
            TableQuery::new("osm_city_point")
                .override_class("'locality'")
                .override_subclass("'hamlet'")
                .filter("name <> '' AND place='hamlet'"),
        )
        .with_table(
            TableQuery::new("osm_water_lakeline")
                .override_class("'water'")
                .override_subclass("'lake'"),
        )
        .with_table(
            TableQuery::new("osm_water_point")
                .override_class("'water'")
                .override_subclass("'water'"),
        )
        .with_table(
            TableQuery::new("osm_marine_point")
                .override_class("'water'")
                .override_subclass("place"),
        );

    if let Some(bbox) = bbox {
        query = query.bbox(bbox);
    }

    query
}
