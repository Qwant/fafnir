//! Utilities arround common mimir operations.

use elasticsearch::Elasticsearch;
use futures::join;
use futures::stream::StreamExt;
use places::admin::Admin;

use mimir::domain::ports::primary::list_documents::ListDocuments;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;

use crate::utils::get_index_creation_date;

/// Prefix to ES index names for mimirsbrunn
pub const MIMIR_PREFIX: &str = "munin";

/// Check if poi index is more recent than addresses.
pub async fn address_updated_after_pois(es: &Elasticsearch) -> bool {
    let (poi_creation_date, addr_creation_date) = join!(
        get_index_creation_date(es, format!("{MIMIR_PREFIX}_poi")),
        get_index_creation_date(es, format!("{MIMIR_PREFIX}_addr"))
    );

    match (poi_creation_date, addr_creation_date) {
        (Some(poi_ts), Some(addr_ts)) => addr_ts > poi_ts,
        _ => true,
    }
}

/// Fetch administrative regions.
pub async fn build_admin_geofinder<G: ListDocuments<Admin>>(mimir: &G) -> AdminGeoFinder {
    mimir
        .list_documents()
        .await
        .expect("administratives regions not found in es db")
        .map(|admin| admin.expect("could not parse admin"))
        .collect()
        .await
}
