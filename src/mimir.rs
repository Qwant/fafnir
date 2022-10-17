//! Utilities arround common mimir operations.

use elasticsearch::Elasticsearch;
use futures::join;

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
