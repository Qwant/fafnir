//! Utilities arround common mimir operations.

use config::Config;
use elasticsearch::Elasticsearch;
use futures::join;
use futures::stream::{Stream, StreamExt};
use places::admin::Admin;

use mimir2::common::document::ContainerDocument;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::primary::generate_index::GenerateIndex;
use mimir2::domain::ports::primary::list_documents::ListDocuments;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;

use crate::utils::get_index_creation_date;
use crate::Error;

/// Prefix to ES index names for mimirsbrunn
pub const MIMIR_PREFIX: &str = "munin";

/// Check if poi index is more recent than addresses.
pub async fn address_updated_after_pois(es: &Elasticsearch) -> bool {
    let (poi_creation_date, addr_creation_date) = join!(
        get_index_creation_date(es, format!("{}_poi", MIMIR_PREFIX)),
        get_index_creation_date(es, format!("{}_addr", MIMIR_PREFIX))
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

pub async fn create_index<G: GenerateIndex, D: ContainerDocument + Send + Sync + 'static>(
    mimir: &G,
    config: &Config,
    dataset: &str,
    visibility: IndexVisibility,
    documents: impl Stream<Item = D> + Send + Sync + 'static,
) -> Result<(), Error> {
    let index_config = Config::builder()
        .add_source(D::default_es_container_config())
        .add_source(config.clone())
        .set_override("container.dataset", dataset.to_string())
        .expect("failed to create config key container.dataset")
        .build()
        .expect("could not build search config");

    mimir
        .generate_index(index_config, documents, visibility)
        .await?;

    Ok(())
}
