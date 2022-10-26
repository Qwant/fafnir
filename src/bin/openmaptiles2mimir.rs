use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use futures::stream::StreamExt;
use futures::{try_join, FutureExt};
use mimir::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir::domain::model::configuration::ContainerConfig;
use mimir::domain::ports::primary::generate_index::GenerateIndex;
use mimir::domain::ports::secondary::remote::Remote;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::settings::admin_settings::AdminSettings;
use serde::Deserialize;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use tracing::info_span;
use tracing_futures::Instrument;

use fafnir::mimir::{address_updated_after_pois, MIMIR_PREFIX};
use fafnir::settings::{FafnirSettings, PostgresSettings};
use fafnir::sources::openmaptiles;
use fafnir::utils::start_postgres_session;

// Size of the buffers of POIs that have to be indexed.
const CHANNEL_SIZE: usize = 10_000;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Settings {
    fafnir: FafnirSettings,
    postgres: PostgresSettings,
    elasticsearch: ElasticsearchStorageConfig,
    container_search: ContainerConfig,
    container_nosearch: ContainerConfig,
}

async fn load_and_index_pois(settings: Settings) {
    // Local Elasticsearch client
    let es = Elasticsearch::new(
        Transport::single_node(settings.elasticsearch.url.as_str())
            .expect("failed to initialize Elasticsearch transport"),
    );

    let mimir_es = connection_pool_url(&settings.elasticsearch.url)
        .conn(settings.elasticsearch)
        .await
        .expect("failed to open Elasticsearch connection");

    // If addresses have not changed since last update of POIs, it is not
    // necessary to perform a reverse again for POIs that don't have an address.
    let addr_updated = address_updated_after_pois(&es).await;
    let try_skip_reverse = settings.fafnir.skip_reverse && !addr_updated;

    if try_skip_reverse {
        info!(
            "addresses have not been updated since last update, reverse on old POIs won't be {}",
            "performed",
        );
    }

    // Fetch admins
    let admins_geofinder = AdminGeoFinder::build(&AdminSettings::Elasticsearch, &mimir_es)
        .await
        .expect("Could not load ES admins");

    // Spawn tasks that will build indexes. These tasks will provide a single
    // stream to mimirsbrunn which is built from data sent into async channels.
    let (poi_channel_search, index_search_task) = {
        let (send, recv) = channel(CHANNEL_SIZE);

        let task = mimir_es
            .generate_index(&settings.container_search, ReceiverStream::new(recv))
            .map(|res| res.map_err(Into::into));

        (send, task)
    };

    let (poi_channel_nosearch, index_nosearch_task) = {
        let (send, recv) = channel(CHANNEL_SIZE);

        let task = mimir_es
            .generate_index(&settings.container_nosearch, ReceiverStream::new(recv))
            .map(|res| res.map_err(Into::into));

        (send, task)
    };

    // Build POIs and send them to indexing tasks
    let mut total_nb_pois: usize = 0;

    let poi_index_name = &format!(
        "{}_poi_{}",
        MIMIR_PREFIX, &settings.container_search.dataset
    );

    let poi_index_nosearch_name = &format!(
        "{}_poi_{}",
        MIMIR_PREFIX, &settings.container_nosearch.dataset
    );

    let pg_client = start_postgres_session(&settings.postgres.url)
        .await
        .expect("Unable to connect to postgres");

    let fetch_pois_task = {
        openmaptiles::fetch_and_locate_pois(
            &pg_client,
            es,
            admins_geofinder,
            poi_index_name,
            poi_index_nosearch_name,
            try_skip_reverse,
            &settings.fafnir,
        )
        .await
        .instrument(info_span!("fetch POIs"))
        .for_each(move |p| {
            let poi_channel_search = poi_channel_search.clone();
            let poi_channel_nosearch = poi_channel_nosearch.clone();

            async move {
                if p.is_searchable {
                    poi_channel_search
                        .send(p.poi)
                        .await
                        .expect("failed to send search POI into channel");
                } else {
                    poi_channel_nosearch
                        .send(p.poi)
                        .await
                        .expect("failed to send nosearch POI into channel");
                }

                // Log advancement
                total_nb_pois += 1;

                if total_nb_pois % settings.fafnir.log_indexed_count_interval == 0 {
                    info!("Number of indexed POIs: {total_nb_pois}")
                }
            }
        })
        .map(Ok::<_, Box<dyn std::error::Error>>)
    };

    // Wait for the indexing tasks to complete
    let (index_search, index_nosearch, _) =
        try_join!(index_search_task, index_nosearch_task, fetch_pois_task)
            .expect("failed to index POIs");

    info!("Created index {index_search:?} for searchable POIs");
    info!("Created index {index_nosearch:?} for non-searchable POIs");
    info!("Total number of pois: {total_nb_pois}");
}

#[tokio::main]
async fn main() {
    fafnir::cli::run(load_and_index_pois).await
}
