use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};

use config::Config;
use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use futures::stream::TryStreamExt;
use futures::try_join;
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::secondary::remote::Remote;
use serde::Deserialize;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use tracing::{error, info_span};
use tracing_futures::Instrument;

use fafnir::mimir::{
    address_updated_after_pois, build_admin_geofinder, create_index, MIMIR_PREFIX,
};
use fafnir::settings::{ContainerConfig, FafnirSettings, PostgresSettings};
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

async fn load_and_index_pois(
    settings: Settings,
    raw_config: Config,
) -> Result<(), mimirsbrunn::Error> {
    // Local Elasticsearch client
    let es = &Elasticsearch::new(
        Transport::single_node(settings.elasticsearch.url.as_str())
            .expect("failed to initialize Elasticsearch transport"),
    );

    let mimir_es = Arc::new(
        connection_pool_url(&settings.elasticsearch.url)
            .conn(settings.elasticsearch)
            .await
            .expect("failed to open Elasticsearch connection"),
    );

    // If addresses have not changed since last update of POIs, it is not
    // necessary to perform a reverse again for POIs that don't have an address.
    let addr_updated = address_updated_after_pois(es).await;
    let try_skip_reverse = settings.fafnir.skip_reverse && !addr_updated;

    if try_skip_reverse {
        info!(
            "addresses have not been updated since last update, reverse on old POIs won't be {}",
            "performed",
        );
    }

    // Fetch admins
    let admins_geofinder = &build_admin_geofinder(mimir_es.as_ref()).await;

    // Spawn tasks that will build indexes. These tasks will provide a single
    // stream to mimirsbrunn which is built from data sent into async channels.
    let (poi_channel_search, index_search_task) = {
        let (send, recv) = channel(CHANNEL_SIZE);

        let task = create_index(
            mimir_es.as_ref(),
            &raw_config,
            &settings.container_search.dataset,
            IndexVisibility::Public,
            ReceiverStream::new(recv),
        );

        (send, task)
    };

    let (poi_channel_nosearch, index_nosearch_task) = {
        let (send, recv) = channel(CHANNEL_SIZE);

        let task = create_index(
            mimir_es.as_ref(),
            &raw_config,
            &settings.container_nosearch.dataset,
            IndexVisibility::Private,
            ReceiverStream::new(recv),
        );

        (send, task)
    };

    // Build POIs and send them to indexing tasks
    let total_nb_pois = AtomicUsize::new(0);

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
        .try_for_each({
            let total_nb_pois = &total_nb_pois;

            move |p| {
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
                    let curr_indexed = 1 + total_nb_pois.fetch_add(1, atomic::Ordering::Relaxed);

                    if curr_indexed % settings.fafnir.log_indexed_count_interval == 0 {
                        info!("Number of indexed POIs: {}", curr_indexed)
                    }

                    Ok(())
                }
            }
        })
    };

    // Wait for the indexing tasks to complete
    let (index_search, index_nosearch, _) =
        try_join!(index_search_task, index_nosearch_task, fetch_pois_task)
            .expect("failed to index POIs");

    let total_nb_pois = total_nb_pois.into_inner();
    info!("Created index {:?} for searchable POIs", index_search);
    info!("Created index {:?} for non-searchable POIs", index_nosearch);
    info!("Total number of pois: {}", total_nb_pois);
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = fafnir::cli::run(load_and_index_pois).await {
        error!("Error while running fafnir: {}", err)
    }
}
