use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};
use structopt::StructOpt;
use tracing::error;

use config::Config;
use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use futures::stream::TryStreamExt;
use futures::try_join;
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::common::config::config_from;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::secondary::remote::Remote;
use mimirsbrunn::utils::logger::logger_init;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use fafnir::mimir::{
    address_updated_after_pois, build_admin_geofinder, create_index, MIMIR_PREFIX,
};
use fafnir::settings::Settings;
use fafnir::sources::openmaptiles;
use fafnir::utils::start_postgres_session;

// Size of the buffers of POIs that have to be indexed.
const CHANNEL_SIZE: usize = 10_000;

pub async fn load_and_index_pois(
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
                    // TODO: maybe we should be exhaustive as before with logs
                    total_nb_pois.fetch_add(1, atomic::Ordering::Relaxed);
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

#[derive(StructOpt, Debug)]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
struct Args {
    /// Defines the config directories
    #[structopt(parse(from_os_str), short = "c", long = "config-dir")]
    pub config_dir: PathBuf,

    /// Defines the run mode in {testing, dev, prod, ...}
    ///
    /// If no run mode is provided, a default behavior will be used.
    #[structopt(short = "m", long = "run-mode")]
    pub run_mode: Option<String>,

    /// Override settings values using key=value
    #[structopt(short = "s", long = "setting")]
    pub settings: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::from_args();

    let raw_config = config_from(
        &args.config_dir,
        &["elasticsearch", "fafnir", "logging"],
        args.run_mode.as_deref(),
        "MIMIR",
        args.settings,
    )
    .expect("could not build fafnir config");

    let settings: Settings = raw_config
        .clone()
        .try_into()
        .expect("invalid fafnir config");

    let _log_guard = logger_init(&settings.logging.path).expect("could not init logger");

    info!(
        "Full configuration:\n{}",
        serde_json::to_string_pretty(
            &raw_config
                .clone()
                .try_into::<serde_json::Value>()
                .expect("could not convert config to json"),
        )
        .expect("could not serialize config"),
    );

    if let Err(err) = load_and_index_pois(settings, raw_config).await {
        error!("Error while running fafnir: {}", err)
    }
}
