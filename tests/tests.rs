pub mod docker_wrapper;
pub mod openmaptiles2mimir;
pub mod tripadvisor2mimir;

use docker_wrapper::PostgresDocker;
use fafnir::settings::PostgresSettings;
use fafnir::utils::start_postgres_session;
use futures::stream::TryStreamExt;
use mimir::adapters::primary::templates;
use mimir::adapters::secondary::elasticsearch::{
    remote, ElasticsearchStorage, ElasticsearchStorageConfig,
};
use mimir::common::document::{ContainerDocument, Document};
use mimir::domain::model::configuration::{ContainerConfig, ContainerVisibility};
use mimir::domain::model::query::Query;
use mimir::domain::ports::primary::generate_index::GenerateIndex;
use mimir::domain::ports::primary::list_documents::ListDocuments;
use mimir::domain::ports::primary::search_documents::SearchDocuments;
use mimir::domain::ports::secondary::remote::Remote;
use mimir::domain::ports::secondary::storage::Storage;
use mimir::utils::docker;
use places::poi::Poi;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::process::Command;

// Dataset name used for tests.
static DATASET: &str = "test";
static TRIPADVISOR_DATASET: &str = "tripadvisor";

#[derive(Clone)]
pub enum PostgresWrapper {
    /// Connect to an existing Postgres
    External(String),
    /// Manage a Postgres that was spawned in docker by the test suite
    Managed(Arc<PostgresDocker>),
}

impl PostgresWrapper {
    pub fn host(&self) -> String {
        match self {
            PostgresWrapper::External(host) => host.to_string(),
            PostgresWrapper::Managed(docker) => docker.host(),
        }
    }

    pub async fn get_conn(&self) -> tokio_postgres::Client {
        start_postgres_session(PostgresSettings {
            host: format!(r#"{}""#, &self.host),
            port: 5432,
            user: format!("test"),
            password: format!(""),
            database: format!("test"),
        })
        .await
        .unwrap_or_else(|err| panic!("Unable to connect to postgres: {err}"))
    }

    pub async fn get_rows(&self, table: &str) -> Vec<tokio_postgres::row::Row> {
        self.get_conn()
            .await
            .query(&*format!("SELECT * FROM {table}"), &[])
            .await
            .unwrap()
    }
}

pub struct ElasticSearchWrapper {
    host: String,
    es: ElasticsearchStorage,
}

impl ElasticSearchWrapper {
    pub async fn new() -> ElasticSearchWrapper {
        let host = match std::env::var("FAFNIR_TEST_ELASTICSEARCH_URL") {
            Ok(host) => host,
            Err(_) => {
                let _docker = docker::initialize()
                    .await
                    .expect("could not initialize ElasticSearch docker");
                let host = "http://localhost:9202".into();
                std::env::set_var("MIMIR_TEST_ELASTICSEARCH_URL", &host);
                host
            }
        };

        let es = remote::connection_test_pool()
            .conn(ElasticsearchStorageConfig::default_testing())
            .await
            .expect("could not connect ElasticSearch pool");

        let path: PathBuf = [
            env!("CARGO_MANIFEST_DIR"),
            "vendor",
            "mimirsbrunn",
            "config",
            "elasticsearch",
            "templates",
            "components",
        ]
        .iter()
        .collect();

        templates::import(es.clone(), path, templates::Template::Component)
            .await
            .unwrap();

        let path: PathBuf = [
            env!("CARGO_MANIFEST_DIR"),
            "vendor",
            "mimirsbrunn",
            "config",
            "elasticsearch",
            "templates",
            "indices",
        ]
        .iter()
        .collect();

        templates::import(es.clone(), path, templates::Template::Index)
            .await
            .unwrap();

        Self { host, es }
    }

    pub fn host(&self) -> String {
        self.host.to_string()
    }

    pub async fn index<I, T>(&mut self, dataset: &str, objects: I)
    where
        T: ContainerDocument + Send + Sync + 'static,
        I: Iterator<Item = T> + Send + Sync + 'static,
    {
        self.es
            .generate_index(
                &ContainerConfig {
                    name: T::static_doc_type().to_string(),
                    dataset: dataset.to_string(),
                    visibility: ContainerVisibility::Public,
                    number_of_shards: 1,
                    number_of_replicas: 0,
                },
                futures::stream::iter(objects),
            )
            .await
            .expect("could not create index");
    }

    pub async fn init(&mut self) {
        self.es
            .delete_container("_all".to_string())
            .await
            .expect("could not swipe indices")
    }

    pub async fn get_all_nosearch_pois(&mut self) -> impl Iterator<Item = Poi> {
        #[derive(Deserialize, Serialize)]
        #[serde(transparent)]
        pub struct PoiNoSearch(Poi);

        impl Document for PoiNoSearch {
            fn id(&self) -> std::string::String {
                self.0.id()
            }
        }

        impl ContainerDocument for PoiNoSearch {
            fn static_doc_type() -> &'static str {
                "poi_nosearch"
            }
        }

        self.es
            .list_documents()
            .await
            .expect("could not query a list of POIs from ES")
            .try_collect::<Vec<_>>()
            .await
            .expect("could not fetch a POI from ES")
            .into_iter()
            .map(|PoiNoSearch(poi)| poi)
    }

    pub async fn get_all_tripadvisor_pois(&mut self) -> impl Iterator<Item = Poi> {
        #[derive(Deserialize, Serialize)]
        #[serde(transparent)]
        pub struct PoiTripadvisor(Poi);

        impl Document for PoiTripadvisor {
            fn id(&self) -> std::string::String {
                self.0.id()
            }
        }

        impl ContainerDocument for PoiTripadvisor {
            fn static_doc_type() -> &'static str {
                "poi_tripadvisor"
            }
        }

        self.es
            .list_documents()
            .await
            .expect("could not query a list of POIs from ES")
            .try_collect::<Vec<_>>()
            .await
            .expect("could not fetch a POI from ES")
            .into_iter()
            .map(|PoiTripadvisor(poi)| poi)
    }

    pub async fn search_and_filter<F>(
        &self,
        word: &str,
        predicate: F,
    ) -> impl Iterator<Item = places::Place>
    where
        F: FnMut(&places::Place) -> bool,
    {
        let indices = ["munin_admin", "munin_addr", "munin_poi"]
            .into_iter()
            .map(String::from)
            .collect();

        self.es
            .search_documents(indices, Query::QueryString(word.to_string()), 100, None)
            .await
            .unwrap_or_else(|err| panic!("could not search for {word}: {err}"))
            .into_iter()
            .map(|val| serde_json::from_value(val).unwrap())
            .filter(predicate)
    }
}

async fn launch_and_assert(cmd: &'static str, args: Vec<std::string::String>) {
    let mut command = Command::new(cmd);
    command.args(&args).env("RUST_BACKTRACE", "1");
    let output = command.output().await.unwrap();

    if !output.status.success() {
        eprintln!("=== stdout for {cmd}");
        eprintln!("{}", String::from_utf8(output.stdout).unwrap());
        eprintln!("=== stderr for {cmd}");
        eprintln!("{}", String::from_utf8(output.stderr).unwrap());
        eprintln!("===");
        panic!("`{cmd}` failed {}", output.status);
    }
}

#[tokio::test]
async fn fafnir_test() {
    let pg_wrapper = match std::env::var("FAFNIR_TEST_POSTGRES_URL") {
        Ok(host) => PostgresWrapper::External(host),
        Err(_) => {
            let pg_docker = PostgresDocker::new()
                .await
                .expect("could not initialize Postgres Docker");

            PostgresWrapper::Managed(Arc::new(pg_docker))
        }
    };

    openmaptiles2mimir::main_test(ElasticSearchWrapper::new().await, pg_wrapper.clone()).await;
    openmaptiles2mimir::bbox_test(ElasticSearchWrapper::new().await, pg_wrapper.clone()).await;

    openmaptiles2mimir::test_with_langs(ElasticSearchWrapper::new().await, pg_wrapper.clone())
        .await;

    openmaptiles2mimir::test_address_format(ElasticSearchWrapper::new().await, pg_wrapper.clone())
        .await;

    openmaptiles2mimir::test_current_country_label(
        ElasticSearchWrapper::new().await,
        pg_wrapper.clone(),
    )
    .await;

    tripadvisor2mimir::main_test(ElasticSearchWrapper::new().await).await;
}
