pub mod docker_wrapper;
pub mod fafnir_tests;

use config::Config;
use docker_wrapper::PostgresDocker;
use fafnir::utils::start_postgres_session;
use futures::stream::TryStreamExt;
use hyper::client::response::Response;
use log::{info, warn};
use mimir2::adapters::secondary::elasticsearch::{remote, ElasticsearchStorage};
use mimir2::common::document::ContainerDocument;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::primary::generate_index::GenerateIndex;
use mimir2::domain::ports::primary::list_documents::ListDocuments;
use mimir2::domain::ports::secondary::remote::Remote;
use mimir2::utils::docker;
use serde_json::value::Value;
use std::sync::MutexGuard;
use tokio::process::Command;

// Dataset name used for tests.
static DATASET: &str = "test";

pub struct PostgresWrapper<'a> {
    docker_wrapper: &'a PostgresDocker,
}

impl<'a> PostgresWrapper<'a> {
    pub fn host(&self) -> String {
        self.docker_wrapper.host()
    }

    pub async fn get_conn(&self) -> tokio_postgres::Client {
        start_postgres_session(&format!("postgres://test@{}/test", &self.host()))
            .await
            .unwrap_or_else(|err| panic!("Unable to connect to postgres: {}", err))
    }

    pub async fn get_rows(&self, table: &str) -> Vec<tokio_postgres::row::Row> {
        self.get_conn()
            .await
            .query(&*format!("SELECT * FROM {}", table), &[])
            .await
            .unwrap()
    }

    pub fn new(docker_wrapper: &PostgresDocker) -> PostgresWrapper {
        PostgresWrapper { docker_wrapper }
    }
}

/// Code below comes from https://github.com/CanalTP/mimirsbrunn/tree/master/tests
trait ToJson {
    fn to_json(self) -> Value;
}

impl ToJson for Response {
    fn to_json(self) -> Value {
        match serde_json::from_reader(self) {
            Ok(v) => v,
            Err(e) => {
                panic!("could not get json value from response: {:?}", e);
            }
        }
    }
}

pub struct ElasticSearchWrapper {
    _docker: MutexGuard<'static, ()>,
    host: String,
    es: ElasticsearchStorage,
}

impl ElasticSearchWrapper {
    pub async fn new() -> ElasticSearchWrapper {
        let host = "http://localhost:9201".to_string();
        std::env::set_var("ELASTICSEARCH_TEST_URL", &host);

        let _docker = docker::initialize()
            .await
            .expect("could not initialize ElasticSearch docker");

        let pool = remote::connection_pool_url(&host)
            .await
            .expect("could not create ElasticSearch connection pool");

        let es = pool
            .conn()
            .await
            .expect("could not connect ElasticSearch pool");

        let mut res = Self { _docker, host, es };
        res.init().await;
        res
    }

    pub fn host(&self) -> String {
        self.host.to_string()
    }

    pub async fn index<I, T>(&mut self, dataset: &str, objects: I)
    where
        T: ContainerDocument + Send + Sync + 'static,
        I: Iterator<Item = T> + Send + Sync + 'static,
    {
        let config = Config::builder()
            .add_source(T::default_es_container_config())
            .set_override("container.dataset", dataset)
            .expect("could not update config")
            .build()
            .expect("could not build config");

        self.es
            .generate_index(
                config,
                futures::stream::iter(objects),
                IndexVisibility::Public,
            )
            .await
            .expect("could not create index");
    }

    pub async fn get_pois(&mut self) -> Vec<places::poi::Poi> {
        self.es
            .list_documents()
            .await
            .expect("could not query a list of POIs from ES")
            .try_collect()
            .await
            .expect("could not fetch a POI from ES")
    }

    pub async fn init(&mut self) {
        // TODO
        // self.rubber.delete_index(&"_all".to_string()).unwrap();
    }

    pub async fn refresh(&self) {
        // TODO: is this necessary?
        // info!("Refreshing ES indexes");
        //
        // let res = hyper::client::Client::new()
        //     .get(&format!("{}/_refresh", self.host()))
        //     .send()
        //     .unwrap();
        // assert!(res.status == hyper::Ok, "Error ES refresh: {:?}", res);
    }

    /// simple search on an index
    /// assert that the result is OK and transform it to a json Value
    pub fn search(&self, word: &str) -> serde_json::Value {
        //         let res = self
        //             .rubber
        //             .get(&format!("munin/_search?q={}&size=100", word))
        //             .unwrap();
        //         assert!(res.status().is_success());
        //         res.json().expect("failed to parse json")
        todo!()
    }

    pub fn search_on_global_stop_index(&self, word: &str) -> serde_json::Value {
        //         let res = self
        //             .rubber
        //             .get(&format!("munin_global_stops/_search?q={}", word))
        //             .unwrap();
        //         assert!(res.status().is_success());
        //         res.json().expect("failed to parse json")
        todo!()
    }

    pub fn search_and_filter<'b, F>(
        &self,
        word: &str,
        predicate: F,
    ) -> Box<dyn Iterator<Item = places::Place> + 'b>
    where
        F: 'b + FnMut(&places::Place) -> bool,
    {
        //         self.search_and_filter_on_index(word, predicate, false)
        todo!()
    }

    fn search_and_filter_on_index<'b, F>(
        &self,
        word: &str,
        predicate: F,
        search_on_global_stops: bool,
    ) -> Box<dyn Iterator<Item = places::Place> + 'b>
    where
        F: 'b + FnMut(&places::Place) -> bool,
    {
        //         use serde_json::map::{Entry, Map};
        //         fn into_object(json: Value) -> Option<Map<String, Value>> {
        //             match json {
        //                 Value::Object(o) => Some(o),
        //                 _ => None,
        //             }
        //         }
        //         fn get(json: Value, key: &str) -> Option<Value> {
        //             into_object(json).and_then(|mut json| match json.entry(key.to_string()) {
        //                 Entry::Occupied(o) => Some(o.remove()),
        //                 _ => None,
        //             })
        //         }
        //         let json = if search_on_global_stops {
        //             self.search_on_global_stop_index(word)
        //         } else {
        //             self.search(word)
        //         };
        //         get(json, "hits")
        //             .and_then(|json| get(json, "hits"))
        //             .and_then(|hits| {
        //                 match hits {
        //                     Value::Array(v) => {
        //                         Some(Box::new(v
        //                             .into_iter()
        //                             .filter_map(into_object)
        //                             .filter_map(|obj| obj
        //                                 .get("_type")
        //                                 .and_then(|doc_type| doc_type.as_str())
        //                                 .map(|doc_type| doc_type.into())
        //                                 .and_then(|doc_type: String| {
        //                                     // The real object is contained in the _source section.
        //                                     obj.get("_source").and_then(|src| {
        //                                         let v = src.clone();
        //                                         match doc_type.as_ref() {
        //                                             "addr" => convert(v, mimir::Place::Addr),
        //                                             "street" => convert(v, mimir::Place::Street),
        //                                             "admin" => convert(v, mimir::Place::Admin),
        //                                             "poi" => convert(v, mimir::Place::Poi),
        //                                             "stop" => convert(v, mimir::Place::Stop),
        //                                             _ => {
        //                                                 panic!("unknown ES return value, _type field = {}", doc_type);
        //                                             }
        //                                         }
        //                                     })
        //                                 })
        //                             )
        //                             .filter(predicate),
        //                         ) as Box<dyn Iterator<Item = mimir::Place>>)
        //                     }
        //                     _ => None,
        //                 }
        //             })
        //             .unwrap_or(Box::new(None.into_iter()) as Box<dyn Iterator<Item = mimir::Place>>)
        todo!()
    }
}

fn convert<T>(v: serde_json::Value, f: fn(T) -> places::Place) -> Option<places::Place>
where
    for<'de> T: serde::Deserialize<'de>,
{
    //     serde_json::from_value::<T>(v)
    //         .map_err(|err| warn!("Impossible to load ES result: {}", err))
    //         .ok()
    //         .map(f)
    todo!()
}

async fn launch_and_assert(
    cmd: &'static str,
    args: Vec<std::string::String>,
    es_wrapper: &ElasticSearchWrapper,
) {
    let mut command = Command::new(cmd);
    command.args(&args).env("RUST_BACKTRACE", "1");
    let output = command.output().await.unwrap();

    if !output.status.success() {
        eprintln!("=== stdout for {}", cmd);
        eprintln!("{}", String::from_utf8(output.stdout).unwrap());
        eprintln!("=== stderr for {}", cmd);
        eprintln!("{}", String::from_utf8(output.stderr).unwrap());
        eprintln!("===");
        panic!("`{}` failed {}", cmd, output.status);
    }

    es_wrapper.refresh().await;
}

#[tokio::test]
async fn main_test() {
    let pg_docker = PostgresDocker::new().await.unwrap();

    fafnir_tests::main_test(
        ElasticSearchWrapper::new().await,
        PostgresWrapper::new(&pg_docker),
    )
    .await;

    fafnir_tests::bbox_test(
        ElasticSearchWrapper::new().await,
        PostgresWrapper::new(&pg_docker),
    )
    .await;

    fafnir_tests::test_with_langs(
        ElasticSearchWrapper::new().await,
        PostgresWrapper::new(&pg_docker),
    )
    .await;

    fafnir_tests::test_address_format(
        ElasticSearchWrapper::new().await,
        PostgresWrapper::new(&pg_docker),
    )
    .await;

    fafnir_tests::test_current_country_label(
        ElasticSearchWrapper::new().await,
        PostgresWrapper::new(&pg_docker),
    )
    .await;
}
