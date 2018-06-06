extern crate bragi;
extern crate fafnir;
extern crate hyper;
extern crate mimir;
extern crate mimirsbrunn;
extern crate rs_es;
extern crate serde_json;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate postgres;

pub mod docker_wrapper;
pub mod fafnir_tests;

use docker_wrapper::*;
use hyper::client::response::Response;
use postgres::rows;
use postgres::{Connection, TlsMode};
use serde_json::value::Value;
use std::iter;
use std::process::Command;

// Dataset name used for tests.
static DATASET: &'static str = "test";

pub struct PostgresWrapper<'a> {
    docker_wrapper: &'a PostgresDocker,
}

impl<'a> PostgresWrapper<'a> {
    pub fn host(&self) -> String {
        self.docker_wrapper.host()
    }

    pub fn get_conn(&self) -> Connection {
        Connection::connect(
            format!("postgres://test@{}/test", &self.host()),
            TlsMode::None,
        ).unwrap_or_else(|err| {
            panic!(
                "Unable to connect to postgres: {} with ip: {}",
                err,
                &self.host()
            );
        })
    }

    pub fn get_rows(&self) -> rows::Rows {
        let conn = self.get_conn();
        conn.query("SELECT * FROM osm_poi_point", &[]).unwrap()
    }

    pub fn new(docker_wrapper: &PostgresDocker) -> PostgresWrapper {
        let pg_wrapper = PostgresWrapper {
            docker_wrapper: docker_wrapper,
        };
        pg_wrapper
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

pub struct ElasticSearchWrapper<'a> {
    docker_wrapper: &'a ElasticsearchDocker,
    pub rubber: mimir::rubber::Rubber,
}

impl<'a> ElasticSearchWrapper<'a> {
    pub fn make_addr_index(&mut self, dataset: &str, test_address: &mimir::Addr) {
        let addr_index = self.rubber.make_index(dataset).unwrap();
        let iter_one_addr = iter::once(test_address);
        let _nb = self.rubber.bulk_index(&addr_index, iter_one_addr).unwrap();
        self.rubber.publish_index(dataset, addr_index).unwrap();
        self.refresh();
    }

    pub fn get_pois(&mut self) -> Vec<mimir::Poi> {
        self.rubber.get_all_objects_from_index(&"test").unwrap()
    }

    pub fn host(&self) -> String {
        self.docker_wrapper.host()
    }

    pub fn init(&mut self) {
        self.rubber.delete_index(&"_all".to_string()).unwrap();
    }

    pub fn refresh(&self) {
        info!("Refreshing ES indexes");

        let res = hyper::client::Client::new()
            .get(&format!("{}/_refresh", self.host()))
            .send()
            .unwrap();
        assert!(res.status == hyper::Ok, "Error ES refresh: {:?}", res);
    }

    pub fn new(docker_wrapper: &ElasticsearchDocker) -> ElasticSearchWrapper {
        let mut es_wrapper = ElasticSearchWrapper {
            docker_wrapper: docker_wrapper,
            rubber: mimir::rubber::Rubber::new(&docker_wrapper.host()),
        };
        es_wrapper.init();
        es_wrapper
    }

    /// simple search on an index
    /// assert that the result is OK and transform it to a json Value
    pub fn search(&self, word: &str) -> serde_json::Value {
        let res = self
            .rubber
            .get(&format!("munin/_search?q={}", word))
            .unwrap();
        assert!(res.status == hyper::Ok);
        res.to_json()
    }

    pub fn search_on_global_stop_index(&self, word: &str) -> serde_json::Value {
        let res = self
            .rubber
            .get(&format!("munin_global_stops/_search?q={}", word))
            .unwrap();
        assert!(res.status == hyper::Ok);
        res.to_json()
    }

    pub fn search_and_filter<'b, F>(
        &self,
        word: &str,
        predicate: F,
    ) -> Box<Iterator<Item = mimir::Place> + 'b>
    where
        F: 'b + FnMut(&mimir::Place) -> bool,
    {
        self.search_and_filter_on_index(word, predicate, false)
    }

    fn search_and_filter_on_index<'b, F>(
        &self,
        word: &str,
        predicate: F,
        search_on_global_stops: bool,
    ) -> Box<Iterator<Item = mimir::Place> + 'b>
    where
        F: 'b + FnMut(&mimir::Place) -> bool,
    {
        use serde_json::map::{Entry, Map};
        use serde_json::value::Value;
        fn into_object(json: Value) -> Option<Map<String, Value>> {
            match json {
                Value::Object(o) => Some(o),
                _ => None,
            }
        }
        fn get(json: Value, key: &str) -> Option<Value> {
            into_object(json).and_then(|mut json| match json.entry(key.to_string()) {
                Entry::Occupied(o) => Some(o.remove()),
                _ => None,
            })
        }
        let json = if search_on_global_stops {
            self.search_on_global_stop_index(word)
        } else {
            self.search(word)
        };
        get(json, "hits")
            .and_then(|json| get(json, "hits"))
            .and_then(|hits| {
                match hits {
                    Value::Array(v) => {
                        Some(Box::new(
                            v.into_iter()
                                .filter_map(|json| {
                                    into_object(json).and_then(|obj| {
                                        let doc_type = obj
                                            .get("_type")
                                            .and_then(|doc_type| doc_type.as_str())
                                            .map(|doc_type| doc_type.into());

                                        doc_type.and_then(|doc_type| {
                                            // The real object is contained in the _source section.
                                            obj.get("_source").and_then(|src| {
                                                bragi::query::make_place(
                                                    doc_type,
                                                    Some(Box::new(src.clone())),
                                                )
                                            })
                                        })
                                    })
                                })
                                .filter(predicate),
                        )
                            as Box<Iterator<Item = mimir::Place>>)
                    }
                    _ => None,
                }
            })
            .unwrap_or(Box::new(None.into_iter()) as Box<Iterator<Item = mimir::Place>>)
    }
}

fn launch_and_assert(
    cmd: &'static str,
    args: Vec<std::string::String>,
    es_wrapper: &ElasticSearchWrapper,
) {
    let status = Command::new(cmd).args(&args).status().unwrap();
    assert!(status.success(), "`{}` failed {}", cmd, &status);
    es_wrapper.refresh();
}

#[test]
fn main_test() {
    let _guard = mimir::logger_init();

    let mut el_docker = ElasticsearchDocker::new().unwrap();
    let pg_docker = PostgresDocker::new().unwrap();

    fafnir_tests::main_test(
        ElasticSearchWrapper::new(&mut el_docker),
        PostgresWrapper::new(&pg_docker),
    );
}
