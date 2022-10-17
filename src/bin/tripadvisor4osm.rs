use std::time::Duration;

use futures::stream::StreamExt;
use futures::future;
use mimir::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir::domain::model::query::Query::QueryDSL;
use mimir::domain::model::update::UpdateOperation;
use mimir::domain::ports::primary::search_documents::SearchDocuments;
use mimir::domain::ports::secondary::list::{List, Parameters};
use mimir::domain::ports::secondary::remote::Remote;
use mimir::domain::ports::secondary::storage::Storage;
use places::poi::Poi;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Settings {
    elasticsearch: ElasticsearchStorageConfig,
}

async fn load_tripadvisor(settings: Settings) {
    let mimir_es = connection_pool_url(&settings.elasticsearch.url)
        .conn(settings.elasticsearch)
        .await
        .expect("failed to open Elasticsearch connection");

    // Init Index
    let index_container = mimir_es
        .find_container("munin_poi".to_string())
        .await
        .expect("could not get index");

    let index_generator = {
        let update_ta_id = mimir_es.list_documents(Parameters {
            doc_type: "poi_tripadvisor".to_string(),
        })
            .await
            .expect("could not query a list of POIs from ES")
            .filter(
                |poi: &Result<Poi, mimir::domain::ports::secondary::list::Error>| {
                    future::ready(
                        poi.as_ref()
                            .unwrap()
                            .properties
                            .get("poi_class")
                            .unwrap_or(&"".to_string())
                            == &"hotel".to_string(),
                    )
                },
            )
            .then(
                |poi: Result<Poi, mimir::domain::ports::secondary::list::Error>| {
                    mimir_es.search_documents::<Poi>(
                        vec!["munin_poi".to_string()],
                        QueryDSL(json!({
                        "query": {
                            "match": {"name": poi.unwrap().label}
                        }
                    })),
                        1,
                        Option::from(Duration::new(10, 0)),
                    )
                },
            )
            .filter_map(
                |result: Result<Vec<Poi>, mimir::domain::model::error::Error>| {
                    future::ready(result.as_ref().unwrap().first().map(|osm_poi| {
                        (
                            osm_poi.clone().id,
                            vec![UpdateOperation::Set {
                                ident: "properties.ta:id".to_string(),
                                value: String::new(),
                            }],
                        )
                    }))
                },
            );

        let index_generator = index_container
            .update_documents(update_ta_id)
            .await
            .expect("could not update documents from index");
        index_generator
    };

    // Publish index
    index_generator
        .publish()
        .await
        .expect("could not publish index");
}

#[tokio::main]
async fn main() {
    fafnir::cli::run(load_tripadvisor).await
}
