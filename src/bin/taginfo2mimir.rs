use mimir::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir::common::document::{ContainerDocument, Document};
use mimir::domain::model::configuration::ContainerConfig;
use mimir::domain::ports::primary::generate_index::GenerateIndex;
use mimir::domain::ports::secondary::remote::Remote;
use mimir::domain::ports::secondary::storage::Storage;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(tag = "type", rename = "street")]
pub struct Brand {
    pub count: f64,
    pub name: String,
}

impl Document for Brand {
    fn id(&self) -> String {
        self.name.clone()
    }
}

impl ContainerDocument for Brand {
    fn static_doc_type() -> &'static str {
        "brand"
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct TagInfoResults {
    data: Vec<TagInfoResult>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TagInfoResult {
    value: String,
    count: f64,
    fraction: f64,
    in_wiki: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Settings {
    elasticsearch: ElasticsearchStorageConfig,
    container_taginfo: ContainerConfig,
}

async fn load_and_index_taginfo(settings: Settings) {
    let mimir_es = connection_pool_url(&settings.elasticsearch.url)
        .conn(settings.elasticsearch)
        .await
        .expect("failed to open Elasticsearch connection");

    // Init Index
    let index_generator = mimir_es
        .init_container(&settings.container_taginfo)
        .await
        .expect("could not create index");

    let response = reqwest::get(
        "https://taginfo.openstreetmap.org/api/4/key/values?key=brand&filter=all\
    &lang=fr&sortname=count&sortorder=desc&page=1&rp=999&qtype=value&format=json_pretty",
    )
    .await
    .unwrap();

    // on success, parse our JSON to an APIResponse
    let tag_info_results = response.json::<TagInfoResults>().await.unwrap();

    let index_generator = {
        let brands: Vec<Brand> = tag_info_results
            .data
            .into_iter()
            .map(|result| Brand {
                count: result.count,
                name: result.value,
            })
            .collect();

        let index_generator = index_generator
            .insert_documents(futures::stream::iter(brands))
            .await
            .expect("could not insert brand into index");
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
    fafnir::cli::run(load_and_index_taginfo).await
}
