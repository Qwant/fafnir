//! Shared settings structs.
use std::path::PathBuf;

use mimir2::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Settings {
    pub fafnir: FafnirSettings,
    pub postgres: PostgresSettings,
    pub elasticsearch: ElasticsearchStorageConfig,
    pub container_search: ContainerConfig,
    pub container_nosearch: ContainerConfig,
    pub logging: LogConfig,
}

#[derive(Deserialize)]
pub struct FafnirSettings {
    pub bounding_box: Option<[f64; 4]>,
    pub langs: Vec<String>,
    pub skip_reverse: bool,
    #[serde(default = "num_cpus::get")]
    pub concurrent_blocks: usize,
    pub max_query_batch_size: usize,
}

#[derive(Deserialize)]
pub struct PostgresSettings {
    pub url: String,
}

#[derive(Deserialize)]
pub struct ContainerConfig {
    pub dataset: String,
}

#[derive(Deserialize)]
pub struct LogConfig {
    pub path: PathBuf,
}
