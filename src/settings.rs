//! Shared settings structs.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct FafnirSettings {
    pub bounding_box: Option<[f64; 4]>,
    pub langs: Vec<String>,
    pub skip_reverse: bool,
    #[serde(default = "num_cpus::get")]
    pub concurrent_blocks: usize,
    pub max_query_batch_size: usize,
    pub log_indexed_count_interval: usize,
}

#[derive(Debug, Deserialize)]
pub struct PostgresSettings {
    pub host: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub port: u16,
}
