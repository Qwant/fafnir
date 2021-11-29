//! Models for TripAdvisor's XML PhotoList feed structure.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Property {
    pub id: u32,
    #[serde(rename = "Photos")]
    pub photos: Photos,
}

#[derive(Debug, Deserialize)]
pub struct Photos {
    #[serde(rename = "Photo")]
    pub inner: Vec<Photo>,
}

#[derive(Debug, Deserialize)]
pub struct Photo {
    #[serde(rename = "OriginalSizeURL")]
    pub original_size_url: Option<String>,
    #[serde(rename = "StandardSizeURL")]
    pub standard_size_url: Option<String>,
    #[serde(rename = "FullSizeURL")]
    pub full_size_url: Option<String>,
    #[serde(rename = "LargeThumbnailURL")]
    pub large_thumbnail_url: Option<String>,
    #[serde(rename = "ThumbnailURL")]
    pub thumbnail_url: Option<String>,
}
