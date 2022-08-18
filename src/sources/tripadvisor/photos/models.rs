//! Models for TripAdvisor's JSON PhotoList feed structure.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Property {
    pub id: u32,
    #[serde(rename = "Photos")]
    pub photos: Vec<Photo>,
}

#[derive(Debug, Deserialize)]
pub struct Photo {
    #[serde(rename = "OriginalSizeURL")]
    pub original_size: Option<PhotoDetail>,
    #[serde(rename = "StandardSizeURL")]
    pub standard_size: Option<PhotoDetail>,
    #[serde(rename = "FullSizeURL")]
    pub full_size: Option<PhotoDetail>,
    #[serde(rename = "LargeThumbnailURL")]
    pub large_thumbnail: Option<PhotoDetail>,
    #[serde(rename = "ThumbnailURL")]
    pub thumbnail: Option<PhotoDetail>,
}

#[derive(Debug, Deserialize)]
pub struct PhotoDetail {
    pub height: Option<u32>,
    pub width: Option<u32>,
    pub url: Option<String>,
}
