//! Models for TripAdvisor's XML feed structure.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Property {
    pub name: Vec<I18nProperty>,
    pub category: Vec<I18nProperty>,
    pub address: Vec<I18nProperty>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub average_rating: Option<f64>,
    pub review_count: u64,

    #[serde(rename = "id")]
    pub id: u32,

    #[serde(default)]
    pub sub_categories: SubCategories,

    #[serde(default)]
    pub cuisine: Cuisine,

    #[serde(rename = "TripAdvisorURL")]
    pub ta_url: Option<String>,

    #[serde(rename = "ViewPhotosURL")]
    pub ta_photos_url: Option<String>,

    #[serde(rename = "PropertyURL")]
    pub url: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Cuisine {
    #[serde(rename = "Item")]
    pub inner: Vec<Item>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Item {
    pub name: Vec<I18nProperty>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SubCategories {
    #[serde(rename = "SubCategory")]
    pub inner: Vec<SubCategory>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SubCategory {
    pub name: Vec<I18nProperty>,
}

#[derive(Debug, Deserialize)]
pub struct I18nProperty {
    pub lang: String,
    #[serde(rename = "$value")]
    pub value: Option<String>,
}
