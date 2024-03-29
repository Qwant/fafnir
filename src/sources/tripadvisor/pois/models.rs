//! Models for TripAdvisor's JSON PropertyList feed structure.

use serde::{Deserialize, Deserializer};

pub use places::i18n_properties::I18nProperties;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Property {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub average_rating: Option<f64>,
    pub review_count: u64,
    pub hours: Hours,

    #[serde(rename = "id")]
    pub id: u32,

    #[serde(deserialize_with = "deserialize_i18n")]
    pub name: I18nProperties,

    #[serde(deserialize_with = "deserialize_i18n")]
    pub category: I18nProperties,

    #[serde(deserialize_with = "deserialize_i18n")]
    pub address: I18nProperties,

    #[serde(default)]
    pub sub_categories: Vec<SubCategory>,

    #[serde(default)]
    pub cuisine: Cuisine,

    #[serde(rename = "TripAdvisorURL")]
    pub ta_url: Option<String>,

    #[serde(rename = "ViewPhotosURL")]
    pub ta_photos_url: Option<String>,

    #[serde(rename = "PropertyURL")]
    pub url: Option<String>,

    #[serde(rename = "PhoneNumber")]
    pub phone: Option<Phone>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Phone {
    #[serde(rename = "type")]
    pub _type: Option<String>,
    #[serde(rename = "Number")]
    pub number: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Cuisine {
    #[serde(rename = "Item")]
    pub inner: Vec<Item>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Item {
    #[serde(deserialize_with = "deserialize_i18n")]
    pub name: I18nProperties,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SubCategory {
    #[serde(deserialize_with = "deserialize_i18n")]
    pub name: I18nProperties,
}

#[derive(Debug, Default, Deserialize)]
pub struct Hours {
    #[serde(rename = "Day")]
    pub inner: Vec<Day>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Day {
    pub day_name: String,
    pub time: Option<Vec<Time>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Time {
    pub open_time: String,
    pub close_time: String,
}

/// Serialize i18n info into mimirsbrunn's I18nProperty:
///
/// [{
///     "lang": "el"
/// },
/// {
///     "lang": "en",
///     "value": "Bergrestaurant Suecka2"
/// }]
/// ...
pub fn deserialize_i18n<'de, D>(deserializer: D) -> Result<I18nProperties, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    pub struct JsonI18nProperty {
        pub lang: String,
        pub value: Option<String>,
    }

    let json_i18n: Vec<JsonI18nProperty> = Deserialize::deserialize(deserializer)?;

    let properties = json_i18n
        .into_iter()
        .filter_map(|prop| {
            Some(places::Property {
                key: prop.lang,
                value: prop.value?,
            })
        })
        .collect();

    Ok(I18nProperties(properties))
}
