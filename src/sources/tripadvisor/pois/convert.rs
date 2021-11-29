//! Utilities to convert a super::models::Property into mimir's Poi.

use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use once_cell::sync::Lazy;
use places::admin::find_country_codes;
use places::coord::Coord;
use places::i18n_properties::I18nProperties;
use places::poi::{Poi, PoiType};
use places::street::Street;
use places::Address;
use std::collections::HashMap;

use super::models::Property;
use crate::langs::COUNTRIES_LANGS;
use crate::sources::tripadvisor::build_id;

/// Required review count to get the maximal weight of 1.
const MAX_REVIEW_COUNT: u64 = 1000;

const OSM_CUISINE: &[&str] = &[
    "african",
    "american",
    "asian",
    "barbecue",
    "caribbean",
    "chinese",
    "french",
    "german",
    "greek",
    "italian",
    "indian",
    "japanese",
    "latin_american",
    "lebanese",
    "mediterranean",
    "mexican",
    "pakistani",
    "pizza",
    "seafood",
    "steak_house",
    "sushi",
    "spanish",
    "thai",
    "vietnamese",
    "western",
];

static CUISINE_CONVERTER: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    [
        ("steakhouse", "steak_house"),
        ("mexican_&_southwestern", "mexican"),
        ("south_american", "latin_american"),
        ("mexican_&_european", "western"),
    ]
    .into_iter()
    .collect()
});

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    MissingField(&'static str),
    EmptyAdmins,
}

pub fn build_poi(property: Property, geofinder: &AdminGeoFinder) -> Result<(u32, Poi), BuildError> {
    let coord = Coord::new(
        property
            .longitude
            .ok_or(BuildError::MissingField("longitude"))?,
        property
            .latitude
            .ok_or(BuildError::MissingField("latitude"))?,
    );

    let administrative_regions = geofinder.get(&coord);

    if administrative_regions.is_empty() {
        return Err(BuildError::EmptyAdmins);
    }

    let id = build_id(property.id);
    let names = property.name;
    let labels = names.clone();
    let weight = (property.review_count as f64 / MAX_REVIEW_COUNT as f64).clamp(0., 1.);
    let approx_coord = Some(coord.into());
    let country_codes = find_country_codes(administrative_regions.iter().map(AsRef::as_ref));

    let name = get_local_string(&country_codes, &names)
        .ok_or(BuildError::MissingField("name"))?
        .to_string();

    let label = name.clone();

    let zip_codes = administrative_regions
        .iter()
        .find(|admin| !admin.zip_codes.is_empty())
        .map(|admin| admin.zip_codes.clone())
        .unwrap_or_default();

    // Read address label
    let address = get_local_string(&country_codes, &property.address).map(|label| {
        Address::Street(Street {
            coord,
            label: label.to_string(),
            ..Default::default()
        })
    });

    // Build poi_type
    let category = get_local_string(&["us".to_string()], &property.category)
        .ok_or(BuildError::MissingField("category"))?
        .to_lowercase();

    let sub_category = (property.sub_categories.inner)
        .iter()
        .find_map(|sub_category| get_local_string(&["us".to_string()], &sub_category.name))
        .unwrap_or(&category)
        .replace(" ", "_")
        .to_lowercase();

    let cuisine = (property.cuisine.inner)
        .iter()
        .filter_map(|item| get_local_string(&["us".to_string()], &item.name))
        .map(|ta_cuisine| {
            CUISINE_CONVERTER
                .get(ta_cuisine.to_lowercase().replace(" ", "_").as_str())
                .copied()
                .unwrap_or(ta_cuisine)
        })
        .find(|cuisine| OSM_CUISINE.contains(&cuisine.to_lowercase().as_str()));

    let poi_type_name = cuisine
        .map(|cuisine| {
            format!(
                "class_{} subclass_{} cuisine:{}",
                category,
                sub_category,
                cuisine.to_lowercase()
            )
        })
        .unwrap_or_else(|| format!("class_{} subclass_{}", category, sub_category));

    let poi_type = PoiType {
        id: format!("class_{}:subclass_{}", category, sub_category),
        name: poi_type_name,
    };

    let properties = [
        ("website", property.url),
        ("ta:url", property.ta_url),
        ("ta:photos_url", property.ta_photos_url),
        ("ta:review_count", Some(property.review_count.to_string())),
        (
            "ta:average_rating",
            property.average_rating.map(|x| x.to_string()),
        ),
    ]
    .into_iter()
    .filter_map(|(key, val)| Some((key.to_string(), val?)))
    .collect();

    Ok((
        property.id,
        Poi {
            id,
            label,
            name,
            coord,
            approx_coord,
            administrative_regions,
            weight,
            zip_codes,
            poi_type,
            properties,
            address,
            country_codes,
            names,
            labels,
            distance: None,
            context: None,
        },
    ))
}

/// Read a property from local country langs if available, if not defined
/// fallback to English or any arbitrary value as a last resort.
fn get_local_string<'a>(country_codes: &[String], props: &'a I18nProperties) -> Option<&'a str> {
    country_codes
        .iter()
        .flat_map(|cc| {
            COUNTRIES_LANGS
                .get(cc.as_str())
                .into_iter()
                .copied()
                .flatten()
                .copied()
        })
        .chain(["en"]) // fallback to English if no local language is defined
        .find_map(|lang| Some(props.0.iter().find(|prop| prop.key == lang)?.value.as_str()))
        .or_else(|| Some(props.0.first()?.value.as_str()))
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::MissingField(field) => write!(f, "missing field `{}`", field),
            BuildError::EmptyAdmins => write!(f, "empty admins"),
        }
    }
}
