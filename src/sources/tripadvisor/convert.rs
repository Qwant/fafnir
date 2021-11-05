//! Utilities to convert a super::models::Property into mimir's Poi.

use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use places::admin::find_country_codes;
use places::coord::Coord;
use places::i18n_properties::I18nProperties;
use places::poi::Poi;
use places::street::Street;
use places::Address;

use super::models::{LangProperty, Property};

/// Required review count to get the maximal weight of 1.
const MAX_REVIEW_COUNT: u64 = 1000;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum BuildError {
    MissingField(&'static str),
    EmptyAdmins,
}

pub fn build_poi(property: Property, geofinder: &AdminGeoFinder) -> Result<Poi, BuildError> {
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

    let id = format!("ta:poi:{}", property.id);
    let names = build_i18n_property(property.name);
    let labels = names.clone();
    let weight = (property.review_count as f64 / MAX_REVIEW_COUNT as f64).clamp(0., 1.);
    let address_i18n = build_i18n_property(property.address);
    let approx_coord = Some(coord.into());

    // TODO: requires to read mapping correctly
    let poi_type = Default::default();
    let name = names
        .0
        .first()
        .map(|prop| prop.value.as_str())
        .unwrap_or("")
        .to_string();

    let country_codes = find_country_codes(administrative_regions.iter().map(AsRef::as_ref));
    let zip_codes = Default::default();

    let address = address_i18n.0.first().map(|label| {
        Address::Street(Street {
            coord,
            label: label.value.to_string(),
            ..Default::default()
        })
    });

    let label = name.clone();

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

    Ok(Poi {
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
    })
}

fn build_i18n_property(props: Vec<LangProperty>) -> I18nProperties {
    I18nProperties(
        props
            .into_iter()
            .filter_map(|LangProperty { lang, value }| {
                Some(places::Property {
                    key: lang,
                    value: value?,
                })
            })
            .collect(),
    )
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::MissingField(field) => write!(f, "missing field `{}`", field),
            BuildError::EmptyAdmins => write!(f, "empty admins"),
        }
    }
}
