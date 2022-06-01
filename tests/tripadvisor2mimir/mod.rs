use std::iter;
use std::sync::Arc;

use geo_types as geo;
use tokio::join;

use super::ElasticSearchWrapper;
use super::TRIPADVISOR_DATASET;

const TRIPADVISOR2MIMIR_BIN: &str = concat!(env!("OUT_DIR"), "/../../../tripadvisor2mimir");
const CONFIG_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/config");
const PROPERTY_LIST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/tripadvisor2mimir/data/propertylist_liechtenstein.xml.gz"
);
const PHOTO_LIST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/tripadvisor2mimir/data/photolist_liechtenstein.xml.gz"
);

// const REVIEW_LIST: &str = concat!(
//     env!("CARGO_MANIFEST_DIR"),
//     "/tests/tripadvisor2mimir/data/reviewlist_liechtenstein.xml.gz"
// );

async fn init_tests(es_wrapper: &mut ElasticSearchWrapper, country_code: &str) {
    join!(load_es_data(es_wrapper, country_code),);
}

async fn load_es_data(es_wrapper: &mut ElasticSearchWrapper, country_code: &str) {
    let city = make_test_admin("bob's town", country_code);
    let test_address = make_test_address(city.clone());
    let addresses = std::iter::once(test_address);
    es_wrapper.index(TRIPADVISOR_DATASET, addresses).await;

    let cities = std::iter::once(city);
    es_wrapper.index(TRIPADVISOR_DATASET, cities).await;
}

fn make_test_admin(name: &str, country_code: &str) -> places::admin::Admin {
    let p = |x, y| geo::Coordinate { x, y };

    let boundary = geo::MultiPolygon(vec![geo::Polygon::new(
        geo::LineString(vec![
            p(0., 50.),
            p(20., 50.),
            p(20., 40.),
            p(0., 40.),
            p(0., 50.),
        ]),
        vec![],
    )]);
    places::admin::Admin {
        id: name.to_string(),
        level: 8,
        name: name.to_string(),
        label: name.to_string(),
        zip_codes: vec!["421337".to_string()],
        weight: 0f64,
        coord: places::coord::Coord::new(9.55, 47.14),
        boundary: Some(boundary),
        insee: "outlook".to_string(),
        zone_type: Some(cosmogony::ZoneType::City),
        labels: places::i18n_properties::I18nProperties::default(),
        names: places::i18n_properties::I18nProperties::default(),
        codes: iter::once(("ISO3166-1:alpha2".to_string(), country_code.to_string())).collect(),
        ..Default::default()
    }
}

fn make_test_address(city: places::admin::Admin) -> places::addr::Addr {
    let country_codes = places::admin::find_country_codes(std::iter::once(&city));

    let street = places::street::Street {
        id: "1234".to_string(),
        name: "test".to_string(),
        label: "test (bob's town)".to_string(),
        administrative_regions: vec![Arc::new(city)],
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
        coord: places::coord::Coord::new(1., 1.),
        country_codes: country_codes.clone(),
        ..Default::default()
    };
    places::addr::Addr {
        id: format!("addr:{};{}", 1., 1.),
        house_number: "1234".to_string(),
        name: "1234 test".to_string(),
        street,
        label: "1234 test (bob's town)".to_string(),
        coord: places::coord::Coord::new(1., 1.),
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
        distance: None,
        approx_coord: None,
        country_codes,
        context: None,
    }
}

pub async fn main_test(mut es_wrapper: ElasticSearchWrapper) {
    init_tests(&mut es_wrapper, "FR").await;

    super::launch_and_assert(
        TRIPADVISOR2MIMIR_BIN,
        vec![
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
            "-s".to_string(),
            format!(r#"container-tripadvisor.dataset="{}""#, TRIPADVISOR_DATASET),
            "-s".to_string(),
            format!(r#"container-tripadvisor.visibility="{}""#, "public"),
            "-s".to_string(),
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(r#"tripadvisor.properties="{}""#, PROPERTY_LIST),
            "-s".to_string(),
            format!(r#"tripadvisor.photos="{}""#, PHOTO_LIST),
            // "-s".to_string(),
            // format!(r#"tripadvisor.reviews="{}""#, REVIEW_LIST),
        ],
    )
    .await;

    assert_eq!(es_wrapper.get_all_tripadvisor_pois().await.count(), 4);

    // Test that the place "Gasthof Au" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Gasthof*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);
    let gasthof_au = &pois[0].poi().unwrap();

    assert_eq!(gasthof_au.poi_type.id, "class_restaurant:subclass_sit_down");
    assert_eq!(
        gasthof_au.poi_type.name,
        "class_restaurant subclass_sit_down"
    );

    // OriginalSizeURL is available for image
    assert_eq!(
        gasthof_au.properties.get("image"),
        Some(&"https://media-cdn.tripadvisor.com/media/photo-o/15/33/ff/4a/europe.jpg".to_string())
    );

    assert_eq!(
        gasthof_au.properties.get("phone"),
        Some(&"+423 232 11 17".to_string())
    );

    assert_eq!(
        gasthof_au.properties.get("opening_hours"),
        Some(&"Mo 11:00-00:00; Tu 11:00-00:00; We 11:00-00:00; Th 11:00-00:00; Fr 11:00-00:00; Sa 11:00-12:30,14:00-18:00".to_string())
    );

    // Test that the place "b'eat Restaurant & Bar" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Restaurant*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);
    let b_eat = pois[0].poi().unwrap();

    // This POI should have reviews
    // assert!(serde_json::from_str::<serde_json::Value>(&*b_eat.properties["ta:reviews:0"]).is_ok());
    // assert!(serde_json::from_str::<serde_json::Value>(&*b_eat.properties["ta:reviews:1"]).is_ok());

    // Cuisine should match a OpenstreetMap cuisine tag
    assert_eq!(b_eat.poi_type.id, "class_restaurant:subclass_sit_down");
    assert_eq!(
        b_eat.poi_type.name,
        "class_restaurant subclass_sit_down cuisine:italian"
    );

    // Image should have fallback to StandardSizeURL
    assert_eq!(
        b_eat.properties.get("image"),
        Some(&"https://media-cdn.tripadvisor.com/media/photo-s/01/9a/8a/54/asia.jpg".to_string())
    );

    // Test that the place "Bergrestaurant Suecka" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Suecka*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);
    let suecka = &pois[0];
    assert!(&suecka.is_poi());

    let suecka_poi = suecka.poi().unwrap();
    assert_eq!(&suecka_poi.properties.get("opening_hours"), &None);
    assert_eq!(&suecka_poi.properties.get("average_rating"), &None);

    // Hotel subclass should match a OpenstreetMap category tag
    let poi_type = &suecka_poi.poi_type;
    assert_eq!(poi_type.id, "class_hotel:subclass_bed_and_breakfast");
    assert_eq!(poi_type.name, "class_hotel subclass_bed_and_breakfast");

    // Test that the place "Mr B's - A Bartolotta Steakhouse - Brookfield" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Bartolotta*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);
    let bartolotta = &pois[0].poi().unwrap();

    // TA cuisine tag should be converted to OSM (steakhouse -> steak_house)
    assert_eq!(bartolotta.poi_type.id, "class_restaurant:subclass_sit_down");
    assert_eq!(
        bartolotta.poi_type.name,
        "class_restaurant subclass_sit_down cuisine:steak_house"
    );

    // There is no image in the feed for this POI
    assert!(!bartolotta.properties.contains_key("image"));
}
