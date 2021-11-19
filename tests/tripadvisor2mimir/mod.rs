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
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(r#"tripadvisor.properties="{}""#, PROPERTY_LIST),
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
    let gasthof_au = &pois[0];
    assert!(&gasthof_au.is_poi());

    let poi_type = &gasthof_au.poi().unwrap().poi_type;
    assert_eq!(poi_type.id, "class_restaurant:subclass_sitdown");
    assert_eq!(poi_type.name, "class_restaurant subclass_sitdown");

    // Test that the place "b'eat Restaurant & Bar" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Restaurant*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);
    let b_eat = &pois[0];
    assert!(&b_eat.is_poi());

    // Cuisine should match a OpenstreetMap cuisine tag
    let poi_type = &b_eat.poi().unwrap().poi_type;
    assert_eq!(poi_type.id, "class_restaurant:subclass_sitdown");
    assert_eq!(
        poi_type.name,
        "class_restaurant subclass_sitdown cuisine:italian"
    );

    // Test that the place "Bergrestaurant Suecka" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Suecka*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);
    let suecka = &pois[0];
    assert!(&suecka.is_poi());

    // Hotel subclass should match a OpenstreetMap category tag
    let poi_type = &suecka.poi().unwrap().poi_type;
    assert_eq!(poi_type.id, "class_hotel:subclass_bedandbreakfast");
    assert_eq!(poi_type.name, "class_hotel subclass_bedandbreakfast");

    // Test that the place "Mr B's - A Bartolotta Steakhouse - Brookfield" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Bartolotta*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);
    let bartolotta = &pois[0];
    assert!(&bartolotta.is_poi());

    // TA cuisine tag should be converted to OSM (steakhouse -> steak_house)
    let poi_type = &bartolotta.poi().unwrap().poi_type;
    assert_eq!(poi_type.id, "class_restaurant:subclass_sitdown");
    assert_eq!(
        poi_type.name,
        "class_restaurant subclass_sitdown cuisine:steak_house"
    );
}
