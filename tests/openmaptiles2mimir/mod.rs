use super::DATASET;
use super::{ElasticSearchWrapper, PostgresWrapper};
use approx::assert_relative_eq;
use geo_types as geo;
use places::addr::Addr;
use places::street::Street;
use places::Address;
use std::iter;
use std::sync::Arc;
use tokio::join;

const OPENMAPTILES2MIMIR_BIN: &str = concat!(env!("OUT_DIR"), "/../../../openmaptiles2mimir");
const CONFIG_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/config");

// Init the Postgres Wrapper

async fn init_tests(
    es_wrapper: &mut ElasticSearchWrapper,
    pg_wrapper: &PostgresWrapper,
    country_code: &str,
) {
    join!(
        load_es_data(es_wrapper, country_code),
        load_pg_data(pg_wrapper),
    );
}

async fn load_pg_data(pg_wrapper: &PostgresWrapper) {
    let conn = pg_wrapper.get_conn().await;

    conn.batch_execute(include_str!("data/tables.sql"))
        .await
        .expect("failed to initialize tables");

    conn.batch_execute(include_str!("data/data.sql"))
        .await
        .expect("failed to populate tables");

    conn.batch_execute(include_str!("data/functions.sql"))
        .await
        .expect("failed to define SQL functions");
}

async fn load_es_data(es_wrapper: &mut ElasticSearchWrapper, country_code: &str) {
    let city = make_test_admin("bob's town", country_code);
    let test_address = make_test_address(city.clone());
    let addresses = std::iter::once(test_address);
    es_wrapper.index(DATASET, addresses).await;

    let cities = std::iter::once(city);
    es_wrapper.index(DATASET, cities).await;
}

fn make_test_admin(name: &str, country_code: &str) -> places::admin::Admin {
    let p = |x, y| geo::Coordinate { x, y };

    let boundary = geo::MultiPolygon(vec![geo::Polygon::new(
        geo::LineString(vec![
            p(0., 0.),
            p(20., 0.),
            p(20., 20.),
            p(0., 20.),
            p(0., 0.),
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
        coord: places::coord::Coord::new(4.0, 4.0),
        boundary: Some(boundary),
        insee: "outlook".to_string(),
        zone_type: Some(cosmogony::ZoneType::City),
        labels: places::i18n_properties::I18nProperties::default(),
        names: places::i18n_properties::I18nProperties::default(),
        codes: iter::once(("ISO3166-1:alpha2".to_string(), country_code.to_string())).collect(),
        ..Default::default()
    }
}

fn unwrap_addr(address: &Address) -> &Addr {
    match address {
        Address::Street(_) => panic!("unwrap_addr() on street"),
        Address::Addr(addr) => addr,
    }
}

fn unwrap_street(address: &Address) -> &Street {
    match address {
        Address::Street(street) => street,
        Address::Addr(_) => panic!("unwrap_street() on addr"),
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

pub async fn main_test(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR").await;

    super::launch_and_assert(
        OPENMAPTILES2MIMIR_BIN,
        vec![
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
            "-s".to_string(),
            format!(r#"container-search.dataset="{}""#, DATASET),
            "-s".to_string(),
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(
                r#"postgres.url="postgres://test@{}/test""#,
                &pg_wrapper.host()
            ),
        ],
    )
    .await;

    let rows = &pg_wrapper.get_rows("osm_poi_point").await;
    assert_eq!(rows.len(), 7);
    let rows = &pg_wrapper.get_rows("osm_poi_polygon").await;
    assert_eq!(rows.len(), 3);

    assert_eq!(
        es_wrapper
            .search_and_filter("name:*", |p| p.is_poi())
            .await
            .count(),
        9 // 5 valid points + 2 valid polygons + 1 airport + 1 hamlet
    );

    // Test that the place "Ocean Studio" has been imported in the elastic wrapper
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Ocean*", |_| true)
        .await
        .collect();
    assert_eq!(&pois.len(), &1);

    // Test that the place "Ocean Studio" is a POI
    let ocean_place = &pois[0];
    assert!(&ocean_place.is_poi());

    // Test that the coord property of a POI has been well loaded
    // We test latitude and longitude
    let ocean_poi = &ocean_place.poi().unwrap();
    assert_eq!(&ocean_poi.id, "osm:node:5589618289");
    let coord_ocean_poi = &ocean_poi.coord;
    assert_relative_eq!(coord_ocean_poi.lat(), 1., epsilon = f64::EPSILON);
    assert_relative_eq!(coord_ocean_poi.lon(), 1., epsilon = f64::EPSILON);

    // Test Label
    let label_ocean_poi = &ocean_poi.label;
    assert_eq!(label_ocean_poi, &"Ocean Studio (bob's town)");
    // Test poi_type
    let poi_type_ocean_poi = &ocean_poi.poi_type.name;
    assert_eq!(
        poi_type_ocean_poi,
        &"class_cafe subclass_cafe cuisine:japanese cuisine:coffee_shop"
    );

    // Test Properties: the amenity property for this POI should be "cafe"
    let properties_ocean_poi = &ocean_poi.properties;
    let amenity_tag = properties_ocean_poi
        .iter()
        .find(|&(key, _)| key == "amenity")
        .unwrap();
    assert_eq!(amenity_tag.1, "cafe");

    // Test Address: we get the address from elasticsearch associated to a POI and we check that
    // its associated information are correct.
    // To guarantee the rubber found an address we have put a fake address close to the location of
    // the POI in the init() method.
    let address_ocean_poi = unwrap_addr(ocean_poi.address.as_ref().unwrap());
    assert_eq!(address_ocean_poi.label, "1234 test (bob's town)");
    assert_eq!(address_ocean_poi.house_number, "1234");
    assert_eq!(address_ocean_poi.zip_codes, ["12345".to_string()]);
    let address_coord = address_ocean_poi.coord;
    assert_relative_eq!(address_coord.lat(), 1., epsilon = f64::EPSILON);
    assert_relative_eq!(address_coord.lon(), 1., epsilon = f64::EPSILON);

    let le_nomade_query: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Le nomade", |_| true)
        .await
        .collect();
    assert_eq!(&le_nomade_query.len(), &1);
    let le_nomade = le_nomade_query[0].poi().expect("should be a POI");
    assert_eq!(le_nomade.id, "osm:way:42"); // the id in the database is '-42', so it's a way
                                            // this poi has addresses osm tags, we should have read it
    let le_nomade_addr = unwrap_addr(le_nomade.address.as_ref().unwrap());
    assert_eq!(le_nomade_addr.label, "7 rue spontini (bob's town)");
    assert_eq!(le_nomade_addr.name, "7 rue spontini");
    assert_eq!(le_nomade_addr.house_number, "7");
    assert_eq!(le_nomade_addr.zip_codes, ["75016".to_string()]);

    // Test that the airport 'Isla Cristina Agricultural Airstrip' has been imported in the elastic wrapper
    let airport_cristina: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Isla Cristina", |_| true)
        .await
        .collect();
    assert_eq!(&airport_cristina.len(), &1);
    assert!(&airport_cristina[0].is_poi());

    // Test the airport id
    let airport = &airport_cristina[0].poi().unwrap();
    assert_eq!(&airport.id, "osm:node:4505823836");

    // Test the airport coord
    let airport_coord = &airport.coord;
    assert_relative_eq!(airport_coord.lat(), 5.0, epsilon = f64::EPSILON);
    assert_relative_eq!(airport_coord.lon(), 5.0, epsilon = f64::EPSILON);

    // Test the airport poi_class and poi_subclass
    let properties_airport = &airport.properties;
    let poi_class = properties_airport
        .iter()
        .find(|&(key, _)| key == "poi_class")
        .unwrap();
    assert_eq!(poi_class.1, "aerodrome");
    let poi_subclass = properties_airport
        .iter()
        .find(|&(key, _)| key == "poi_subclass")
        .unwrap();
    assert_eq!(poi_subclass.1, "airport");

    // the '4 gusto' has a tag addr:street but no housenumber, we therefore get an address without
    // a housenumber.
    let gusto_query: Vec<places::Place> = es_wrapper
        .search_and_filter("name:4 gusto", |_| true)
        .await
        .collect();
    assert_eq!(&gusto_query.len(), &1);
    let gusto = &gusto_query[0];
    assert!(&gusto.is_poi());
    let gusto = &gusto.poi().unwrap();
    assert_eq!(&gusto.id, "osm:node:5590601521");
    let bob = unwrap_street(gusto.address.as_ref().unwrap());
    assert_eq!(bob.label, "rue spontini (bob's town)");

    // the Spagnolo has some osm address tags and no addr:postcode
    // we should still read it's address from osm
    let spagnolo_query: Vec<places::Place> = es_wrapper
        .search_and_filter("name:spagnolo", |_| true)
        .await
        .collect();
    assert_eq!(&spagnolo_query.len(), &1);
    let spagnolo = &spagnolo_query[0];
    assert!(&spagnolo.is_poi());
    let spagnolo = &spagnolo.poi().unwrap();
    assert_eq!(&spagnolo.id, "osm:node:5590210422");
    let spagnolo_addr = unwrap_addr(spagnolo.address.as_ref().unwrap());
    assert_eq!(spagnolo_addr.label, "12 rue bob (bob's town)");
    assert_eq!(spagnolo_addr.house_number, "12");
    // Spagnolo has no postcode but it is read from admins
    assert_eq!(spagnolo.zip_codes, ["421337".to_string()]);
    assert_eq!(spagnolo_addr.zip_codes, ["421337".to_string()]);

    // Test that two "Tour Eiffel" POI should have been imported: the hotel + the monument
    let eiffels: Vec<places::Place> = es_wrapper
        .search_and_filter("name:(Tour Eiffel)", |_| true)
        .await
        .collect();
    assert_eq!(&eiffels.len(), &2);

    // Test they are both POI
    assert!(&eiffels.iter().all(|p| p.is_poi()));

    // Test their weight are not both equal to 0.0
    assert!(!&eiffels
        .iter()
        .map(|ref mut p| p.poi().unwrap())
        .all(|p| p.weight == 0.0f64));

    let hamlet_somewhere: Vec<places::Place> = es_wrapper
        .search_and_filter("name:(I am a lost sheep)", |_| true)
        .await
        .collect();
    assert_eq!(&hamlet_somewhere.len(), &1);
    assert!(&hamlet_somewhere[0].is_poi());
    let hamlet_somewhere = &hamlet_somewhere[0].poi().unwrap();
    let properties_hamlet_somewhere = &hamlet_somewhere.properties;
    let poi_class = properties_hamlet_somewhere
        .iter()
        .find(|&(key, _)| key == "poi_class")
        .unwrap();
    assert_eq!(poi_class.1, "locality");
    let poi_subclass = properties_hamlet_somewhere
        .iter()
        .find(|&(key, _)| key == "poi_subclass")
        .unwrap();
    assert_eq!(poi_subclass.1, "hamlet");

    // Test class/subclass for place_of_worship
    let church_query: Vec<places::Place> = es_wrapper
        .search_and_filter("name:saint-ambroise", |_| true)
        .await
        .collect();
    assert_eq!(&church_query.len(), &1);
    let church = &church_query[0].poi().unwrap();
    assert_eq!(church.name, "Église Saint-Ambroise");
    let church_class = church
        .properties
        .iter()
        .find(|&(key, _)| key == "poi_class")
        .unwrap();
    let church_subclass = church
        .properties
        .iter()
        .find(|&(key, _)| key == "poi_subclass")
        .unwrap();
    assert_eq!(church_class.1, "place_of_worship");
    assert_eq!(church_subclass.1, "christian");

    // 2 pois in nosearch index
    assert_eq!(es_wrapper.get_all_nosearch_pois().await.count(), 2);

    // Test existance of water POIs
    let res = es_wrapper
        .search_and_filter("name:(Fontaine-Lavoir Saint-Guimond)", |_| true)
        .await;
    assert_eq!(res.count(), 1);
    let res = es_wrapper
        .search_and_filter("name:(Baie du Mont Saint-Michel)", |_| true)
        .await;
    assert_eq!(res.count(), 1);

    // Filter by poi_type.name
    let res = es_wrapper
        .search_and_filter("poi_type.name:(subclass_cafe)", |_| true)
        .await;
    assert_eq!(res.count(), 2);
    let res = es_wrapper
        .search_and_filter("poi_type.name:(cuisine\\:coffee_shop)", |_| true)
        .await;
    assert_eq!(res.count(), 1);
}

pub async fn bbox_test(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR").await;
    super::launch_and_assert(
        OPENMAPTILES2MIMIR_BIN,
        vec![
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
            "-s".to_string(),
            format!(r#"container-search.dataset="{}""#, DATASET),
            "-s".to_string(),
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(
                r#"postgres.url="postgres://test@{}/test""#,
                &pg_wrapper.host()
            ),
            "-s".to_string(),
            "fafnir.bounding_box=[0,0,3.5,3.5]".to_string(),
        ],
    )
    .await;

    // We filtered the import by a bounding box, we still have 6 rows in PG
    let rows = &pg_wrapper.get_rows("osm_poi_point").await;
    assert_eq!(rows.len(), 7);
    // but there is only 3 elements in the ES now, 'Le nomade' and 'Isla Cristina Agricultural Airstrip'
    // have been filtered
    assert_eq!(
        es_wrapper
            .search_and_filter("name:*", |p| p.is_poi())
            .await
            .count(),
        5
    );
}

pub async fn test_with_langs(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR").await;
    super::launch_and_assert(
        OPENMAPTILES2MIMIR_BIN,
        vec![
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
            "-s".to_string(),
            r#"fafnir.langs=["ru","it"]"#.to_string(),
            "-s".to_string(),
            format!(r#"container-search.dataset="{}""#, DATASET),
            "-s".to_string(),
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(
                r#"postgres.url="postgres://test@{}/test""#,
                &pg_wrapper.host()
            ),
        ],
    )
    .await;

    // Test that the place "Ocean Studio" has been imported in the elastic wrapper
    // with the fields "labels" and "names"
    let pois: Vec<places::Place> = es_wrapper
        .search_and_filter("name:Ocean*", |_| true)
        .await
        .collect();
    let ocean_poi = &pois[0].poi().unwrap();
    assert!(ocean_poi
        .names
        .0
        .iter()
        .any(|p| p.key == "ru" && p.value == "студия океана"));

    assert!(ocean_poi
        .names
        .0
        .iter()
        .any(|p| p.key == "it" && p.value == "Oceano Studioso"));

    assert!(ocean_poi
        .labels
        .0
        .iter()
        .any(|p| p.key == "ru" && p.value == "студия океана (bob\'s town)"));

    assert!(ocean_poi
        .labels
        .0
        .iter()
        .any(|p| p.key == "it" && p.value == "Oceano Studioso (bob\'s town)"));
}

pub async fn test_address_format(
    mut es_wrapper: ElasticSearchWrapper,
    pg_wrapper: PostgresWrapper,
) {
    // Import data with DE as country code in admins
    init_tests(&mut es_wrapper, &pg_wrapper, "DE").await;
    super::launch_and_assert(
        OPENMAPTILES2MIMIR_BIN,
        vec![
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
            "-s".to_string(),
            format!(r#"container-search.dataset="{}""#, DATASET),
            "-s".to_string(),
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(
                r#"postgres.url="postgres://test@{}/test""#,
                &pg_wrapper.host()
            ),
        ],
    )
    .await;

    let spagnolo_query: Vec<places::Place> = es_wrapper
        .search_and_filter("name:spagnolo", |_| true)
        .await
        .collect();
    let spagnolo = &spagnolo_query[0];
    assert!(&spagnolo.is_poi());
    let spagnolo = &spagnolo.poi().unwrap();
    assert_eq!(&spagnolo.id, "osm:node:5590210422");
    let spagnolo_addr = unwrap_addr(spagnolo.address.as_ref().unwrap());

    // German format: housenumber comes after street name
    assert_eq!(spagnolo_addr.label, "rue bob 12 (bob's town)");
    assert_eq!(spagnolo_addr.house_number, "12");
}

pub async fn test_current_country_label(
    mut es_wrapper: ElasticSearchWrapper,
    pg_wrapper: PostgresWrapper,
) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR").await;
    super::launch_and_assert(
        OPENMAPTILES2MIMIR_BIN,
        vec![
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
            "-s".to_string(),
            format!(r#"container.dataset="{}""#, DATASET),
            "-s".to_string(),
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(
                r#"postgres.url="postgres://test@{}/test""#,
                &pg_wrapper.host()
            ),
            "-s".to_string(),
            r#"fafnir.langs=["en"]"#.to_string(),
        ],
    )
    .await;

    let eiffels: Vec<places::Place> = es_wrapper
        .search_and_filter("names.en:(Eiffel Tower)", |_| true)
        .await
        .collect();

    assert_eq!(eiffels.len(), 1);
    let eiffel_tower = eiffels[0].poi().unwrap();

    assert!(!eiffel_tower.labels.0.iter().any(|l| l.key == "fr"));

    // Now check that we have the fr label too!
    super::launch_and_assert(
        OPENMAPTILES2MIMIR_BIN,
        vec![
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
            "-s".to_string(),
            format!(r#"container.dataset="{}""#, DATASET),
            "-s".to_string(),
            format!(r#"elasticsearch.url="{}""#, &es_wrapper.host()),
            "-s".to_string(),
            format!(
                r#"postgres.url="postgres://test@{}/test""#,
                &pg_wrapper.host()
            ),
            "-s".to_string(),
            r#"fafnir.langs=["fr","en"]"#.to_string(),
        ],
    )
    .await;
    let eiffels: Vec<places::Place> = es_wrapper
        .search_and_filter("names.en:(Eiffel Tower)", |_| true)
        .await
        .collect();
    assert_eq!(eiffels.len(), 1);
    let eiffel_tower = eiffels[0].poi().unwrap();
    assert!(eiffel_tower
        .labels
        .0
        .iter()
        .any(|l| l.key == "fr" && l.value == "Tour Eiffel (bob's town)"));
    assert!(eiffel_tower
        .names
        .0
        .iter()
        .any(|n| n.key == "fr" && n.value == "Tour Eiffel"))
}
