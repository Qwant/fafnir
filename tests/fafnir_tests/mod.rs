use super::mimir;
use super::DATASET;
use super::{ElasticSearchWrapper, PostgresWrapper};
use geo_types as geo;
use mimirsbrunn::utils;
use postgres::Client;
use std;
use std::f64;
use std::sync::Arc;

// Init the Postgres Wrapper
fn init_tests(
    es_wrapper: &mut ElasticSearchWrapper,
    pg_wrapper: &PostgresWrapper,
    country_code: &str,
) {
    let mut conn = pg_wrapper.get_conn();
    create_tests_tables(&mut conn);
    populate_tables(&mut conn);
    load_poi_class_function(&mut conn);
    load_osm_hash_from_imposm_function(&mut conn);
    load_global_id_from_imposm_function(&mut conn);
    load_labelgrid_function(&mut conn);
    load_poi_class_rank_function(&mut conn);
    load_all_pois_function(&mut conn);
    load_poi_display_weight_function(&mut conn);
    load_es_data(es_wrapper, country_code);
}

fn create_tests_tables(conn: &mut Client) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS osm_poi_point(
                         id                 serial primary key,
                         osm_id             bigint,
                         name               varchar,
                         name_en            varchar,
                         name_de            varchar,
                         tags               hstore,
                         subclass           varchar,
                         mapping_key        varchar,
                         station            varchar,
                         funicular          varchar,
                         information        varchar,
                         uic_ref            varchar,
                         religion           varchar,
                         level              integer,
                         indoor             boolean,
                         layer              integer,
                         sport              varchar,
                         geometry           geometry,
                         agg_stop           integer
                       )",
        &[],
    )
    .unwrap();
    conn.execute("TRUNCATE TABLE osm_poi_point", &[]).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS osm_poi_polygon (
                         id                 serial primary key,
                         osm_id             bigint,
                         name               varchar,
                         name_en            varchar,
                         name_de            varchar,
                         tags               hstore,
                         subclass           varchar,
                         mapping_key        varchar,
                         station            varchar,
                         funicular          varchar,
                         information        varchar,
                         uic_ref            varchar,
                         religion           varchar,
                         level              integer,
                         indoor             boolean,
                         layer              integer,
                         sport              varchar,
                         geometry           geometry
        )",
        &[],
    )
    .unwrap();
    conn.execute("TRUNCATE TABLE osm_poi_polygon", &[]).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS osm_aerodrome_label_point(
                         id                         serial primary key,
                         osm_id                     bigint,
                         name                       varchar,
                         name_en                    varchar,
                         name_de                    varchar,
                         tags                       hstore,
                         aerodrome_type             varchar,
                         aerodrome                  varchar,
                         military                   varchar,
                         iata                       varchar,
                         icao                       varchar,
                         ele                        varchar,
                         geometry                   geometry
                       )",
        &[],
    )
    .unwrap();
    conn.execute("TRUNCATE TABLE osm_aerodrome_label_point", &[])
        .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS osm_city_point(
                         id                         serial primary key,
                         osm_id                     bigint,
                         name                       varchar,
                         name_en                    varchar,
                         name_de                    varchar,
                         tags                       hstore,
                         place                      varchar,
                         population                 integer,
                         capital                    varchar,
                         geometry                   geometry
                       )",
        &[],
    )
    .unwrap();
    conn.execute("TRUNCATE TABLE osm_city_point", &[]).unwrap();
}

fn populate_tables(conn: &mut Client) {
    // this poi is located at lon=1, lat=1
    conn.execute("INSERT INTO osm_poi_point (osm_id, level, indoor, layer, sport, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) VALUES (5589618289, 14, TRUE, 0, 'sport', 'Ocean Studio',null,null, 'cafe', 'amenity',null,null,null,null, '0101000020E6100000000000000000F03F000000000000F03F'
    , '\"name\"=>\"Ocean Studio\", \"amenity\"=>\"cafe\", \"name:ru\"=>\"студия океана\", \"name:it\"=>\"Oceano Studioso\", \"name_int\"=>\"Ocean Studio\", \"name:latin\"=>\"Ocean Studio\"')", &[]).unwrap();
    // this poi is located at lon=2, lat=2
    conn.execute("INSERT INTO osm_poi_point (osm_id, level, indoor, layer, sport, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) VALUES (5590210422, 14, TRUE, 0, 'sport', 'Spagnolo',null,null, 'clothes', 'shop',null,null,null,null, '0101000020E610000000000000000000400000000000000040'
    , '\"name\"=>\"Spagnolo\", \"shop\"=>\"clothes\", \"name_int\"=>\"Spagnolo\", \"name:latin\"=>\"Spagnolo\",\"addr:housenumber\"=>\"12\",\"addr:street\"=>\"rue bob\"')", &[]).unwrap();
    // this poi is located at lon=3, lat=3
    conn.execute("INSERT INTO osm_poi_point (osm_id, level, indoor, layer, sport, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) VALUES (5590601521, 14, TRUE, 0, 'sport', '4 gusto',null,null, 'cafe', 'amenity',null,null,null,null, '0101000020E610000000000000000008400000000000000840'
    , '\"name\"=>\"4 gusto\", \"amenity\"=>\"cafe\", \"name_int\"=>\"4 gusto\", \"name:latin\"=>\"4 gusto\",\"addr:street\"=>\"rue spontini\"')", &[]).unwrap();
    // this poi is located at lon=4, lat=4
    conn.execute("INSERT INTO osm_poi_point (osm_id, level, indoor, layer, sport, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) VALUES (-42, 14, TRUE, 0, 'sport', 'Le nomade',null,null, 'bar', 'amenity',null,null,null,null, '0101000020E610000000000000000010400000000000001040'
    , '\"name\"=>\"Le nomade\", \"amenity\"=>\"bar\", \"name:es\"=>\"Le nomade\", \"name_int\"=>\"Le nomade\", \"name:latin\"=>\"Le nomade\",\"addr:housenumber\"=>\"7\",\"addr:street\"=>\"rue spontini\",\"addr:postcode\"=>\"75016\"')", &[]).unwrap();
    // this poi is located at lon=5, lat=5
    conn.execute("INSERT INTO osm_aerodrome_label_point (id, osm_id, name, name_en, name_de, aerodrome_type, aerodrome, military, iata, icao, ele, geometry, tags) VALUES (5934, 4505823836, 'Isla Cristina Agricultural Airstrip', null, null, null, null, null, null,  null, null, '0101000020E610000000000000000014400000000000001440'
    , '\"name\"=>\"Isla Cristina Agricultural Airstrip\", \"aeroway\"=>\"aerodrome\", \"name_int\"=>\"Isla Cristina Agricultural Airstrip\", \"name:latin\"=>\"Isla Cristina Agricultural Airstrip\"')", &[]).unwrap();

    // we also add a poi located at lon=-1, lat=-1, it won't be in an admin, so it must not be imported
    conn.execute("INSERT INTO osm_poi_point (osm_id, level, indoor, layer, sport, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) VALUES (12321, 14, TRUE, 0, 'sport', 'poi too far',null,null, 'bar', 'amenity',null,null,null,null, '0101000020E6100000000000000000F0BF000000000000F0BF'
    , '\"name\"=>\"poi too far\"')", &[]).unwrap();

    // aerodrom at the South Pole at lon=0, lat=-90 (Invalid coordinates in EPSG:4326)
    conn.execute("INSERT INTO osm_aerodrome_label_point (id, osm_id, name, name_en, name_de, aerodrome_type, aerodrome, military, iata, icao, ele, geometry, tags) VALUES (30334, 1042050310, 'South Pole Station Airport',null, null, null, null, null, null,  null, null, '0101000020110F0000714501E743E172BF010000000000F87F',
     '\"name\"=>\"South Pole Station Airport\", \"aeroway\"=>\"aerodrome\", \"name_int\"=>\"South Pole Station Airport\", \"name:latin\"=>\"South Pole Station Airport\"')", &[]).unwrap();

    // Some lost hamlet
    conn.execute("INSERT INTO osm_city_point (id, osm_id, name, name_en, name_de, place, population, capital, geometry, tags) VALUES (30336, 1042050311, 'I am a lost sheep',null, null, 'hamlet', 3, 'somewhere', '0101000020E610000000000000000014400000000000001440',
     '\"name\"=>\"I am a lost sheep\",\"population\"=>\"3\",\"capital\"=>\"somewhere\"')", &[]).unwrap();
    // Other city_point (not imported)
    conn.execute("INSERT INTO osm_city_point (id, osm_id, name, name_en, name_de, place, population, capital, geometry, tags) VALUES (303362, 1042050311, 'I am a lost sheep',null, null, 'other', 3, 'somewhere', '0101000020E610000000000000000014400000000000001440',
     '\"name\"=>\"I am a lost sheep\",\"population\"=>\"3\",\"capital\"=>\"somewhere\"')", &[]).unwrap();

    // Insert the "Eiffel Tower" POI
    conn.execute("INSERT INTO osm_poi_polygon (id, level, indoor, layer, sport, osm_id, name, name_en, name_de, tags, subclass, mapping_key, station, funicular, information, uic_ref, religion, geometry) VALUES (1175, 14, TRUE, 0, 'sport', -5013364, 'Tour Eiffel', 'Eiffel Tower', 'Eiffelturm', '\"fee\"=>\"10-25€\", \"3dmr\"=>\"4\", \"name\"=>\"Tour Eiffel\", \"layer\"=>\"2\", \"height\"=>\"324\", \"name:af\"=>\"Eiffel-toring\", \"name:ar\"=>\"برج إيفل\", \"name:ba\"=>\"Эйфель башняһы\", \"name:be\"=>\"Вежа Эйфеля\", \"name:cs\"=>\"Eiffelova věž\", \"name:da\"=>\"Eiffeltårnet\", \"name:de\"=>\"Eiffelturm\", \"name:el\"=>\"Πύργος του Άιφελ\", \"name:en\"=>\"Eiffel Tower\", \"name:eo\"=>\"Eiffel-Turo\", \"name:es\"=>\"Torre Eiffel\", \"name:et\"=>\"Eiffeli torn\", \"name:fa\"=>\"برج ایفل\", \"name:fi\"=>\"Eiffel-torni\", \"name:hr\"=>\"Eiffelov toranj\", \"name:hu\"=>\"Eiffel-torony\", \"name:ia\"=>\"Turre Eiffel\", \"name:id\"=>\"Menara Eiffel\", \"name:io\"=>\"Turmo Eiffel\", \"name:it\"=>\"Torre Eiffel\", \"name:ja\"=>\"エッフェル塔\", \"name:ku\"=>\"Barûya Eyfelê\", \"name:la\"=>\"Turris Eiffelia\", \"name:lb\"=>\"Eiffeltuerm\", \"name:nl\"=>\"Eiffeltoren\", \"name:pl\"=>\"Wieża Eiffla\", \"name:pt\"=>\"Torre Eiffel\", \"name:ru\"=>\"Эйфелева башня\", \"name:sk\"=>\"Eiffelova veža\", \"name:sr\"=>\"Ајфелова кула\", \"name:sv\"=>\"Eiffeltornet\", \"name:tr\"=>\"Eyfel Kulesi\", \"name:tt\"=>\"Эйфель манарасы\", \"name:uk\"=>\"Ейфелева вежа\", \"name:vi\"=>\"Tháp Eiffel\", \"name:me:vo\"=>\"Tüm di Eiffel\", \"name:zh\"=>\"埃菲尔铁塔\", \"ref:mhs\"=>\"PA00088801\", \"tourism\"=>\"attraction\", \"website\"=>\"http://toureiffel.paris\", \"building\"=>\"yes\", \"heritage\"=>\"3\", \"historic\"=>\"yes\", \"man_made\"=>\"tower\", \"name:ast\"=>\"Torrne Eiffel\", \"name_int\"=>\"Eiffel Tower\", \"operator\"=>\"Société d’Exploitation de la Tour Eiffel\", \"wikidata\"=>\"Q243\", \"addr:city\"=>\"Paris\", \"architect\"=>\"Stephen Sauvestre;Gustave Eiffel;Maurice Koechlin;Émile Nouguier\", \"wikipedia\"=>\"fr:Tour Eiffel\", \"importance\"=>\"international\", \"name:latin\"=>\"Tour Eiffel\", \"start_date\"=>\"C19\", \"tower:type\"=>\"communication;observation\", \"wheelchair\"=>\"yes\", \"addr:street\"=>\"Avenue Anatole France\", \"addr:postcode\"=>\"75007\", \"opening_hours\"=>\"09:30-23:45; Jun 21-Sep 02: 09:00-00:45; Jul 14,Jul 15 off\", \"building:shape\"=>\"pyramidal\", \"building:colour\"=>\"#706550\", \"source:heritage\"=>\"data.gouv.fr, Ministère de la Culture - 2016\", \"addr:housenumber\"=>\"5\", \"building:material\"=>\"iron\", \"heritage:operator\"=>\"mhs\", \"tower:construction\"=>\"lattice\", \"building:min_height\"=>\"0\", \"communication:radio\"=>\"fm\", \"mhs:inscription_date\"=>\"1964-06-24\", \"communication:television\"=>\"dvb-t\"', 'attraction', 'tourism',null,null,null,null,null, '0101000020E610000000000000000000400000000000000040')", &[]).unwrap();

    // Insert the "Hôtel Auteuil Tour Eiffel" POI
    conn.execute("INSERT INTO osm_poi_polygon (id, level, indoor, layer, sport, osm_id, name, name_en, name_de, tags, subclass, mapping_key, station, funicular, information, uic_ref, religion, geometry) VALUES (10980, 14, TRUE, 0, 'sport', -84194390, 'Hôtel Auteuil Tour Eiffel', null, null, '\"name\"=>\"Hôtel Auteuil Tour Eiffel\", \"source\"=>\"cadastre-dgi-fr source : Direction Générale des Impôts - Cadastre. Mise à jour : 2010\", \"tourism\"=>\"hotel\", \"building\"=>\"yes\", \"name_int\"=>\"Hôtel Auteuil Tour Eiffel\", \"name:latin\"=>\"Hôtel Auteuil Tour Eiffel\", \"addr:street\"=>\"Rue Félicien David\", \"addr:postcode\"=>\"75016\", \"addr:housenumber\"=>\"10\"','hotel', 'tourism', null, null, null, null, null, '0101000020E610000000000000000000400000000000000040')", &[]).unwrap();

    // A church with "religion" defined
    conn.execute("INSERT INTO osm_poi_polygon (osm_id, name, subclass, mapping_key, religion, geometry) VALUES
        (-63638108, 'Église Saint-Ambroise', 'place_of_worship', 'amenity', 'christian', '0101000020E610000000000000000014400000000000001440')", &[]).unwrap();

    // Not searchable bus station
    conn.execute("INSERT INTO osm_poi_point (osm_id, name, subclass, mapping_key, geometry) VALUES
        (901, 'Victor Hugo - Poincaré', 'bus_stop', 'highway', ST_GeomFromText('POINT(5.901 5.901)', 4326))", &[]).unwrap();

    // Not searchable poi (with no name)
    conn.execute(
        "INSERT INTO osm_poi_point (osm_id, name, subclass, mapping_key, geometry) VALUES
        (902, NULL, 'place_of_worship', 'amenity', ST_GeomFromText('POINT(5.902 5.902)', 4326))",
        &[],
    )
    .unwrap();
}

/// This function uses the poi_class function from
/// https://github.com/openmaptiles/openmaptiles/blob/master/layers/poi/class.sql
fn load_poi_class_function(conn: &mut Client) {
    conn.execute("
            CREATE OR REPLACE FUNCTION poi_class(subclass TEXT, mapping_key TEXT)
            RETURNS TEXT AS $$
                SELECT CASE
                    WHEN subclass IN ('accessories','antiques','beauty','bed','boutique','camera','carpet','charity','chemist','chocolate','coffee','computer','confectionery','convenience','copyshop','cosmetics','garden_centre','doityourself','erotic','electronics','fabric','florist','frozen_food','furniture','video_games','video','general','gift','hardware','hearing_aids','hifi','ice_cream','interior_decoration','jewelry','kiosk','lamps','mall','massage','motorcycle','mobile_phone','newsagent','optician','outdoor','perfumery','perfume','pet','photo','second_hand','shoes','sports','stationery','tailor','tattoo','ticket','tobacco','toys','travel_agency','watches','weapons','wholesale') THEN 'shop'
                    WHEN subclass IN ('townhall','public_building','courthouse','community_centre') THEN 'town_hall'
                    WHEN subclass IN ('golf','golf_course','miniature_golf') THEN 'golf'
                    WHEN subclass IN ('fast_food','food_court') THEN 'fast_food'
                    WHEN subclass IN ('park','bbq') THEN 'park'
                    WHEN subclass IN ('bus_stop','bus_station') THEN 'bus'
                    WHEN (subclass='station' AND mapping_key = 'railway') OR subclass IN ('halt', 'tram_stop', 'subway') THEN 'railway'
                    WHEN (subclass='station' AND mapping_key = 'aerialway') THEN 'aerialway'
                    WHEN subclass IN ('subway_entrance','train_station_entrance') THEN 'entrance'
                    WHEN subclass IN ('camp_site','caravan_site') THEN 'campsite'
                    WHEN subclass IN ('laundry','dry_cleaning') THEN 'laundry'
                    WHEN subclass IN ('supermarket','deli','delicatessen','department_store','greengrocer','marketplace') THEN 'grocery'
                    WHEN subclass IN ('books','library') THEN 'library'
                    WHEN subclass IN ('university','college') THEN 'college'
                    WHEN subclass IN ('hotel','motel','bed_and_breakfast','guest_house','hostel','chalet','alpine_hut','camp_site') THEN 'lodging'
                    WHEN subclass IN ('chocolate','confectionery') THEN 'ice_cream'
                    WHEN subclass IN ('post_box','post_office') THEN 'post'
                    WHEN subclass IN ('cafe') THEN 'cafe'
                    WHEN subclass IN ('school','kindergarten') THEN 'school'
                    WHEN subclass IN ('alcohol','beverages','wine') THEN 'alcohol_shop'
                    WHEN subclass IN ('bar','nightclub') THEN 'bar'
                    WHEN subclass IN ('marina','dock') THEN 'harbor'
                    WHEN subclass IN ('car','car_repair','taxi') THEN 'car'
                    WHEN subclass IN ('hospital','nursing_home', 'clinic') THEN 'hospital'
                    WHEN subclass IN ('grave_yard','cemetery') THEN 'cemetery'
                    WHEN subclass IN ('attraction','viewpoint') THEN 'attraction'
                    WHEN subclass IN ('biergarten','pub') THEN 'beer'
                    WHEN subclass IN ('music','musical_instrument') THEN 'music'
                    WHEN subclass IN ('american_football','stadium','soccer','pitch') THEN 'stadium'
                    WHEN subclass IN ('art','artwork','gallery','arts_centre') THEN 'art_gallery'
                    WHEN subclass IN ('bag','clothes') THEN 'clothing_store'
                    WHEN subclass IN ('swimming_area','swimming') THEN 'swimming'
                    WHEN subclass IN ('castle','ruins') THEN 'castle'
                    ELSE subclass
                END;
            $$ LANGUAGE SQL IMMUTABLE;", &[]).unwrap();
}

fn load_labelgrid_function(conn: &mut Client) {
    conn.execute(
        "
create or replace function LabelGrid (
        g geometry,
        grid_size numeric
    )
    returns text
    language plpgsql immutable as
$func$
begin
    if grid_size <= 0 then
        return 'null';
    end if;
    if GeometryType(g) <> 'POINT' then
        g := (select (ST_DumpPoints(g)).geom limit 1);
    end if;
    return ST_AsText(ST_SnapToGrid(
        g,
        grid_size/2,  -- x origin
        grid_size/2,  -- y origin
        grid_size,    -- x size
        grid_size     -- y size
    ));
end;
$func$;",
        &[],
    )
    .unwrap();
}

fn load_all_pois_function(conn: &mut Client) {
    conn.execute(
        r#"
    CREATE OR REPLACE FUNCTION all_pois(zoom_level integer)
    RETURNS TABLE(osm_id bigint, global_id text, geometry geometry, name text, name_en text,
        name_de text, tags hstore, class text, subclass text, agg_stop integer, layer integer,
        level integer, indoor integer, mapping_key text)
    AS $$
        SELECT osm_id_hash AS osm_id, global_id,
            geometry, NULLIF(name, '') AS name,
            COALESCE(NULLIF(name_en, ''), name) AS name_en,
            COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
            tags,
            poi_class(subclass, mapping_key) AS class,
            CASE
                WHEN subclass = 'information'
                    THEN NULLIF(information, '')
                WHEN subclass = 'place_of_worship'
                    THEN NULLIF(religion, '')
                WHEN subclass = 'pitch'
                    THEN NULLIF(sport, '')
                ELSE subclass
            END AS subclass,
            agg_stop,
            NULLIF(layer, 0) AS layer,
            "level",
            CASE WHEN indoor=TRUE THEN 1 ELSE NULL END as indoor,
            mapping_key
        FROM (
            -- etldoc: osm_poi_point ->  layer_poi:z12
            -- etldoc: osm_poi_point ->  layer_poi:z13
            SELECT *,
                osm_hash_from_imposm(osm_id) AS osm_id_hash,
                global_id_from_imposm(osm_id) as global_id
            FROM osm_poi_point
                WHERE zoom_level BETWEEN 12 AND 13
                    AND ((subclass='station' AND mapping_key = 'railway')
                        OR subclass IN ('halt', 'ferry_terminal'))
            UNION ALL

            -- etldoc: osm_poi_point ->  layer_poi:z14_
            SELECT *,
                osm_hash_from_imposm(osm_id) AS osm_id_hash,
                global_id_from_imposm(osm_id) as global_id
            FROM osm_poi_point
                WHERE zoom_level >= 14
                    AND (name <> '' OR (subclass <> 'garden' AND subclass <> 'park'))

            UNION ALL
            -- etldoc: osm_poi_polygon ->  layer_poi:z12
            -- etldoc: osm_poi_polygon ->  layer_poi:z13
            SELECT *,
                NULL::INTEGER AS agg_stop,
                osm_hash_from_imposm(osm_id) AS osm_id_hash,
                global_id_from_imposm(osm_id) as global_id
            FROM osm_poi_polygon
                WHERE zoom_level BETWEEN 12 AND 13
                    AND ((subclass='station' AND mapping_key = 'railway')
                        OR subclass IN ('halt', 'ferry_terminal'))

            UNION ALL
            -- etldoc: osm_poi_polygon ->  layer_poi:z14_
            SELECT *,
                NULL::INTEGER AS agg_stop,
                osm_hash_from_imposm(osm_id) AS osm_id_hash,
                global_id_from_imposm(osm_id) as global_id
            FROM osm_poi_polygon
                WHERE zoom_level >= 14
                    AND (name <> '' OR (subclass <> 'garden' AND subclass <> 'park'))
            ) as poi_union
        ;
    $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
    "#,
        &[],
    )
    .unwrap();
}

fn load_poi_class_rank_function(conn: &mut Client) {
    conn.execute(
        "
CREATE OR REPLACE FUNCTION poi_class_rank(class TEXT)
RETURNS INT AS $$
    SELECT CASE class
        WHEN 'hospital' THEN 20
        WHEN 'railway' THEN 40
        WHEN 'bus' THEN 50
        WHEN 'attraction' THEN 70
        WHEN 'harbor' THEN 75
        WHEN 'college' THEN 80
        WHEN 'school' THEN 85
        WHEN 'stadium' THEN 90
        WHEN 'zoo' THEN 95
        WHEN 'town_hall' THEN 100
        WHEN 'campsite' THEN 110
        WHEN 'cemetery' THEN 115
        WHEN 'park' THEN 120
        WHEN 'library' THEN 130
        WHEN 'police' THEN 135
        WHEN 'post' THEN 140
        WHEN 'golf' THEN 150
        WHEN 'shop' THEN 400
        WHEN 'grocery' THEN 500
        WHEN 'fast_food' THEN 600
        WHEN 'clothing_store' THEN 700
        WHEN 'bar' THEN 800
        ELSE 1000
    END;
$$ LANGUAGE SQL IMMUTABLE;
    ",
        &[],
    )
    .unwrap();
}

fn load_osm_hash_from_imposm_function(conn: &mut Client) {
    conn.execute(
        "
CREATE OR REPLACE FUNCTION osm_hash_from_imposm(imposm_id bigint)
RETURNS bigint AS $$
    SELECT CASE
        WHEN imposm_id < -1e17 THEN (-imposm_id-1e17) * 10 + 4 -- Relation
        WHEN imposm_id < 0 THEN  (-imposm_id) * 10 + 1 -- Way
        ELSE imposm_id * 10 -- Node
    END::bigint;
$$ LANGUAGE SQL IMMUTABLE;
    ",
        &[],
    )
    .unwrap();
}

fn load_global_id_from_imposm_function(conn: &mut Client) {
    conn.execute(
        "
CREATE OR REPLACE FUNCTION global_id_from_imposm(imposm_id bigint)
RETURNS TEXT AS $$
    SELECT CONCAT(
        'osm:',
        CASE WHEN imposm_id < -1e17 THEN CONCAT('relation:', -imposm_id-1e17)
             WHEN imposm_id < 0 THEN CONCAT('way:', -imposm_id)
             ELSE CONCAT('node:', imposm_id)
        END
    );
$$ LANGUAGE SQL IMMUTABLE;
    ",
        &[],
    )
    .unwrap();
}

/// This is a quick placeholder for the actual weight function.
fn load_poi_display_weight_function(conn: &mut Client) {
    conn.execute(
        "
        CREATE OR REPLACE FUNCTION poi_display_weight(
            name varchar,
            subclass varchar,
            mapping_key varchar,
            tags hstore
        )
        RETURNS REAL AS $$
            DECLARE
                result REAL;
            BEGIN
                SELECT INTO result
                    1.0 - 1.0 / (1.0 + LENGTH(name)::real);
                RETURN result;
            END
        $$ LANGUAGE plpgsql IMMUTABLE;
        ",
        &[],
    )
    .unwrap();
}

fn load_es_data(es_wrapper: &mut ElasticSearchWrapper, country_code: &str) {
    let city = make_test_admin("bob's town", country_code);
    let test_address = make_test_address(city.clone());
    let addresses = std::iter::once(test_address);
    es_wrapper.index(DATASET, addresses);
    let cities = std::iter::once(city);

    es_wrapper.index(DATASET, cities);
}

fn make_test_admin(name: &str, country_code: &str) -> mimir::Admin {
    let p = |x, y| geo::Coordinate { x: x, y: y };

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
    mimir::Admin {
        id: name.to_string(),
        level: 8,
        name: name.to_string(),
        label: name.to_string(),
        zip_codes: vec!["421337".to_string()],
        weight: 0f64,
        coord: ::mimir::Coord::new(4.0, 4.0),
        boundary: Some(boundary),
        insee: "outlook".to_string(),
        zone_type: Some(cosmogony::ZoneType::City),
        labels: mimir::I18nProperties::default(),
        names: mimir::I18nProperties::default(),
        codes: vec![mimir::Code {
            name: "ISO3166-1:alpha2".to_string(),
            value: country_code.to_string(),
        }],
        ..Default::default()
    }
}

fn make_test_address(city: mimir::Admin) -> mimir::Addr {
    let country_codes = utils::find_country_codes(std::iter::once(&city));

    let street = mimir::Street {
        id: "1234".to_string(),
        name: "test".to_string(),
        label: "test (bob's town)".to_string(),
        administrative_regions: vec![Arc::new(city)],
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
        coord: mimir::Coord::new(1., 1.),
        country_codes: country_codes.clone(),
        ..Default::default()
    };
    mimir::Addr {
        id: format!("addr:{};{}", 1., 1.),
        house_number: "1234".to_string(),
        name: "1234 test".to_string(),
        street: street,
        label: "1234 test (bob's town)".to_string(),
        coord: mimir::Coord::new(1., 1.),
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
        distance: None,
        approx_coord: None,
        country_codes,
        context: None,
    }
}

fn get_label(address: &mimir::Address) -> &str {
    match address {
        &mimir::Address::Street(ref s) => &s.label,
        &mimir::Address::Addr(ref a) => &a.label,
    }
}

fn get_name(address: &mimir::Address) -> &str {
    match address {
        &mimir::Address::Street(ref s) => &s.name,
        &mimir::Address::Addr(ref a) => &a.name,
    }
}

fn get_house_number(address: &mimir::Address) -> &str {
    match address {
        &mimir::Address::Street(_) => &"",
        &mimir::Address::Addr(ref a) => &a.house_number,
    }
}

fn get_coord(address: &mimir::Address) -> &mimir::Coord {
    match address {
        &mimir::Address::Street(ref s) => &s.coord,
        &mimir::Address::Addr(ref a) => &a.coord,
    }
}

fn get_zip_codes(address: &mimir::Address) -> Vec<String> {
    match address {
        &mimir::Address::Street(ref s) => s.zip_codes.clone(),
        &mimir::Address::Addr(ref a) => a.zip_codes.clone(),
    }
}

pub fn main_test(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
        ],
        &es_wrapper,
    );

    let rows = &pg_wrapper.get_rows(&"osm_poi_point");
    assert_eq!(rows.len(), 7);
    let rows = &pg_wrapper.get_rows(&"osm_poi_polygon");
    assert_eq!(rows.len(), 3);

    assert_eq!(
        es_wrapper
            .search_and_filter("name:*", |p| p.is_poi())
            .collect::<Vec<_>>()
            .len(),
        9 // 5 valid points + 2 valid polygons + 1 airport + 1 hamlet
    );

    // Test that the place "Ocean Studio" has been imported in the elastic wrapper
    let pois: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Ocean*", |_| true)
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
    assert_eq!(poi_type_ocean_poi, &"class_cafe subclass_cafe");

    // Test Properties: the amenity property for this POI should be "cafe"
    let properties_ocean_poi = &ocean_poi.properties;
    let amenity_tag = properties_ocean_poi
        .into_iter()
        .find(|&p| p.key == "amenity")
        .unwrap();
    assert_eq!(amenity_tag.value, "cafe".to_string());

    // Test Address: we get the address from elasticsearch associated to a POI and we check that
    // its associated information are correct.
    // To guarantee the rubber found an address we have put a fake address close to the location of
    // the POI in the init() method.
    let address_ocean_poi = ocean_poi.address.as_ref().unwrap();
    let address_label = get_label(&address_ocean_poi);
    assert_eq!(address_label, &"1234 test (bob's town)".to_string());
    let address_house_number = get_house_number(&address_ocean_poi);
    assert_eq!(address_house_number, "1234".to_string());
    let address_coord = get_coord(&address_ocean_poi);
    assert_eq!(address_coord.lat(), 1.);
    assert_eq!(address_coord.lon(), 1.);
    let zip_code = get_zip_codes(&address_ocean_poi);
    assert_eq!(zip_code, vec!["12345".to_string()]);

    let le_nomade_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Le nomade", |_| true)
        .collect();
    assert_eq!(&le_nomade_query.len(), &1);
    let le_nomade = &le_nomade_query[0];
    assert!(&le_nomade.is_poi());
    let le_nomade = &le_nomade.poi().unwrap();
    assert_eq!(&le_nomade.id, "osm:way:42"); // the id in the database is '-42', so it's a way
                                             // this poi has addresses osm tags, we should have read it
    let le_nomade_addr = le_nomade.address.as_ref().unwrap();
    assert_eq!(get_label(le_nomade_addr), "7 rue spontini (bob's town)");
    assert_eq!(get_name(le_nomade_addr), "7 rue spontini");
    assert_eq!(get_house_number(le_nomade_addr), &"7".to_string());
    assert_eq!(get_zip_codes(le_nomade_addr), vec!["75016".to_string()]);

    // Test that the airport 'Isla Cristina Agricultural Airstrip' has been imported in the elastic wrapper
    let airport_cristina: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Isla Cristina", |_| true)
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
        .into_iter()
        .find(|&p| p.key == "poi_class")
        .unwrap();
    assert_eq!(poi_class.value, "aerodrome".to_string());
    let poi_subclass = properties_airport
        .into_iter()
        .find(|&p| p.key == "poi_subclass")
        .unwrap();
    assert_eq!(poi_subclass.value, "airport".to_string());

    // the '4 gusto' has a tag addr:street but no housenumber, we therefore get an address without
    // a housenumber.
    let gusto_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:4 gusto", |_| true)
        .collect();
    assert_eq!(&gusto_query.len(), &1);
    let gusto = &gusto_query[0];
    assert!(&gusto.is_poi());
    let gusto = &gusto.poi().unwrap();
    assert_eq!(&gusto.id, "osm:node:5590601521");
    let bob = gusto.address.as_ref().unwrap();
    assert_eq!(get_label(bob), "rue spontini (bob's town)");
    assert_eq!(get_house_number(bob), "");

    // the Spagnolo has some osm address tags and no addr:postcode
    // we should still read it's address from osm
    let spagnolo_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:spagnolo", |_| true)
        .collect();
    assert_eq!(&spagnolo_query.len(), &1);
    let spagnolo = &spagnolo_query[0];
    assert!(&spagnolo.is_poi());
    let spagnolo = &spagnolo.poi().unwrap();
    assert_eq!(&spagnolo.id, "osm:node:5590210422");
    let spagnolo_addr = spagnolo.address.as_ref().unwrap();
    assert_eq!(get_label(spagnolo_addr), "12 rue bob (bob's town)");
    assert_eq!(get_house_number(spagnolo_addr), &"12".to_string());
    assert!(get_zip_codes(spagnolo_addr).is_empty());

    // Test that two "Tour Eiffel" POI should have been imported: the hotel + the monument
    let eiffels: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:(Tour Eiffel)", |_| true)
        .collect();
    assert_eq!(&eiffels.len(), &2);

    // Test they are both POI
    assert!(&eiffels.iter().all(|ref p| p.is_poi()));

    // Test their weight are not both equal to 0.0
    assert!(!&eiffels
        .iter()
        .map(|ref mut p| p.poi().unwrap())
        .all(|p| p.weight == 0.0f64));

    let hamlet_somewhere: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:(I am a lost sheep)", |_| true)
        .collect();
    assert_eq!(&hamlet_somewhere.len(), &1);
    assert!(&hamlet_somewhere[0].is_poi());
    let hamlet_somewhere = &hamlet_somewhere[0].poi().unwrap();
    let properties_hamlet_somewhere = &hamlet_somewhere.properties;
    let poi_class = properties_hamlet_somewhere
        .into_iter()
        .find(|&p| p.key == "poi_class")
        .unwrap();
    assert_eq!(poi_class.value, "locality".to_string());
    let poi_subclass = properties_hamlet_somewhere
        .into_iter()
        .find(|&p| p.key == "poi_subclass")
        .unwrap();
    assert_eq!(poi_subclass.value, "hamlet".to_string());

    // Test class/subclass for place_of_worship
    let church_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:saint-ambroise", |_| true)
        .collect();
    assert_eq!(&church_query.len(), &1);
    let church = &church_query[0].poi().unwrap();
    assert_eq!(church.name, "Église Saint-Ambroise");
    let church_class = church
        .properties
        .iter()
        .find(|&p| p.key == "poi_class")
        .unwrap();
    let church_subclass = church
        .properties
        .iter()
        .find(|&p| p.key == "poi_subclass")
        .unwrap();
    assert_eq!(church_class.value, "place_of_worship");
    assert_eq!(church_subclass.value, "christian");

    // 2 pois in nosearch index
    let nosearch_pois: Vec<mimir::Poi> = es_wrapper
        .rubber
        .get_all_objects_from_index("munin_poi_nosearch")
        .expect("failed to fetch poi_nosearch documents");
    assert_eq!(nosearch_pois.len(), 2);
}

pub fn bbox_test(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
            format!("--bounding-box=0, 0, 3.5, 3.5"),
        ],
        &es_wrapper,
    );

    // We filtered the import by a bounding box, we still have 6 rows in PG
    let rows = &pg_wrapper.get_rows(&"osm_poi_point");
    assert_eq!(rows.len(), 7);
    // but there is only 3 elements in the ES now, 'Le nomade' and 'Isla Cristina Agricultural Airstrip'
    // have been filtered
    assert_eq!(
        es_wrapper
            .search_and_filter("name:*", |p| p.is_poi())
            .collect::<Vec<_>>()
            .len(),
        5
    );
}

pub fn test_with_langs(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            "--lang=ru".into(),
            "--lang=it".into(),
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
        ],
        &es_wrapper,
    );

    // Test that the place "Ocean Studio" has been imported in the elastic wrapper
    // with the fields "labels" and "names"
    let pois: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Ocean*", |_| true)
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

pub fn test_address_format(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    // Import data with DE as country code in admins
    init_tests(&mut es_wrapper, &pg_wrapper, "DE");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
        ],
        &es_wrapper,
    );

    let spagnolo_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:spagnolo", |_| true)
        .collect();
    let spagnolo = &spagnolo_query[0];
    assert!(&spagnolo.is_poi());
    let spagnolo = &spagnolo.poi().unwrap();
    assert_eq!(&spagnolo.id, "osm:node:5590210422");
    let spagnolo_addr = spagnolo.address.as_ref().unwrap();

    // German format: housenumber comes after street name
    assert_eq!(get_label(spagnolo_addr), "rue bob 12 (bob's town)");
    assert_eq!(get_house_number(spagnolo_addr), &"12".to_string());
}

pub fn test_current_country_label(
    mut es_wrapper: ElasticSearchWrapper,
    pg_wrapper: PostgresWrapper,
) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
            "--lang=it".into(),
        ],
        &es_wrapper,
    );

    let eiffels: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:(Tour Eiffel)", |_| true)
        .collect();

    assert!(!eiffels.iter().map(|ref mut p| p.poi().unwrap()).any(|p| p
        .labels
        .0
        .iter()
        .any(|l| l.key == "fr")));

    // Now check that we have the fr label too!
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
            "--lang=fr".into(),
        ],
        &es_wrapper,
    );
    let eiffels: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:(Tour Eiffel)", |_| true)
        .collect();
    assert!(eiffels.iter().map(|ref mut p| p.poi().unwrap()).any(|p| p
        .labels
        .0
        .iter()
        .any(|l| l.key == "fr" && l.value == "Tour Eiffel (bob's town)")));
}
