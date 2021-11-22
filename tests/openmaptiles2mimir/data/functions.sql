--
-- osm_hash_from_imposm
--

CREATE OR REPLACE FUNCTION osm_hash_from_imposm(imposm_id bigint)
RETURNS bigint AS $$
    SELECT CASE
        WHEN imposm_id < -1e17 THEN (-imposm_id-1e17) * 10 + 4 -- Relation
        WHEN imposm_id < 0 THEN  (-imposm_id) * 10 + 1 -- Way
        ELSE imposm_id * 10 -- Node
    END::bigint;
$$ LANGUAGE SQL IMMUTABLE;

--
-- global_id_from_imposm
--

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

--
-- poi_class
--

-- This function uses the poi_class function from
-- https://github.com/openmaptiles/openmaptiles/blob/master/layers/poi/class.sql
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
$$ LANGUAGE SQL IMMUTABLE;

--
-- poi_class_rank
--

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

--
-- poi_display_weight
--

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

--
-- LabelGrid
--

CREATE OR REPLACE FUNCTION LabelGrid (
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
$func$;

--
-- all_pois
--

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
