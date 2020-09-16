--
-- osm_poi_point
--

INSERT INTO osm_poi_point (
    osm_id, level, indoor, layer, sport, name, subclass, mapping_key,
    geometry, tags
) VALUES
    -- POI located at lon=1, lat=1
    (
        5589618289,
        14,
        TRUE,
        0,
        'sport',
        'Ocean Studio',
        'cafe',
        'amenity',
        '0101000020E6100000000000000000F03F000000000000F03F',
        '
            "name" => "Ocean Studio",
            "amenity" => "cafe",
            "name:ru" => "студия океана",
            "name:it" => "Oceano Studioso",
            "name_int" => "Ocean Studio",
            "name:latin" => "Ocean Studio"
        '
    ),
    -- POI located at lon=2, lat=2
    (
        5590210422,
        14,
        TRUE,
        0,
        'sport',
        'Spagnolo',
        'clothes',
        'shop',
        '0101000020E610000000000000000000400000000000000040',
        '
            "name" => "Spagnolo",
            "shop" => "clothes",
            "name_int" => "Spagnolo",
            "name:latin" => "Spagnolo",
            "addr:housenumber" => "12",
            "addr:street" => "rue bob"
        '
    ),
    -- POI located at lon=3, lat=3
    (
        5590601521,
        14,
        TRUE,
        0,
        'sport',
        '4 gusto',
        'cafe',
        'amenity',
        '0101000020E610000000000000000008400000000000000840',
        '
            "name" => "4 gusto",
            "amenity" => "cafe",
            "name_int" => "4 gusto",
            "name:latin" => "4 gusto",
            "addr:street" => "rue spontini"
        '
    ),
    -- POI located at lon=4, lat=4
    (
        -42,
        14,
        TRUE,
        0,
        'sport',
        'Le nomade',
        'bar',
        'amenity',
        '0101000020E610000000000000000010400000000000001040',
        '
            "name" => "Le nomade",
            "amenity" => "bar",
            "name:es" => "Le nomade",
            "name_int" =>"Le nomade",
            "name:latin" => "Le nomade",
            "addr:housenumber" => "7",
            "addr:street" => "rue spontini",
            "addr:postcode" => "75016"
        '
    ),
    -- POI at lon=-1, lat=-1, it won't be in an admin, so it must not be imported
    (
        12321,
        14,
        TRUE,
        0,
        'sport',
        'poi too far',
        'bar',
        'amenity',
        '0101000020E6100000000000000000F0BF000000000000F0BF',
        '"name" => "poi too far"'
    ),
    -- Not searchable bus station
    (
        901,
        null,
        null,
        null,
        null,
        'Victor Hugo - Poincaré',
        'bus_stop',
        'highway',
        ST_GeomFromText('POINT(5.901 5.901)', 4326),
        ''
    ),
    -- Not searchable poi (with no name)
    (
        902,
        null,
        null,
        null,
        null,
        NULL,
        'place_of_worship',
        'amenity',
        ST_GeomFromText('POINT(5.902 5.902)', 4326),
        ''
    );

--
-- osm_poi_polygon
--

INSERT INTO osm_poi_polygon (
    id, level, indoor, layer, sport, osm_id, name, name_en, name_de, subclass,
    mapping_key, station, funicular, information, uic_ref, religion, geometry, tags
) VALUES
    -- The Eiffel Tour
    (
        1175,
        14,
        TRUE,
        0,
        'sport',
        -5013364,
        'Tour Eiffel',
        'Eiffel Tower',
        'Eiffelturm',
        'attraction',
        'tourism',
        null,
        null,
        null,
        null,
        null,
        '0101000020E610000000000000000000400000000000000040',
        '
            "fee" => "10-25€",
            "3dmr" => "4",
            "name" => "Tour Eiffel",
            "layer" => "2",
            "height" => "324",
            "name:af" => "Eiffel-toring",
            "name:ar" => "برج إيفل",
            "name:ba" => "Эйфель башняһы",
            "name:be" => "Вежа Эйфеля",
            "name:cs" => "Eiffelova věž",
            "name:da" => "Eiffeltårnet",
            "name:de" => "Eiffelturm",
            "name:el" => "Πύργος του Άιφελ",
            "name:en" => "Eiffel Tower",
            "name:eo" => "Eiffel-Turo",
            "name:es" => "Torre Eiffel",
            "name:et" => "Eiffeli torn",
            "name:fa" => "برج ایفل",
            "name:fi" => "Eiffel-torni",
            "name:hr" => "Eiffelov toranj",
            "name:hu" => "Eiffel-torony",
            "name:ia" => "Turre Eiffel",
            "name:id" => "Menara Eiffel",
            "name:io" => "Turmo Eiffel",
            "name:it" => "Torre Eiffel",
            "name:ja" => "エッフェル塔",
            "name:ku" => "Barûya Eyfelê",
            "name:la" => "Turris Eiffelia",
            "name:lb" => "Eiffeltuerm",
            "name:nl" => "Eiffeltoren",
            "name:pl" => "Wieża Eiffla",
            "name:pt" => "Torre Eiffel",
            "name:ru" => "Эйфелева башня",
            "name:sk" => "Eiffelova veža",
            "name:sr" => "Ајфелова кула",
            "name:sv" => "Eiffeltornet",
            "name:tr" => "Eyfel Kulesi",
            "name:tt" => "Эйфель манарасы",
            "name:uk" => "Ейфелева вежа",
            "name:vi" => "Tháp Eiffel",
            "name:me:vo" => "Tüm di Eiffel",
            "name:zh" => "埃菲尔铁塔",
            "ref:mhs" => "PA00088801",
            "tourism" => "attraction",
            "website" => "http://toureiffel.paris",
            "building" => "yes",
            "heritage" => "3",
            "historic" => "yes",
            "man_made" => "tower",
            "name:ast" => "Torrne Eiffel",
            "name_int" => "Eiffel Tower",
            "operator" => "Société d’Exploitation de la Tour Eiffel",
            "wikidata" => "Q243",
            "addr:city" => "Paris",
            "architect" => "Stephen Sauvestre;Gustave Eiffel;Maurice Koechlin;Émile Nouguier",
            "wikipedia" => "fr:Tour Eiffel",
            "importance" => "international",
            "name:latin" => "Tour Eiffel",
            "start_date" => "C19",
            "tower:type" => "communication;observation",
            "wheelchair" => "yes",
            "addr:street" => "Avenue Anatole France",
            "addr:postcode" => "75007",
            "opening_hours" => "09:30-23:45; Jun 21-Sep 02: 09:00-00:45; Jul 14,Jul 15 off",
            "building:shape" => "pyramidal",
            "building:colour" => "#706550",
            "source:heritage" => "data.gouv.fr, Ministère de la Culture - 2016",
            "addr:housenumber" => "5",
            "building:material" => "iron",
            "heritage:operator" => "mhs",
            "tower:construction" => "lattice",
            "building:min_height" => "0",
            "communication:radio" => "fm",
            "mhs:inscription_date" => "1964-06-24",
            "communication:television" => "dvb-t"
        '
    ),
    -- Hôtel Auteuil Tour Eiffel
    (
        10980,
        14,
        TRUE,
        0,
        'sport',
        -84194390,
        'Hôtel Auteuil Tour Eiffel',
        null,
        null,
        'hotel',
        'tourism',
        null,
        null,
        null,
        null,
        null,
        '0101000020E610000000000000000000400000000000000040',
        '
            "name" => "Hôtel Auteuil Tour Eiffel",
            "source" => "cadastre-dgi-fr source : Direction Générale des Impôts - Cadastre. Mise à jour : 2010",
            "tourism" => "hotel",
            "building" => "yes",
            "name_int" => "Hôtel Auteuil Tour Eiffel",
            "name:latin" => "Hôtel Auteuil Tour Eiffel",
            "addr:street" => "Rue Félicien David",
            "addr:postcode" => "75016",
            "addr:housenumber" => "10"
        '
    ),
    -- A church with "religion" defined
    (
        -63638108,
        null,
        null,
        null,
        null,
        null,
        'Église Saint-Ambroise',
        null,
        null,
        'place_of_worship',
        'amenity',
        null,
        null,
        null,
        null,
        'christian',
        '0101000020E610000000000000000014400000000000001440',
        ''
    );

--
-- osm_city_point
--

INSERT INTO osm_city_point (
    id, osm_id, name, name_en, name_de, place, population, capital, geometry, tags
) VALUES
    -- Some lost hamlet
    (
        30336,
        1042050311,
        'I am a lost sheep',
        null,
        null,
        'hamlet',
        3,
        'somewhere',
        '0101000020E610000000000000000014400000000000001440',
         '
            "name" => "I am a lost sheep",
            "population" => "3",
            "capital" => "somewhere"'
    ),
    -- Other city_point (not imported)
    (
        303362,
        1042050311,
        'I am a lost sheep',
        null,
        null,
        'other',
        3,
        'somewhere',
        '0101000020E610000000000000000014400000000000001440',
        '
            "name" => "I am a lost sheep",
            "population" => "3",
            "capital" => "somewhere"
        '
    );

--
-- osm_aerodrome_label_point
--

INSERT INTO osm_aerodrome_label_point (
    id, osm_id, name, name_en, name_de, aerodrome_type, aerodrome, military, iata,
    icao, ele, geometry, tags
) VALUES
    -- POI located at lon=5, lat=5
    (
        5934,
        4505823836,
        'Isla Cristina Agricultural Airstrip',
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        '0101000020E610000000000000000014400000000000001440',
        '
            "name" => "Isla Cristina Agricultural Airstrip",
            "aeroway" => "aerodrome",
            "name_int" => "Isla Cristina Agricultural Airstrip",
            "name:latin" => "Isla Cristina Agricultural Airstrip"
        '
    ),
    -- POI at lon=0, lat=-90 - South Pole (Invalid coordinates in EPSG:4326)
    (
        30334,
        1042050310,
        'South Pole Station Airport',
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        '0101000020110F0000714501E743E172BF010000000000F87F',
        '
             "name" => "South Pole Station Airport",
             "aeroway" => "aerodrome",
             "name_int" => "South Pole Station Airport",
             "name:latin" => "South Pole Station Airport"
        '
    );

--
-- osm_water_point
--

INSERT INTO osm_water_point (osm_id, name, area, geometry, tags)
VALUES
    (
        -438255678,
        'Fontaine-Lavoir Saint-Guimond',
        27.6204336789181,
        '0101000020110F000008304ADAEA3212C1B637DF4A3EA15741',
        '
            "name" => "Fontaine-Lavoir Saint-Guimond",
            "natural" => "water",
            "name_int" => "Fontaine-Lavoir Saint-Guimond",
            "name:latin" => "Fontaine-Lavoir Saint-Guimond"
        '
    );

--
-- osm_water_polygon
--

INSERT INTO osm_water_polygon (id, osm_id, area, name, name_en, "natural", geometry, tags)
VALUES
    (
        258,
        -100000000002824804,
        8.5789e8,
        'Baie du Mont Saint-Michel',
        'Mont Saint-Michel Bay',
        'bay',
        '0103000020110F00000100000004000000F27883B9B67009C18F0786D67CB5574195D4017BC25905C17CD2E1E06DC55741A6FF1B7A406202C11717A7032BB35741F27883B9B67009C18F0786D67CB55741',
        '
            "name" => "Baie du Mont Saint-Michel",
            "type" => "multipolygon",
            "name:br" => "Bae Menez-Mikael",
            "name:en" => "Mont Saint-Michel Bay",
            "name:fr" => "Baie du Mont Saint-Michel",
            "natural" => "bay"
        '
    );
