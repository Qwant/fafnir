CREATE TABLE IF NOT EXISTS osm_poi_point(
    id                         serial primary key,
    osm_id                     bigint,
    name                       varchar,
    name_en                    varchar,
    name_de                    varchar,
    tags                       hstore,
    subclass                   varchar,
    mapping_key                varchar,
    station                    varchar,
    funicular                  varchar,
    information                varchar,
    uic_ref                    varchar,
    religion                   varchar,
    level                      integer,
    indoor                     boolean,
    layer                      integer,
    sport                      varchar,
    geometry                   geometry,
    agg_stop                   integer
);

TRUNCATE TABLE osm_poi_point;

CREATE TABLE IF NOT EXISTS osm_poi_polygon (
    id                         serial primary key,
    osm_id                     bigint,
    name                       varchar,
    name_en                    varchar,
    name_de                    varchar,
    tags                       hstore,
    subclass                   varchar,
    mapping_key                varchar,
    station                    varchar,
    funicular                  varchar,
    information                varchar,
    uic_ref                    varchar,
    religion                   varchar,
    level                      integer,
    indoor                     boolean,
    layer                      integer,
    sport                      varchar,
    geometry                   geometry
);

TRUNCATE TABLE osm_poi_polygon;

CREATE TABLE IF NOT EXISTS osm_aerodrome_label_point(
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
);

TRUNCATE TABLE osm_aerodrome_label_point;

CREATE TABLE IF NOT EXISTS osm_city_point(
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
);

TRUNCATE TABLE osm_city_point;

CREATE TABLE IF NOT EXISTS osm_water_lakeline(
    osm_id                     bigint,
    geometry                   geometry,
    name                       varchar,
    name_en                    varchar,
    name_de                    varchar,
    tags                       hstore,
    area                       real,
    is_intermittent            boolean
);

TRUNCATE TABLE osm_water_lakeline;

CREATE TABLE IF NOT EXISTS osm_water_point(
    osm_id                     bigint,
    geometry                   geometry,
    name                       varchar,
    name_en                    varchar,
    name_de                    varchar,
    tags                       hstore,
    area                       real,
    is_intermittent            boolean
);

TRUNCATE TABLE osm_water_point;


CREATE TABLE IF NOT EXISTS osm_water_polygon(
    id                         integer,
    osm_id                     bigint,
    area                       real,
    name                       varchar,
    name_en                    varchar,
    name_de                    varchar,
    tags                       hstore,
    "natural"                  varchar,
    landuse                    varchar,
    waterway                   varchar,
    is_intermittent            boolean,
    is_tunnel                  boolean,
    is_bridge                  boolean,
    geometry                   geometry
);

TRUNCATE TABLE osm_water_polygon;

CREATE TABLE IF NOT EXISTS osm_marine_point(
    id                         integer,
    osm_id                     bigint,
    name                       varchar,
    name_en                    varchar,
    name_de                    varchar,
    tags                       hstore,
    place                      varchar,
    rank                       integer,
    is_intermittent            boolean,
    geometry                   geometry
);

TRUNCATE TABLE osm_marine_point;
