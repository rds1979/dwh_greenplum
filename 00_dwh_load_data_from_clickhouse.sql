-----------------------------------------------------------------------------------------------------------------------------
-- ClickHouse
-----------------------------------------------------------------------------------------------------------------------------

CREATE DATABASE tripdata ON CLUSTER default_cluster;

-- DROP TABLE tripdata.tripdata_local ON CLUSTER default_cluster SYNC;
CREATE TABLE IF NOT EXISTS tripdata.tripdata_local ON CLUSTER default_cluster
(
    `pickup_date` Date,
    `id` UInt64,
    `vendor_id` String,
    `pickup_datetime` DateTime CODEC(Delta, LZ4),
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Float32,
    `pickup_longitude` Float32,
    `pickup_latitude` Float32,
    `rate_code_id` String,
    `store_and_fwd_flag` String,
    `dropoff_longitude` Float32,
    `dropoff_latitude` Float32,
    `payment_type` LowCardinality(String),
    `fare_amount` Float32,
    `extra` String,
    `mta_tax` Float32,
    `tip_amount` Float32,
    `tolls_amount` Float32,
    `improvement_surcharge` Float32,
    `total_amount` Float32,
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `junk1` String,
    `junk2` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY (vendor_id, pickup_location_id, pickup_datetime);

-- DROP TABLE tripdata.tripdata_dist ON CLUSTER default_cluster SYNC;
CREATE TABLE tripdata.tripdata_dist ON CLUSTER default_cluster
	AS tripdata.tripdata_local
ENGINE = Distributed(
	default_cluster,
	tripdata,
	tripdata_local,
	cityHash64(pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, pickup_location_id));


SET max_memory_usage = 24000000000;

SELECT getSetting('max_memory_usage');

INSERT INTO tripdata.tripdata_dist
SELECT * FROM s3Cluster(
  'default_cluster',
  'https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-*.csv.gz',
  'CSVWithNames',
  'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime,
   passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String,
   store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String),
   fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32,
   total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String',
  'gzip')
settings max_threads=8, max_insert_threads=8, input_format_parallel_parsing=0;

OPTIMIZE TABLE tripdata.tripdata_local ON CLUSTER default_cluster FINAL;

SELECT shardNum(), count(1) FROM tripdata.tripdata_dist GROUP BY shardNum();

SELECT MIN(pickup_date), MAX(pickup_date) FROM tripdata.tripdata_dist;

-- DROP TABLE tripdata.tripdata_local_postgres_engine ON CLUSTER default_cluster SYNC;
CREATE TABLE tripdata.tripdata_local_postgres_engine ON CLUSTER default_cluster
(
    `pickup_date` Date DEFAULT toDate(pickup_datetime) CODEC(Delta, LZ4),
    `id` UInt64,
    `vendor_id` String,
    `pickup_datetime` DateTime CODEC(Delta, LZ4),
    `dropoff_datetime` DateTime,
    `passenger_count` UInt8,
    `trip_distance` Float32,
    `pickup_longitude` Float32,
    `pickup_latitude` Float32,
    `rate_code_id` String,
    `store_and_fwd_flag` String,
    `dropoff_longitude` Float32,
    `dropoff_latitude` Float32,
    `payment_type` String,
    `fare_amount` Float32,
    `extra` String,
    `mta_tax` Float32,
    `tip_amount` Float32,
    `tolls_amount` Float32,
    `improvement_surcharge` Float32,
    `total_amount` Float32,
    `pickup_location_id` UInt16,
    `dropoff_location_id` UInt16,
    `junk1` String,
    `junk2` String
)
ENGINE = PostgreSQL('$GREENPLUM_MASTER_HOST:5432', 'dwh', 'staging.tripdata_stg', 'etl_user', 'etl_password');

-- DROP TABLE tripdata.tripdata_dist_postgres_engine ON CLUSTER default_cluster SYNC;
CREATE TABLE tripdata.tripdata_dist_postgres_engine ON CLUSTER default_cluster
	AS tripdata.tripdata_local_postgres_engine
ENGINE = Distributed(default_cluster, tripdata, tripdata_local_postgres_engine, cityHash64(
pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, pickup_location_id));


SELECT MIN(pickup_date), MAX(pickup_date) FROM tripdata.tripdata_dist;

SELECT DISTINCT pickup_date FROM tripdata.tripdata_dist WHERE pickup_date BETWEEN '2009-01-01' AND '2009-12-31' ORDER BY pickup_date;

SELECT COUNT(*) FROM tripdata.tripdata_dist WHERE pickup_date BETWEEN '2009-01-01' AND '2009-12-31';

INSERT INTO tripdata.tripdata_dist_postgres_engine SELECT * FROM tripdata.tripdata_dist WHERE pickup_date BETWEEN '2009-01-01' AND '2009-12-31' LIMIT 1000;


-----------------------------------------------------------------------------------------------------------------------------
-- GREENPLUM
-----------------------------------------------------------------------------------------------------------------------------

CREATE ROLE dwh WITH NOLOGIN RESOURCE GROUP default_group;
COMMENT ON ROLE dwh IS 'DWH objects owner';

CREATE ROLE dwh_admin WITH LOGIN CREATEDB CREATEROLE IN ROLE dwh ENCRYPTED PASSWORD 'ij8Soogak' RESOURCE GROUP default_group;
COMMENT ON ROLE dwh_admin IS 'DWH database administrator';

CREATE ROLE dwh_ro WITH NOLOGIN RESOURCE GROUP default_group;
COMMENT ON ROLE dwh_ro IS 'DWH read-only users';

CREATE ROLE dwh_rw WITH NOLOGIN RESOURCE GROUP default_group;
COMMENT ON ROLE dwh_rw IS 'DWH read-write users';

CREATE ROLE etl_user WITH NOLOGIN RESOURCE GROUP default_group ENCRYPTED PASSWORD 'etl_password';
COMMENT ON ROLE etl_user IS 'ETL user';


CREATE DATABASE dwh OWNER dwh;

/*
psql -h rds-mdw01 -d dwh -U dwh_admin
*/

SET ROLE dwh;
SELECT current_user, session_user;

CREATE SCHEMA sandbox;
CREATE SCHEMA staging;

-- DROP TABLE IF EXISTS staging.tripdata_stg;
CREATE TABLE IF NOT EXISTS staging.tripdata_stg(
  pickup_date DATE,
  id BIGINT,
  vendor_id TEXT,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance FLOAT,
  pickup_longitude FLOAT,
  pickup_latitude FLOAT,
  rate_code_id TEXT,
  store_and_fwd_flag TEXT,
  dropoff_longitude FLOAT,
  dropoff_latitude FLOAT,
  payment_type TEXT,
  fare_amount FLOAT,
  extra TEXT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  pickup_location_id BIGINT,
  dropoff_location_id BIGINT,
  junk1 TEXT,
  junk2 TEXT
)WITH (APPENDOPTIMIZED = TRUE, orientation=COLUMN, compresstype=ZSTD, compresslevel=3)
DISTRIBUTED BY(id);

COMMENT ON TABLE staging.tripdata_stg IS 'Staging table for primary load trip data from ClickHouse';

CREATE ROLE etl_user WITH NOLOGIN RESOURCE GROUP default_group ENCRYPTED PASSWORD 'etl_password';
COMMENT ON ROLE etl_user IS 'ETL user';

GRANT CONNECT ON DATABASE dwh TO etl_user;
GRANT USAGE ON SCHEMA staging, sandbox TO etl_user;
GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO etl_user;
