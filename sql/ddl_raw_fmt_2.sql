create
or replace TABLE CITI_BIKE_RAW_FMT_2 (
    "ride_id" VARCHAR(50),
    "rideable_type" VARCHAR(50),
    "started_at" TIMESTAMP_NTZ(9),
    "ended_at" TIMESTAMP_NTZ(9),
    "start_station_name" VARCHAR(250),
    "start_station_id" VARCHAR(50),
    "end_station_name" VARCHAR(250),
    "end_station_id" VARCHAR(50),
    "start_lat" VARCHAR(250),
    "start_lng" VARCHAR(250),
    "end_lat" VARCHAR(250),
    "end_lng" VARCHAR(250),
    "member_casual" VARCHAR(50)
);