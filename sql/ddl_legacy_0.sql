create
or replace TABLE CITI_BIKE.CITI_BIKE.CITI_BIKE_LEGACY_0 (
    "Trip Duration" NUMBER(38, 0),
    "Start Time" TIMESTAMP_NTZ(9),
    "Stop Time" TIMESTAMP_NTZ(9),
    "Start Station ID" VARCHAR(50),
    "Start Station Name" VARCHAR(250),
    "Start Station Latitude" VARCHAR(250),
    "Start Station Longitude" VARCHAR(250),
    "End Station ID" VARCHAR(50),
    "End Station Name" VARCHAR(250),
    "End Station Latitude" VARCHAR(250),
    "End Station Longitude" VARCHAR(250),
    "Bike ID" VARCHAR(50),
    "User Type" VARCHAR(50),
    "Birth Year" NUMBER(38, 0),
    "Gender" VARCHAR(50)
);