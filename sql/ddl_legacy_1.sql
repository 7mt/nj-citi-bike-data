create
or replace TABLE CITI_BIKE.CITI_BIKE.CITI_BIKE_LEGACY_1 (
    "tripduration" NUMBER(38, 0),
    "starttime" TIMESTAMP_NTZ(9),
    "stoptime" TIMESTAMP_NTZ(9),
    "start station id" VARCHAR(50),
    "start station name" VARCHAR(250),
    "start station latitude" VARCHAR(250),
    "start station longitude" VARCHAR(250),
    "end station id" VARCHAR(50),
    "end station name" VARCHAR(250),
    "end station latitude" VARCHAR(250),
    "end station longitude" VARCHAR(250),
    "bikeid" VARCHAR(50),
    "usertype" VARCHAR(50),
    "birth year" NUMBER(38, 0),
    "gender" VARCHAR(50)
);