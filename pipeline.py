import requests
import xmltodict
import zipfile
import io
import time
import os
import pandas as pd
from google.cloud import bigquery


# TODO: configure logger


def download():
    def extract(zip_file_url):
        r = requests.get(zip_file_url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        for info in z.infolist():
            if not info.filename.startswith('__MACOSX') and info.filename.endswith('.csv'):
                filepath = os.path.join('data', info.filename)
                last_modified = time.mktime(info.date_time + (0, 0, -1))
                # Download file if not already downloaded or updated version is available
                if not os.path.exists(filepath) or (
                        os.path.exists(filepath) and last_modified > os.path.getmtime(filepath)):
                    z.extract(info, 'data')
                    os.utime(filepath, (last_modified, last_modified))

    base_url = 'https://s3.amazonaws.com/tripdata'
    data = xmltodict.parse(requests.get(base_url).content)
    for item in data['ListBucketResult']['Contents']:
        if item['Key'].endswith('.zip') and item['Key'].startswith('JC'):
            s3_url = base_url + '/' if not item['Key'].startswith('/') else base_url
            extract(f"{s3_url}{item['Key'].replace(' ', '%20')}")


def concat():
    # Define data formats
    formats = [{'id': 0,
                'raw_cols': ['Trip Duration', 'Start Time', 'Stop Time', 'Start Station ID', 'Start Station Name',
                             'Start Station Latitude', 'Start Station Longitude', 'End Station ID',
                             'End Station Name',
                             'End Station Latitude', 'End Station Longitude', 'Bike ID', 'User Type', 'Birth Year',
                             'Gender'],
                'data': pd.DataFrame(
                    columns=['trip_duration', 'start_time', 'stop_time', 'start_station_id', 'start_station_name',
                             'start_station_latitude', 'start_station_longitude', 'end_station_id',
                             'end_station_name',
                             'end_station_latitude', 'end_station_longitude', 'bike_id', 'user_type', 'birth_year',
                             'gender']),
                'dtypes': {'trip_duration': 'Int64', 'start_time': 'datetime64', 'stop_time': 'datetime64',
                           'start_station_id': 'Int64', 'start_station_name': 'object',
                           'start_station_latitude': 'float64', 'start_station_longitude': 'float64',
                           'end_station_id': 'Int64', 'end_station_name': 'object', 'end_station_latitude': 'float64',
                           'end_station_longitude': 'float64', 'bike_id': 'Int64', 'user_type': 'object',
                           'birth_year': 'Int64', 'gender': 'int64'}
                },
               {'id': 1,
                'raw_cols': ['tripduration', 'starttime', 'stoptime', 'start station id', 'start station name',
                             'start station latitude', 'start station longitude', 'end station id',
                             'end station name',
                             'end station latitude', 'end station longitude', 'bikeid', 'usertype', 'birth year',
                             'gender'],
                'data': pd.DataFrame(
                    columns=['trip_duration', 'start_time', 'stop_time', 'start_station_id', 'start_station_name',
                             'start_station_latitude', 'start_station_longitude', 'end_station_id',
                             'end_station_name',
                             'end_station_latitude', 'end_station_longitude', 'bike_id', 'user_type', 'birth_year',
                             'gender']),
                'dtypes': {'trip_duration': 'Int64', 'start_time': 'datetime64', 'stop_time': 'datetime64',
                           'start_station_id': 'Int64', 'start_station_name': 'object',
                           'start_station_latitude': 'float64', 'start_station_longitude': 'float64',
                           'end_station_id': 'Int64', 'end_station_name': 'object', 'end_station_latitude': 'float64',
                           'end_station_longitude': 'float64', 'bike_id': 'Int64', 'user_type': 'object',
                           'birth_year': 'Int64', 'gender': 'int64'}
                },
               {'id': 2,
                'raw_cols': ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name',
                             'start_station_id',
                             'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng',
                             'member_casual'],
                'data': pd.DataFrame(
                    columns=['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name',
                             'start_station_id',
                             'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng',
                             'member_casual']),
                'dtypes': {'ride_id': 'object', 'rideable_type': 'object', 'started_at': 'datetime64',
                           'ended_at': 'datetime64', 'start_station_name': 'object',
                           'start_station_id': 'object', 'end_station_name': 'object',
                           'end_station_id': 'object', 'start_lat': 'float64', 'start_lng': 'float64',
                           'end_lat': 'float64', 'end_lng': 'float64', 'member_casual': 'object'}
                }]

    def get_format(df):
        for fmt in formats:
            if list(df.columns) == fmt['raw_cols']:
                return fmt
        return None

    for file in os.listdir('data'):
        source_df = pd.read_csv(os.path.join('data', file))
        source_format = get_format(source_df)
        if source_format:
            source_df.columns = list(source_format['data'].columns)
            # Concatenate source data
            source_format['data'] = pd.concat([source_df, source_format['data']], ignore_index=True)
        else:
            # Log warning if source data does not meet an expected data format
            # TODO: configure logger
            pass
            # logger.warning(f"Unexpected format in {os.path.join('data', file)}")
    return formats


def load(formats):
    with bigquery.Client():
        # Load Citi Bike data
        for fmt in formats:
            # Load data
            fmt['data'] = fmt['data'].astype(dtype=fmt['dtypes'])
            fmt['data'].to_gbq(destination_table=f"citi_bike.raw_citi_bike_fmt_{str(fmt['id'])}", if_exists='replace')


def main():
    download()
    load(concat())


if __name__ == "__main__":
    main()
