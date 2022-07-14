import requests
import xmltodict
import zipfile
import io
import time
import os
import yaml
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas


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
                'data': pd.DataFrame(
                    columns=['Trip Duration', 'Start Time', 'Stop Time', 'Start Station ID', 'Start Station Name',
                             'Start Station Latitude', 'Start Station Longitude', 'End Station ID',
                             'End Station Name',
                             'End Station Latitude', 'End Station Longitude', 'Bike ID', 'User Type', 'Birth Year',
                             'Gender'])},
               {'id': 1,
                'data': pd.DataFrame(
                    columns=['tripduration', 'starttime', 'stoptime', 'start station id', 'start station name',
                             'start station latitude', 'start station longitude', 'end station id',
                             'end station name',
                             'end station latitude', 'end station longitude', 'bikeid', 'usertype', 'birth year',
                             'gender'])},
               {'id': 2,
                'data': pd.DataFrame(
                    columns=['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name',
                             'start_station_id',
                             'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng',
                             'member_casual'])}]

    def get_format(df):
        for fmt in formats:
            if list(df.columns) == list(fmt['data'].columns):
                return fmt
        return None

    for file in os.listdir('data'):
        source_df = pd.read_csv(os.path.join('data', file))
        source_format = get_format(source_df)
        if source_format:
            # Concatenate source data
            source_format['data'] = pd.concat([source_df, source_format['data']], ignore_index=True)
        else:
            # Log warning if source data does not meet an expected data format
            # TODO: configure logger
            pass
            # logger.warning(f"Unexpected format in {os.path.join('data', file)}")
    return formats


def load(formats):
    with open('resources/snowflake_credentials.yml', 'r') as file:
        credentials = yaml.safe_load(file)
    with snow.connect(user=credentials['user'],
                      password=credentials['password'],
                      account=credentials['account'],
                      warehouse=credentials['warehouse'],
                      database='CITI_BIKE',
                      schema='STG_CITI_BIKE') as conn:
        # Load Citi Bike data
        for fmt in formats:
            with open(f"sql/ddl_raw_fmt_{str(fmt['id'])}.sql", 'r') as f:
                sql = f.read()
            # Create table in database
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            # Load data
            write_pandas(conn, fmt['data'], table_name=f"RAW_CITI_BIKE_FMT_{str(fmt['id']).upper()}")

        # Create sequence to facilitate creation of surrogate key in fact table
        cur = conn.cursor()
        cur.execute("CREATE SEQUENCE IF NOT EXISTS seq START = 1000")
        cur.close()


def main():
    # download()
    load(concat())


if __name__ == "__main__":
    main()
