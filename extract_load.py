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
    schemas = [{'schema': 'legacy_0',
                'data': pd.DataFrame(
                    columns=['Trip Duration', 'Start Time', 'Stop Time', 'Start Station ID', 'Start Station Name',
                             'Start Station Latitude', 'Start Station Longitude', 'End Station ID',
                             'End Station Name',
                             'End Station Latitude', 'End Station Longitude', 'Bike ID', 'User Type', 'Birth Year',
                             'Gender'])},
               {'schema': 'legacy_1',
                'data': pd.DataFrame(
                    columns=['tripduration', 'starttime', 'stoptime', 'start station id', 'start station name',
                             'start station latitude', 'start station longitude', 'end station id',
                             'end station name',
                             'end station latitude', 'end station longitude', 'bikeid', 'usertype', 'birth year',
                             'gender'])},
               {'schema': 'current',
                'data': pd.DataFrame(
                    columns=['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name',
                             'start_station_id',
                             'end_station_name', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng',
                             'member_casual'])}]

    def get_schema(df):
        for schema in schemas:
            if list(df.columns) == list(schema['data'].columns):
                return schema
        return None

    for file in os.listdir('data'):
        source_df = pd.read_csv(os.path.join('data', file))
        source_schema = get_schema(source_df)
        if source_schema:
            # Concatenate source data
            source_schema['data'] = pd.concat([source_df, source_schema['data']], ignore_index=True)
        else:
            # Log warning if source data does not meet an expected data format
            # TODO: configure logger
            pass
            # logger.warning(f"Unexpected schema in {os.path.join('data', file)}")
    return schemas


def load(schemas):
    with open('resources/snowflake_credentials.yml', 'r') as file:
        credentials = yaml.safe_load(file)
    with snow.connect(user=credentials['user'],
                      password=credentials['password'],
                      account=credentials['account'],
                      warehouse=credentials['warehouse'],
                      database=credentials['database'],
                      schema=credentials['schema']) as conn:
        for schema in schemas:
            with open(f"sql/ddl_{schema['schema']}.sql", 'r') as f:
                sql = f.read()
            # Create table in database
            cur = conn.cursor()
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {credentials['database']}.{credentials['schema']};")
            cur.execute(sql)
            cur.close()
            # Load data
            write_pandas(conn, schema['data'], table_name=f"CITI_BIKE_{schema['schema'].upper()}")


def main():
    download()
    schemas = concat()
    load(schemas)


if __name__ == "__main__":
    main()
