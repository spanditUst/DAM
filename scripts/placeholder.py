import duckdb
import pandas as pd
from datetime import datetime
import dam_common_utils as dcu


def read_cos_rockdb():
    duckdb.sql("INSTALL httpfs;")
    duckdb.sql("LOAD httpfs;")
    duckdb.sql("SET s3_endpoint='s3.private.ap.cloud-object-storage.appdomain.cloud';")
    # duckdb.sql("SET s3_access_key_id = '5f521e204d874072a1561d2a20ec6537';")
    # duckdb.sql("SET s3_secret_access_key = '74bb0dc1c018d0a972e6eae556cc337305a612ff0f6e08b7';")

    duckdb.sql("SET s3_access_key_id = 'd09d9d41f56b47b28a430269ca1f4404';")
    duckdb.sql("SET s3_secret_access_key = 'b0c07d56cc48666e76c8eca34ac66aef46d1f9cd69d5fd81';")
    t1 = datetime.now()
    df = duckdb.sql("select * from read_parquet('s3://wabco-can-poc/em=202304/359207066492503_20230502_25088.parquet') where eDateTime > 0")
    print(df.columns)
    time_taken = datetime.now() - t1
    print("Time Taken: ", str(time_taken.total_seconds()))


def load_PC_statistics():
    df = pd.read_csv("can3bs6_statistical_data.csv")
    df['generated_as_on_time'] = datetime.now()
    df['created_time'] = datetime.now()
    df.to_sql(name='parameter_summary_statistics', con=dcu.mysql_connection_uptime(), if_exists='append', index=False)


def volvo_dtc_load():
    query = "select DISTINCT concat(dtccode, ':', ftb) as field_name from dtcmasterbs4;"

    df = dcu.execute_query(dcu.mysql_connection_protech(), query, return_type='return')
    df['field_source_container_column_name'] = df['field_name']
    df['historical_source_column_name'] = df['field_name']
    df['field_source_name'] = 'Cloudant'
    df['field_source_type_name'] = 'dtc'
    df['field_source_container_name'] = 'vfaults'
    df['is_field_value_bounded'] = 0
    df['historical_source_name'] = 'volvofaultitems'
    df['is_active'] = 1
    df['is_deleted'] = 0
    df['created_by'] = 'SYSADMIN'
    df['created_time'] = datetime.now()

    df.to_sql(name='field_tag', con=dcu.mysql_connection_uptime(), if_exists='append', index=False)

def wabco_dtc_load():
    query = "select distinct a.dtccode as field_name " \
            "from (select DISTINCT concat(dtccode, ':', ftb) as dtccode " \
            "from dtcmasterbs6 UNION select DISTINCT concat(dtc, '-', ftb) as dtccode " \
            "from dtcmasterobd2) a;"

    query = "select distinct a.dtccode as field_name " \
            "from (select DISTINCT concat(dtccode, '-', ftb) as dtccode " \
            "from dtcmasterbs6 UNION select DISTINCT concat(dtc, '-', ftb) as dtccode " \
            "from dtcmasterobd2) a;"

    df = dcu.execute_query(dcu.mysql_connection_protech(), query, return_type='return')
    df['field_source_container_column_name'] = df['field_name']
    df['historical_source_column_name'] = df['field_name']
    df['field_source_name'] = 'Cloudant'
    df['field_source_type_name'] = 'dtc'
    df['field_source_container_name'] = 'wfaults'
    df['is_field_value_bounded'] = 0
    df['historical_source_name'] = 'wabcofaultitems'
    df['is_active'] = 1
    df['is_deleted'] = 0
    df['created_by'] = 'SYSADMIN'
    df['created_time'] = datetime.now()

    df.to_sql(name='field_tag', con=dcu.mysql_connection_uptime(), if_exists='append', index=False)

def conv_orange_date():
    df = pd.read_csv("wcanbs46_mar_2023_sorted.csv")
    df.columns = df.columns.str.replace(" ", "")
    df.columns = df.columns.str.replace("'", "")
    df['orangeDateTime'] = pd.to_datetime(df['UTC'].astype('int64') + 946684800, unit='s')
    df['orangeDate'] = df['orangeDateTime'].dt.date
    df.to_csv("bs4_orange_data.csv", index=False)


def read_parquet():
    df = pd.read_parquet('C:/Users/224800/Downloads/20230419_426461.parquet', engine='pyarrow')
    print(df.columns)
    print(len(df.columns))


if __name__ == "__main__":
    # load_PC_statistics()
    # read_cos_rockdb()
    # conv_orange_date()
    # read_parquet()
    volvo_dtc_load()

