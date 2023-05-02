import duckdb
import pandas as pd
from datetime import datetime
import dam_common_utils as dcu


def read_cos_rockdb():
    duckdb.sql("INSTALL httpfs;")
    duckdb.sql("LOAD httpfs;")
    duckdb.sql("SET s3_endpoint='s3.jp-tok.cloud-object-storage.appdomain.cloud';")
    duckdb.sql("SET s3_access_key_id = '5f521e204d874072a1561d2a20ec6537';")
    duckdb.sql("SET s3_secret_access_key = '74bb0dc1c018d0a972e6eae556cc337305a612ff0f6e08b7';")

    duckdb.sql("select * from read_parquet('s3://wabco-can/edt=20230101/pkt=CANBS4/8_20230220_2156138.parquet') "
               "where deviceId = 352467111005398").show()


def load_PC_statistics():
    df = pd.read_csv("can3bs6_statistical_data.csv")
    df['generated_as_on_time'] = datetime.now()
    df['created_time'] = datetime.now()
    df.to_sql(name='parameter_summary_statistics', con=dcu.mysql_connection_uptime(), if_exists='append', index=False)


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
    read_parquet()

