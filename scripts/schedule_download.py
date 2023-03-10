import os
import json
import logging
import sys
from datetime import datetime, timedelta, date
import pandas as pd
from data_access_module.scripts import dam_common_utils as dcu

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

table1 = config["sch_tbl_main"]
table2 = config["sch_lkp_frequency"]
table3 = config["sch_lkp_tbl_status"]
req_table = config['req_tbl_main']
status_table = config['req_lkp_tbl_status']
mod_by = config["modified_by"]


def schedule_process(row):
    """
    This Method check the frequency and prepare the data for insertion into request table
    :param row: schedule request data
    :return: Data to be inserted into request table
    """
    freq = row['frequency']
    temp_from_time = date.today()
    sch_end_time = date.today()
    if freq.upper() == 'DAILY':
        temp_from_time = date.today() - timedelta(days=1)
        sch_end_time = date.today()
    elif freq.upper() == 'WEEKLY':
        temp_from_time = date.today() - timedelta(days=7)
        sch_end_time = date.today() + timedelta(days=7)
    elif freq.upper() == 'FORTNIGHTLY':
        temp_from_time = date.today() - timedelta(days=14)
        sch_end_time = date.today() + timedelta(days=14)
    elif freq.upper() == 'MONTHLY':
        temp_from_time = date.today() - timedelta(days=30)
        sch_end_time = date.today() + timedelta(days=30)

    temp_to_time = date.today() - timedelta(days=1)
    to_time = str(temp_to_time).replace('-', '') + '235959'
    from_time = str(temp_from_time).replace('-', '') + '000000'

    query_status = f"SELECT id as status_id from {status_table} where upper(name) = 'REQUESTED' and is_visible = 1"
    status_id_df = dcu.execute_query(dcu.mysql_connection_uptime(), query_status, 'return')
    status_id = status_id_df['status_id'][0]
    row['request_status_id'] = status_id
    row['request_from_time'] = from_time
    row['request_to_time'] = to_time
    row['is_scheduled'] = 1
    row['is_request_processed'] = 0
    row['is_active'] = 1
    row['is_deleted'] = 0
    row['request_remarks'] = f'{freq} scheduled request'
    row['created_by'] = 'Scheduler'
    row['created_time'] = datetime.now()

    query = f"UPDATE {table1} SET schedule_end_time = {str(sch_end_time).replace('-', '') + '235959'}, " \
            f"modified_by = '{mod_by}', " \
            f"modified_time = '{datetime.now()}' " \
            f"where id = {row['id']}"
    dcu.execute_query(dcu.mysql_connection_uptime(), query, 'no_return')

    return row.to_dict()


def main():
    query = f"SELECT m.id, " \
            f"s1.name as frequency, " \
            f"m.schedule_telematics_name as request_telematics_name, " \
            f"m.schedule_fert_code as request_fert_code, " \
            f"m.schedule_fuel_type as request_fuel_type, " \
            f"m.schedule_bs_norm as request_bs_norm, " \
            f"m.schedule_vehicle_model as request_vehicle_model, " \
            f"m.schedule_engine_series as request_engine_series, " \
            f"m.schedule_vertical as request_vertical, " \
            f"m.schedule_manufacture_year as request_manufacture_year, " \
            f"m.schedule_manufacture_month as request_manufacture_month, " \
            f"m.schedule_filter_condition as request_filter_condition, " \
            f"m.schedule_max_vin_count as request_max_vin_count " \
            f"FROM {table1} m left outer join {table2} s1 " \
            f"on m.schedule_frequency_id = s1.id " \
            f"left outer join {table3} s2 " \
            f"on m.schedule_status_id = s2.id " \
            f"where s1.is_visible = 1 " \
            f"and m.is_active = 1 " \
            f"and s2.is_visible = 1 " \
            f"and UPPER(s2.name) = 'REQUESTED' " \
            f"and (m.schedule_end_time < '{str(date.today())}' " \
            f"or m.schedule_end_time is null);"

    sql_df = dcu.execute_query(dcu.mysql_connection_uptime(), query, 'return')

    data = []
    if not sql_df.empty:
        for ind, row in sql_df.iterrows():
            row1 = schedule_process(row)
            data.append(row1)

        row_df = pd.DataFrame.from_records(data)
        if not row_df.empty:
            fin_df = row_df.fillna(value='')
            fin_df.drop(columns=['id', 'frequency'], inplace=True)

        # Insert df into uptime request table
        fin_df.to_sql(name=req_table, con=dcu.mysql_connection_uptime(), if_exists='append', index=False, )
    else:
        logging.info("No scheduled job found")
        sys.exit(" Exiting process!")


if __name__ == "__main__":
    log_date = datetime.now()
    log_date = datetime.strftime(log_date, format="%Y%m%d%H")
    log_file_path = config["log_filepath"]
    dir_name = log_date[0:8]
    file_name = f'log_{log_date}'
    log_file_path = log_file_path + dir_name
    os.makedirs(log_file_path, exist_ok=True)
    log_file_name = log_file_path + f'/log_scheduler_{log_date}.log'
    logging.basicConfig(filename=log_file_name,
                        filemode='a',
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        level=logging.INFO)

    logging.info("Checking for scheduled download")
    main()
