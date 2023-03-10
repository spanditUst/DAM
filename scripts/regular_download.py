import os
import json
import logging
from datetime import datetime
import dam_download_orchestrator as ddo
import dam_common_utils as dcu

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

table1 = config["req_tbl_main"]
table2 = config["req_lkp_tbl_status"]
field_table = config["lkp_field_tag"]
mod_by = config["modified_by"]


def main():
    query_new_req = f"select m.id, " \
                    f"m.request_from_time, " \
                    f"m.request_to_time, " \
                    f"m.request_vin_list, " \
                    f"m.request_telematics_name, " \
                    f"m.request_fert_code, " \
                    f"m.request_fuel_type, " \
                    f"m.request_bs_norm, " \
                    f"m.request_vehicle_model, " \
                    f"m.request_engine_series, " \
                    f"m.request_vertical, " \
                    f"m.request_manufacture_year, " \
                    f"m.request_manufacture_month, " \
                    f"m.request_filter_condition, " \
                    f"m.request_max_vin_count, " \
                    f"m.request_number_of_months, " \
                    f"group_concat(s2.field_tag_id separator ',') as field_tag_id " \
                    f"from {table1} m " \
                    f"inner join {table2} s1 " \
                    f"on m.request_status_id = s1.id " \
                    f"and UPPER(s1.name) = 'REQUESTED' " \
                    f"and s1.is_visible = 1 " \
                    f"left outer join {field_table} s2 " \
                    f"on m.id = s2.data_access_request_id " \
                    f"where m.is_active = 1 " \
                    f"and m.is_deleted = 0 " \
                    f"and m.is_request_processed = 0;"

    sql_new_req_df = dcu.execute_query(dcu.mysql_connection_uptime(), query_new_req, 'return')

    if not sql_new_req_df.empty:
        fin_df = sql_new_req_df.fillna(value='')
        if not fin_df['id'][0] == '':
            logging.info("New request(s) received.")
            for ind, row in fin_df.iterrows():
                ddo.vin_process(row)

    # Deletion of the expired request
    query_delete = f"select id from {table1} where processed_file_expiry_datetime < '{datetime.now()}' " \
                   f"and processed_file_expiry_datetime is not NULL " \
                   f"and is_request_processed = 1 "
    logging.info("Checking if any request download is expiring.")
    sql_del_df = dcu.execute_query(dcu.mysql_connection_uptime(), query_delete, 'return')

    if not sql_del_df.empty:
        for ind, row in sql_del_df.iterrows():
            logging.info(f"Deleting the expired request with id: {row['id']}")
            dcu.clean_ssd_data(row['id'])
            query_update = f"UPDATE {table1} SET request_remarks = 'Download Expired! Deleted.', " \
                           f"modified_by = '{mod_by}', " \
                           f"modified_time = '{datetime.now()}' " \
                           f"where id = '{row['id']}'"
            dcu.execute_query(dcu.mysql_connection_uptime(), query_update, 'no_return')
    else:
        logging.info("No downloads expired!!")


if __name__ == "__main__":
    log_date = datetime.now()
    log_date = datetime.strftime(log_date, format="%Y%m%d%H")
    log_file_path = config["log_filepath"]
    dir_name = log_date[0:8]
    file_name = f'log_{log_date}'
    log_file_path = log_file_path + dir_name
    os.makedirs(log_file_path, exist_ok=True)
    log_file_name = log_file_path + f'/log_regular_{log_date}.log'
    logging.basicConfig(filename=log_file_name,
                        filemode='a',
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        level=logging.INFO)

    logging.info("\n\n\nStarting the Data Access Module Program")
    main()
