"""This script contains method for reusability and de cluttering of the main method"""
import json
import os
import shutil
import sys
import logging
import ibm_boto3
import pandas as pd
import sqlalchemy as db
from pathlib import Path
from urllib.parse import quote
from ibm_botocore.client import Config
from datetime import datetime, timedelta

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

table1 = config["req_tbl_main"]
table2 = config["req_lkp_tbl_status"]
mod_by = config["modified_by"]
tm_fmt = config["timestamp_fmt"]


def init_cos():
    """
    :return: COS connect string
    """
    cos_endpoint = config['COS_ENDPOINT']
    cos_api = config['COS_API_KEY_ID']
    cos_crn = config['COS_INSTANCE_CRN']
    cos = ibm_boto3.resource("s3", ibm_api_key_id=cos_api, ibm_service_instance_id=cos_crn,
                             config=Config(signature_version="oauth"), endpoint_url=cos_endpoint)
    return cos


def mysql_connection_sa():
    username = config["mysql_username_sa"]
    password = config["mysql_password_sa"]
    ip_address = config["mysql_ip_address_sa"]
    port = config["mysql_port"]
    database_name = config["sa_database_name"]
    connect_args = {'ssl': {'fake_flag_to_enable_tls': True}}
    db_connection_str = ("mysql+pymysql://" + username + ":" + "%s" + "@"
                         + ip_address + ':' + str(port) + "/" + database_name + "")
    engine = db_connection_str % quote(password)
    engine = db.create_engine(engine, connect_args=connect_args)
    conn_str = engine.connect()

    return conn_str


def mysql_connection_uptime():
    username = config["mysql_username_uptime"]
    password = config["mysql_password_uptime"]
    ip_address = config["mysql_ip_address_uptime"]
    port = config["mysql_port"]
    database_name = config["uptime_database_name"]
    connect_args = {'ssl': {'fake_flag_to_enable_tls': True}}
    db_connection_str = ("mysql+pymysql://" + username + ":" + "%s" + "@"
                         + ip_address + ':' + str(port) + "/" + database_name + "")
    engine = db_connection_str % quote(password)
    engine = db.create_engine(engine, connect_args=connect_args)
    conn_str = engine.connect()

    return conn_str


def clean_raw_data(row_id):
    """
    This method removes all the downloaded raw files
    :param row_id: The request id
    :return: Nothing
    """
    try:
        shutil.rmtree(f"{row_id}/")
    except OSError:
        logging.warning("Removal of the raw data failed!")


def clean_ssd_data(row_id):
    """
    This method removes the downloaded file from SSD and update the request table
    :param row_id: The request id
    :param config:
    :return: Nothing
    """
    try:
        ssd_path = config['ssd_path'] + f"{row_id}/"
        shutil.rmtree(ssd_path)
        query = f"UPDATE {table1} " \
                f"SET request_remarks = 'Data Expired! Files deleted from the location', " \
                f"modified_by = '{mod_by}', " \
                f"modified_time = '{datetime.now()}' " \
                f"where id = {row_id};"
        execute_query(mysql_connection_uptime(), query, 'no_return')
    except OSError as e:
        logging.warning("Removal of the SSD data failed!: %s", e)


def execute_query(conn, query, return_type):
    """
    This Method creates the database connection and execute the query
    :param conn: MySQL database connection
    :param query: The query to be executed
    :param return_type: It is the flag for return type
    :return: sql output or None
    """
    try:
        if return_type == 'return':
            sql_df = pd.read_sql(query, conn)
            conn.close()
        else:
            conn.execute(query)
            conn.close()
        return sql_df if return_type == 'return' else ''
    except ConnectionError:
        logging.critical("Database Connection Error!")
        logging.info("Exiting due to database error")
        sys.exit("Exiting Process")


def prep_fields(field, pac, store):
    """
    This Method add the mandatory fields to the requested fields
    :param field: Requested field list
    :param pac: CAN packet / database table name
    :param store: The Storage
    :return: Field List
    """
    if store == 'PROCESSED':
        f_list = list(set(['chassis_number', 'event_datetime'] + field.tolist()))
    else:
        volvo_db_list = config["cloudant_volvo_fuel_db"], config["cloudant_volvo_alert_db"]
        if store == 'COS':
            f_list = field.tolist if pac in [volvo_db_list] else list(set(['deviceId', 'utc'] + field.tolist()))
        else:
            f_list = field.tolist if pac in [volvo_db_list] else list(set(['Device ID', 'UTC'] + field.tolist()))

    return f_list


def field_name_retrieval(field_id_list):
    """
    This function returns the field details based on the ids provided
    :param field_id_list: List if ids of the field selected by user
    :param config: The configuration file.
    :return: Dataframe from field lookup table
    """
    table = config["field_lkp_table"]
    query = f"select field_name as fld_name, " \
            f"field_source_type_name as src_name, " \
            f"field_source_container_name as src_type, " \
            f"historical_source_column_name as cos_name " \
            f"from {table} where id in ({field_id_list})"
    sql_df = execute_query(mysql_connection_uptime(), query, 'return')

    return sql_df


def check_cancellation_request(row_id):
    """
    This function checks for cancellation requests
    param config: The configuration file.
    return: flag and data for new request.
    """
    query = f"select 1 as data " \
            f"from {table1} m " \
            f"inner join {table2} s1 " \
            f"on m.request_status_id = s1.id " \
            f"and UPPER(s1.name) = 'CANCELLED' " \
            f"and m.id = {row_id};"

    try:
        sql_df = execute_query(mysql_connection_uptime(), query, 'return')
        if not sql_df['data'].empty:
            logging.info("The download is cancelled by the user! Proceeding for cleaning")
            query = f"UPDATE {table1} " \
                    f"SET request_status_id = (select id from {table2} where UPPER(name) = 'CANCELLED'), " \
                    f"request_remarks = 'Data Download Request Cancelled by User', " \
                    f"request_end_message = 'Cancelled', " \
                    f"modified_by = '{mod_by}', " \
                    f"modified_time = '{datetime.now()}' " \
                    f"where id = {row_id};"
            execute_query(mysql_connection_uptime(), query, 'no_return')
            clean_raw_data(row_id)
            sys.exit("Gracefully Exiting the Download Process!")
    except Exception as e:
        logging.warning("Except block message: %s", e)
        job_failed_update(row_id, 'Data Download Request failed')


def job_failed_update(row_id, msg):
    query = f"UPDATE {table1} " \
            f"SET request_status_id = (select id from {table2} where UPPER(name) = 'FAILED'), " \
            f"request_remarks = '{msg}', " \
            f"request_end_message = 'Error', " \
            f"modified_by = '{mod_by}', " \
            f"modified_time = '{datetime.now()}' " \
            f"where id = {row_id};"
    execute_query(mysql_connection_uptime(), query, 'no_return')
    logging.critical("Job Failed! Comment updated")
    sys.exit("Exiting Process!")


def store_concat(row_id, packet, cloudant_fields, cos_fields, prcs_flag):
    ret_df = pd.DataFrame()
    logging.info("Starting the concatenation of %s", packet)
    if not prcs_flag:
        store = ''

        try:
            name_replace_dict = {cloudant_fields.tolist()[i]: cos_fields.tolist()[i] for i in
                                 range(len(cloudant_fields))}
            cos_file_list = os.listdir(f'{row_id}/COS/{packet}/')
            if cos_file_list:
                cos_combined_df = pd.concat([pd.read_csv(f"{row_id}/COS/{packet}/{f}") for f in cos_file_list], axis=0)
                cos_combined_df.rename(name_replace_dict, inplace=True)
                store = store + 'cos'
        except FileNotFoundError:
            pass
        try:
            cld_file_list = os.listdir(f'{row_id}/CLOUDANT/{packet}/')
            if cld_file_list:
                cld_combined_df = pd.concat([pd.read_csv(f"{row_id}/CLOUDANT/{packet}/{f}") for f in cld_file_list],
                                            axis=0)
                store = store + 'cld'
        except FileNotFoundError:
            pass

        if store == 'coscld':
            ret_df = pd.concat([cld_combined_df, cos_combined_df], axis=0)
        elif store == 'cos':
            ret_df = cos_combined_df
        elif store == 'cld':
            ret_df = cld_combined_df

    else:
        prcs_file_list = os.listdir(f'{row_id}/PROCESSED/')
        if prcs_file_list:
            ret_df = pd.concat([pd.read_csv(f"{row_id}/PROCESSED/{f}") for f in prcs_file_list], axis=0)

    return ret_df


def merge_data(row_id):
    """
    This Method joins data from different packet based on vin and near by timestamp
    :param row_id: Request ID
    :return: Nothing
    """
    file_list = os.listdir(f'{row_id}/')
    wabco_list = [f for f in file_list if 'wcan' in f and f.endswith('.csv')]
    volvo_list = [f for f in file_list if 'fuel' in f or 'behaviour' in f]
    # prcsd_list = [f for f in file_list if 'tbl' in f]

    logging.info('Merging Data for Volvo')
    merged_v = merge_data_ext(row_id, volvo_list, col=['Chassis_number', 'eventDateTime'])
    logging.info('Merging Data for Wabco')
    merged_w = merge_data_ext(row_id, wabco_list, col=['Device ID', 'UTC'])
    # logging.info('Merging Processed Data')
    # merged_p = merge_data_ext(row_id, prcsd_list, col=['chassis_number', 'event_datetime'])

    if not merged_v.empty:
        merged_v.to_csv(f'{row_id}/volvo_merged_data_{row_id}.csv', index=False)
        for f in volvo_list:
            os.remove(f'{row_id}/{f}')
    if not merged_w.empty:
        merged_w.to_csv(f'{row_id}/wabco_merged_data_{row_id}.csv', index=False)
        for f in wabco_list:
            os.remove(f'{row_id}/{f}')
    # if not merged_p.empty:
    #     merged_p.to_csv(f'{row_id}/processed_merged_data_{row_id}.csv', index=False)
    #     for f in prcsd_list:
    #         os.remove(f'{row_id}/{f}')


def merge_data_ext(row_id, file_list, col):
    """
    This Method is the extention to the join file method
    :param row_id: Request ID
    :param file_list: List of files to be joined together
    :param col: Joining column names
    :return: merged dataframe
    """
    if len(file_list) == 1:
        logging.info("Single File! Returning data.")
        return_df = pd.read_csv(f'{row_id}/{file_list[0]}')
    elif len(file_list) > 1:
        logging.info("Multiple File! Merging Data.")
        ctr = 1
        for file in file_list:
            df = pd.read_csv(f'{row_id}/{file}')
            df.sort_values(by=col[1], axis=0, inplace=True)
            if ctr:
                df_merge = df
            else:
                df_merge = pd.merge_asof(df_merge, df, by=col[0], left_on=col[1], right_on=col[1], direction='forward')
            ctr = 0

        return_df = df_merge
    else:
        logging.info("No files downloaded.")
        return_df = pd.DataFrame()

    return return_df


def vin_selector(req_data):
    """
    This Method applies the tag filter and returns the unique list of vins upto the max number selected by user
    """
    # Creating base query string
    row_id = str(req_data['id'])

    table = config['request_tag_view']
    fuel_type = req_data['request_fuel_type'].replace(',', '\',\'')
    bsnorm = req_data['request_bs_norm'].replace(',', '\',\'')
    veh_mod = req_data['request_vehicle_model'].replace(',', '\',\'')
    eng_ser = req_data['request_engine_series'].replace(',', '\',\'')
    verti = req_data['request_vertical'].replace(',', '\',\'')
    fert_cd = req_data['request_fert_code'].replace(',', '\',\'')
    tele_nm = req_data['request_telematics_name'].replace(',', '\',\'')
    mfg_yr = req_data['request_manufacture_year'].replace(',', '\',\'')
    mfg_mn = req_data['request_manufacture_month'].replace(',', '\',\'')

    base = f"select CASE when upper(telematics_name) = 'VOLVO' then vin else device_id end as device_id from {table} where "

    # Creating query string for filtering
    query = ''
    query += f" and fuel_type in ('{fuel_type}')" if fuel_type != '' else ''
    query += f" and bsnorm in ('{bsnorm}')" if bsnorm != '' else ''
    query += f" and vehicle_model in ('{veh_mod}')" if veh_mod != '' else ''
    query += f" and engine_series in ('{eng_ser}')" if eng_ser != '' else ''
    query += f" and vertical in ('{verti}')" if verti != '' else ''
    query += f" and fert_no in ('{fert_cd}')" if fert_cd != '' else ''
    query += f" and year(mfg_date) in ('{mfg_yr}') and month(mfg_date) in ('{mfg_mn}')" if mfg_yr != '' else ''
    query += f" and telematics_name in ('{tele_nm}')" if tele_nm != '' else ''

    # Creating query string for selecting random
    query += f" ORDER BY RAND() LIMIT {req_data['request_max_vin_count']}"

    # Concatenating strings to build final query
    query = base + query[5:]
    logging.info("Tag filter query: %s", query)

    sql_df = execute_query(mysql_connection_uptime(), query, 'return')

    # Creating list of vins from the returned output
    if not sql_df.empty:
        vin_str = ''
        for vin in sql_df['device_id']:
            vin_str = vin_str + ',' + vin
        return vin_str[1:]
    else:
        job_failed_update(row_id, 'No Vehicle selected after applying the tag filters')
        return 0


def process_value_filter(value_filters, field_id):
    """
    This Method creates a string for applying value filter and adds the fields from value filters to the user selected fields
    :param value_filters: Filter criteria selected by user
    :param field_id: Fields selected by user
    :return: returns field ID list and value filter string
    """
    field_id_str = field_id
    val_filter = ''
    if not value_filters:
        filter_dict = json.loads(value_filters)
        val_filter = ''
        for d in filter_dict:
            field_id_str = field_id_str + ',' + d['id']
            col = d['name'].split('-')[1]
            val_filter = val_filter + f"{col} {d['logicaloperator']} {d['filter']}"
            val_filter = val_filter + ' & '
        k = val_filter.rfind(" & ")
        val_filter = val_filter[:k]

    return field_id_str, val_filter


def apply_value_filter(row_id, value_filter_str):
    """
    This Methods applies the value filters to the downloaded data
    :param row_id: request id
    :param value_filter_str: Filter criteria selected by user
    :return: Nothing
    """
    file_list = [f for f in os.listdir(f'{row_id}/') if f.endswith('.csv')]
    for file in file_list:
        df = pd.read_csv(f"{row_id}/{file}")
        try:
            filtered_df = df.query(value_filter_str)
            filtered_df.to_csv(f"{row_id}/" + file.replace('.csv', '_filtered.csv'))
        except KeyError:
            logging.info("Filters not applicable for the downloaded data.")


def ssd_operation(row_id):
    """This method transfers the data from local VM to SSD location
    This method will update the DB with file location"""
    try:
        logging.info("Starting SSD Operations!")
        ssd_path = config['ssd_path'] + f"{row_id}"
        Path(ssd_path).mkdir(parents=True, exist_ok=True)
        expiry_time = datetime.now() + timedelta(days=config["expiry_duration"])
        file_list = [f for f in os.listdir(f'{row_id}/') if f.endswith('_filtered.csv')]

        for file in file_list:
            os.rename(f"{row_id}/{file}", f"{ssd_path}/{file}")

        query = f"UPDATE {table1} SET processed_file_locator = '{ssd_path}', " \
                f"processed_file_expiry_datetime = '{expiry_time}', " \
                f"modified_by = '{mod_by}', " \
                f"modified_time = '{datetime.now()}' " \
                f"where id = '{row_id}'"
        execute_query(mysql_connection_uptime(), query, 'no_return')

    except OSError as e:
        logging.error("SSD operation Failed: %s", e)
        job_failed_update(row_id, 'SSD operation Failed')
        clean_raw_data(row_id)


def dtc_process2(df_data, fields):
    for ind, rowe in df_data.iterrows():
        dtc_count = {fault['spn']: fault['occuranceCount'] for fault in rowe['faults']}
        for ky, vl in dtc_count.items():
            df_data.at[ind, ky] = vl

    for f in fields:
        if f not in df_data.columns:
            df_data[f] = 0

    return df_data[fields]


def convert_to_utc(date_str, time_str):
    edatetime_str = date_str[4:] + date_str[2:4] + date_str[:2] + time_str
    edatetime = datetime.strptime(edatetime_str, tm_fmt)
    epoch_time = datetime.strptime("20000101000000", tm_fmt)
    return int((edatetime - epoch_time).total_seconds())

