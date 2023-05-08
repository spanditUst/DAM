"""This script contains method for reusability and de cluttering of the main method"""
import json
import os
import sys
import shutil
import logging
import zipfile
import pandas as pd
import sqlalchemy as db
from pathlib import Path
from urllib.parse import quote
from datetime import datetime, timedelta
from exchangelib import Credentials, Account, Message, Mailbox, FileAttachment, Configuration, HTMLBody, DELEGATE

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

table1 = config["req_tbl_main"]
table2 = config["req_lkp_tbl_status"]
mod_by = config["modified_by"]
tm_fmt = config["timestamp_fmt"]
ADMIN_ID = config['email_admin_id']
ADMIN_PASSWORD = config['email_admin_password']


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


def mysql_connection_protech():
    username = config["mysql_username_protech"]
    password = config["mysql_password_protech"]
    ip_address = config["mysql_ip_address_protech"]
    port = config["mysql_port"]
    database_name = config["protech_database_name"]
    connect_args = {'ssl': {'fake_flag_to_enable_tls': True}}
    db_connection_str = ("mysql+pymysql://" + username + ":" + "%s" + "@"
                         + ip_address + ':' + str(port) + "/" + database_name + "")
    engine = db_connection_str % quote(password)
    engine = db.create_engine(engine, connect_args=connect_args)
    conn_str = engine.connect()

    return conn_str


def non_empty_file(path):
    final_list = []
    file_list = os.listdir(path)
    for file in file_list:
        if not os.stat(f"{path}/{file}").st_size == 0:
            final_list.append(file)

    return final_list


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
                f"SET modified_by = '{mod_by}', " \
                f"modified_time = '{datetime.now()}' " \
                f"where id = {row_id};"
        # request_remarks = 'Data Expired! Files deleted from the location'
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
        volvo_db_list = ['fuel', 'behaviour']

        if store == 'COS':
            f_list = ['vin', 'eventDateTime'] if pac in volvo_db_list else ['deviceId', 'utc']
        else:
            f_list = ['vin', 'eventDateTime'] if pac in volvo_db_list else ['Device ID', 'UTC']

    return list(set(f_list + field.tolist()))


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
            f"from {table} where id in ({field_id_list}) " \
            f"and field_source_type_name != 'dtc'"
    sql_df = execute_query(mysql_connection_uptime(), query, 'return')

    return sql_df


def field_name_retrieval_dtc(field_id_list):
    """
    This function returns the field details based on the ids provided
    :param field_id_list: List if ids of the field selected by user
    :param config: The configuration file.
    :return: Dataframe from field lookup table
    """
    table = config["field_lkp_table"]
    query = f"select field_name as curr_fld_name, " \
            f"field_source_type_name as src_name, " \
            f"field_source_container_name as curr_src_name, " \
            f"historical_source_name as hist_src_name, " \
            f"historical_source_column_name as hist_fld_name " \
            f"from {table} where id in ({field_id_list}) " \
            f"and field_source_type_name = 'dtc'"
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
                    f"request_end_message = 'Cancelled', " \
                    f"modified_by = '{mod_by}', " \
                    f"modified_time = '{datetime.now()}' " \
                    f"where id = {row_id};"
            # f"request_remarks = 'Data Download Request Cancelled by User',
            execute_query(mysql_connection_uptime(), query, 'no_return')
            clean_raw_data(row_id)
            sys.exit("Gracefully Exiting the Download Process!")
    except Exception as e:
        logging.warning("Except block message: %s", e)
        job_failed_update(row_id, 'Data Download Request failed')


def job_failed_update(row_id, msg):
    query = f"UPDATE {table1} " \
            f"SET request_status_id = (select id from {table2} where UPPER(name) = 'FAILED'), " \
            f"request_end_message = 'Error', " \
            f"modified_by = '{mod_by}', " \
            f"modified_time = '{datetime.now()}' " \
            f"where id = {row_id};"
    # f"request_remarks = '{msg}', " \
    execute_query(mysql_connection_uptime(), query, 'no_return')
    logging.critical("Job Failed! Comment updated")
    sys.exit("Exiting Process!")


def store_concat(row_id, packet, cloudant_fields, cos_fields, prcs_flag):
    """
    This method will merge the data from multiple files of different vehicle and
    same packet to one file
    :param row_id: request id
    :param packet: Packet name
    :param cloudant_fields: requested field names in Cloudant format
    :param cos_fields: requested field names in COS format
    :param prcs_flag: flag for data download from mysql tables
    :return: Return the merged dataframe
    """
    ret_df = pd.DataFrame()
    logging.info("Starting the concatenation of %s", packet)
    if not prcs_flag:
        store = ''

        try:
            name_replace_dict = {cos_fields.tolist()[i]: cloudant_fields.tolist()[i] for i in range(len(cloudant_fields))}
            cos_file_list = non_empty_file(f'{row_id}/COS/{packet}/')
            if cos_file_list:
                cos_combined_df = pd.concat([pd.read_csv(f"{row_id}/COS/{packet}/{f}") for f in cos_file_list], axis=0)
                cos_combined_df.rename(columns=name_replace_dict, inplace=True)
                store = store + 'cos'
        except FileNotFoundError:
            pass
        try:
            cld_file_list = non_empty_file(f'{row_id}/CLOUDANT/{packet}/')
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
        prcs_file_list = os.listdir(f'{row_id}/PROCESSED/{packet}')
        if prcs_file_list:
            ret_df = pd.concat([pd.read_csv(f"{row_id}/PROCESSED/{packet}/{f}") for f in prcs_file_list], axis=0)

    return ret_df


def merge_data(row_id):
    """
    This Method joins data from different packet based on vin and near by timestamp
    :param row_id: Request ID
    :return: Nothing
    """
    file_list = os.listdir(f'{row_id}/')
    wabco_list = [f for f in file_list if 'wcan' in f and f.endswith('.csv')]
    volvo_list = [f for f in file_list if ('fuel' in f and f.endswith('.csv')) or ('behaviour' in f and f.endswith('.csv'))]
    # prcsd_list = [f for f in file_list if 'tbl' in f]

    logging.info('Merging Data for Volvo')
    merged_v = merge_data_ext(row_id, volvo_list, col=['vin', 'eventDateTime'])
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
    tag_dict = json.loads(req_data['request_attribute_list'])
    table = config['request_tag_view']

    tag_dict = {k: [""] if v is None else v for k, v in tag_dict.items()}
    tag_dict = {k: '\',\''.join(str(x) for x in v) for k, v in tag_dict.items()}
    fuel_type = tag_dict['fuelType']
    bsnorm = tag_dict['bsNorm']
    veh_mod = tag_dict['vehicleModel']
    eng_ser = tag_dict['engineSeries']
    verti = tag_dict['vertical']
    fert_cd = tag_dict['fertNo']
    tele_nm = tag_dict['telematicsName']
    mfg_btch = '\',\''.join(x for x in [y[:7] for y in tag_dict['mfgBatch'].split('\',\'')])

    base = f"select CASE when upper(telematics_name) = 'VOLVO' then vin else device_id end as device_id from {table} where "

    # Creating query string for filtering
    query = ''
    query += f" and fuel_type in ('{fuel_type}')" if fuel_type != '' else ''
    query += f" and bsnorm in ('{bsnorm}')" if bsnorm != '' else ''
    query += f" and vehicle_model in ('{veh_mod}')" if veh_mod != '' else ''
    query += f" and engine_series in ('{eng_ser}')" if eng_ser != '' else ''
    query += f" and vertical in ('{verti}')" if verti != '' else ''
    query += f" and fert_no in ('{fert_cd}')" if fert_cd != '' else ''
    query += f" and DATE_FORMAT(mfg_date, '%%Y-%%m') in ('{mfg_btch}')" if mfg_btch != '' else ''
    query += f" and telematics_name in ('{tele_nm}')" if tele_nm != '' else ''

    # Creating query string for selecting random
    query += f" ORDER BY RAND() LIMIT {int(req_data['request_max_vin_count'])}"

    # Concatenating strings to build final query
    query = base + query[5:]
    logging.info(f"Query for tag filter: {query}")

    sql_df = execute_query(mysql_connection_uptime(), query, 'return')

    # Creating list of vins from the returned output
    if not sql_df.empty:
        logging.info("Vehicles found for the tag filter criteria")
        vin_str = ''
        for vin in sql_df['device_id']:
            vin_str = vin_str + ',' + vin
        return vin_str[1:]
    else:
        logging.warning("No Vehicle found matching the tag filter criteria.")
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
    if value_filters != '':
        filter_dict = json.loads(value_filters)
        val_filter = ''
        for d in filter_dict:
            field_id_str = field_id_str + ',' + str(d['id'])
            col = d['name'].split('-')[1]
            operator = '==' if d['logicalOperator'] == '=' else d['logicalOperator']
            val_filter = val_filter + f"{col} {operator} {d['filter']}"
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
        data_df = pd.read_csv(f"{row_id}/{file}")
        if not value_filter_str == '':
            logging.info("Applying filters on the downloaded data.")
            try:
                value_filter_str = value_filter_str.replace(':', "_")
                data_df.columns = data_df.columns.str.replace(':', "_")
                filtered_df = data_df.query(value_filter_str)
                filtered_df.to_csv(f"{row_id}/" + file.replace('.csv', '_filtered.csv'), index=False)
            except Exception as e:
                logging.info("Filters not applicable for the downloaded data.\nError: ", e)
        else:
            logging.info("No Filter to be applied.")
            data_df.to_csv(f"{row_id}/" + file.replace('.csv', '_filtered.csv'), index=False)


def zip_folder(folder_path, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, os.path.relpath(file_path, folder_path))


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

        zip_folder(f"{ssd_path}/", f"{ssd_path}.zip")

        query = f"UPDATE {table1} SET processed_file_locator = '{ssd_path}.zip', " \
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
    """
    This method process cloudant DTC data into required layout
    :param df_data: cloudant dataframe
    :param fields:
    :return:
    """
    for ind, rowe in df_data.iterrows():
        dtc_count = {fault['spn']: fault['occuranceCount'] for fault in rowe['faults']}
        for ky, vl in dtc_count.items():
            df_data.at[ind, ky] = vl

    for f in fields:
        if f not in df_data.columns:
            df_data[f] = 0

    return df_data[fields]


def convert_to_utc(date_str, time_str):
    """
    This Method convert the time string to epoch time from 01-01-2000
    :param date_str: datetime string
    :param time_str:
    :return: epoch time for the input time
    """
    edatetime_str = date_str[4:] + date_str[2:4] + date_str[:2] + time_str
    edatetime = datetime.strptime(edatetime_str, tm_fmt)
    epoch_time = datetime.strptime("20000101000000", tm_fmt)
    return int((edatetime - epoch_time).total_seconds())


def email_with_text(send_to, cc_to, subject, body_text):
    body_text_for_html = body_text.replace('\n', '<BR>')
    send = []
    for i in send_to.split(","):
        send.append(Mailbox(email_address=i))
    if len(send) == 0:
        return "No Email ID's passed"

    cc = []
    for i in cc_to.split(","):
        cc.append(Mailbox(email_address=i))

    ews_url = 'https://webmail.vecv.in/EWS/Exchange.asmx'
    ews_auth_type = 'NTLM'
    primary_smtp_address = ADMIN_ID
    cred = Credentials(ADMIN_ID, ADMIN_PASSWORD)

    configu = Configuration(service_endpoint=ews_url, credentials=cred, auth_type=ews_auth_type)
    acc = Account( primary_smtp_address=primary_smtp_address, config=configu, autodiscover=False, access_type=DELEGATE)

    if len(cc_to) > 0:
        m = Message(account=acc, subject=subject, body=HTMLBody(body_text_for_html), to_recipients=send, cc_recipients=cc)
    if len(cc_to) == 0:
        m = Message(account=acc, subject=subject, body=HTMLBody(body_text_for_html), to_recipients=send)
    m.send()