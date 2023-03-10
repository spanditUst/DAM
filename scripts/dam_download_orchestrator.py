"""This script will check and download data for new request from user."""
import logging
import json
import sys
from pathlib import Path
import dam_common_utils as dcu
from datetime import datetime, timedelta
from dam_object_store import data_downloader as cos_dd
from dam_cloudant import data_downloader as cld_dd
from dam_processed_data import data_downloader as mysql_dd

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

table1 = config["req_tbl_main"]
table2 = config["req_lkp_tbl_status"]
tm_fmt = config['timestamp_fmt']
mod_by = config["modified_by"]


def vin_process(req_data):
    """
    This is the main method which is called from various program to request data download
    This method separates the process of download if vin list is uploaded or chosen by applying tag filters
    This method calls merging, joining methods. This method updates the request table progressively.
    :param req_data: Dataframe row which contains all the request details
    :param config: Parameter Configuration
    :return: Nothing
    """

    # Common data retrieval
    d_flag = 0
    row_id = str(req_data['id'])
    vin_list = req_data['request_vin_list']
    field_id_plus, value_filter_str = dcu.process_value_filter(req_data['request_filter_condition'],
                                                               req_data['field_tag_id'])

    # Updating the database table for In progress status
    query = f"UPDATE {table1} SET request_status_id = (select id from {table2} where UPPER(name) = 'IN PROGRESS')," \
            f"modified_by = '{mod_by}', " \
            f"modified_time = '{datetime.now()}' " \
            f"where id = {row_id};"
    dcu.execute_query(dcu.mysql_connection_uptime(), query, 'no_return')

    field_df = dcu.field_name_retrieval(field_id_plus)

    # Preparing field name with unique packet name and list of all fields as value
    cloudant_field_dict = {k: g['fld_name'] for k, g in field_df.loc[field_df['src_name'] == 'raw'].groupby('src_type')}
    processed_field_dict = {k: g['fld_name'] for k, g in field_df.loc[field_df['src_name'] == 'processed'].groupby('src_type')}
    cos_field_dict = {k: g['cos_name'] for k, g in field_df.loc[field_df['src_name'] == 'raw'].groupby('src_type')}

    field_tuple = (cloudant_field_dict, cos_field_dict, processed_field_dict)

    if not vin_list:
        logging.info("Vehicles will be randomly selected after filtering wth tags!")
        from_time = str(req_data['request_from_time']).replace('-', '').replace(':', '').replace(' ', '')
        to_time = str(req_data['request_to_time']).replace('-', '').replace(':', '').replace(' ', '')
        d_flag += download_data(row_id, field_tuple, dcu.vin_selector(req_data), from_time, to_time, 'common')

    else:
        logging.info("Vehicles are manually entered by user!")
        offset_month = req_data['request_number_of_months']
        tmp_fmt = config['timestamp_fmt']
        for vin_data in json.loads(vin_list):
            tme = datetime.strptime(vin_data['dateoffailure'], config['date_format'])

            from_time_fmt = tme - timedelta(days=1 * int(offset_month))
            from_time = datetime.strftime(from_time_fmt, tmp_fmt)
            to_time = datetime.strftime(tme, tmp_fmt)
            d_flag += download_data(row_id, field_tuple, vin_data['vin'], str(from_time), str(to_time), vin_data['vin'])


    # Concatenating and Merging Files
    for packet in cloudant_field_dict.keys():
        Path(f"{row_id}/{packet}").mkdir(parents=True, exist_ok=True)
        concat_df = dcu.multi_store_concat(row_id, packet, cloudant_field_dict[packet], cos_field_dict[packet], prcs_flag=0)
        if not concat_df.empty:
            concat_df.to_csv(f'{row_id}/{packet}.csv', index=False)

    for packet in processed_field_dict.keys():
        concat_df = dcu.multi_store_concat(row_id, packet, '', '', prcs_flag=1)
        if not concat_df.empty:
            concat_df.to_csv(f'{row_id}/mysql_merged_data.csv', index=False)

    try:
        dcu.merge_data(row_id)
    except OSError as e:
        logging.error(f"File system issue occured.{e}")
        dcu.job_failed_update(row_id, str(e))
    if not value_filter_str=='':
        dcu.apply_value_filter(row_id, value_filter_str)
    # dcu.ssd_operation(row_id)
    # dcu.clean_raw_data(row_id)

    if d_flag:
        logging.info("All the packets were downloaded")
        query = f"UPDATE {table1} SET request_status_id = (select id from {table2} where UPPER(name) = 'COMPLETED' and is_visible = 1), " \
                f"request_remarks = 'Data Download Request completed successfully', " \
                f"request_end_message = 'Success', " \
                f"is_request_processed = 1, " \
                f"modified_by = '{mod_by}', " \
                f"modified_time = '{datetime.now()}', " \
                f"request_processed_time = '{datetime.now()}' " \
                f" where id = {row_id};"
        dcu.execute_query(dcu.mysql_connection_uptime(), query, 'no_return')
    else:
        logging.warning("All the packets were not downloaded")
        query = f"UPDATE {table1} SET request_status_id = (select id from {table2} where UPPER(name) = 'COMPLETED' and is_visible = 1), " \
                f"request_remarks = 'Data for few packets not available for the time range', " \
                f"request_end_message = 'Success', " \
                f"is_request_processed = 1, " \
                f"modified_by = '{mod_by}', " \
                f"modified_time = '{datetime.now()}', " \
                f"request_processed_time = '{datetime.now()}' " \
                f"where id = {row_id};"
        dcu.execute_query(dcu.mysql_connection_uptime(), query, 'no_return')


def download_data(row_id, field_tuple, vin_list, from_time, to_time, filename):
    """
    This method will check various condition and download data.
    This method will update the SQL table with appropriate status as well
    :param row_id: Request ID
    :param field_id: List of fields
    :param vin_list: List of Vehicle ID
    :param from_time: Starting time (Requested)
    :param to_time: Ending Time (Requested)
    :param config: parameter configuration
    :param filename: String to control data download
    :return: Nothing
    """
    # Creating the directory for all downloads
    logging.info(f"Creating Directory for request id: {row_id}")
    try:
        Path(f"{row_id}/COS").mkdir(parents=True, exist_ok=True)
        Path(f"{row_id}/CLOUDANT").mkdir(parents=True, exist_ok=True)
        Path(f"{row_id}/PROCESSED").mkdir(parents=True, exist_ok=True)
    except OSError:
        logging.error("Failed to create internal folders for download")
        dcu.job_failed_update(row_id, 'Download failed due to file system error')
        sys.exit("Exiting Process")

    # Checking if the requested data is from COS, Cloudant or both
    requested_from_time = datetime.strptime(from_time, tm_fmt)
    requested_to_time = datetime.strptime(to_time, tm_fmt)
    archival_time = datetime.now() - timedelta(days=config['cloudant_retention_days'])
    archival_time_str = datetime.strftime(archival_time, tm_fmt)

    cloudant_field_dict, cos_field_dict, processed_field_dict = field_tuple

    # Calling the download methods based on the storage
    if requested_from_time > archival_time:
        logging.info('Requesting data from only Cloudant DB')
        raw_status = download_iteration_storewise(vin_list, cloudant_field_dict, row_id, from_time, to_time, 'CLOUDANT', filename)
    elif requested_from_time < archival_time and requested_to_time < archival_time:
        logging.info('Requesting data from only COS DB')
        raw_status = download_iteration_storewise(vin_list, cos_field_dict, row_id, from_time, to_time, 'COS', filename)
    else:
        logging.info('Requesting data from both Cloudant and COS DB')
        raw_status1 = download_iteration_storewise(vin_list, cos_field_dict, row_id, from_time, archival_time_str, 'COS', filename)
        raw_status2 = download_iteration_storewise(vin_list, cloudant_field_dict, row_id, archival_time_str, to_time, 'CLOUDANT', filename)
        raw_status = 1 if raw_status1 == 1 and raw_status2 == 1 else 0

    prcsd_status = 1
    if processed_field_dict:
        logging.info("Requesting data from DB tables")
        prcsd_status = download_iteration_storewise(vin_list, processed_field_dict, row_id, archival_time, to_time, 'PROCESSED', filename)

    # Changing the COS field names to cloudant field names

    # Checking if the raw data download and processed data download are successful
    final_status = 1 if raw_status == 1 and prcsd_status == 1 else 0

    # Updating the request table based on the status of the download
    if final_status:
        return 0
    else:
        return 1


def download_iteration_storewise(vin_list, field_dict, row_id, from_time, to_time, store, fname):
    """
    This Method is the extension of the download process function.
    This Method triggers the download from various storage
    :param config: Parameter Configuration
    :param vin_list: List of Vins
    :param field_dict: packet:fields dictionary
    :param row_id: Request ID
    :param from_time: Starting time (Requested)
    :param to_time: Ending Time (Requested)
    :param store: Storage
    :param fname: filename to control the data download
    :return: Status
    """
    logging.info("Checking for request cancellation")
    dcu.check_cancellation_request(row_id)

    logging.info("Request has not been cancelled. Proceeding with the download!")
    for packet, fields in field_dict.items():

        # Adding common field names to the requested field list
        final_field_list = dcu.prep_fields(fields, packet, store)

        # Calling methods based on the storage
        if store == 'COS':
            Path(f"{row_id}/{store}/{packet}").mkdir(parents=True, exist_ok=True)
            dwnld_status = cos_dd(vin_list, packet, int(from_time), int(to_time), final_field_list, row_id, fname,
                                  store)
        elif store == 'CLOUDANT':
            Path(f"{row_id}/{store}/{packet}").mkdir(parents=True, exist_ok=True)
            dwnld_status = cld_dd(vin_list, packet, int(from_time), int(to_time), final_field_list, row_id, fname,
                                  store)
        elif store == 'PROCESSED':
            dwnld_status = mysql_dd(vin_list, packet, from_time, to_time, final_field_list, row_id, fname, store)
        else:
            dwnld_status = 0

    return dwnld_status

