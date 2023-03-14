"""This script fetch data from cloudant"""
import os
import glob
import json
import logging
import pandas as pd
from pathlib import Path
import dam_common_utils as dcu
from datetime import datetime, timedelta

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()


def packet_name(db_txt):
    """
    This function decodes the packet name to match will COS specification
    :param db_txt: packet name
    :return: COS specified packet name
    """
    if 'fuel' in db_txt:
        val = 'FUEL'
    elif 'wcanbs46' in db_txt:
        val = 'CANBS4'
    elif 'wcanbs6' in db_txt:
        val = 'CAN2BS6'
    elif 'wcan3bs6' in db_txt:
        val = 'CAN3BS6'
    else:
        val = 'NA'

    return val


def keyname_decode(cos, bucket_name, key_list):
    files = cos.Bucket(bucket_name).objects.all()
    return [a.key for k in key_list for a in files if k in a.key]


def daterange(date1, date2):
    return (date1 + timedelta(days=i) for i in range((date2 - date1).days + 1))


def date_decode(sta_dt, end_dt):
    """
    This function returns date list of all the dates between start and end date
    :param sta_dt: requested start timestamp
    :param end_dt: requested end timestamp
    :return: Date List
    """
    start_date = datetime.strptime(sta_dt[0:8], '%Y%m%d')
    end_date = datetime.strptime(end_dt[0:8], '%Y%m%d')
    return [d.strftime('%Y%m%d') for d in daterange(start_date, end_date)]


def filename_decode(vehicle_list, final_list):
    # Creating a list of unique values of last character of each vehicle
    veh_last_dig = list(set([a[-1] for a in vehicle_list.split(',')]))
    return [k + '/' + v for v in veh_last_dig for k in final_list]


def dwnld_prcs_data(cos, final_list, bucket_name, sta_dt, end_dt, vehicle_list, field_list, db_txt, row_id, filename, store):
    """
    This method perform operation in the below sequence:
        download the relevant parquet files from COS.
        parse the downloaded file and create dataframe
        filter data based on the requested vins
        filter data based on the requested start and end time
        combine all the data for different time into a single file
    :param cos: COS connect string
    :param final_list: list of files to be downloaded based on the time
    :param bucket_name: COS bucket name
    :param sta_dt: requested start date
    :param end_dt: requested end date
    :param vehicle_list: List of requested vins
    :param field_list: List of fields selected by user
    :return: Status
    """
    if final_list != '':
        for item in final_list:
            logging.info(f"{item} will be downloaded.")

            dwnld_file_name = f'{row_id}/{store}/parquet_folder/' + item.replace('/', '_')
            par_file_list = os.listdir(f'{row_id}/{store}/parquet_folder/')
            if dwnld_file_name.replace(f'{row_id}/{store}/parquet_folder/', '') not in par_file_list:
                logging.info("Checking for request cancellation")
                dcu.check_cancellation_request(row_id)
                try:
                    cos.Object(bucket_name, item).download_file(dwnld_file_name)
                except:
                    logging.warning(f"COS download error for {item}")
                    continue
            else:
                logging.info("File already available!")

            # Reading downloaded file and parsing the parquet file.
            raw_df = pd.read_parquet(dwnld_file_name, engine='pyarrow')
            veh_list = vehicle_list.split(',')

            # fetching data for request vin, datetime and fields.
            raw_df = raw_df[raw_df['deviceId'].isin(veh_list)]
            raw_df['eventDateTime'] = pd.to_datetime(raw_df['utc'].astype('int64') + 946684800, unit='s')
            df1 = raw_df[(raw_df['eventDateTime'] > sta_dt) & (raw_df['eventDateTime'] < end_dt)]
            filtered_df = df1[field_list]

            # Downloading file in CSV format
            filtered_df.to_csv(dwnld_file_name.replace('.parquet', '.csv').replace('parquet_folder', 'intercsv_folder'), index=False)

        # reading all the CSV file into one dataframe
        temp_df = pd.concat([pd.read_csv(f) for f in glob.glob(f"{row_id}/{store}/intercsv_folder/*.csv")], ignore_index=True)

        temp_df.rename(columns={'utc': 'UTC', 'deviceId': 'Device ID'}, inplace=True)

        filename = f"{row_id}/{store}/{db_txt}/{filename}.csv"
        temp_df.to_csv(filename, index=False)

        file_list = os.listdir(f'{row_id}/{store}/intercsv_folder/')
        for f in file_list:
            os.remove(f"{row_id}/{store}/intercsv_folder/{f}")

    else:
        logging.warning("No items for download")

    if os.stat(filename).st_size == 0:
        return 1
    else:
        return 0


def data_downloader(vehicle_list, db_txt, sta_dt, end_dt, field_list, row_id, filename, store):
    """
    This Method creates list of unique file that needs to be downloaded to fulfill the request
    :param config: Configuration file
    :param vehicle_list: List of requested vins
    :param db_txt: packet name
    :param sta_dt: requested start date
    :param end_dt: requested end date
    :param field_list: List of fields selected by user
    :return: Status
    """
    cos = dcu.init_cos()
    bucket_name = config['COS_BUCKET']
    date_list = date_decode(str(sta_dt), str(end_dt))
    key_list = ['edt=' + d for d in date_list]
    pkt_name = 'pkt=' + packet_name(db_txt)
    pre_list = [k1 + '/' + pkt_name for k1 in key_list]
    if vehicle_list != 'ALL':
        pre_list = filename_decode(vehicle_list, pre_list)
    final_list = keyname_decode(cos, bucket_name, pre_list)

    Path(f"{row_id}/{store}/parquet_folder").mkdir(parents=True, exist_ok=True)
    Path(f"{row_id}/{store}/intercsv_folder").mkdir(parents=True, exist_ok=True)

    status = dwnld_prcs_data(cos, final_list, bucket_name, str(sta_dt), str(end_dt), vehicle_list, field_list, db_txt, row_id, filename, store)
    return status

