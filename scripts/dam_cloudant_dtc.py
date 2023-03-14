"""This script fetch data from cloudant"""
import logging
import os
import json
import sys
import pandas as pd
from datetime import datetime
from cloudant.client import Cloudant

with open('data_access_module/conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()


def field_name_map(data_df, col_str, field_list):
    """
    This Method maps value field's data to their respective field name for Wabco
    :param data_df: value field data
    :param col_str: field name
    :param field_list: fields selected by user
    :return: value data with field names
    """
    # Splitting and Mapping 'value' field to their field names
    data_df[col_str.split(',')] = data_df['value'].str.split(',', expand=True)

    # Dropping the 'value' column from dataframe
    data_df.drop(['value'], axis=1, inplace=True)

    # Removing unnecessary characters from dataframe's field names
    data_df.columns = data_df.columns.str.replace("'", "")
    data_df = data_df[field_list]

    # Convert the 'epoch' column to datetime, using 2000-01-01 as the origin and UTC as the timezone
    data_df['eventDateTime'] = pd.to_datetime(data_df['UTC'].astype('int64') + 946684800, unit='s')

    return data_df


def month_process(start_dt, end_dt):
    """
    This method will create the list of unique month falling between date range
    :param start_dt: requested start date
    :param end_dt: requested end date
    :return: unique month list
    """

    if start_dt[4:6] == end_dt[4:6]:
        month_list = [datetime.strptime(start_dt, '%Y%m%d%H%M%S').strftime("%b_%Y").lower()]
    else:
        strt_dt = list(start_dt)
        strt_dt[6:8] = '01'
        new_start_dt = ''.join(strt_dt)
        month_list = pd.date_range(start=new_start_dt, end=end_dt, freq='MS').strftime("%b_%Y").tolist()

    return month_list


def fetch_part_cloudant(filename, db_txt, month_list, start_dt, end_dt, vin_list, row_id):
    """
    This Method download requested data into a file.
    :param filename: name of the downloaded data file
    :param db_txt: packet name
    :param month_list: List of months which falls in given date range
    :param start_dt: requested start date
    :param end_dt: requested end date
    :param vin_list: requested vin list
    :param config: configuration file
    :return: Status
    """

    cloudant_id = config['cloudant_id']
    cloudant_password = config['cloudant_password']
    cloudant_url = config['cloudant_url']

    # Creating the connection with Cloudant database.
    client = Cloudant(cloudant_id, cloudant_password, url=cloudant_url, verify=False)
    client.disconnect()
    client.connect()

    # Creating and opening a file to store downloaded data from all the vehicle(s).
    with open(filename, 'a', encoding='utf-8') as convert_file:

        # Iterating for every month. Each month data will be available in different database.
        for mon in month_list:
            db_name = db_txt + '_' + mon.lower()
            db_conn = client[db_name]

            for veh in vin_list:
                if 'fuel' in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eventDateTime': {'$lte': end_dt,
                                                                                               '$gte': start_dt}})

                elif 'wcanbs46' in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})

                elif 'wcanbs6' in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})

                elif 'wcan3bs6' in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})
                elif 'wfaults' in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})

                else:
                    sys.exit("Input not Valid!!")

                for res in results:
                    # removing the cloudant generated column from data
                    del res['_rev']
                    convert_file.write(json.dumps(res))
                    convert_file.write("\n")

    # closing cloudant database connection
    convert_file.close()
    client.disconnect()

    # Checking if any data is downloaded or not
    if os.stat(filename).st_size == 0:
        logging.info(f"Raw data not found. {filename} file empty!")
        os.remove(filename)
        status = 0
    else:
        logging.info(f"Raw data file {filename} downloaded.")
        status = 1

    return status


def dwnld_prcs_data(filename, db_txt, field_config, field_list):
    """
    This Method have the following function in sequence:
        create dataframe of the downloaded file
        sort the data based on sorting solumn
        create the final output filed based on the format selected
    :param filename: name of the downloaded data file
    :param db_txt: packet name
    :param sort_col: sorting column
    :param field_config: field name configuration string
    :param field_list: fields selected by user
    :return: Status
    """
    # Creating dataframe from the downloaded file.
    df_data = pd.read_json(filename, lines=True, convert_dates=False)

    # Removing the downloaded file
    # os.remove(filename)

    out_filename = filename.replace('.txt', '.csv')

    if 'fuel' in db_txt:
        df_data[field_list].to_csv(out_filename, index=False)

    elif 'wcanbs46' in db_txt:
        field_name_map(df_data, field_config['canbs4'], field_list).to_csv(out_filename, index=False)

    elif 'wcanbs6' in db_txt:
        field_name_map(df_data, field_config['can2bs6'], field_list).to_csv(out_filename, index=False)

    elif 'wcan3bs6' in db_txt:
        field_name_map(df_data, field_config['can3bs6'], field_list).to_csv(out_filename, index=False)

    elif 'wfaults' in db_txt:
        pass

    else:
        sys.exit("Input not Valid!!")
    try:
        return 0 if os.stat(out_filename).st_size == 0 else 1
    except FileNotFoundError:
        logging.info(f"{out_filename} file not found")


def data_downloader(vins, db_txt, start_dt, end_dt, field_list, row_id, fname, store):
    """
    This method prepares the parameter values and call the method to download data
    :param config: configuration file string
    :param vins: requested vin list
    :param db_txt: packet name
    :param start_dt: requested start date
    :param end_dt: requested end date
    :return: Status
    """
    status = 0
    logging.info(f"Initiating download from Cloudant for request {row_id}.")
    # Reading data from the field name files
    with open('data_access_module/conf/dam_field_name.json', encoding='utf-8') as field_file:
        field_config = json.load(field_file)
    field_file.close()

    filename = f"data_access_module/scripts/{row_id}/{store}/{db_txt}/{fname}.txt"
    month_list = month_process(str(start_dt), str(end_dt))
    vin_list = vins.split(',')
    try:
        status = fetch_part_cloudant(filename, db_txt, month_list, start_dt, end_dt, vin_list, row_id)
    except KeyError as e:
        pass

    if status:
        fin_status = dwnld_prcs_data(filename, db_txt, field_config, field_list)
    else:
        fin_status = 0

    return fin_status


if __name__ == "__main__":
    # data_downloader('35218066227302', 'wfaults', 20230126164152, 20230128164152, ['UTC','Live','Longitude','Device ID', 'eventDateTime'], '75', 'common', 'CLOUDANT')

    df = pd.read_json('data_access_module/scripts/75/CLOUDANT/wfaults/common.txt', lines=True, convert_dates=False)
    df1 = pd.io.json.json_normalize(df['faults'])
    print(df1)



