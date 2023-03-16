"""This script fetch data from cloudant"""
import logging
import os
import json
import sys
import pandas as pd
from datetime import datetime
from cloudant.client import Cloudant
import dam_common_utils as dcu

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

fuel = config["cloudant_volvo_fuel_db"]
behaviour = config["cloudant_volvo_alert_db"]
wcanbs46 = config["cloudant_wabco_canbs4_db"]
wcanbs6 = config["cloudant_wabco_canbs6_db"]
wcan3bs6 = config["cloudant_wabco_can3bs6_db"]
walert = config["cloudant_wabco_alert_db"]
dtc = config["cloudant_dtc_db"]


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


def fetch_part_cloudant(filename, db_txt, month_list, start_dt, end_dt, vin_list):
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

    # Creating the connection with Cloudant database.
    client = Cloudant(config['cloudant_id'], config['cloudant_password'], url=config['cloudant_url'], verify=False)
    client.disconnect()
    client.connect()

    # Creating and opening a file to store downloaded data from all the vehicle(s).
    with open(filename, 'a', encoding='utf-8') as convert_file:

        # Iterating for every month. Each month data will be available in different database.
        for mon_yr in month_list:
            db_name = db_txt + '_' + mon_yr.lower()
            db_conn = client[db_name]

            for veh in vin_list:
                if fuel in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eventDateTime': {'$lte': end_dt,
                                                                                               '$gte': start_dt}})
                elif behaviour in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eventDateTime': {'$lte': end_dt,
                                                                                               '$gte': start_dt}})
                elif wcanbs46 in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})
                elif wcanbs6 in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})
                elif wcan3bs6 in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})
                elif walert in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})
                elif dtc in db_txt:
                    results = db_conn.get_partitioned_query_result(partition_key=veh,
                                                                   selector={'eDateTime': {'$lte': end_dt,
                                                                                           '$gte': start_dt}})
                else:
                    sys.exit("Input not Valid!!")

                for res in results:
                    # removing the cloudant generated column from data
                    del res['_rev']
                    del res['_id']
                    convert_file.write(json.dumps(res))
                    convert_file.write("\n")

    # closing cloudant database connection and opened file
    convert_file.close()
    client.disconnect()

    # Checking if any data is downloaded or not
    try:
        if os.stat(filename).st_size == 0:
            logging.info("Raw data not found. %s file empty!", filename)
            os.remove(filename)
            return 0
        else:
            logging.info("Raw data file %s downloaded.", filename)
            return 1
    except FileNotFoundError:
        logging.warning('%s file not found', filename)


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
    os.remove(filename)

    out_filename = filename.replace('.txt', '.csv')

    # selecting only user selected columns and generating CSV files for output.
    if fuel in db_txt:
        df_data[field_list].to_csv(out_filename, index=False)

    elif behaviour in db_txt:
        df_data[field_list].to_csv(out_filename, index=False)

    elif wcanbs46 in db_txt:
        field_name_map(df_data, field_config[wcanbs46], field_list).to_csv(out_filename, index=False)

    elif wcanbs6 in db_txt:
        field_name_map(df_data, field_config[wcanbs6], field_list).to_csv(out_filename, index=False)

    elif wcan3bs6 in db_txt:
        field_name_map(df_data, field_config[wcan3bs6], field_list).to_csv(out_filename, index=False)

    elif walert in db_txt:
        walert_process(df_data, field_config, field_list).to_csv(out_filename, index=False)

    elif dtc in db_txt:
        dcu.dtc_process2(df_data, field_list).to_csv(out_filename, index=False)

    else:
        sys.exit("Input not Valid!!")

    # Checking if the file has data, and sending relevant status
    try:
        return 0 if os.stat(out_filename).st_size == 0 else 1
    except FileNotFoundError:
        logging.info("%s file not found", out_filename)


def data_downloader(vins, db_txt, start_dt, end_dt, field_list_dd, row_id, fname, store):
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
    logging.info("Initiating download from Cloudant for request: %s.", row_id)
    # Reading data from the field name files
    with open('data_access_module/conf/dam_field_name.json', encoding='utf-8') as field_file:
        field_config = json.load(field_file)
    field_file.close()

    # creating file name from input to create separate files for separate vins in same request
    filename = f"data_access_module/scripts/{row_id}/{store}/{db_txt}/{fname}.txt"

    month_list = month_process(str(start_dt), str(end_dt))
    vin_list = vins.split(',')
    try:
        status = fetch_part_cloudant(filename, db_txt, month_list, start_dt, end_dt, vin_list)
    except KeyError:
        pass

    if status:
        status = dwnld_prcs_data(filename, db_txt, field_config, field_list_dd)

    return status


def walert_process(alert_data, field_config, field_list):
    ais_col = field_config['ais']
    non_ais_col = field_config['non-ais']

    ais_alert_data = alert_data.loc[alert_data['type'].isin(["ALT_BS6"])]
    if not ais_alert_data.empty:
        ais_alert_data[ais_col.split(',')] = ais_alert_data['value'].str.split(',', expand=True)
        ais_alert_data = ais_alert_data.loc[ais_alert_data['Packet Type'].isin(['HA', 'HB'])]
        ais_alert_data['UTC'] = (ais_alert_data['Date'], ais_alert_data['Time']).apply(dcu.convert_to_utc)
        ais_alert_data = ais_alert_data.pivot_table(columns='Packet Type', values="Speed", index=["Device ID", "UTC"]).reset_index()
        ais_alert_data.filter(field_list)

    non_ais_alert_data = alert_data.loc[alert_data['type'].isin(["ALT_ACC", "ALT_BRAKE"])]
    if not non_ais_alert_data.empty:
        non_ais_alert_data[non_ais_col.split(',')] = non_ais_alert_data['value'].str.split(',', expand=True)
        non_ais_alert_data["HA"] = non_ais_alert_data["Severity"] if non_ais_alert_data["type"] == 'ALT_ACC' else 0
        non_ais_alert_data["HB"] = non_ais_alert_data["Severity"] if non_ais_alert_data["type"] == 'ALT_BRAK' else 0
        non_ais_alert_data.filter(field_list)

    return pd.concat([ais_alert_data, non_ais_alert_data], axis=0)


if __name__ == "__main__":
    pass
    # data_downloader('35218066227302', 'wfaults', 20230126164152, 20230128164152, ['UTC','Live','Longitude','Device ID', 'eventDateTime'], '75', 'common', 'CLOUDANT')
    # file = 'common.txt'
    # df = pd.read_json(file, lines=True)
    # df.rename(columns={"devID": "Device ID", "utc": "UTC"}, inplace=True)
    #
    # field_list = ['Device ID', 'UTC', 'P0789', 'P0790', 'P0791']
    # # dtc_field_list = [d for d in field_list if d.startswith('dtc_')]
    #
    # df1 = pd.DataFrame(data=[['789','5','P0789'], ['790','5','P0790'], ['791', '5','P0791']],
    #                   columns=['spn', 'fmi', 'dtc_code'])
    #
    # for i, row in df.iterrows():
    #     spn_count = {df1.query(f"spn == '{fault['spn']}' & fmi == '{fault['fmi']}'")['dtc_code'].iloc[-1]: fault['occuranceCount'] for fault in row['faults']}
    #     for k, v in spn_count.items():
    #         df.at[i, k] = v
    #
    # for spn in field_list:
    #     if spn not in df.columns:
    #         df[spn] = 0
    #
    # print(df[field_list].to_string())
