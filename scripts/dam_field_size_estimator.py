"""This Script is to update average size of a column data in SQL table"""
import json
import os
import numpy as np
import pandas as pd
from dam_orchestrator import mysql_connection as sql
from dam_cloudant import data_downloader as dd


def size_estimate(veh_list, config, col_list, packet_name):
    """
    This Method have the below functions in sequence:
        download data file for one day
        calculate mean size of the each column data in bytes from a given set of vins
    :param veh_list: List of vehicle considered for average
    :param config: Configuration file string
    :param col_list: List of columns
    :param packet_name: packet name
    :return: List of tuples of column and the average size
    """
    size_dict = {}
    for veh in veh_list:
        dd(config, veh, packet_name, 20221201000000, 20221201235959, 'csv', ['ALL'])
        data_df = pd.read_csv(f'{packet_name}_sorted.csv')
        size_list = []
        for col in col_list:
            filename = ''.join(e for e in f'{col}.csv.gz' if e.isalnum())
            data_df[col].to_csv(filename, index=False, compression="gzip")
            file_size = os.path.getsize(filename)
            size_list.append(file_size)
            os.remove(filename)
        size_dict[veh] = size_list

    average_list = [np.mean(k) for k in zip(*size_dict.values())]
    return list(zip(col_list, average_list))


def update_col_size(config, col_size_list, pac):
    """
    This method is used to update the SQL table with the column size
    :param config: Configuration file string
    :param col_size_list: List of tuple of column size
    :param pac: packet name
    :return: Status
    """
    table = config['field_lkp_table']
    conn = sql(config)
    for data in col_size_list:
        query = f"UPDATE {table} SET size_in_bytes = '{data[1]}' where field_name='{data[1]}' and packet_name='{pac}'"
        # conn.execute(query)
        print(query)
    conn.close()


def main():
    """
    Main method to orchestrate all the functionality
    :return: status
    """
    with open('../../configuration.json', encoding='utf-8') as config_file:
        config = json.load(config_file)
    config_file.close()

    with open('../../field_name.json', encoding='utf-8') as field_file:
        field_config = json.load(field_file)
    field_file.close()

    packet_list = ['volvo:fuel', 'canbs4:wcanbs46', 'can2bs6:wcanbs6', 'can3bs6:wcan3bs6']
    volvo_veh_list = ['MC2BAHRC0LJ065928', 'MC2BBMRC0MA068464']
    wabco_veh_list = ['359218066341780', '352467110465742']

    for packet in packet_list:
        pac = packet.split(':')
        if pac[0] == 'volvo':
            col_list = field_config[pac[0]].replace("'", '').replace(", ", ",").split(',')
            col_size_list = size_estimate(volvo_veh_list, config, col_list, pac[1])
            update_col_size(config, col_size_list, pac[1])
        else:
            col_list = field_config[pac[0]].replace("'", '').replace(", ", ",").split(',')
            col_size_list = size_estimate(wabco_veh_list, config, col_list, pac[1])
            update_col_size(config, col_size_list, pac[1])


if __name__ == "__main__":
    main()
