""" DTC processing module"""
import logging
import json
from datetime import datetime
import dam_common_utils as dcu
from dam_cloudant import data_downloader as cld_dd

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

wabco_date = config["dtc_wabco_retention_date"]
volvo_date = config["dtc_volvo_retention_date"]
cutt_off = config['dtc_cuttoff_time']
wabco_tbl = config["sql_dtc_wabco_tbl"]
volvo_tbl = config["sql_dtc_volvo_tbl"]
cld_dtc_db = config['cloudant_dtc_db']
tm_fmt = config['timestamp_fmt']


def file_prcsng(query, row_id, pkt_name, fname):
    """
    This method processes the dataframe and save as csv file in local folder structure
    :param query: SQL query for dtc data fetch
    :param row_id: request ID
    :param pkt_name: Table name
    :param fname: Filename for final data
    :return: returns the status based on the file size
    """
    sql_df = dcu.execute_query(dcu.mysql_connection_protech(), query, 'return')
    t_df = sql_df.pivot(index=['chasisid', 'packetdateime'], columns='dtccode', values='occurancecount')
    t_df.fillna(0, inplace=True)
    filename = f"{row_id}/PROCESSED/{pkt_name}/{fname}.csv"
    t_df.to_csv(filename, index=False)

    # return 0 if os.stat(filename).st_size != 0 else 1


def data_downloader(row_id, field_df, vin_list, from_time, to_time, fname):
    # Checking if the request beyond the cutt off date.
    if to_time > cutt_off:
        # Setting the variables
        field_list_curr = ''
        field_list_hist = ''
        wabco_dt = "'" + str(wabco_date) + "'"
        volvo_dt = "'" + str(volvo_date) + "'"
        s_dt = "'" + str(datetime.strptime(from_time, tm_fmt)) + "'"
        e_dt = "'" + str(datetime.strptime(to_time, tm_fmt)) + "'"
        wabco_flag = 1 if field_df['curr_src_name'][0] == cld_dtc_db else 0
        curr_field_dict = {k: g['curr_fld_name'] for k, g in field_df.loc[field_df['src_name'] == 'dtc'].groupby('curr_src_name')}
        hist_field_dict = {k: g['hist_fld_name'] for k, g in field_df.loc[field_df['src_name'] == 'dtc'].groupby('hist_src_name')}
        
        # Setting the default columns
        for p, f in curr_field_dict.items():
            field_list_curr = list(set(['chasisid', 'event_datetime'] + f.tolist()))
            field_list_curr = str(field_list_curr).replace("'", "").replace('[', '').replace(']', '')
        for p, f in hist_field_dict.items():
            field_list_hist = str(f.tolist()).replace('[', '(').replace(']', ')')
        
        # Wabco DTC data fetching process
        if wabco_flag:
            if from_time >= wabco_date:
                cld_dd(vin_list, cld_dtc_db, int(from_time), int(to_time), field_list_curr, row_id, fname, 'CLOUDANT')
            elif to_time < wabco_date:
                query = f"select chasisid, packetdateime, CONCAT(dtccode, '-', ftb) as dtccode, occurancecount from {wabco_tbl} " \
                        f"where chasisid in ({vin_list}) and packetdateime > {s_dt} and packetdateime < {e_dt} and dtccode in {field_list_hist};"
                file_prcsng(query, row_id, wabco_tbl, fname)
            else:
                cld_dd(vin_list, cld_dtc_db, int(wabco_date), int(to_time), field_list_curr, row_id, fname, 'CLOUDANT')
                query = f"select chasisid, packetdateime, CONCAT(dtccode, '-', ftb) as dtccode, occurancecount from {wabco_tbl} " \
                        f"where chasisid in ({vin_list}) and packetdateime > {s_dt} and packetdateime < {wabco_dt} and dtccode in {field_list_hist};"
                file_prcsng(query, row_id, wabco_tbl, fname)

        # Volvo DTC data fetching process
        else:
            if from_time >= volvo_date:
                cld_dd(vin_list, 'vfaults', int(from_time), int(to_time), field_list_curr, row_id, fname, 'CLOUDANT')
            elif to_time < volvo_date:
                query = f"select chasisid, packetdateime, CONCAT(dtccode, '-', HEX(failuretypevalue)) as dtccode, occurancecount from volvofaultitems " \
                        f"where chasisid in ({vin_list}) and packetdateime > {s_dt} and packetdateime < {e_dt} and dtccode in {field_list_hist};"
                file_prcsng(query, row_id, 'volvofaultitems', fname)
            else:
                cld_dd(vin_list, 'vfaults', int(volvo_date), int(to_time), field_list_curr, row_id, fname, 'CLOUDANT')
                query = f"select chasisid, packetdateime, CONCAT(dtccode, '-', HEX(failuretypevalue)) as dtccode, occurancecount from volvofaultitems " \
                        f"where chasisid in ({vin_list}) and packetdateime > {s_dt} and packetdateime < {volvo_dt} and dtccode in {field_list_hist};"
                file_prcsng(query, row_id, 'volvofaultitems', fname)

    else:
        logging.warning("Alert!! The Requested data is beyond %s.", cutt_off)


# if __name__=='__main__':
#     field_list = [254, 255]
#     fld_list = str(field_list).replace("'", "").replace('[', '').replace(']', '')
#     df = dcu.field_name_retrieval_dtc(fld_list)
#     data_downloader(95, df, '62795,77155', '20230401000000', '20230430125959')

