import os
import dam_common_utils as dcu


def data_downloader(vehicle_list, db_txt, sta_dt, end_dt, field_list, row_id, filename, store):
    """
    This is the main method for downloading processed data.
    :param config: Configuration file
    :param vehicle_list: The list of requested vins
    :param db_txt: table name to which the requested fields belong to
    :param sta_dt: request start time
    :param end_dt: request end time
    :param filefmt: requested file format
    :param field_list: requested list of fields to be downloaded.
    :return: Return the status
    """
    vehicle_id = 'vin' if len(vehicle_list[0]) == 17 else 'device_id'
    v_list = str(vehicle_list.split(',')).replace('[', '(').replace(']', ')')
    f_list = str(field_list).replace("'", ""). replace('[', '').replace(']', '')
    f_list = f_list.replace('chassis_number', vehicle_id).replace( 'Device ID', vehicle_id)
    s_dt = "'" + str(sta_dt) + "'"
    e_dt = "'" + str(end_dt) + "'"
    query = f"select {f_list} from {db_txt} where {vehicle_id} in {v_list} and event_datetime > {s_dt} and " \
            f"event_datetime < {e_dt};"

    sql_df = dcu.execute_query(dcu.mysql_connection_uptime(), query, 'return')
    filename = f"{row_id}/{store}/{db_txt}/{db_txt}_tbl_{filename}.csv"
    sql_df.to_csv(filename, index=False)

    if os.stat(filename).st_size == 0:
        return 0
    else:
        return 1

