import os
import json
import sys
import pandas as pd
import traceback
from cloudant.client import Cloudant


def field_name_map(df, col_str):
    col = col_str.split(',')
    try:
        df[col] = df.value.str.split(',', expand=True)
    except Exception as e:
        print(str(e))
        df.to_csv('error_file.csv', index=False)
    df.drop(['value'], axis=1, inplace=True)

    return df


def prcs_data(filename, output, proc):
    df = pd.read_json(filename, lines=True, convert_dates=False)
    # df.sort_values(by=sort_col, axis=0, inplace=True)
    os.remove(filename)

    with open('field_name.json') as config_file:
        config = json.load(config_file)

    # out_filename = filename.replace('.txt', '_sorted.txt')
    if output == 'raw':
        df.to_json(filename, orient='records', lines=True)
    else:
        if 'fuel' in proc:
            df.to_csv(filename.replace('.txt', '.csv'), index=False)
        elif 'wcanbs46' in proc:
            field_name_map(df, config['wcanbs46']).to_csv(filename.replace('.txt', '.csv'), index=False)
        elif 'wcanbs6' in proc:
            field_name_map(df, config['wcanbs6']).to_csv(filename.replace('.txt', '.csv'), index=False)
        elif 'wcan3bs6' in proc:
            field_name_map(df, config['wcan3bs6']).to_csv(filename.replace('.txt', '.csv'), index=False)
        else:
            print("Input not Valid!!")
            sys.exit(1)


def main(a, b, c, d, e):
    print("Process Starts!")

    # n = len(sys.argv)
    # print(n)
    # if n != 6:
    #     print("Incorrect number of Inputs!")
    #     print(sys.argv)
    #     sys.exit(1)

    # list_w = sys.argv[1].split(',')
    # db_txt = sys.argv[2]
    # sta_dt = int(sys.argv[3])
    # end_dt = int(sys.argv[4])
    # output = sys.argv[5]
    list_w = a.split(',')
    db_txt = b
    sta_dt = int(c)
    end_dt = int(d)
    output = e

    print("User Inputs:")
    print("Input 1:", str(list_w))
    print("Input 2:", str(db_txt))
    print("Input 3:", str(sta_dt))
    print("Input 4:", str(end_dt))
    print("Input 5:", str(output))

    filename = db_txt + '.txt'

    cloudant_id = "c5039c1d-4056-474f-bac5-55479a60a1ae-bluemix"
    cloudant_password = "2e3e2a10dbea25ab6a9518ddd130abb9eb18f4acd3060b4d70751e4d011235ea"
    cloudant_url = "https://c5039c1d-4056-474f-bac5-55479a60a1ae-bluemix:2e3e2a10dbea25ab6a9518ddd130abb9eb18f4acd3060b4d70751e4d011235ea@c5039c1d-4056-474f-bac5-55479a60a1ae-bluemix.cloudantnosqldb.appdomain.cloud"
    client = Cloudant(cloudant_id, cloudant_password, url=cloudant_url)
    client.connect()

    db = client[db_txt]

    try:
        with open(filename, 'a') as convert_file:
            for veh in list_w:
                if 'fuel' in db_txt:
                    results = db.get_partitioned_query_result(partition_key=veh,
                                                              selector={'eventDateTime': {'$lte': end_dt, '$gte': sta_dt}})
                elif 'behaviour' in db_txt:
                    results = db.get_partitioned_query_result(partition_key=veh,
                                                              selector={'eventDateTime': {'$lte': end_dt, '$gte': sta_dt}})
                elif 'wcanbs46' in db_txt:
                    results = db.get_partitioned_query_result(partition_key=veh,
                                                              selector={'eDateTime': {'$lte': end_dt, '$gte': sta_dt}},
                                                              fields=["_id", "value", "devId", "eDateTime", "_rev"])
                elif 'wcanbs6' in db_txt:
                    results = db.get_partitioned_query_result(partition_key=veh,
                                                              selector={'eDateTime': {'$lte': end_dt, '$gte': sta_dt}},
                                                              fields=["_id", "value", "devId", "eDateTime", "_rev"])
                elif 'wcan3bs6' in db_txt:
                    results = db.get_partitioned_query_result(partition_key=veh,
                                                              selector={'eDateTime': {'$lte': end_dt, '$gte': sta_dt}},
                                                              fields=["_id", "value", "devId", "eDateTime", "_rev"])
                elif 'walert' in db_txt:
                    results = db.get_partitioned_query_result(partition_key=veh,
                                                              selector={'eDateTime': {'$lte': end_dt, '$gte': sta_dt}},
                                                              fields=["_id", "value", "devId", "eDateTime", "_rev"])
                else:
                    print("Input not Valid!!")
                    sys.exit(1)
                if not results == []:
                    for r in results:
                        del r['_rev']
                        del r['_id']
                        convert_file.write(json.dumps(r))
                        convert_file.write("\n")
                else:
                    print("Data Not Available")
                    sys.exit(1)

        prcs_data(filename, output, db_txt)

    except Exception as e:
        traceback.print_exc()
        print("Error in processing: ", e)

    print("Process Ends!")


if __name__ == "__main__":
    # main('359207066492503', 'wcanbs46_apr_2023', 20230401000000, 20230430235959, 'csv')
    # main('359207066492503', 'wcanbs6_apr_2023', 20230401000000, 20230430235959, 'csv')
    # main('359207066492503', 'wcan3bs6_apr_2023', 20230401000000, 20230430235959, 'csv')
    pass
