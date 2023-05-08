"""This Module sends email for expiring downloads"""
import json
import dam_common_utils as dcu

with open('../conf/dam_configuration.json', encoding='utf-8') as config_file:
    config = json.load(config_file)
config_file.close()

table1 = config['req_tbl_main']
table2 = config['req_lkp_tbl_status']
alert_days = 4


def expiry_notification_email():
    query = f"select m.id, m.processed_file_locator, m.processed_file_expiry_datetime, m.created_by " \
            f"from {table1} m inner join {table2} s1 " \
            f"on m.request_status_id = s1.id " \
            f"and UPPER(s1.name) = 'COMPLETED' " \
            f"and s1.is_visible = 1 " \
            f"where is_request_processed = 1 " \
            f"and DATEDIFF(DATE(processed_file_expiry_datetime), CURDATE()) BETWEEN 0 and {alert_days};"

    sql_df = dcu.execute_query(dcu.mysql_connection_uptime(), query, 'return')

    if not sql_df.empty:
        for ind, row in sql_df.iterrows():
            email_id = row['created_by']
            email_id = config['sent_email']
            request_number = row['id']
            expiry_time = row['processed_file_expiry_datetime']
            dcu.email_with_text(email_id, '',
                                'Data Access Module Notification',
                                'Dear User, \n\n'
                                f'The data download request for request id {request_number} is nearing expiry.\n'
                                f'The expiration date is : {expiry_time}, after which the download shall be removed.'
                                '\n\nThanks,\nData Access Module')


if __name__ == "__main__":
    expiry_notification_email()
