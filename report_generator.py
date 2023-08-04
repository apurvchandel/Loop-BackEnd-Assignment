from datetime import datetime, timedelta
from pytz import timezone, utc
import csv
import io

def create_reports_table(connection):
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            report_id TEXT PRIMARY KEY,
            status TEXT
        )
    ''')
    connection.commit()
    cursor.close()

def get_store_status_between_times(store_id, start_time, end_time, conn):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT timestamp_utc, status FROM store_status 
        WHERE store_id = ? AND timestamp_utc >= ? AND timestamp_utc <= ?
        ORDER BY timestamp_utc
    ''', (store_id, start_time, end_time))
    return cursor.fetchall()

def calculate_uptime_and_downtime(store_id, start_time, end_time, business_hours, conn):
    local_timezone = get_local_timezone(store_id, conn)
    start_time_local = start_time.astimezone(local_timezone)
    end_time_local = end_time.astimezone(local_timezone)

    uptime_minutes = 0
    downtime_minutes = 0

    current_time = start_time_local
    while current_time < end_time_local:
        day_of_week = current_time.weekday()
        business_hour = business_hours[day_of_week]
        start_time_business_hour = current_time.replace(hour=business_hour[0].hour, minute=business_hour[0].minute)
        end_time_business_hour = current_time.replace(hour=business_hour[1].hour, minute=business_hour[1].minute)

        store_status = get_store_status_between_times(store_id, current_time, end_time_business_hour, conn)
        if store_status:
            status = store_status[-1][1]
            if status == 'active':
                uptime_minutes += (end_time_business_hour - current_time).seconds // 60
            else:
                downtime_minutes += (end_time_business_hour - current_time).seconds // 60

        current_time = end_time_business_hour

    return uptime_minutes, downtime_minutes

def get_local_timezone(store_id, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT timezone_str FROM store_timezone WHERE store_id = ?', (store_id,))
    row = cursor.fetchone()
    if row:
        return timezone(row[0])
    return timezone('America/Chicago') 

def calculate_report(conn):
    current_timestamp = datetime.utcnow()
    business_hours_dict = {}

    cursor = conn.cursor()
    cursor.execute('SELECT store_id, start_time_local, end_time_local FROM business_hours')
    for row in cursor.fetchall():
        store_id, start_time_local_str, end_time_local_str = row
        start_time_local = datetime.strptime(start_time_local_str, '%H:%M:%S').time()
        end_time_local = datetime.strptime(end_time_local_str, '%H:%M:%S').time()
        business_hours_dict[store_id] = (start_time_local, end_time_local)

    cursor.execute('SELECT DISTINCT store_id FROM store_status')
    stores = cursor.fetchall()

    report_data = []
    for store_id in stores:
        store_id = store_id[0]
        uptime, downtime = calculate_uptime_and_downtime(store_id, current_timestamp - timedelta(weeks=1), current_timestamp, business_hours_dict, conn)
        uptime_last_hour, downtime_last_hour = calculate_uptime_and_downtime(store_id, current_timestamp - timedelta(hours=1), current_timestamp, business_hours_dict, conn)
        uptime_last_day, downtime_last_day = calculate_uptime_and_downtime(store_id, current_timestamp - timedelta(days=1), current_timestamp, business_hours_dict, conn)
        uptime_last_week, downtime_last_week = calculate_uptime_and_downtime(store_id, current_timestamp - timedelta(weeks=1), current_timestamp - timedelta(hours=1), business_hours_dict, conn)
        report_data.append((store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week))

    return report_data

def generate_report(connection):
    create_reports_table(connection) 
    cursor = connection.cursor()
    report_id = str(datetime.utcnow().timestamp())
    cursor.execute('INSERT INTO reports (report_id, status) VALUES (?, ?)', (report_id, 'Running'))
    connection.commit()
    cursor.close()
    return report_id

def is_report_generation_complete(report_id, connection):
    cursor = connection.cursor()
    cursor.execute('SELECT status FROM reports WHERE report_id = ?', (report_id,))
    status = cursor.fetchone()
    cursor.close()
    return status == 'Complete'

def get_report_data(report_id, connection):
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM reports WHERE report_id = ?', (report_id,))
    report = cursor.fetchone()
    cursor.close()
    return report

def create_csv_from_report(report_data):
    headers = [
        'store_id',
        'uptime_last_hour(in minutes)',
        'uptime_last_day(in hours)',
        'update_last_week(in hours)',
        'downtime_last_hour(in minutes)',
        'downtime_last_day(in hours)',
        'downtime_last_week(in hours)'
    ]

    csv_output = io.StringIO()
    writer = csv.writer(csv_output)
    writer.writerow(headers)
    writer.writerows(report_data)

    return csv_output.getvalue()
