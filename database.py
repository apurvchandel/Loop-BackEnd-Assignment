import sqlite3
import csv

def get_connection():
    return sqlite3.connect('restaurant_data.db')

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS store_status (
            store_id INTEGER,
            timestamp_utc TEXT,
            status TEXT,
            PRIMARY KEY (store_id, timestamp_utc)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS business_hours (
            store_id INTEGER,
            day_of_week INTEGER,
            start_time_local TEXT,
            end_time_local TEXT,
            PRIMARY KEY (store_id, day_of_week)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS store_timezone (
            store_id INTEGER,
            timezone_str TEXT,
            PRIMARY KEY (store_id)
        )
    ''')
    conn.commit()

def load_data_into_tables(conn):
    with open('store_status.csv') as f:
        reader = csv.reader(f)
        next(reader)
        conn.executemany('INSERT OR IGNORE INTO store_status VALUES (?, ?, ?)', reader)

    with open('business_hours.csv') as f:
        reader = csv.reader(f)
        next(reader) 
        conn.executemany('INSERT OR IGNORE INTO business_hours VALUES (?, ?, ?, ?)', reader)

    with open('store_timezone.csv') as f:
        reader = csv.reader(f)
        next(reader)  
        conn.executemany('INSERT OR IGNORE INTO store_timezone VALUES (?, ?)', reader)

    conn.commit()
