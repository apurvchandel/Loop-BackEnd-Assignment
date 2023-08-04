import pandas as pd
import sqlite3
from datetime import datetime, timedelta, timezone

def debug_df_merged():
    conn = sqlite3.connect('store_data.db')
    
    # Load only the necessary data from CSV 1 (store activity)
    query = 'SELECT store_id, timestamp_utc, status FROM store_activity'
    df_store_activity = pd.read_sql_query(query, conn)

    # Load data from CSV 2 (store business hours)
    df_store_hours = pd.read_sql_query('SELECT * FROM store_hours', conn)

    # Load data from CSV 3 (store timezones)
    df_store_timezones = pd.read_sql_query('SELECT * FROM store_timezones', conn)

    # Merge dataframes to get complete information
    df_merged = df_store_activity.merge(df_store_hours, on='store_id', how='left')
    df_merged = df_merged.merge(df_store_timezones, on='store_id', how='left')

    print("Columns of df_merged:", df_merged.columns)

    conn.close()

if __name__ == "__main__":
    debug_df_merged()
