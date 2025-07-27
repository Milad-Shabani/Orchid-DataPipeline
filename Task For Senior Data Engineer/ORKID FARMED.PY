from sqlalchemy import create_engine
import pandas as pd
import os
import json
import pyodbc
from datetime import datetime
import pandas as pd
import pyodbc


pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', 1000)


## Drop Tables Cleaner


class DropTablesCleaner:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def drop_tables_if_exist(self):
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()

        drop_sql = '''
        IF OBJECT_ID('user_activity', 'U') IS NOT NULL
            DROP TABLE user_activity;

        IF OBJECT_ID('user_events', 'U') IS NOT NULL
            DROP TABLE user_events;

        IF OBJECT_ID('user_profiles', 'U') IS NOT NULL
            DROP TABLE user_profiles;
        '''

        cursor.execute(drop_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print("[DropTablesCleaner] Tables dropped if existed.")


##User Profiles Loader


class UserProfilesLoader:
    def __init__(self, csv_path, connection_string):
        self.csv_path = csv_path
        self.connection_string = connection_string

    def load(self):
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()

        cursor.execute('''
        IF OBJECT_ID('user_profiles', 'U') IS NULL
        BEGIN
            CREATE TABLE user_profiles (
                user_id INT PRIMARY KEY,
                name NVARCHAR(100),
                registration_date DATE,
                location NVARCHAR(100)
            )
        END
        ''')
        conn.commit()

        inserted_count = 0
        error_count = 0

        with open(self.csv_path, 'r', encoding='utf-8-sig') as f:
            next(f)
            for line in f:
                row = line.strip().split(',')
                if len(row) != 4:
                    print(f"Skipped invalid row: {row}")
                    continue
                try:
                    user_id = int(row[0].strip('"'))
                    name = row[1].strip('"')
                    registration_date = row[2].strip('"')
                    location = row[3].strip('"')

                    cursor.execute("""
                        INSERT INTO user_profiles (user_id, name, registration_date, location)
                        VALUES (?, ?, ?, ?)
                    """, (user_id, name, registration_date, location))
                    inserted_count += 1
                except Exception as e:
                    error_count += 1
                    print(f"Error inserting row {row}: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[user_profiles] Inserted: {inserted_count}, Errors: {error_count}")


## User Events Loader


class UserEventsLoader:
    def __init__(self, json_path, connection_string):
        self.json_path = json_path
        self.connection_string = connection_string

    def load(self):
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()

        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='user_events' AND xtype='U')
        BEGIN
            CREATE TABLE user_events (
                event_id INT IDENTITY(1,1) PRIMARY KEY,
                user_id INT NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                timestamp DATETIME2 NOT NULL,
                page_url NVARCHAR(500) NULL,
                referrer NVARCHAR(500) NULL,
                duration_ms INT NULL,
                button_id NVARCHAR(100) NULL,
                filter_param NVARCHAR(100) NULL,
                item_id NVARCHAR(100) NULL,
                price BIGINT NULL,
                quantity INT NULL,
                currency NVARCHAR(10) NULL,
                item_count INT NULL,
                duration_session_ms BIGINT NULL,
                details_count INT NULL    
            )
        END
        """
        cursor.execute(create_table_sql)
        conn.commit()

        with open(self.json_path, 'r', encoding='utf-8') as f:
            events = json.load(f)


        insert_sql = """
        INSERT INTO user_events (
            user_id, event_type, timestamp, page_url, referrer, duration_ms,
            button_id, filter_param, item_id, price, quantity, currency,
            item_count, duration_session_ms, details_count  
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
        """


        event_count = 0
        for event in events:
            try:
                user_id = event['user_id']
                event_type = event['event_type']
                timestamp = datetime.fromisoformat(event['timestamp'].replace("Z", "+00:00"))
                details = event.get('details', {})

                details_count = len(details)    

                cursor.execute(insert_sql,
                    user_id,
                    event_type,
                    timestamp,
                    details.get('page_url'),
                    details.get('referrer'),
                    details.get('duration_ms'),
                    details.get('button_id'),
                    details.get('filter_param'),
                    details.get('item_id'),
                    details.get('price'),
                    details.get('quantity'),
                    details.get('currency'),
                    details.get('item_count'),
                    details.get('duration_session_ms'),
                    details_count       
                )
                event_count += 1
            except Exception as e:
                print(f"[user_events] Error inserting event: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[user_events] Inserted events: {event_count}")


## join_and_save


def join_and_save(connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    cursor.execute('''
    IF OBJECT_ID('user_activity', 'U') IS NOT NULL
        DROP TABLE user_activity;

    SELECT 
        e.event_id,
        p.user_id,
        p.name,
        p.location,
        p.registration_date,
        e.event_type,
        e.timestamp,
        CAST(e.timestamp AS DATE) AS event_date,
        e.details_count AS details_raw, 
        e.page_url,
        e.button_id,
        e.item_id,
        e.referrer,
        e.duration_ms,
        e.filter_param,
        e.price,
        e.quantity,
        e.currency,
        e.item_count,
        e.duration_session_ms
    INTO user_activity
    FROM user_profiles p
    INNER JOIN user_events e ON p.user_id = e.user_id
    ''')
    
    conn.commit()
    
    
## user_activity_sample.xlsx

 
    sample_df = pd.read_sql_query('SELECT TOP 10 * FROM user_activity', conn)
    output_excel_path = r"C:\Users\MILAD\Desktop\Task For Senior Data Engineer\user_activity_sample.xlsx"
    sample_df.to_excel(output_excel_path, index=False)
    print(f"Sample data exported to Excel file at: {output_excel_path}")

    cursor.close()
    conn.close()
    print("Joined data saved to [user_activity] table with event_date extracted.")

def main():
    connection_string = r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=DW-ORKID;Trusted_Connection=yes;"
    csv_path = r"C:\Users\MILAD\Desktop\Task For Senior Data Engineer\user_profiles.csv"
    json_path = r"C:\Users\MILAD\Desktop\Task For Senior Data Engineer\user_events_20231026.json"

    print("---- Dropping existing tables if any ----")
    cleaner = DropTablesCleaner(connection_string)
    cleaner.drop_tables_if_exist()

    print("---- Start Loading user_profiles ----")
    user_profiles_loader = UserProfilesLoader(csv_path, connection_string)
    user_profiles_loader.load()

    print("---- Start Loading user_events ----")
    user_events_loader = UserEventsLoader(json_path, connection_string)
    user_events_loader.load()

    print("---- Joining and Saving to user_activity ----")
    join_and_save(connection_string)

    print(" All steps completed successfully.")


if __name__ == "__main__":
    main()


## parquet output


def export_to_parquet_sqlalchemy(connection_string, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    engine = create_engine(connection_string)

    query = "SELECT * FROM user_activity"
    df = pd.read_sql(query, engine)

    df['event_date'] = pd.to_datetime(df['event_date'])

    df.to_parquet(
        output_dir,
        engine="pyarrow", 
        partition_cols=["event_date"],
        index=False
    )

    print(f"Parquet files saved to {output_dir}")

connection_string = (
    "mssql+pyodbc://localhost/DW-ORKID?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

output_dir = r"C:\Users\MILAD\Desktop\Task For Senior Data Engineer\parquet_output"

print("---- Exporting to Parquet with SQLAlchemy ----")
export_to_parquet_sqlalchemy(connection_string, output_dir)