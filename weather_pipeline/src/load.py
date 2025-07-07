import os
import sqlite3
import logging

def create_weather_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            station_name TEXT,
            timestamp TEXT,
            date TEXT,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            precipitation REAL
        )
        ''')

        # Question: What purpose does this index serve?
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_weather_date
        ON weather_data (date)
        ''')

        conn.commit()
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        raise

def load_weather_data(data, db_path):
    if not data:
        logging.warning("No data to load")
        return False

    logging.info(f"Loading {len(data)} records into database at {db_path}")

    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = sqlite3.connect(db_path)

        create_weather_table(conn)

        cursor = conn.cursor()
        for record in data:
            try:
                cursor.execute('''
                INSERT INTO weather_data 
                (station_id, station_name, timestamp, date, temperature, humidity, wind_speed, precipitation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(record['station_id']),
                    str(record['station_name']),
                    str(record['timestamp']),
                    str(record['date']),
                    float(record['temperature']) if record['temperature'] is not None else None,
                    float(record['humidity']) if record['humidity'] is not None else None,
                    float(record['wind_speed']) if record['wind_speed'] is not None else None,
                    float(record['precipitation']) if record['precipitation'] is not None else None
                ))
            except Exception as e:
                logging.error(f"Error inserting record {record}: {str(e)}")

        conn.commit()
        conn.close()

        logging.info(f"Successfully loaded {len(data)} records into database")
        return True

    except Exception as e:
        logging.error(f"Error loading data into database: {str(e)}")
        return False