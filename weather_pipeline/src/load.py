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
        with sqlite3.connect(db_path) as conn:
            try:
                create_weather_table(conn)
            except Exception as e:
                logging.error(f"Failed to create table: {str(e)}")
                return False

            cursor = conn.cursor()
            for record in data:
                try:
                    cursor.execute('''
                    INSERT INTO weather_data 
                    (station_id, station_name, timestamp, date, temperature, humidity, wind_speed, precipitation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        record['station_id'],
                        record['station_name'],
                        record['timestamp'],
                        record['date'],
                        record['temperature'],
                        record['humidity'],
                        record['wind_speed'],
                        record['precipitation']
                    ))
                except Exception as e:
                    logging.error(f"Error inserting record {record.get('station_id', 'Unknown')}: {str(e)}")
                    continue

            conn.commit()

        logging.info(f"Successfully loaded {len(data)} records into database")
        return True

    except Exception as e:
        logging.error(f"Error loading data into database: {str(e)}")
        return False