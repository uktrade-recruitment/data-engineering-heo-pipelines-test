import os
import csv
import logging
from datetime import datetime

def extract_weather_data(directory_path, date_str):
    """
    Extract weather data from CSV files in the specified directory.

    Args:
        directory_path: Path to the directory containing CSV files
        date_str: Date string in YYYY-MM-DD format to filter files

    Returns:
        List of dictionaries containing the raw weather data
    """
    logging.info(f"Extracting weather data from {directory_path} for date {date_str}")

    try:
        all_data = []

        for filename in os.listdir(directory_path):
            if date_str.lower() in filename.lower() and filename.lower().endswith('.csv'):
                file_path = os.path.join(directory_path, filename)
                logging.info(f"Processing file: {file_path}")

                with open(file_path, 'r') as csvfile:
                    csv_reader = csv.DictReader(csvfile)
                    file_data = list(csv_reader)
                    if not file_data:
                        logging.warning(f"No data found in file: {file_path}")
                        continue
                    all_data.extend(file_data)

                logging.info(f"Extracted {len(file_data)} records from {filename}")

        return all_data

    except Exception as e:
        logging.error(f"Error extracting data: {str(e)}")
        raise