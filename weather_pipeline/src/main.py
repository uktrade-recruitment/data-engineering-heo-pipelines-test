import os
import logging
import configparser
import csv
from datetime import datetime
import sqlite3

from src.extract import extract_weather_data
from src.transform import transform_weather_data
from src.load import load_weather_data
from src.validation import validate_weather_data

logging.basicConfig(
    filename='weather_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_config():
    """Purely for loading configuration settings."""
    try:
        config = configparser.ConfigParser()
        config.read('config/config.ini')
        return config
    except Exception as e:
        logging.error(f"Failed configuration: {e}")
        raise

def process_weather_data(config, date_str=None):
    """
    Main function to process weather data.
    """
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    logging.info(f"Starting weather data processing for {date_str}")

    try:
        input_directory = config['Paths']['input_directory']
        raw_data = extract_weather_data(input_directory, date_str)

        if not raw_data:
            logging.warning(f"No data found for {date_str}")
            return False

        transformed_data = transform_weather_data(raw_data)

        validation_result = validate_weather_data(transformed_data)

        if not validation_result['valid']:
            logging.error(f"Data validation failed: {validation_result['errors']}")
            return False

        db_path = os.path.join(config['Paths']['output_directory'], 'weather.db')
        success = load_weather_data(transformed_data, db_path)

        if success:
            logging.info(f"Weather data processing completed successfully for {date_str}")
            return True
        else:
            logging.error("Failed to load data into database")
            return False

    except Exception as e:
        logging.error(f"Error processing weather data: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    config = load_config()
    process_weather_data(config)