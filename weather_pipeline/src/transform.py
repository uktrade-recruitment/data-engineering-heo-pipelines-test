import logging
from datetime import datetime

def transform_weather_data(raw_data):
    """
    Transform raw weather data by:
    - Converting temperature from Fahrenheit to Celsius
    - Standardizing station names
    - Converting timestamp strings to datetime objects
    - Removing any records with missing critical data

    Args:
        raw_data: List of dictionaries containing raw weather data

    Returns:
        List of dictionaries containing transformed weather data
    """
    logging.info(f"Transforming {len(raw_data)} weather records")

    transformed_data = []

    for record in raw_data:
        try:
            if not all(key in record and record[key] for key in ['station_id', 'timestamp', 'temperature']):
                continue

            transformed_record = {}

            transformed_record['station_id'] = record['station_id']
            transformed_record['station_name'] = record['station_name'].strip().upper() if 'station_name' in record else None

            try:
                timestamp = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')
                transformed_record['timestamp'] = timestamp.isoformat()
                transformed_record['date'] = timestamp.strftime('%Y-%m-%d')
            except ValueError:
                logging.warning(f"Invalid timestamp format for record {record['station_id']}: {record['timestamp']}")
                continue

            # Question: What is the expected format for temperature in the raw data?
            try:
                temp_f = float(record['temperature'])
                temp_c = (temp_f - 32) * 5/9
                transformed_record['temperature'] = round(temp_c, 2)
            except (ValueError, TypeError):
                logging.warning(f"Invalid temperature value: {record['temperature']}")
                continue
        
            if 'humidity' in record and record['humidity']:
                try:
                    transformed_record['humidity'] = float(record['humidity'])
                except (ValueError, TypeError):
                    transformed_record['humidity'] = None
            else:
                transformed_record['humidity'] = None

            if 'wind_speed' in record and record['wind_speed']:
                try:
                    transformed_record['wind_speed'] = float(record['wind_speed'])
                except (ValueError, TypeError):
                    transformed_record['wind_speed'] = None
            else:
                transformed_record['wind_speed'] = None

            if 'precipitation' in record and record['precipitation']:
                try:
                    transformed_record['precipitation'] = float(record['precipitation'])
                except (ValueError, TypeError):
                    transformed_record['precipitation'] = 0.0
            else:
                transformed_record['precipitation'] = 0.0

            transformed_data.append(transformed_record)

        except Exception as e:
            logging.error(f"Error transforming record {record}: {str(e)}")

    logging.info(f"Transformation complete - {len(transformed_data)} valid records processed.")
    return transformed_data