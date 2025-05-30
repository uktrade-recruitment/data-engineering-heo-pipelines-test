import logging
import re

def validate_weather_data(data):
    """
    Validate the transformed weather data.

    Args:
        data: List of transformed weather data dictionaries

    Returns:
        Dictionary with validation results
    """
    logging.info(f"Validating {len(data)} weather records")

    result = {
        'valid': True,
        'errors': []
    }

    if not data:
        result['valid'] = False
        result['errors'].append("No data to validate")
        return result

    required_fields = ['station_id', 'station_name', 'timestamp', 'date', 'temperature']

    for i, record in enumerate(data):
        for field in required_fields:
            if field not in record or record[field] is None:
                result['valid'] = False
                result['errors'].append(f"Record {i} (station_id: {record.get('station_id', 'Unknown')}) missing required field: {field}")

        if 'timestamp' in record and record['timestamp']:
            if not re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', record['timestamp']):
                result['valid'] = False
                result['errors'].append(f"Record {i} (station_id: {record.get('station_id', 'Unknown')}) has invalid timestamp format: {record['timestamp']}")

        if 'temperature' in record and record['temperature'] is not None:
            temp = record['temperature']
            if not (-60 <= temp <= 60):
                result['valid'] = False
                result['errors'].append(f"Record {i} (station_id: {record.get('station_id', 'Unknown')}) has invalid temperature: {temp}")

        if 'humidity' in record and record['humidity'] is not None:
            humidity = record['humidity']
            if not (0 <= humidity <= 100):
                result['valid'] = False
                result['errors'].append(f"Record {i} (station_id: {record.get('station_id', 'Unknown')}) has invalid humidity: {humidity}")

        if 'wind_speed' in record and record['wind_speed'] is not None:
            wind_speed = record['wind_speed']
            if wind_speed < 0:
                result['valid'] = False
                result['errors'].append(f"Record {i} (station_id: {record.get('station_id', 'Unknown')}) has negative wind speed: {wind_speed}")

        if 'precipitation' in record and record['precipitation'] is not None:
            precipitation = record['precipitation']
            if precipitation < 0:
                result['valid'] = False
                result['errors'].append(f"Record {i} (station_id: {record.get('station_id', 'Unknown')}) has negative precipitation: {precipitation}")

    if len(result['errors']) > 10:
        error_count = len(result['errors'])
        result['errors'] = result['errors'][:10]
        result['errors'].append(f"... and {error_count - 10} more errors")

    logging.info(f"Validation complete. Valid: {result['valid']}")
    if not result['valid']:
        logging.warning(f"Validation errors: {result['errors']}")

    return result