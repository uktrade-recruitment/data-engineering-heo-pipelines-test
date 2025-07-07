import logging

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

    # Question: What are the required fields for validation?
    required_fields = ['station_id', 'station_name', 'timestamp', 'date', 'temperature']

    for i, record in enumerate(data):
        for field in required_fields:
            if field not in record or record[field] is None:
                result['valid'] = False
                result['errors'].append(f"Record {i} missing required field: {field}")

        # A reasonable range for a temperature is -60 to 60 degrees Celsius
        if 'temperature' in record and record['temperature'] is not None:
            temp = record['temperature']
            if not (-60 <= temp <= 60):
                result['valid'] = False
                result['errors'].append(f"Record {i} has invalid temperature: {temp}")

        # Humidity can only be between 0 and 100 percent
        if 'humidity' in record and record['humidity'] is not None:
            humidity = record['humidity']
            if not (0 <= humidity <= 100):
                result['valid'] = False
                result['errors'].append(f"Record {i} has invalid humidity: {humidity}")

        # Wind speed should be positive
        if 'wind_speed' in record and record['wind_speed'] is not None:
            wind_speed = record['wind_speed']
            if wind_speed < 0:
                result['valid'] = False
                result['errors'].append(f"Record {i} has negative wind speed: {wind_speed}")

        # Precipitation should be positive
        if 'precipitation' in record and record['precipitation'] is not None:
            precipitation = record['precipitation']
            if precipitation < 0:
                result['valid'] = False
                result['errors'].append(f"Record {i} has negative precipitation: {precipitation}")

    # If we have more than 10 errors, just summarise, we don't have all day
    if len(result['errors']) > 10:
        error_count = len(result['errors'])
        result['errors'] = result['errors'][:10]
        result['errors'].append(f"... and {error_count - 10} more errors")

    logging.info(f"Validation complete. Valid: {result['valid']}")
    if not result['valid']:
        logging.warning(f"Validation errors: {result['errors']}")

    return result