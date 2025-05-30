import os
import unittest
import tempfile
import sqlite3
from src.extract import extract_weather_data
from src.transform import transform_weather_data
from src.load import load_weather_data
from src.validation import validate_weather_data

class TestWeatherPipeline(unittest.TestCase):
    # This creates temporary directory for testing
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        # Sample weather data
        self.sample_data = [
            {
                'station_id': 'STN001',
                'station_name': 'London',
                'timestamp': '2025-01-01 12:00:00',
                'temperature': '20.0',
                'humidity': '75.5',
                'wind_speed': '10.3',
                'precipitation': '0.5'
            },
            {
                'station_id': 'STN002',
                'station_name': 'Manchester',
                'timestamp': '2025-01-01 12:00:00',
                'temperature': '20.0',
                'humidity': '80.0',
                'wind_speed': '5.5',
                'precipitation': '1.2'
            }
        ]

    def test_transform_weather_data(self):
        """Test that transformation converts temperature correctly."""
        test_data = [
            {'station_id': 'STN001', 'station_name': 'London', 'timestamp': '2025-01-01 12:00:00', 'temperature': '68.0', 'humidity': '75.5', 'wind_speed': '10.3', 'precipitation': '0.5'},
            {'station_id': 'STN002', 'station_name': 'Manchester', 'timestamp': '2025-01-01 12:00:00', 'temperature': '59.0', 'humidity': '80.0', 'wind_speed': '5.5', 'precipitation': '1.2'}
        ]
        transformed = transform_weather_data(test_data)
        self.assertEqual(len(transformed), 2)
        self.assertAlmostEqual(transformed[0]['temperature'], 20.0)
        self.assertAlmostEqual(transformed[1]['temperature'], 15.0)
        self.assertEqual(transformed[0]['station_name'], 'LONDON')

    def test_validation(self):
        """Test that validation correctly identifies issues."""
        good_data = [
            {
                'station_id': 'STN001',
                'station_name': 'LONDON',
                'timestamp': '2025-01-01T12:00:00',
                'date': '2025-01-01',
                'temperature': 20.0,
                'humidity': 75.5,
                'wind_speed': 10.3,
                'precipitation': 0.5
            }
        ]
        bad_data = [
            {
                'station_id': 'STN002',
                'station_name': 'MANCHESTER',
                'timestamp': '2025-01-01T12:00:00',
                'date': '2025-01-01',
                'temperature': 120.0,
                'humidity': 75.5,
                'wind_speed': 10.3,
                'precipitation': 0.5
            }
        ]
        result_good = validate_weather_data(good_data)
        self.assertTrue(result_good['valid'])
        result_bad = validate_weather_data(bad_data)
        self.assertFalse(result_bad['valid'])

    def test_load_weather_data(self):
        transformed_data = transform_weather_data(self.sample_data)
        db_path = os.path.join(self.test_dir, 'weather.db')
        success = load_weather_data(transformed_data, db_path)
        self.assertTrue(success)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM weather_data')
            count = cursor.fetchone()[0]
        self.assertEqual(count, 2)

    def test_transform_invalid_timestamp(self):
        invalid_data = [{'station_id': 'STN001', 'station_name': 'London', 'timestamp': 'invalid', 'temperature': '68.0'}]
        transformed = transform_weather_data(invalid_data)
        self.assertEqual(len(transformed), 0, "Should skip records with invalid timestamps")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.test_dir)

if __name__ == '__main__':
    unittest.main()