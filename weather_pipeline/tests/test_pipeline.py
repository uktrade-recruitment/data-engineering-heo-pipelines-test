import os
import unittest
import tempfile
import sqlite3
from src.extract import extract_weather_data
from src.transform import transform_weather_data
from src.load import load_weather_data
from src.validation import validate_weather_data

class TestWeatherPipeline(unittest.TestCase):

    def setUp(self):
        # This creates temporary directory for testing
        self.test_dir = tempfile.mkdtemp()

        # Sample weather data
        self.sample_data = [
            {
                'station_id': 'STN001',
                'station_name': 'London',
                'timestamp': '2025-01-01 12:00:00',
                'temperature': '20.0',  # Fahrenheit - should convert to Celsius
                'humidity': '75.5',
                'wind_speed': '10.3',
                'precipitation': '0.5'
            },
            {
                'station_id': 'STN002',
                'station_name': 'Manchester',
                'timestamp': '2025-01-01 12:00:00',
                'temperature': '20.0',  # Fahrenheit - should convert to Celsius
                'humidity': '80.0',
                'wind_speed': '5.5',
                'precipitation': '1.2'
            }
        ]

    def test_transform_weather_data(self):
        """Test that transformation converts temperature correctly."""
        self.sample_data[0]['temperature'] = '68.0'  # 68.0°F -> 20.0°C
        self.sample_data[1]['temperature'] = '59.0'  # 59.0°F -> 15.0°C

        transformed = transform_weather_data(self.sample_data)
        self.assertEqual(len(transformed), 2)

        # (68.0 - 32) * 5/9 = 20.0°C
        self.assertAlmostEqual(transformed[0]['temperature'], 20.0)
        # (59.0 - 32) * 5/9 = 15.0°C
        self.assertAlmostEqual(transformed[1]['temperature'], 15.0)

        self.assertEqual(transformed[0]['station_name'], 'LONDON')

    def test_validation(self):
        """Test that validation correctly identifies issues."""
        # Valid data
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

        # Data with problems
        bad_data = [
            {
                'station_id': 'STN002',
                'station_name': 'MANCHESTER',
                'timestamp': '2025-01-01T12:00:00',
                'date': '2025-01-01',
                'temperature': 120.0,  # Invalid temperature
                'humidity': 75.5,
                'wind_speed': 10.3,
                'precipitation': 0.5
            }
        ]

        # Test good data passes validation
        result_good = validate_weather_data(good_data)
        self.assertTrue(result_good['valid'])

        # Test bad data fails validation
        result_bad = validate_weather_data(bad_data)
        self.assertFalse(result_bad['valid'])

    def test_load_weather_data(self):
        # Prepare sample transformed data
        transformed_data = transform_weather_data(self.sample_data)

        # This creates temporary directory for testing
        db_path = os.path.join(self.test_dir, 'weather.db')

        success = load_weather_data(transformed_data, db_path)
        print(success)
        self.assertTrue(success)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM weather_data')
        count = cursor.fetchone()[0]
        conn.close()

        self.assertEqual(count, 2)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.test_dir)

if __name__ == '__main__':
    unittest.main