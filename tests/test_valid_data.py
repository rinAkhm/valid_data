import unittest
from main import create_tables, connect_db, check_json, recording_data
from unittest import TestCase
import os


class RecordingDbTestCase(TestCase):
    def setUp(self) -> None:
        self.name_database = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Goods.db"
        )
        self.file1 = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_content_goods_file.json"
        )
        self.json_schema = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "goods.schema.json",
        )
        self.data = check_json(self.file1, self.json_schema)

    def test_create_db(self):
        """Проверяет работу функции create_db."""
        con, cursor = connect_db(self.name_database)
        result = create_tables(con, cursor)
        self.assertTrue(result)

    def test_recording_data(self):
        """Проверяет работу функции recording_data."""
        con, cursor = connect_db(self.name_database)
        result = recording_data(con, cursor, self.data)
        self.assertTrue(result)

    def test_read_json(self):
        """Проверяет работу функции read_json."""
        self.assertIsInstance(self.data, dict)


if __name__ == '__main__':
    unittest.main()
