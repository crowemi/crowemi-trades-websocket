import unittest
from datetime import datetime

from server import get_record


class TestGetRecord(unittest.TestCase):
    def test_get_record(self):
        now = datetime.utcnow()
        record = get_record(datetime(now.year, now.month, now.day, 0, 0, 0, 0))
        self.assertTrue(record)
