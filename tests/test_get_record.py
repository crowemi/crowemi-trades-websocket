import sys
import unittest
from datetime import datetime, timedelta
from time import sleep


class TestGetRecord(unittest.TestCase):
    def test_get_record(self):
        # now = datetime.utcnow()
        # now = datetime(now.year, now.month, now.day, 0, 0, 0, 0)
        # now = datetime(2023, 8, 6, 23, 58)
        # record = Producer().get_record(now)
        # self.assertTrue(record)
        print(sys.path)
        assert True

class TestGetPeriod(unittest.TestCase):
    def test_time_periods(self):
        current_period = update_period()
        last_period = current_period - timedelta(minutes=1)
        
        while True:
            if current_period > last_period:
                print(f"current period: {current_period}")
                last_period = current_period
            else:
                print(f"waiting...")
                sleep(30)
                current_period = update_period()


def update_period() -> datetime:
    now = datetime.utcnow()
    return datetime(
        now.year,
        now.month,
        now.day,
        now.hour,
        now.minute,
        0,
        0,
    )