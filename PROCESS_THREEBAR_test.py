from redisUtil import bar_key, TimeStamp, RedisTimeFrame, TimeSeriesAccess, AlpacaAccess
from unittest import mock, TestCase
from PROCESS_THREEBAR import Process_ThreeBar


class TestProcessThreeBar(TestCase):
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_process_pass_4(self):
        point = Process_ThreeBar.run(11.60, 11, 12)
        self.assertEqual(point, 4)

    def test_process_pass_2(self):
        point = Process_ThreeBar.run(11.10, 11, 12)
        self.assertEqual(point, 4)

    def test_process_fail_low(self):
        point = Process_ThreeBar.run(12.50, 11, 12)
        self.assertEqual(point, 0)

    def test_process_fail_high(self):
        point = Process_ThreeBar.run(10.0, 11, 12)
        self.assertEqual(point, 0)
