from FILTER_THREEBAR import Filter_ThreeBar
from redisTimeseriesData import ComposeData, RealTimeBars
from redisUtil import bar_key, TimeStamp, RedisTimeFrame, TimeSeriesAccess, AlpacaAccess
from unittest import mock, TestCase


class TestFilterThreeBar(TestCase):
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_filter_pass_3bars(self):
        prices = [(1300, 14.0), (1240, 15.0), (1180, 13.0),
                  (1120, 12.95), (1060, 12.95)]
        ok, data = Filter_ThreeBar.potentialList(
            "", prices, RedisTimeFrame.MIN1)
        self.assertTrue(ok)

    def test_filter_pass_4bars(self):
        prices = [(1300, 14.0), (1240, 15.0), (1180, 15.0),
                  (1120, 13.0), (1060, 12.95)]
        ok, data = Filter_ThreeBar.potentialList(
            "", prices, RedisTimeFrame.MIN1)
        self.assertTrue(ok)

    def test_filter_fail_flat(self):
        prices = [(1300, 13.0), (1240, 13.0), (1180, 13.02),
                  (1120, 12.95), (1060, 12.95)]
        ok, data = Filter_ThreeBar.potentialList(
            "", prices, RedisTimeFrame.MIN1)
        self.assertFalse(ok)

    def test_filter_fail_price_low(self):
        prices = [(1300, 4.0), (1240, 5.0), (1180, 3.0),
                  (1120, 2.95), (1060, 2.95)]
        ok, data = Filter_ThreeBar.potentialList(
            "", prices, RedisTimeFrame.MIN1)
        self.assertFalse(ok)

    def test_filter_fail_price_too_high(self):
        prices = [(1300, 220.0), (1240, 230.0), (1180, 210.0),
                  (1120, 210.95), (1060, 210.95)]
        ok, data = Filter_ThreeBar.potentialList(
            "", prices, RedisTimeFrame.MIN1)
        self.assertFalse(ok)

    def test_filter_fail_too_many_bars(self):
        prices = [(1300, 14.01), (1240, 15.0), (1180, 15.02),
                  (1120, 15.95), (1060, 12.95)]
        ok, data = Filter_ThreeBar.potentialList(
            "", prices, RedisTimeFrame.MIN1)
        self.assertFalse(ok)
