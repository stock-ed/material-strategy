from FILTER_THREEBAR import Filter_ThreeBar, Filter_3Bars
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

    def test_filter_3bar_pass_3bars(self):
        tf = 60 * 2
        sample = [
            {"t": 1000000 + 4*tf, "o": 12.36, "h": 13.28, "l": 10.42,
                "c": 12.21, "v": 18055355, "n": 218268, "vw": 11.728827},
            {"t": 1000000 + 3*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 2*tf, "o": 9.81, "h": 15.09, "l": 9.26,
                "c": 10.01, "v": 27158104, "n": 289329, "vw": 12.771049},
            {"t": 1000000 + tf,   "o": 11.06, "h": 12.31, "l": 8.69,
                "c": 9.51, "v": 25925275, "n": 299119, "vw": 10.268236},
            {"t": 1000000,        "o": 35.16, "h": 12.50, "l": 12.43,
                "c": 9.65, "v": 33315153, "n": 401535, "vw": 8.758565}
        ]
        filter = Filter_3Bars(sample, RedisTimeFrame.MIN2)
        ok, data = filter.run()
        self.assertTrue(ok)

    def test_filter_3bar_pass_4bars(self):
        tf = 60 * 2
        sample = [
            {"t": 1000000 + 5*tf, "o": 12.36, "h": 13.28, "l": 10.42,
                "c": 12.21, "v": 18055355, "n": 218268, "vw": 11.728827},
            {"t": 1000000 + 4*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 3*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 2*tf, "o": 9.81, "h": 15.09, "l": 9.26,
                "c": 10.01, "v": 27158104, "n": 289329, "vw": 12.771049},
            {"t": 1000000 + tf,   "o": 11.06, "h": 12.31, "l": 8.69,
                "c": 9.51, "v": 25925275, "n": 299119, "vw": 10.268236},
            {"t": 1000000,        "o": 35.16, "h": 12.50, "l": 12.43,
                "c": 9.65, "v": 33315153, "n": 401535, "vw": 8.758565}
        ]
        filter = Filter_3Bars(sample, RedisTimeFrame.MIN2)
        ok, data = filter.run()
        self.assertTrue(ok)

    def test_filter_3bar_fail_flat(self):
        tf = 60 * 2
        sample = [
            {"t": 1000000 + 5*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.10, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 4*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.04, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 3*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.02, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 2*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + tf,   "o": 11.06, "h": 12.31, "l": 8.69,
                "c": 13.51, "v": 25925275, "n": 299119, "vw": 10.268236},
            {"t": 1000000,        "o": 35.16, "h": 12.50, "l": 12.43,
                "c": 13.65, "v": 33315153, "n": 401535, "vw": 8.758565}
        ]
        filter = Filter_3Bars(sample, RedisTimeFrame.MIN2)
        ok, data = filter.run()
        self.assertFalse(ok)

    def test_filter_3bar_fail_price_low(self):
        tf = 60 * 2
        sample = [
            {"t": 1000000 + 4*tf, "o": 12.36, "h": 13.28, "l": 10.42,
                "c": 2.21, "v": 18055355, "n": 218268, "vw": 11.728827},
            {"t": 1000000 + 3*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 4.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 2*tf, "o": 9.81, "h": 15.09, "l": 9.26,
                "c": 1.01, "v": 27158104, "n": 289329, "vw": 12.771049},
            {"t": 1000000 + tf,   "o": 11.06, "h": 12.31, "l": 8.69,
                "c": 1.51, "v": 25925275, "n": 299119, "vw": 10.268236},
            {"t": 1000000,        "o": 35.16, "h": 12.50, "l": 12.43,
                "c": 1.65, "v": 33315153, "n": 401535, "vw": 8.758565}
        ]
        filter = Filter_3Bars(sample, RedisTimeFrame.MIN2)
        ok, data = filter.run()
        self.assertFalse(ok)

    def test_filter_3bar_fail_price_too_high(self):
        tf = 60 * 2
        sample = [
            {"t": 1000000 + 4*tf, "o": 12.36, "h": 13.28, "l": 10.42,
                "c": 112.21, "v": 18055355, "n": 218268, "vw": 11.728827},
            {"t": 1000000 + 3*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 114.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 2*tf, "o": 9.81, "h": 15.09, "l": 9.26,
                "c": 110.01, "v": 27158104, "n": 289329, "vw": 12.771049},
            {"t": 1000000 + tf,   "o": 11.06, "h": 12.31, "l": 8.69,
                "c": 109.51, "v": 25925275, "n": 299119, "vw": 10.268236},
            {"t": 1000000,        "o": 35.16, "h": 12.50, "l": 12.43,
                "c": 109.65, "v": 33315153, "n": 401535, "vw": 8.758565}
        ]
        filter = Filter_3Bars(sample, RedisTimeFrame.MIN2)
        ok, data = filter.run()
        self.assertFalse(ok)

    def test_filter_3bar_fail_too_many_bars(self):
        tf = 60 * 2
        sample = [
            {"t": 1000000 + 6*tf, "o": 12.36, "h": 13.28, "l": 10.42,
                "c": 12.21, "v": 18055355, "n": 218268, "vw": 11.728827},
            {"t": 1000000 + 5*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 4*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 3*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 25296111, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 2*tf, "o": 9.81, "h": 15.09, "l": 9.26,
                "c": 10.01, "v": 27158104, "n": 289329, "vw": 12.771049},
            {"t": 1000000 + tf,   "o": 11.06, "h": 12.31, "l": 8.69,
                "c": 9.51, "v": 25925275, "n": 299119, "vw": 10.268236},
            {"t": 1000000,        "o": 35.16, "h": 12.50, "l": 12.43,
                "c": 9.65, "v": 33315153, "n": 401535, "vw": 8.758565}
        ]
        filter = Filter_3Bars(sample, RedisTimeFrame.MIN2)
        ok, data = filter.run()
        self.assertFalse(ok)

    def test_filter_3bar_volume_too_low(self):
        tf = 60 * 2
        sample = [
            {"t": 1000000 + 4*tf, "o": 12.36, "h": 13.28, "l": 10.42,
                "c": 12.21, "v": 180, "n": 218268, "vw": 11.728827},
            {"t": 1000000 + 3*tf, "o": 12.76, "h": 13.24, "l": 10.37,
                "c": 14.01, "v": 252, "n": 269886, "vw": 11.458344},
            {"t": 1000000 + 2*tf, "o": 9.81, "h": 15.09, "l": 9.26,
                "c": 10.01, "v": 271, "n": 289329, "vw": 12.771049},
            {"t": 1000000 + tf,   "o": 11.06, "h": 12.31, "l": 8.69,
                "c": 9.51, "v": 259, "n": 299119, "vw": 10.268236},
            {"t": 1000000,        "o": 35.16, "h": 12.50, "l": 12.43,
                "c": 9.65, "v": 333, "n": 401535, "vw": 8.758565}
        ]
        filter = Filter_3Bars(sample, RedisTimeFrame.MIN2)
        ok, data = filter.run()
        self.assertFalse(ok)
