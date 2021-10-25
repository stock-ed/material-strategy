##
# Create various Redis TimeSeries for storing stock prices
# and technical indicators
# Author: Prasanna Rajagopal
##

from alpaca_trade_api.rest import TimeFrame
from datetime import datetime, timedelta
from redisUtil import bar_key, TimeStamp, RedisTimeFrame, TimeSeriesAccess, AlpacaAccess
from redistimeseries.client import Client
import time
from alpacaHistorical import AlpacaHistorical
import json
import pandas as pd

# def bar_key(symbol, suffix, time_frame):
#     return "data_" + suffix + "_" + time_frame + ":" + symbol


class RealTimeBars:
    def __init__(self, rts=None):
        self.rts: Client = TimeSeriesAccess.connection(rts)

    def redisAddRealtime(self, data):
        timeframe = RedisTimeFrame.REALTIME
        ts = TimeStamp.now()
        symbol = data['symbol']
        bar_list = []
        bar1 = (bar_key(symbol, "close", timeframe), ts, data['close'])
        bar2 = (bar_key(symbol, "volume", timeframe), ts, data['volume'])
        bar_list.append(bar1)
        bar_list.append(bar2)
        # for bar in bar_list:
        #     self.rts.add(bar[0], bar[1], bar[2])
        self.rts.madd(bar_list)

    def redisAdd1Min(self, data):
        try:
            timeframe = RedisTimeFrame.MIN1
            ts = data['t'].seconds
            symbol = data['S']
            bar_list = []
            bar1 = (bar_key(symbol, "close", timeframe), ts, data['c'])
            bar2 = (bar_key(symbol, "high", timeframe), ts, data['h'])
            bar3 = (bar_key(symbol, "low", timeframe), ts, data['l'])
            bar4 = (bar_key(symbol, "open", timeframe), ts, data['o'])
            bar5 = (bar_key(symbol, "volume", timeframe), ts, data['v'])
            bar_list.append(bar1)
            bar_list.append(bar2)
            bar_list.append(bar3)
            bar_list.append(bar4)
            bar_list.append(bar5)
            # for bar in bar_list:
            #     self.rts.add(bar[0], bar[1], bar[2])
            self.rts.madd(bar_list)
        except Exception as e:
            print('redisAdd1Min:', e)
            return None

    def _timeframe_start(self, timeframe):
        switcher = {
            TimeFrame.Minue: datetime.now() - timedelta(days=7),
            TimeFrame.Hour: datetime.now() - timedelta(days=90),
            TimeFrame.Day: datetime.now() - timedelta(days=360),
        }
        dt = switcher.get(timeframe, datetime.now())
        date_string = dt.strftime('%Y-%m-%d')
        return date_string
        # return "2021-02-08"

    def _timeframe_end(self, timeframe):
        dt = datetime.now()
        date_string = dt.strftime('%Y-%m-%d %h:%M:%s')
        return date_string
        # return "2021-02-10"

    # def _bar_adjustBar(self, prices, timeframe):
    #     # get timestamp
    #     ts = TimeStamp()
    #     switcher = {
    #         RedisTimeFrame.MIN1: 1,
    #         RedisTimeFrame.MIN2: 2,
    #         RedisTimeFrame.MIN5: 5,
    #     }
    #     mins = switcher.get(timeframe)
    #     pass

    def _bar_realtime(self, rts, datatype, symbol, timeframe):
        try:
            ts = TimeStamp()
            key = bar_key(symbol, datatype, timeframe)
            startt = ts.get_starttime(timeframe)
            endt = ts.get_endtime(timeframe)
            close_prices = rts.revrange(key, from_time=startt, to_time=endt)
            return close_prices
        except Exception as e:
            print('_bar_realtime: ' + symbol + " - ", e)
            return None

    def mergeTimeseries(self, code1, data1, code2, data2):
        return None

    def barRealtimeVolume(self, rts, symbol, timeframe):
        try:
            ts = TimeStamp()
            startt = ts.get_starttime(timeframe)
            endt = ts.get_endtime(timeframe)
            key1 = bar_key(symbol, 'close', timeframe)
            closes = rts.revrange(key1, from_time=startt, to_time=endt)
            key2 = bar_key(symbol, 'volume', timeframe)
            volumes = rts.revrange(key2, from_time=startt, to_time=endt)
            return self.mergeTimeseries('c', closes, 'v', volumes)
        except Exception as e:
            print('_bar_realtime: ' + symbol + " - ", e)
            return None

    def firstTimestamp(self, now: int, ts1: int, mins: int):
        if (ts1 + mins) < now:
            return self.firstTimestamp(now, ts1 + mins, mins)
        else:
            return ts1

    def composeStockData(self, stamps: [], data):
        result = []
        for ts in stamps:
            isFound = False
            value = -1
            for item in data:
                if ts >= item[0]:
                    oneitem = (ts, item[1])
                    result.append(oneitem)
                    value = item[1]
                    isFound = True
                    break
            if not isFound and value >= 0:
                oneitem = (ts, value)
                result.append(oneitem)
        return result

    def _bar_adjustBar(self, data, timeframe, ts=time.time()):
        # get timestamp
        switcher = {
            RedisTimeFrame.MIN1: 60,
            RedisTimeFrame.MIN2: 120,
            RedisTimeFrame.MIN5: 300,
        }
        mins = switcher.get(timeframe, 60) * 1000
        tstamps = []
        ts1 = self.firstTimestamp(ts, data[0][0], mins)
        tstamps.append(ts1 - mins * 4)
        tstamps.append(ts1 - mins * 3)
        tstamps.append(ts1 - mins * 2)
        tstamps.append(ts1 - mins)
        tstamps.append(ts1)
        result = self.composeStockData(tstamps, data)
        # reverse result array
        return result

    def _bar_readtime_adjust(self, rts, datatype, symbol, timeframe):
        data = self._bar_realtime(rts, datatype, symbol, timeframe)
        if data == [] or data is None:
            return []
        result = self._bar_adjustBar(data, timeframe)
        if len(result) > 1:
            revResult = []
            for idx in range(len(result) - 1, -1, -1):
                revResult.append(result[idx])
            return revResult
        else:
            return result

    def _bar_historical(self, symbol, timeframe, datatype):
        historical = AlpacaHistorical()
        data = historical.HistoricalPrices(symbol, timeframe, datatype)
        return data
        # api = AlpacaAccess.connection()
        # ts = TimeStamp()
        # startt = ts.get_starttime(timeframe)
        # endt = ts.get_endtime(timeframe)

        # bar_iter = api.get_bars_iter(
        #     symbol, timeframe, startt, endt, limit=1000, adjustment='raw')
        # return bar_iter

    def redisGetRealtimeData(self, datatype, symbol, timeframe):
        switcher = {
            RedisTimeFrame.REALTIME: self._bar_realtime,
            RedisTimeFrame.MIN1:  self._bar_readtime_adjust,
            RedisTimeFrame.MIN2:  self._bar_readtime_adjust,
            RedisTimeFrame.MIN5:  self._bar_readtime_adjust,
            RedisTimeFrame.DAILY: self._bar_historical
        }
        callMethod = switcher.get(timeframe)
        data = callMethod(self.rts, datatype, symbol, timeframe)
        if data is None or len(data) < 10:
            return self._bar_historical(symbol, timeframe, datatype)

    def RedisGetDataClose(self, api, symbol, timeframe):
        return self.redisGetRealtimeData("close", symbol, timeframe)

    def RedisGetDataVolume(self, api, symbol, timeframe):
        data1 = self.redisGetRealtimeData("close", symbol, timeframe)
        data2 = self.redisGetRealtimeData("volume", symbol, timeframe)
        data = {'close': data1, 'volume': data2}
        df = pd.DataFrame(data)
        return df.to_json(orient='records')

    def _get_active_stocks(self, rts, assets):
        # remove all active stocks
        rts.zrembyrank('active_stocks', 0, -1)
        for asset in assets:
            rts.zadd('active_stocks', 0, assets.symbol)
        print('get active stocks')

    # ts.queryindex INDICATOR=max TIMEFRAME=1MIN

    def all_keys(self):
        symbols = []
        for key in self.rts.queryindex(['INDICATOR=max', 'TIMEFRAME=1MIN']):
            symbol = key.split(':')[1]
            symbols.append(symbol)
        return symbols
