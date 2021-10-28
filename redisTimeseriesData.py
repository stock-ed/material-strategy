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


class ComposeData:
    def __init__(self):
        pass

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

    def AdjustBars(self, data, timeframe, ts=time.time()):
        # get timestamp
        switcher = {
            RedisTimeFrame.MIN1: 60,
            RedisTimeFrame.MIN2: 120,
            RedisTimeFrame.MIN5: 300,
        }
        mins = switcher.get(timeframe, 60)
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


class RealTimeBars:
    def __init__(self, rts=None):
        self.rts: Client = TimeSeriesAccess.connection(rts)

    def RedisAddTrade(self, data):
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

    @staticmethod
    def TimeseriesRealtimeDataFormat(studyType, symbol, timeframe, data):
        return {
            'type': studyType,
            'symbol': symbol,
            'period': timeframe,
            'data': data
        }

    # def RedisAddBar(self, data):
    #     try:
    #         timeframe = RedisTimeFrame.MIN1
    #         ts = data['t'].seconds
    #         symbol = data['S']
    #         bar_list = []
    #         bar1 = (bar_key(symbol, "close", timeframe), ts, data['c'])
    #         bar2 = (bar_key(symbol, "high", timeframe), ts, data['h'])
    #         bar3 = (bar_key(symbol, "low", timeframe), ts, data['l'])
    #         bar4 = (bar_key(symbol, "open", timeframe), ts, data['o'])
    #         bar5 = (bar_key(symbol, "volume", timeframe), ts, data['v'])
    #         bar_list.append(bar1)
    #         bar_list.append(bar2)
    #         bar_list.append(bar3)
    #         bar_list.append(bar4)
    #         bar_list.append(bar5)
    #         self.rts.madd(bar_list)
    #     except Exception as e:
    #         print('RedisAddBar:', e)
    #         return None

    def RedisAddBar(self, data):
        try:
            timeframe = RedisTimeFrame.MIN1
            ts = data['t'].seconds
            symbol = data['S']
            bar_list = []
            self.rts.add(bar_key(symbol, "close", timeframe), ts, data['c'])
            self.rts.add(bar_key(symbol, "high", timeframe), ts, data['h'])
            self.rts.add(bar_key(symbol, "low", timeframe), ts, data['l'])
            self.rts.add(bar_key(symbol, "open", timeframe), ts, data['o'])
            self.rts.add(bar_key(symbol, "volume", timeframe), ts, data['v'])
        except Exception as e:
            print('RedisAddBar:', e)
            return None

    def _bar_realtime(self, rts, datatype, symbol, timeframe, startt, endt):
        try:
            key = bar_key(symbol, datatype, timeframe)
            close_prices = rts.revrange(key, from_time=startt, to_time=endt)
            return close_prices
        except Exception as e:
            print('_bar_realtime: ' + symbol + " - ", e)
            return None

    def _bar_readtime_adjust(self, rts, datatype, symbol, timeframe, startt, endt):
        data = self._bar_realtime(
            rts, datatype, symbol, timeframe, startt, endt)
        if data == [] or data is None:
            return []
        composeData = ComposeData()
        result = composeData.AdjustBars(data, timeframe)
        return result
        # if len(result) > 1:
        #     revResult = []
        #     for idx in range(len(result) - 1, -1, -1):
        #         revResult.append(result[idx])
        #     return revResult
        # else:
        #     return result

    def _bar_historical(self, symbol, timeframe, datatype, startt, endt):
        historical = AlpacaHistorical()
        result = historical.HistoricalPrices(
            symbol, timeframe, datatype, startt, endt)
        return result

    def mergeRealtimeData(self, symbol, close, open, high, low, volume):
        result = []
        for ix in range(len(close)):
            item = {
                "t": close[ix][0],
                "c": close[ix][1],
                'o': open[ix][1],
                'h': high[ix][1],
                'l': low[ix][1],
                'v': volume[ix][1]
            }
            result.append(item)
        return result

    def RedisGetRealtimeData(self, datatype, symbol, timeframe):
        switcher = {
            RedisTimeFrame.REALTIME: self._bar_realtime,
            RedisTimeFrame.MIN1:  self._bar_readtime_adjust,
            RedisTimeFrame.MIN2:  self._bar_readtime_adjust,
            RedisTimeFrame.MIN5:  self._bar_readtime_adjust,
            RedisTimeFrame.DAILY: self._bar_historical
        }
        callMethod = switcher.get(timeframe)
        ts = TimeStamp()
        startt = ts.get_starttime(timeframe)
        endt = ts.get_endtime(timeframe)
        close = callMethod(self.rts, 'close', symbol, timeframe, startt, endt)
        if close is None or len(close) < 5:
            data = self._bar_historical(
                symbol, timeframe, None, ts.DatetimeString(startt), ts.DatetimeString(endt))
            return RealTimeBars.TimeseriesRealtimeDataFormat("threebars", symbol, timeframe, data)
        else:
            open = callMethod(self.rts, 'open', symbol,
                              timeframe, startt, endt)
            high = callMethod(self.rts, 'high', symbol,
                              timeframe, startt, endt)
            low = callMethod(self.rts, 'low', symbol, timeframe, startt, endt)
            volume = callMethod(self.rts, 'volume', symbol,
                                timeframe, startt, endt)
            data = self.mergeRealtimeData(
                symbol, close, open, high, low, volume)
            return RealTimeBars.TimeseriesRealtimeDataFormat("threebars", symbol, timeframe, data)

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


# if __name__ == "__main__":
#     rts = RealTimeBars()
#     redisSubscriber = RedisSubscriber(['EVENT_BAR'], None, rts.RedisAddBar)
#     redisSubscriber.start()
