##
# Create various Redis TimeSeries for storing stock prices
# and technical indicators
# Author: Prasanna Rajagopal
##

import alpaca_trade_api as alpaca
from alpaca_trade_api.rest import TimeFrame
from datetime import datetime, timedelta
from redisUtil import RedisAccess, TimeStamp, bar_key, RedisTimeFrame, AlpacaAccess, TimeSeriesAccess
from redistimeseries.client import Client


# def bar_key(symbol, suffix, time_frame):
#     return "data_" + suffix + "_" + time_frame + ":" + symbol


class TimeseriesTable:

    def __init__(self, rts=None):
        self.rts = TimeSeriesAccess.connection(rts)
        self.redis = RedisAccess.connection()
        api = AlpacaAccess.connection()
        self.assets = api.list_assets(status='active')

    def _createSymbolItem(self, rts, symbol, suffix, aggr, index, description, companyName, timeframe):
        name0 = bar_key(symbol, suffix, timeframe)
        retention = TimeStamp.retention_in_ms(timeframe)
        labels0 = {'SYMBOL': symbol, 'DESC': 'RELATIVE_STRENGTH_INDEX', 'INDEX': 'DJIA',
                   'TIMEFRAME': timeframe, 'INDICATOR': aggr}
        rts.create(name0, retention_msecs=retention,
                   labels=labels0)
        return name0

    def _createSymbol(self, rts, symbol, suffix, aggr, index, description, companyName):
        if suffix == 'close' or suffix == 'volume':
            name0 = self._createSymbolItem(rts, symbol, suffix, aggr,
                                           index, description, companyName, RedisTimeFrame.REALTIME)
            name10s = self._createSymbolItem(rts, symbol,  suffix, aggr,
                                             index, description, companyName, RedisTimeFrame.SEC10)
            rts.createrule(name0, name10s, aggr, 10*1000)
        name1 = self._createSymbolItem(rts, symbol, suffix, aggr,
                                       index, description, companyName, RedisTimeFrame.MIN1)
        name2 = self._createSymbolItem(rts, symbol, suffix, aggr,
                                       index, description, companyName, RedisTimeFrame.MIN2)
        name5 = self._createSymbolItem(rts, symbol, suffix, aggr,
                                       index, description, companyName, RedisTimeFrame.MIN5)
        # rts.createrule(name0, name1, aggr, 60*1000)
        rts.createrule(name1, name2, aggr, 2*60)
        rts.createrule(name1, name5, aggr, 5*60)

    def _createRedisStockSymbol(self, rts, symbol, index, description, companyName):
        self._createSymbol(rts, symbol, "high", "max",
                           index, description, companyName)
        self._createSymbol(rts, symbol, "low", "min",
                           index, description, companyName)
        self._createSymbol(rts, symbol, "open", "first",
                           index, description, companyName)
        self._createSymbol(rts, symbol, "close", "last",
                           index, description, companyName)
        self._createSymbol(rts, symbol, "volume", "sum",
                           index, description, companyName)

    def CreateRedisStockSymbol(self, symbols):
        for symbol in symbols:
            print(f"{symbol}  \t{symbol}")
            name0 = bar_key(symbol, 'close', RedisTimeFrame.MIN1)
            if not self.redis.exists(name0):
                self._createRedisStockSymbol(
                    self.rts, symbol, '', '', 'SYMBOL-' + symbol)

    def run(self):
        for asset in self.assets:
            print(f"{asset.symbol}  \t{asset.name}")
            self._createRedisStockSymbol(
                self.rts, asset.symbol, '', '', asset.name)


if __name__ == "__main__":
    app = TimeseriesTable()
    app.run()
