from redisUtil import RedisTimeFrame, KeyName, SetInterval
from redisTSBars import RealTimeBars
from redisHash import StoreStack, ActiveBars
from datetime import datetime
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys
import time
import sys


# StudyThreeBarsFilter

class StudyThreeBarsFilter:
    _MinimumPriceJump = 0.2

    #
    # return a column in a array matrix
    #
    @staticmethod
    def _column(matrix, i):
        return [row[i] for row in matrix]

    # In 3 bar play, it looks for a pattern like this.
    # price = [2, 4, 3].  There is a sharp rise of price from 2 to 4.
    # and it follows a drop to 3 (or 50% retrace).  This pattern may happen
    # across 3 or 4 bars.  We are looking for that pattern between 3 prices passed in.
    #

    @staticmethod
    def _isFirstTwoBars(price0, price1, price2):
        if (price0 < 3) or (price0 > 20):
            return False
        first = price0 - price2
        second = price1 - price2
        if (abs(second) < StudyThreeBarsFilter._MinimumPriceJump):
            return False
        percentage = 0 if second == 0 else first / second
        if percentage >= 0.3 and percentage < 0.7:
            return True
        return False

    # This is the data format for the Stack.
    @staticmethod
    def barCandidate(symbol, firstPrice, secondPrice, thirdPrice, timeframe):
        return {'symbol': symbol, 'value': {
            'firstPrice': firstPrice,
            'secondPrice': secondPrice,
            'thirdPrice': thirdPrice,
            'timeFrame': timeframe
        }}

    # It looks for 3 bar patterns on 3 or 4 bars.
    @staticmethod
    def potentialList(symbol, prices, timeframe):
        if len(prices) > 2 and StudyThreeBarsFilter._isFirstTwoBars(prices[0][1], prices[1][1], prices[2][1]):
            return True, StudyThreeBarsFilter.barCandidate(symbol, prices[0][1], prices[1][1], prices[2][1], timeframe)
        elif len(prices) > 3 and StudyThreeBarsFilter._isFirstTwoBars(prices[0][1], prices[2][1], prices[3][1]):
            return True, StudyThreeBarsFilter.barCandidate(symbol, prices[0][1], prices[2][1], prices[3][1], timeframe)
        else:
            return False, {}
        # else:
        #     return {'symbol': symbol, 'value': {
        #         'firstPrice': 14.00,
        #         'secondPrice': 15.00,
        #         'thirdPrice': 14.52,
        #     }}


#
# This class filters the Acitve Bars (stocks that are moving)
# and filter out the stocks that meets the 3 bar criteria.
# It is saved to a redis hash table.  It is named STUDYTHREEBARSTACK
# or just stack.
# It also manages subscribe/unsubscribe table for Alpaca Stream.
# We subscribe/unsubscribe to real time data stream for the
# real-time live data.  We subscribe to the trade stream of the
# stocks taht are in the Stack
#
class StudyThreeBarsCandidates:

    def __init__(self, stack: StoreStack = None):
        # StoreStack: class to access the redis Stack.
        if (stack == None):
            self.stack = StoreStack()
        else:
            self.stack = stack
        # rtb: RealTimeBars.  access to the redis real time data (timeseries).
        self.rtb: RealTimeBars = RealTimeBars()
        # store: A temporary list to hold Stack candidates.
        self.store = []
        # ab: ActiveBars.  List of stocks that has its 1 minute data updated.
        self.ab = ActiveBars()

    # get all potential 3 bar candidates and return a merged list.
    def getAllKeys(self):
        symbols = []
        symbols1 = self.ab.getAllSymbols().copy()
        symbols2 = self.stack.getAllSymbols().copy()
        symbols = set(symbols1 + symbols2)
        # the ActiveBars is deleted once they are processed.
        self.ab.deleteAll(symbols)
        return symbols

    # remove a symbol from the Stack.
    # this happens when a stock price no longer matches the 3 bar pattern.

    def deleteScoreOfCandidate(self, redis, symbol):
        try:
            redis.hdel(KeyName.KEY_THREEBARSCORE, symbol)
        except Exception as e:
            print(e)

    # Looking at a stock, and see if it matches the 3 bar pattern.
    # the result is saved in the class store, and process to
    # Stack, subscribe and unsubscribe table.
    def _candidate(self, symbol, timeframe, getPriceData):
        prices = getPriceData(None, symbol, timeframe)
        addData, data = StudyThreeBarsFilter.potentialList(
            symbol, prices, timeframe)
        if addData:
            # package = json.dumps(data)
            self.store.append(data)

    # return all symbols stored in the Stack (not used)
    def getStacks(self):
        self.stack.getAll()

    def run(self, keys=None, getPriceData=None):
        try:
            # get all active symbols from the ActiveBars and Stack.
            if (keys == None):
                keys = self.getAllKeys()
                tables = CreateRedisStockTimeSeriesKeys()
                tables.CreateRedisStockSymbol(keys)
            # default method to get the price data.
            if (getPriceData == None):
                getPriceData = self.rtb.RedisGetDataClose
            # for each active symbol, check and see if they match the 3 bar pattern.
            for symbol in keys:
                self._candidate(symbol, RedisTimeFrame.MIN5, getPriceData)
                self._candidate(symbol, RedisTimeFrame.MIN2, getPriceData)
            # add the symbol candidates to Stack and subscribe/unsubscribe to the trade stream.
            self.stack.openMark()
            for stock in self.store:
                self.stack.addSymbol(stock['symbol'], stock)
            self.stack.closeMark()
            print('done')
        except Exception as e:
            print('run: ' + e)


def testGetPriceData(item, symbol, timeframe):
    return [
        (1603713600, 13.47),
        (1603712700, 14.49),
        (1603711800, 12.42),
        (1603710900, 12.40),
        (1603710000, 0.49),
        (1603709100, 1.01),
        (1603708200, 0.37)
    ]


app: StudyThreeBarsCandidates = None

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        keys = ['FANG']
        app = StudyThreeBarsCandidates()
        app.run(keys, testGetPriceData)
    else:
        print('StudyThreeBarsCandidates Begin')
        app = StudyThreeBarsCandidates()
        obj_now = datetime.now()
        secWait = 60 - obj_now.second
        time.sleep(secWait + 4)
        app.run()
        SetInterval(15, app.run)
