import sys
import json
import os
import logging
from pubsubKeys import PUBSUB_KEYS
from redisPubsub import RedisPublisher, RedisSubscriber


# StudyThreeBarsFilter

class StudyThreeBarsFilter:
    _MinimumPriceJump = 0.2
    _MinimumPrice = os.environ.get(
        'THREEBAR_LIMIT_PRICE_LOW', 5.0)
    _MaximumPrice = os.environ.get(
        'THREEBAR_LIMIT_PRICE_HIGH', 20.0)
    _MinimumPercent = os.environ.get(
        'THREEBAR_LIMIT_PERCENT_LOW', 0.3)
    _MaximumPercent = os.environ.get(
        'THREEBAR_LIMIT_PERCENT_HIGH', 0.7)

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
        if (price0 < StudyThreeBarsFilter._MinimumPrice) or (price0 > StudyThreeBarsFilter._MaximumPrice):
            return False
        first = price0 - price2
        second = price1 - price2
        if (abs(second) < StudyThreeBarsFilter._MinimumPriceJump):
            return False
        percentage = 0 if second == 0 else first / second
        if percentage >= StudyThreeBarsFilter._MinimumPercent and percentage < StudyThreeBarsFilter._MaximumPercent:
            return True
        return False

    # This is the data format for the Stack.
    @staticmethod
    def barCandidate(firstPrice, secondPrice, timeframe, ts, op):
        return {"indicator": "price",
                "timeframe": timeframe,
                "filter": [firstPrice, secondPrice],
                "timestamp": ts,
                "operation": op
                }

    # It looks for 3 bar patterns on 3 or 4 bars.
    @staticmethod
    def potentialList(symbol, prices, timeframe):
        if len(prices) > 2 and StudyThreeBarsFilter._isFirstTwoBars(prices[0][1], prices[1][1], prices[2][1]):
            return True, StudyThreeBarsFilter.barCandidate(prices[0][1], prices[1][1], timeframe, prices[0][0], 'ADD')
        elif len(prices) > 3 and StudyThreeBarsFilter._isFirstTwoBars(prices[0][1], prices[2][1], prices[3][1]):
            return True, StudyThreeBarsFilter.barCandidate(prices[0][1], prices[2][1], timeframe, prices[0][0], 'ADD')
        else:
            return False, StudyThreeBarsFilter.barCandidate(0, 0, timeframe, prices[0][0], 'DEL')
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

    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.publisher = RedisPublisher(PUBSUB_KEYS.EVENT_BAR_STACK_ADD)
        self.publisherTrade = RedisPublisher(PUBSUB_KEYS.EVENT_BAR_TRADE_ADD)
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_BAR_CANDIDATE_CHECK, None, self.filterCheck)

    # return all symbols stored in the Stack (not used)
    def getStacks(self):
        self.stack.getAll()

    def getPriceData(self, data):
        result = []
        for item in data:
            item = (item['t'], item['c'])
            result.append(item)
        return result

    def filterCheck(self, data):
        try:
            symbol = data['symbol']
            logging.info(
                f'EVENT_BAR_CANDIDATE_CHECK.StudyThreeBarsCandidates.filterCheck {symbol}')
            timeframe = data['period']
            prices = self.getPriceData(data['data'])
            _, result = StudyThreeBarsFilter.potentialList(
                symbol, prices, timeframe)
            data['action'] = result
            self.publisher.publish(data)
            self.publisherTrade.publish(data)
            print('done')
        except Exception as e:
            logging.warning(
                f'Error EVENT_BAR_CANDIDATE_CHECK.StudyThreeBarsCandidates.filterCheck - {data} {e}')

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.warning(
                f'Error EVENT_BAR_CANDIDATE_CHECK.StudyThreeBarsCandidates.start - {e}')

    @staticmethod
    def run():
        logging.info('EVENT_BAR_CANDIDATE_CHECK.StudyThreeBarsCandidates.run')
        app = StudyThreeBarsCandidates()
        app.start()


if __name__ == "__main__":
    app: StudyThreeBarsCandidates = None
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        data = {"type": "threebars", "symbol": "FANG", "period": "2Min",
                "data": [
                    {"t": 1635369840, "c": 10.4, "o": 10.6,
                        "h": 10.8, "l": 10.15, "v": 2000.0},
                    {"t": 1635369960, "c": 10.6, "o": 10.6,
                        "h": 10.8, "l": 10.25, "v": 2000.0},
                    {"t": 1635370080, "c": 10.2, "o": 10.3,
                        "h": 10.5, "l": 10.05, "v": 2000.0},
                    {"t": 1635370200, "c": 10.7, "o": 10.1,
                        "h": 10.8, "l": 10.05, "v": 2000.0},
                    {"t": 1635370320, "c": 10.7, "o": 10.1,
                        "h": 10.8, "l": 10.05, "v": 2000.0}
                ]}
        app = StudyThreeBarsCandidates()
        app.filterCheck(data)
