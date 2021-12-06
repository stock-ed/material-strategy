import sys
import json
import os
import logging
from pubsubKeys import PUBSUB_KEYS
from redisPubsub import RedisPublisher, RedisSubscriber
from FILTER_THREEBAR import Filter_ThreeBar

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
            _, result = Filter_ThreeBar.potentialList(
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
