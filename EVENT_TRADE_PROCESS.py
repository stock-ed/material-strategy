import sys
import json
import logging
from redisHash import StoreStack
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS


class EventTradeScoreProcess:
    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.stack = StoreStack()
        self.publisher = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_SCORE)
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_TRADE_PROCESS, None, self.addStock)

    # two scoring.  This one tests for basic accpetable trade.

    def _isPriceRangeOptimal(self, newPrice, price1, price2):
        return (newPrice < price2 and newPrice > price1)

    # two scoring.  This one tests for optimal trade pattern.
    def _isPriceRangeUsable(self, newPrice, price1, price2):
        priceChange = price2 - price1
        priceTop = price2 + (priceChange / 2)
        if (newPrice >= price2 and newPrice < priceTop):
            return True
        return False

    #
    # score an individual stock pricing.
    # 4 point is given for optimal trade.
    # 2 point is given for acceptable trade.
    # 0 point is given for unacceptable trade.
    #
    def threeBarPlay(self, newPrice, price1, price2):
        point = 0
        if (self._isPriceRangeOptimal(newPrice, price1, price2)):
            point = 4
        if (self._isPriceRangeUsable(newPrice, price1, price2)):
            point = 2
        return point

    def addStock(self, data):
        try:
            logging.info(f"EVENT_TRADE_PROCESS.addStock {data}")
            stacks = data['stack']
            trade = data['trade']
            for stack in stacks:
                price1 = stack['action']['filter'][0]
                price2 = stack['action']['filter'][1]
                newPrice = trade['close']
                ts = stack['data'][0]['t']
                point = self.threeBarPlay(newPrice, price1, price2)
                data = {'type': stack['type'], 'symbol': stack['symbol'], 'period': stack['period'],
                        'indicator': stack['action']['indicator'], 'timestamp': ts, 'point': point,
                        'data': stack['data'], 'trade': trade}
                self.publisher.publish(data)
        except Exception as e:
            logging.warning(f"Error EVENT_TRADE_PROCESS.addStock {e}")

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("EVENT_TRADE_PROCESS.run: run")
        app = EventTradeScoreProcess()
        app.start()


if __name__ == "__main__":
    app: EventTradeScoreProcess = EventTradeScoreProcess()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        data = {"stack": [{"type": "threebars", "symbol": "FANG", "period": "2Min", "data": [{"t": 1635370080, "c": 10.4, "o": 10.6, "h": 10.8, "l": 10.15, "v": 2000.0}, {"t": 1635369960, "c": 10.4, "o": 10.6, "h": 10.8, "l": 10.15, "v": 2000.0}, {"t": 1635369840, "c": 10.4, "o": 10.6, "h": 10.8, "l": 10.15, "v": 2000.0}, {"t": 1635369960, "c": 10.6, "o": 10.6, "h": 10.8,
                                                                                                                                                                                                                                                                                                                                     "l": 10.25, "v": 2000.0}, {"t": 1635370080, "c": 10.2, "o": 10.3, "h": 10.5, "l": 10.05, "v": 2000.0}, {"t": 1635370320, "c": 10.7, "o": 10.1, "h": 10.8, "l": 10.05, "v": 2000.0}], "action": {"indicator": "price", "timeframe": "2Min", "filter": [10.4, 10.6], "timestamp": 1635370080, "operation": "ADD"}}], "trade": {"symbol": "FANG", "close": 10.5, "volume": 100}}
        app.addStock(data)
