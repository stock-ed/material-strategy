import logging
import sys
import json
from redisHash import StoreTradeSubscription
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS


class RedisTradeSubscription:
    def __init__(self):
        self.subscription = StoreTradeSubscription()
        self.publisher = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_SUBSCRIBE)
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_BAR_TRADE_ADD, None, self.addStock)

    def addStock(self, data):
        logging.info(f"EVENT_TRADE_ADD.RedisTradeSubscription.start: {data}")
        try:
            symbol = data['symbol']
            if not self.subscription.isSymbolExist(symbol) and data['action']['operation'] == "ADD":
                self.subscription.set(symbol)
                data = {"symbol": symbol,
                        "operation": "SUBSCRIBE"}
                self.publisher.publish(data)
        except Exception as e:
            logging.error(
                f"Error EVENT_TRADE_ADD.RedisTradeSubscription.start: {e}")
            sys.exit(1)

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("EVENT_TRADE_ADD.RedisTradeSubscription.run")
        candidate = RedisTradeSubscription()
        candidate.start()
        # app = RedisSubscriber(
        #     PUBSUB_KEYS.EVENT_BAR_TRADE_ADD, None, candidate.start)
        # app.start()


if __name__ == "__main__":
    app: RedisTradeSubscription = None
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        data = {"type": "threebars", "symbol": "FANG", "period": "2Min",
                "data": [
                    {"t": 1635370320, "c": 10.4, "o": 10.6,
                        "h": 10.8, "l": 10.15, "v": 2000.0},
                    {"t": 1635370200, "c": 10.6, "o": 10.6,
                        "h": 10.8, "l": 10.25, "v": 2000.0},
                    {"t": 1635370080, "c": 10.2, "o": 10.3,
                        "h": 10.5, "l": 10.05, "v": 2000.0},
                    {"t": 1635370320, "c": 10.7, "o": 10.1, "h": 10.8, "l": 10.05, "v": 2000.0}],
                "action":
                    {"indicator": "price", "timeframe": "2Min",
                     "filter": [10.4, 10.6], "timestamp": 1635370080, "operation": "ADD"}}
        app = RedisTradeSubscription()
        app.start(data)
        print('done')
