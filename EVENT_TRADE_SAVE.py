from redisTimeseriesData import RealTimeBars
from redisPubsub import RedisSubscriber
from pubsubKeys import PUBSUB_KEYS
import sys
import json
import logging


class EventTradeSave:
    def __init__(self):
        self.rtb = RealTimeBars()
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_TRADE_SAVE, None, self.addStock)

    def addStock(self, data):
        try:
            symbol = data['symbol']
            logging.info(f"EVENT_TRADE_SAVE.addStock: {symbol}")
            self.rtb.RedisAddTrade(data)
        except Exception as e:
            logging.warning(f"Error EVENT_TRADE_SAVE.addStock: {e} {data} ")

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("Starting EVENT_TRADE_SAVE.run: run")
        app = EventTradeSave()
        app.start()


if __name__ == "__main__":
    app: EventTradeSave = EventTradeSave()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        trade = {'symbol': 'FANG', 'close': 10.50, 'volume': 100}
        app.run(trade)
