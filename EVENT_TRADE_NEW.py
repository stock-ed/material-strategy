import sys
import json
import logging
from redisHash import StoreStack
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS
from EVENT_BAR_NEWS_ADD import AddNewsSymbol


class TradeNewStock:
    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.stack = StoreStack()
        self.publisherSave = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_SAVE)
        self.publisherProcess = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_PROCESS)
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_TRADE_NEW, None, self.addStock)

    def addStock(self, trade):
        try:
            logging.info(f"EVENT_TRADE_NEW.addStock {trade}")
            symbol = trade['symbol']
            stk = self.stack.value(symbol)
            if stk is not None:
                data = {"stack": stk, "trade": trade}
                self.publisherProcess.publish(data)
            self.publisherSave.publish(trade)
            # add news symbol
            logging.info(
                f"EVENT_TRADE_ADD.AddNewsSymbol Call: {symbol}")
            AddNewsSymbol(symbol)
        except Exception as e:
            logging.warning(f"Error EVENT_TRADE_NEW.addStock {e}")

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("Starting EVENT_TRADE_NEW.TradeNewStock: run")
        candidate = TradeNewStock()
        candidate.start()


if __name__ == "__main__":
    app: TradeNewStock = TradeNewStock()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        trade = {'symbol': 'FANG', 'close': 10.50, 'volume': 100}
        app.addStock(trade)
