import sys
import json
from redisHash import StoreStack
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS


class TradeNewStock:
    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.stack = StoreStack()
        self.publisherSave = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_SAVE)
        self.publisherProcess = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_PROCESS)

    def run(self, trade):
        symbol = trade['symbol']
        stk = self.stack.value(symbol)
        if stk is not None:
            data = {"stack": stk, "trade": trade}
            self.publihser.publisherProcess(data)
        self.publisherSave.publish(trade)


if __name__ == "__main__":
    app: TradeNewStock = TradeNewStock()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        trade = {'symbol': 'FANG', 'close': 10.50, 'volume': 100}
        app.run(trade)
    else:
        sub = RedisSubscriber(
            PUBSUB_KEYS.EVENT_TRADE_NEW, None, app.run)
        sub.start()
