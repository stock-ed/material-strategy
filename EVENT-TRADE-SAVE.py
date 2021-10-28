from redisTimeseriesData import RealTimeBars
from redisPubsub import RedisSubscriber
from pubsubKeys import PUBSUB_KEYS
import sys
import json


class EventTradeSave:
    def __init__(self):
        self.rtb = RealTimeBars()

    def run(self, data):
        self.rtb.RedisAddTrade(data)


if __name__ == "__main__":
    app: EventTradeSave = EventTradeSave()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        trade = {'symbol': 'FANG', 'close': 10.50, 'volume': 100}
        app.run(trade)
    else:
        sub = RedisSubscriber(
            PUBSUB_KEYS.EVENT_TRADE_SAVE, app.run)
        sub.start()
