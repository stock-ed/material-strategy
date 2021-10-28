
import sys
import json
from redisHash import StoreScore
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS


class EventTradeScore:
    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.score = StoreScore()

    def isNotScore(self, data, score):
        if score['type'] == data['type'] and score['period'] == data['period'] and score['indicator'] == data['indicator']:
            return False
        return True

    def run(self, data):
        symbol = data['symbol']
        def x(a): return self.isNotScore(data, a)
        scores = [] if self.score.value(
            symbol) is None else self.score.value(symbol)
        if len(scores) > 0:
            scores = list(filter(x, scores))
            self.score.delete(symbol)
        scores.append(data)
        self.score.add(symbol, scores)


if __name__ == "__main__":
    app: EventTradeScore = EventTradeScore()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        data = {"type": "threebars", "symbol": "FANG",
                "period": "5Min", "indicator": "price", "point": 0}
        app.run(data)
        print('done')
    else:
        sub = RedisSubscriber(
            PUBSUB_KEYS.EVENT_TRADE_NEW, app.run)
        sub.start()
