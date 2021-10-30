
import sys
import json
import logging
from redisHash import StoreScore
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS


class EventTradeScore:
    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.score: StoreScore = StoreScore()
        self.subscriber: RedisSubscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_TRADE_SCORE, None, self.addStock)

    def isNotScore(self, data, score):
        if score['type'] == data['type'] and score['period'] == data['period'] and score['indicator'] == data['indicator']:
            return False
        return True

    def addStock(self, data):
        try:
            logging.info(f'EventTradeScore: addStock: data: {data}')
            symbol = data['symbol']
            def x(a): return self.isNotScore(data, a)
            scores = [] if self.score.value(
                symbol) is None else self.score.value(symbol)
            if len(scores) > 0:
                scores = list(filter(x, scores))
                self.score.delete(symbol)
            scores.append(data)
            self.score.add(symbol, scores)
        except Exception as e:
            logging.warning(f'Error EventTradeScore: addStock: data: {e}')

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("EVENT_TRADE_SCORE.run: run")
        app = EventTradeScore()
        app.start()


if __name__ == "__main__":
    app: EventTradeScore = EventTradeScore()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        data = {"type": "threebars", "symbol": "FANG",
                "period": "5Min", "indicator": "price", "point": 0}
        app.run(data)
        print('done')
