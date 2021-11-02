import logging
from redisHash import RedisHash
from redisUtil import KeyName, TimeStamp
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS


class EventTradeAdd(RedisHash):
    def __init__(self, key=None, r=None):
        if key is None:
            key = KeyName.KEY_LAST_TRADE
        super().__init__(key, r)
        self.subscriber = RedisSubscriber(PUBSUB_KEYS.EVENT_TRADE_ADD, None, self.NewTrade):
        self.publisher = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_NEW)

    def NewTrade(self, data):
        try:
            ts1 = TimeStamp.now()
            symbol = data['symbol']
            ts2 = 0
            try:
                ts2 = self.value(symbol)['seconds']
            except:
                ts2 = 0
            if abs(ts1 - ts2) >= 5:
                data = {'seconds': ts1}
                self.add(symbol, data)
                self.publisher.publish(data)
            self.publisher.publish(data)
        except Exception as e:
            logging.error(e)

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("EventTradeAdd start.")
        app = EventTradeAdd()
        app.start()
