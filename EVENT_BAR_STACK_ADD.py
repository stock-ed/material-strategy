import sys
import json
import logging
from redisHash import StoreStack
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS


class RedisStack:

    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.stack = StoreStack()
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_BAR_STACK_ADD, None, self.addStock)

    def isFieldExist(self, store, data):
        if store is None:
            return False
        for field in store:
            if field['type'] == data['type'] and field['period'] == data['period']:
                if (data['action']['timestamp'] > field['action']['timestamp']):
                    return True, True
                else:
                    return True, False
        return False, True

    def popField(self, store, datatype, dataperiod):
        for field in store:
            if field['type'] == datatype and field['period'] == dataperiod:
                store.remove(field)
        return store

    def addStock(self, data):
        try:
            symbol = data['symbol']
            logging.info(f"EVENT-BAR-STACK-ADD: start - {symbol}")
            store = [] if self.stack.value(
                symbol) is None else self.stack.value(symbol)
            if data['action']["operation"] == 'ADD':
                fieldExist, newData = self.isFieldExist(store, data)
                if fieldExist and not newData:
                    return
                elif fieldExist:
                    store = self.popField(store, data['type'], data['period'])
                    self.stack.delete(symbol)
                    store.append(data)
                else:
                    store.append(data)
                self.stack.add(symbol, store)
            elif data['action']["operation"] == 'DEL':
                fieldExist, newData = self.isFieldExist(store, data)
                if fieldExist:
                    # store = self.popField(store, data['type'], data['period'])
                    self.stack.delete(symbol)
            else:
                logging.warning(f"Error: Unknown operation {data}")
        except Exception as e:
            logging.error(f"EVENT-BAR-STACK-ADD: start {e} {data}")

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("Starting RedisStack")
        candidate = RedisStack()
        candidate.start()


if __name__ == "__main__":
    app: RedisStack = None
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        data = {"type": "threebars", "symbol": "FANG", "period": "2Min",
                "data": [
                    {"t": 1635370080, "c": 10.4, "o": 10.6,
                        "h": 10.8, "l": 10.15, "v": 2000.0},
                    {"t": 1635369960, "c": 10.4, "o": 10.6,
                        "h": 10.8, "l": 10.15, "v": 2000.0},
                    {"t": 1635369840, "c": 10.4, "o": 10.6,
                        "h": 10.8, "l": 10.15, "v": 2000.0},
                    {"t": 1635369960, "c": 10.6, "o": 10.6,
                        "h": 10.8, "l": 10.25, "v": 2000.0},
                    {"t": 1635370080, "c": 10.2, "o": 10.3,
                        "h": 10.5, "l": 10.05, "v": 2000.0},
                    {"t": 1635370320, "c": 10.7, "o": 10.1, "h": 10.8, "l": 10.05, "v": 2000.0}],
                "action":
                    {"indicator": "price", "timeframe": "2Min",
                     "filter": [10.4, 10.6], "timestamp": 1635370080, "operation": "ADD"}}
        app = RedisStack()
        app.start(data)
        print('done')
