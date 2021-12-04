import sys
import json
import logging
from redisHash import StoreStack
from redisPubsub import RedisPublisher, RedisSubscriber
from pubsubKeys import PUBSUB_KEYS
import os
import requests


def AddNewsSymbol(symbol):
    try:
        logging.warning(f"AddNewsSymbol: start - {symbol}")

        isEnableNewsAnalysis = os.environ.get('ENABLE_NEWS_ANALYSIS', True)
        logging.warning(f"AddNewsSymbol: start - {isEnableNewsAnalysis}")
        if isEnableNewsAnalysis:
            # rest api post with json body  {"symbol": "AAPL"}
            url = os.environ.get(
                'URL_NEWS_SYMBOL', 'http://0.0.0.0:3004/symbol')

            response = requests.post(url, json={"symbol": symbol})
            logging.warning(f"AddNewsSymbol: response - {response}")
            if response.status_code == 200:
                logging.info(
                    f"AddNewsSymbol: success - {response.status_code}")
            else:
                logging.info(
                    f"AddNewsSymbol: error - {response.status_code}")
    except Exception as e:
        logging.error(f"ERROR - AddNewsSymbol: start {e} {data}")


class RedisEventAddNewsSymbol:

    def __init__(self):
        # StoreStack: class to access the redis Stack.
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_BAR_TRADE_ADD, None, self.addNewsSymbol)
        self.url = os.environ.get(
            'URL_NEWS_SYMBOL', 'http://0.0.0.0:3004/symbol')

    def addNewsSymbol(self, data):
        try:
            symbol = data['symbol']
            logging.warning(f"EVENT_BAR_NEWS_ADD: start - {symbol}")
            # rest api post with json body  {"symbol": "AAPL"}

            response = requests.post(self.url, json={"symbol": symbol})
            logging.warning(f"EVENT_BAR_NEWS_ADD: response - {response}")
            if response.status_code == 200:
                logging.info(
                    f"EVENT_BAR_NEWS_ADD: success - {response.status_code}")
            else:
                logging.info(
                    f"EVENT_BAR_NEWS_ADD: error - {response.status_code}")
        except Exception as e:
            logging.error(f"ERROR - EVENT_BAR_NEWS_ADD: start {e} {data}")

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        isEnableNewsAnalysis = os.environ.get('ENABLE_NEWS_ANALYSIS', False)
        logging.warn("Starting RedisEventAddNewsSymbol")
        if isEnableNewsAnalysis == 'True':
            candidate = RedisEventAddNewsSymbol()
            candidate.start()


if __name__ == "__main__":
    formatter = '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s'
    logging.basicConfig(level=logging.WARN, format=formatter,
                        datefmt='%d-%b-%y %H:%M:%S', filename="three-bar.log")
    RedisEventAddNewsSymbol.run()
    data = {'symbol': 'FANG'}
    pub = RedisPublisher(PUBSUB_KEYS.EVENT_BAR_TRADE_ADD)
    pub.publish({"symbol": "AAPL"})
