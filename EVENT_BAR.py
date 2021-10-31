import logging
import time
import sys
from redisUtil import AlpacaStreamAccess
from redisPubsub import RedisPublisher
import time
import alpaca_trade_api as alpaca
from alpaca_trade_api.stream import Stream
from pubsubKeys import PUBSUB_KEYS
import json


class MinuteBarStream:
    publisher: RedisPublisher = None
    stream: Stream = None

    @staticmethod
    def init(conn: Stream = None):
        try:
            MinuteBarStream.publisher = RedisPublisher(
                PUBSUB_KEYS.EVENT_BAR_CANDIDATE)
            if (conn == None):
                MinuteBarStream.stream = AlpacaStreamAccess.connection()
            else:
                MinuteBarStream.stream = conn
        except Exception as e:
            logging.warning(f'Exception from websocket connection (init): {e}')

    @staticmethod
    def run_connection(conn):
        try:
            conn.run()
        except Exception as e:
            logging.warning(f'Exception from websocket connection: {e}')
        finally:
            logging.info("Trying to re-establish connection")
            time.sleep(3)
            MinuteBarStream.run_connection(conn)

    #
    # bar['t'].seconds = 1635351060, nanoseconds=0 - timestamp, matches local time
    # {"T": "b", "S": "POLY", "o": 24.05, "c": 24.04, "h": 24.06, "l": 24.04, "v": 1354, "t": 0, "n": 31, "vw": 24.049808}
    # timestamp is not serialzieable, so we need to convert it to a string.
    @staticmethod
    async def handleBar(bar):
        # logging.info('bar: ', bar)
        seconds = bar['t'].seconds
        bar['t'] = seconds
        MinuteBarStream.publisher.publish(bar)

    @staticmethod
    def start():
        try:
            MinuteBarStream.stream.subscribe_bars(
                MinuteBarStream.handleBar, '*')

            MinuteBarStream.stream.run()
            MinuteBarStream.run_connection(MinuteBarStream.stream)
        except Exception as e:
            logging.warning(
                f'Exception from websocket connection (start): {e}')

    @staticmethod
    def run():
        logging.info('MinuteBarStream.run()')
        MinuteBarStream.init()
        MinuteBarStream.start()


if __name__ == "__main__":
    MinuteBarStream.run()
