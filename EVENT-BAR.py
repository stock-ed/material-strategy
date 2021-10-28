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
    log = None
    publisher: RedisPublisher = None
    stream: Stream = None

    @staticmethod
    def init(conn: Stream = None):
        MinuteBarStream.log = logging.getLogger(__name__)
        MinuteBarStream.publisher = RedisPublisher(
            PUBSUB_KEYS.EVENT_BAR_CANDIDATE)
        if (conn == None):
            MinuteBarStream.stream = AlpacaStreamAccess.connection()
        else:
            MinuteBarStream.stream = conn

    @staticmethod
    def run_connection(conn):
        try:
            conn.run()
        except Exception as e:
            print(f'Exception from websocket connection: {e}')
        finally:
            print("Trying to re-establish connection")
            time.sleep(3)
            MinuteBarStream.run_connection(conn)

    #
    # bar['t'].seconds = 1635351060, nanoseconds=0 - timestamp, matches local time
    # {"T": "b", "S": "POLY", "o": 24.05, "c": 24.04, "h": 24.06, "l": 24.04, "v": 1354, "t": 0, "n": 31, "vw": 24.049808}
    # timestamp is not serialzieable, so we need to convert it to a string.
    @staticmethod
    async def handleBar(bar):
        # print('bar: ', bar)
        bar['t'] = 0
        data = json.dumps(bar)
        MinuteBarStream.publisher.publish(bar)

    @staticmethod
    def run():
        try:
            logging.basicConfig(level=logging.INFO)
            MinuteBarStream.stream.subscribe_bars(
                MinuteBarStream.handleBar, '*')

            MinuteBarStream.stream.run()
            MinuteBarStream.run_connection(MinuteBarStream.stream)
        except Exception as e:
            print(f'Exception from websocket connection: {e}')


if __name__ == "__main__":
    MinuteBarStream.init()
    MinuteBarStream.run()
