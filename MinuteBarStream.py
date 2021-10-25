import logging
import time
import sys
from redisUtil import AlpacaStreamAccess
from redisPubsub import StreamBarsPublisher, StreamBarsSubscriber
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys
from redisTSBars import RealTimeBars
from datetime import datetime
import time
import alpaca_trade_api as alpaca
from alpaca_trade_api.stream import Stream
from redisHash import ActiveBars


class MinuteBarStream:
    log = None
    publisher: StreamBarsPublisher = None
    stream: Stream = None

    @staticmethod
    def init(conn: Stream = None):
        MinuteBarStream.log = logging.getLogger(__name__)
        MinuteBarStream.publisher = StreamBarsPublisher()
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

    @staticmethod
    async def handleBar(bar):
        # print('bar: ', bar)
        # bar['t'] = 0

        # publish/subscribe
        MinuteBarStream.publisher.publish(bar)

\

        @staticmethod
    def run():
        logging.basicConfig(level=logging.INFO)
        MinuteBarStream.stream.subscribe_bars(MinuteBarStream.handleBar, '*')

        MinuteBarStream.stream.run()
        MinuteBarStream.run_connection(MinuteBarStream.stream)


if __name__ == "__main__":
    MinuteBarStream.init()
    MinuteBarStream.run()
