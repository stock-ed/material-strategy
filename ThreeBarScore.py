import logging
import time
import json
from redisUtil import AlpacaStreamAccess, KeyName
from redisPubsub import RedisSubscriber, RedisPublisher
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys
from redis3barScore import StudyThreeBarsScore
import asyncio
import threading
import sys

import alpaca_trade_api as alpaca
from alpaca_trade_api.stream import Stream
from MinuteBarStream import MinuteBarStream
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys

# from alpaca_trade_api.common import URL
# from redistimeseries.client import Client
# from alpaca_trade_api.rest import REST
# from redisTSBars import RealTimeBars

# Trade schema:
# T - string, message type, always “t”
# S - string, symbol
# i - int, trade ID
# x - string, exchange code where the trade occurred
# p - number, trade price
# s - int, trade size
# t - string, RFC-3339 formatted timestamp with nanosecond precision.
# c - array < string >, trade condition
# z - string, tape

#
# conn: Alpaca data stream - subscribe/unsubscribe to Alpaca.  Also get the real-time trade data.
# process: StudyThreeBarsScore - This scores each stock with the three bar studies.
# subscriber: RedisSubscriber - This subscribes/unsubscribes to the Alpaca real-time trade data.
#


def init():
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    # Alpaca real time stream data access
    global conn
    conn = AlpacaStreamAccess.connection()
    # conn.run()
    global process
    process = StudyThreeBarsScore()
    global subscriber
    subscriber = RedisSubscriber(
        KeyName.EVENT_NEW_CANDIDATES, callback=candidateEvent)
    subscriber.start()


# PUBLISH RPS_THREEBARSTACK_NEW "{ 'data': { 'subscribe' : '[AAPL, GOOG]', 'unsubscribe': '[]' }}"
# Handle real time trade data.  Process the pricing data with three bar studies.
# It scores the stock with the three bar studies.
#
async def _handleTrade(trade):
    # try:
    #     # make sure we have an event loop, if not create a new one
    #     loop = asyncio.get_event_loop()
    #     loop.set_debug(True)
    # except RuntimeError:
    #     asyncio.set_event_loop(asyncio.new_event_loop())
    data = {'symbol': trade['S'],
            'close': trade['p'], 'volume': trade['s']}
    print('TRADE: ', data)
    process.study(data)


#
# The system dynamically subscribe/unsubscribe to the real time trade stream
# based on which symbol has meet the three bar pattern.
#
def candidateEvent(data):
    print(data)
    if (conn == None):
        return
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    # subscribe to Alpaca Trade data Stream
    if ('subscribe' in data and len(data['subscribe']) > 0):
        for symbol in data['subscribe']:
            try:
                print('subscribe to: ', symbol)
                conn.subscribe_trades(_handleTrade, symbol)
            except Exception as e:
                print('subscribe failed: ', symbol)
                print(e)
    # unsubscribe from Alpaca Trade data Stream
    if ('unsubscribe' in data and len(data['unsubscribe']) > 0):
        for symbol in data['unsubscribe']:
            try:
                print('unsubscribe to: ', symbol)
                conn.unsubscribe_trades(symbol)
            except Exception as e:
                print('unsubscribe failed: ', symbol)
                print(e)

#
# The system dynamically subscribe/unsubscribe to the real time alpaca trade stream
# it also handles trade data and scores the three bar study.
#


def run():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    threading.Thread(target=init).start()

    loop = asyncio.get_event_loop()
    time.sleep(5)  # give the initial connection time to be established

    # subs = ['AAPL', 'FB']
    # unsubs = []
    # data = {'subscribe': subs, 'unsubscribe': unsubs}
    # publisher.publish(data)

    print('RUNNING...')
    while 1:
        pass


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        app = CreateRedisStockTimeSeriesKeys()
        app.run()
    # app = CreateRedisStockTimeSeriesKeys()
    # app.run()
    run()
