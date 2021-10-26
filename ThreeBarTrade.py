import logging
import time
from redisUtil import AlpacaStreamAccess, KeyName
from redisPubsub import RedisSubscriber, RedisPublisher
import asyncio
import threading

# from alpaca_trade_api.common import URL
# from redistimeseries.client import Client
# from alpaca_trade_api.rest import REST
# from redisTimeseriesData import RealTimeBars

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
    global subscriber
    subscriber = RedisSubscriber(
        ['EVENT_TRADE_ADD'], callback=subscribeToTrade)
    subscriber.start()
    global publisher
    publisher = RedisPublisher(['EVENT_TRADE'])


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
    publisher.publish(data)


def subscribeToTrade(data):
    if (conn == None):
        return
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    try:
        symbol = data['symbol']
        op = data['operation']
        if (op == 'subscribe'):
            print('subscribe to: ', symbol)
            conn.subscribe_trades(_handleTrade, symbol)
        else:
            print('unsubscribe to: ', symbol)
            conn.unsubscribe_trades(symbol)
    except Exception as e:
        print('subscribe failed: ', symbol)
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
    run()
