import logging
import time
import sys
import asyncio
import threading
from redisUtil import AlpacaStreamAccess, KeyName
from redisPubsub import RedisSubscriber, RedisPublisher
from pubsubKeys import PUBSUB_KEYS

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


async def handleBar(bar):
    # MinuteBarStream.rtb.RedisAddBar(bar)
    seconds = bar['t'].seconds
    bar['t'] = seconds
    print(bar)


def init() -> None:
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
        PUBSUB_KEYS.EVENT_TRADE_SUBSCRIBE, None, callback=subscribeToTrade)
    subscriber.start()
    global publisher
    publisher = RedisPublisher(PUBSUB_KEYS.EVENT_TRADE_NEW)
    # for debug only
    conn.subscribe_bars(handleBar, '*')


# PUBLISH RPS_THREEBARSTACK_NEW "{ 'data': { 'subscribe' : '[AAPL, GOOG]', 'unsubscribe': '[]' }}"
# Handle real time trade data.  Process the pricing data with three bar studies.
# It scores the stock with the three bar studies.
#
async def handleTrade(trade) -> None:
    # try:
    #     # make sure we have an event loop, if not create a new one
    #     loop = asyncio.get_event_loop()
    #     loop.set_debug(True)
    # except RuntimeError:
    #     asyncio.set_event_loop(asyncio.new_event_loop())
    data = {'symbol': trade['S'],
            'close': trade['p'], 'volume': trade['s']}
    print(data)
    # publisher.publish(trade)


def subscription(data, isTestOnly: bool = False) -> None:
    try:
        logging.info(f'EVENT-TRADE.subscription started')
        symbol = data['symbol']
        op = data['operation']
        if (op == 'SUBSCRIBE'):
            logging.info(f'subscribe to: {symbol}')
            if not isTestOnly:
                conn.subscribe_trades(handleTrade, symbol)
        else:
            logging.info('unsubscribe to: {symbol}')
            if not isTestOnly:
                conn.unsubscribe_trades(symbol)
    except Exception as e:
        logging.warning(f'EVENT-TRADE.subscription exception - {e}')


def subscribeToTrade(data) -> None:
    logging.info(f'EVENT-TRADE.subscribeToTrade start - {data}')
    try:
        if (conn == None):
            return
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    except Exception as e:
        logging.error(f'EVENT-TRADE.subscribeToTrade exception - {e}')
        return
    subscription(data)

#
# The system dynamically subscribe/unsubscribe to the real time alpaca trade stream
# it also handles trade data and scores the three bar study.
#


def StreamTradeRun():
    logging.info('StreamTradeRun')
    threading.Thread(target=init).start()

    loop = asyncio.get_event_loop()
    time.sleep(5)  # give the initial connection time to be established

    asyncio.run(sleepCall())

    app = RedisSubscriber(
        PUBSUB_KEYS.EVENT_TRADE_SUBSCRIBE, None, subscribeToTrade)
    app.start()

    print('RUNNING...')

    # ------------------------------------------------
    # subs = ['AAPL', 'FB']
    # unsubs = []
    # data = {'subscribe': subs, 'unsubscribe': unsubs}
    # publisher.publish(data)

    # while 1:
    #     pass


async def sleepCall():
    print(' sleepCall() --------------------------- ')
    await asyncio.sleep(5)
    data1 = {'symbol': 'AAPL', 'operation': 'SUBSCRIBE'}
    subscribeToTrade(data1)
    data2 = {'symbol': 'FB', 'operation': 'SUBSCRIBE'}
    subscribeToTrade(data2)


if __name__ == "__main__":
    # args = sys.argv[1:]
    # if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
    #     data = {"symbol": "FANG",
    #             "operation": "SUBSCRIBE"}
    #     subscription(data, True)
    # else:
    #     StreamTradeRun()
    StreamTradeRun()
