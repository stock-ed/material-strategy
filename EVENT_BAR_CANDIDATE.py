import logging
import json
from redisTimeseriesData import RealTimeBars
from redisPubsub import RedisSubscriber, RedisPublisher
from pubsubKeys import PUBSUB_KEYS
from redisUtil import RedisTimeFrame


class EventBarCandidate:
    '''A 1-Minute Bar happened.  Save the data.  And get 2 min and 5 min data for analysis later.'''

    def __init__(self, pubDataCheck=None, pubDataSave=None):
        self.rtb = RealTimeBars()
        self.publisher_check = RedisPublisher(
            PUBSUB_KEYS.EVENT_BAR_CANDIDATE_CHECK) if pubDataCheck is None else pubDataCheck
        self.publisher_save = RedisPublisher(
            PUBSUB_KEYS.EVENT_BAR_SAVE) if pubDataSave is None else pubDataSave
        self.subscriber = RedisSubscriber(
            PUBSUB_KEYS.EVENT_BAR_CANDIDATE, None, self.AddBar)

    def makePubData(self, symbol: str, timeframe: str, data: list):
        return {
            "symbol": symbol,
            "period": timeframe,
            "data": data
        }

    def publish2Min(self, symbol: str, timeframe: str):
        data2 = self.rtb.RedisGetRealtimeData(None, symbol, timeframe)
        logging.info(f"EventBarCandidate.publish2Min {symbol}")
        arrLen = len(data2['data'])
        if data2 is not None and arrLen >= 3:
            self.publisher_check.publish(data2)
        else:
            logging.info(
                f"EventBarCandidate.publish2Min: Not Enough {symbol} {timeframe} {arrLen}")

    def AddBar(self, data=None):
        try:
            symbol: str = ''
            if data is None:
                symbol = 'FANG'
            else:
                symbol = data['S']
                logging.info(
                    f"EVENT_BAR_CANDIDATE.EventBarCandidate.AddBar: {symbol} - {data} ")
                self.rtb.RedisAddBar(data)
                self.rtb.RedisAddBarAggregation(data)
            self.publish2Min(symbol, RedisTimeFrame.MIN2)
            self.publish2Min(symbol, RedisTimeFrame.MIN5)
        except Exception as e:
            logging.error(
                f"Error EVENT_BAR_CANDIDATE.EventBarCandidate.AddBar {e} {data} ")

    def start(self):
        try:
            self.subscriber.start()
        except KeyboardInterrupt:
            self.subscriber.stop()
        except Exception as e:
            logging.error(e)

    @staticmethod
    def run():
        logging.info("EVENT_BAR_CANDIDATE.EventBarCandidate.run")
        eventBarCandidate = EventBarCandidate()
        eventBarCandidate.start()


if __name__ == "__main__":
    logging.info("EVENT_BAR_CANDIDATE.EventBarCandidate.run")
    ebc = EventBarCandidate()
    #data2 = ebc.rtb.RedisGetRealtimeData(None, 'MSFT', RedisTimeFrame.MIN2)
    ebc.AddBar()
    # ebc.run()


# input:    { 'c': 136.02, 'h': 136.06, 'l': 136.0, 'o': 136.04, 'S': 'ALLE', 't': 1627493640000000000, 'v': 712})
# output:
# {
#     "symbol": "ALLE",
#     "period": "1MIN",
#     "data": [
#         {
#             "timestamp": 000010,
#             "close": 136.02,
#             "high": 136.06,
#             "low": 136.0,
#             "open": 136.04,
#             "volume": 712
#         },
#         {
#             "timestamp": 000009,
#             "close": 136.02,
#             "high": 136.06,
#             "low": 136.0,
#             "open": 136.04,
#             "volume": 712
#         },
#         {
#             "timestamp": 000008,
#             "close": 136.02,
#             "high": 136.06,
#             "low": 136.0,
#             "open": 136.04,
#             "volume": 712
#         },
#         {
#             "timestamp": 000007,
#             "close": 136.02,
#             "high": 136.06,
#             "low": 136.0,
#             "open": 136.04,
#             "volume": 712
#         },
#         {
#             "timestamp": 000006,
#             "close": 136.02,
#             "high": 136.06,
#             "low": 136.0,
#             "open": 136.04,
#             "volume": 712
#         }
#     ]
# }
