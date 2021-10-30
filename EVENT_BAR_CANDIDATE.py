import logging
import json
from redisTimeseriesData import RealTimeBars
from redisPubsub import RedisSubscriber, RedisPublisher
from pubsubKeys import PUBSUB_KEYS
from redisUtil import RedisTimeFrame


class EventBarCandidate:
    '''A 1-Minute Bar happened.  Save the data.  And get 2 min and 5 min data for analysis later.'''

    def __init__(self):
        self.rtb = RealTimeBars()
        self.publisher_check = RedisPublisher(
            PUBSUB_KEYS.EVENT_BAR_CANDIDATE_CHECK)
        self.publisher_save = RedisPublisher(
            PUBSUB_KEYS.EVENT_BAR_SAVE)
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
        logging.info("EventBarCandidate.publish2Min " + json.dumps(data2))
        arrLen = len(data2['data'])
        if data2 is not None and arrLen > 4:
            self.publisher_check.publish(data2)
        else:
            logging.info(
                f"EventBarCandidate.publish2Min: Not Enough {arrLen}")

    def AddBar(self, data=None):
        try:
            logging.info(
                f"EVENT_BAR_CANDIDATE.EventBarCandidate.AddBar:{data} ")
            symbol: str = ''
            if data is None:
                symbol = 'FANG'
            else:
                symbol = data['S']
                self.rtb.RedisAddBar(data)
            self.publish2Min(symbol, RedisTimeFrame.MIN2)
            self.publish2Min(symbol, RedisTimeFrame.MIN5)
        except Exception as e:
            logging.error(
                f"Error EVENT_BAR_CANDIDATE.EventBarCandidate.AddBar{e}")

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
