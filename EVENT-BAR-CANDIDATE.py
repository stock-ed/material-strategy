from redisTimeseriesData import RealTimeBars
from redisPubsub import RedisSubscriber, RedisPublisher
from pubsubKeys import PUBSUB_KEYS
from redisUtil import RedisTimeFrame
import json

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


class EventBarCandidate:
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

    def AddBar(self, data=None):
        symbol: str = ''
        if data is None:
            symbol = 'FANG'
        else:
            self.rtb.AddBar(data)
            symbol = data['S']
            self.rtb.RedisAddBar(data)
        timeframe = RedisTimeFrame.MIN2
        data2 = self.rtb.RedisGetRealtimeData(None, symbol, timeframe)
        if data2 is not None and len(data2) > 4:
            self.publisher_check.publish(data2)
        timeframe = RedisTimeFrame.MIN5
        data5 = self.rtb.RedisGetRealtimeData(None, symbol, timeframe)
        if data5 is not None and len(data2) > 4:
            self.publisher_check.publish(data5)

    def run(self):
        self.subscriber.start()


if __name__ == "__main__":
    ebc = EventBarCandidate()
    #data2 = ebc.rtb.RedisGetRealtimeData(None, 'MSFT', RedisTimeFrame.MIN2)
    ebc.AddBar()
    # ebc.run()
