from redis3barScore import StudyThreeBarsScore
from redisTimeseriesData import RealTimeBars
from redisHash import StoreStack, StoreScore
from redisUtil import RedisTimeFrame
import requests
import os
import json


class TechnicalAnalysisThreeBars:
    def __init__(self):
        self.threebars = StudyThreeBarsScore()
        self.rtb = RealTimeBars()
        self.stack = StoreStack()

    def priceScore(self, rtData):
        process = StudyThreeBarsScore()
        process.study(rtData)

    def volumeScore(self, rtData):
        url = os.getenv('URL_OBV', 'http://0.0.0.0:8102/study/obv')
        symbol = rtData['symbol']
        timeframe = RedisTimeFrame.MIN1
        data = self.rtb.RedisGetDataVolume(None, symbol, timeframe)
        jsondata = json.dumps(data)
        # http post with json body text
        result = requests.post(url, data=jsondata)
        print(result)

    def run(self, rtData):
        self.volumeScore(rtData)
        self.priceScore(rtData)


if __name__ == "__main__":
    data = {
        'symbol': 'FANG',
        'close': 10.45,
        'volume': 100
    }
    print('TRADE: ', data)
    ta = TechnicalAnalysisThreeBars()
    ta.run(data)
