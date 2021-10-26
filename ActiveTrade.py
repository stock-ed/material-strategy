from redisUtil import KeyName
from redisHash import RedisHash
from redisPubsub import RedisPublisher, RedisSubscriber
import json


class ActiveTrade (RedisHash):

    def __init__(self, isPublish=True, r=None):
        super().__init__(key=KeyName.VARIABLE_ACTIVE_BARS, r=r)
        self.publisher = RedisPublisher(['EVENT_TRADE_ADD'])
        self.isPublish = isPublish

    def addSymbol(self, data):
        if (not self.isSymbolExist(data.symbol)):
            if self.isPublish:
                self.publisher.publish(data)
            self.add(data.symbol, [])

    def deleteSymbol(self, symbol):
        if (self.isSymbolExist(symbol)):
            self.delete(symbol)

    def deleteAll(self, symbols=None):
        allsymbols = symbols if symbols is None else self.getAllSymbols()
        for symbol in allsymbols:
            self.deleteSymbol(symbol)
        for symbol in allsymbols:
            if (self.isSymbolExist(symbol)):
                self.delete(symbol)


if __name__ == "__main__":
    candidate = ActiveTrade()
    app = redisSubscriber = RedisSubscriber(
        ['EVENT_CANDIDATE_SUBSCRIBE'], None, candidate.addSymbol)
    app.start()
