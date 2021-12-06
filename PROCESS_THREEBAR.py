
class Process_ThreeBar:

    def _isPriceRangeOptimal(self, newPrice, price1, price2):
        return (newPrice < price2 and newPrice > price1)

    # two scoring.  This one tests for optimal trade pattern.
    def _isPriceRangeUsable(self, newPrice, price1, price2):
        priceChange = price2 - price1
        priceTop = price2 + (priceChange / 2)
        if (newPrice >= price2 and newPrice < priceTop):
            return True
        return False

    #
    # score an individual stock pricing.
    # 4 point is given for optimal trade.
    # 2 point is given for acceptable trade.
    # 0 point is given for unacceptable trade.
    #
    def threeBarPlay(self, newPrice, price1, price2):
        point = 0
        if (self._isPriceRangeOptimal(newPrice, price1, price2)):
            point = 4
        if (self._isPriceRangeUsable(newPrice, price1, price2)):
            point = 2
        return point

    @staticmethod
    def run(newPrice, price1, price2):
        score = Process_ThreeBar()
        return score.threeBarPlay(newPrice, price1, price2)
