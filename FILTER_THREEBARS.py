import os

class Filter_ThreeBars:
    def __init__(self, data):
        self.data = data
        self.MinimumPriceJump = 0.2
        # convert string to integer

        self.MinimumPrice = float(os.environ.get(
            'THREEBAR_LIMIT_PRICE_LOW', '5.0'))
        self.MaximumPrice = float(os.environ.get(
            'THREEBAR_LIMIT_PRICE_HIGH', '20.0'))
        self.MinimumPercent = float(os.environ.get(
            'THREEBAR_LIMIT_PERCENT_LOW', '0.3'))
        self.MaximumPercent = float(os.environ.get(
            'THREEBAR_LIMIT_PERCENT_HIGH', '0.7'))

    def isFirstTwoBars(self, price0, price1, price2):
        if (price0 < self.MinimumPrice) or (price0 > self.MaximumPrice):
            return False
        first = price0 - price2
        second = price1 - price2
        if (abs(second) < self.MinimumPriceJump):
            return False
        percentage = 0 if second == 0 else first / second
        if percentage >= self.MinimumPercent and percentage < self.MaximumPercent:
            return True
        return False

    # This is the data format for the Stack.
    def barCandidate(self, firstPrice, secondPrice, timeframe, ts, op):
        return {"indicator": "price",
                "timeframe": timeframe,
                "filter": [firstPrice, secondPrice],
                "timestamp": ts,
                "operation": op
                }

    # It looks for 3 bar patterns on 3 or 4 bars.
    def filterCloses(self, prices):
        if len(prices) > 2 and self.isFirstTwoBars(prices[0][1], prices[1][1], prices[2][1]):
            return 3
        elif len(prices) > 3 and self.isFirstTwoBars(prices[0][1], prices[2][1], prices[3][1]):
            return 4
        else:
            return 0

    def filterVolumes(self, volumes):
        pass

    def getColumneData(self, data, column):
        result = []
        for item in data:
            item = (item['t'], item['c'])
            result.append(item)
        return result

    def getVolumeData(self, data):
        return self.getColumneData(data, 'v')

    def getCloseData(self, data):
        return self.getColumneData(data, 'c')

    def getCloses(self, dataList=None):
        if (dataList == None):
            dataList = self.data
        closes = self.getCloseData(dataList)
        return closes

    def filterOnCloses(self, timeframe, dataList=None):
        if (dataList == None):
            dataList = self.data
        closes = self.getCloseData(dataList)
        return self.filterCloses(closes, timeframe)

    def getVolumes(self, timeframe, dataList=None):
        if (dataList == None):
            dataList = self.data
        volumes = self.getVolumeData(dataList)
        return volumes

    def filterOnVolumes(self, timeframe, dataList=None):
        if (dataList == None):
            dataList = self.data
        volumes = self.getVolumeData(dataList)
        return self.filterVolumes(volumes, timeframe)
