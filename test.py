
# data = {
#     "bars": [
#         {
#             "t": "2021-11-01T08:25:00Z",
#             "o": 332.9,
#             "h": 332.9,
#             "l": 332.9,
#             "c": 332.9,
#             "v": 694,
#             "n": 34,
#             "vw": 332.988963
#         },
#         {
#             "t": "2021-11-01T08:28:00Z",
#             "o": 332.9,
#             "h": 332.9,
#             "l": 332.9,
#             "c": 332.9,
#             "v": 419,
#             "n": 16,
#             "vw": 332.934129
#         }
#     ],
#     "symbol": "MSFT",
#     "next_page_token": "null"
# }

# from redisTimeseriesData import RealTimeBars
# from redisUtil import RedisTimeFrame, TimeStamp, AlpacaAccess
# from datetime import datetime

# rtb = RealTimeBars()


# def print_bar(data):
#     print("Bars:")
#     for bar in data["data"]:

#         # timestamp to datetime
#         dt_object = datetime.fromtimestamp(bar['t'])
#         # format datetime to string
#         dt_string = dt_object.strftime("%H:%M:%S")
#         bar['t'] = dt_string
#         print("\t{}".format(bar))


# def print_datetime_now():
#     # timestamp to datetime
#     dt_object = datetime.fromtimestamp(TimeStamp.now())
#     # format datetime to string
#     dt_string = dt_object.strftime("%H:%M:%S")
#     print("\t{}".format(dt_string))


# symbol = "FANG"
# data1 = rtb.RedisGetRealtimeData(None, symbol, RedisTimeFrame.MIN1)
# data2 = rtb.RedisGetRealtimeData(None, symbol, RedisTimeFrame.MIN2)
# data5 = rtb.RedisGetRealtimeData(None, symbol, RedisTimeFrame.MIN5)

# print_datetime_now()
# print_bar(data1)
# print_bar(data2)
# print_bar(data5)

# api = AlpacaAccess.connection()
# assets = api.list_assets(status='active')
# print(assets)

def sqr(x):
    return x ** 2


def takeit(func):
    data = func()
    print(data)


# use lambda
takeit(lambda: sqr(5))
