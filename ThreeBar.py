import logging
import sys
import time
from multiprocessing import Process
from RedisTimeseriesTable import TimeseriesTable
from EVENT_BAR_CANDIDATE import EventBarCandidate
from EVENT_BAR_CANDIDATE_CHECK import StudyThreeBarsCandidates
from EVENT_BAR_STACK_ADD import RedisStack
from EVENT_BAR_TRADE_ADD import RedisTradeSubscription
from EVENT_TRADE_NEW import TradeNewStock
from EVENT_TRADE_SAVE import EventTradeSave
from EVENT_TRADE_PROCESS import EventTradeScoreProcess
from EVENT_TRADE_SCORE import EventTradeScore
from EVENT_REALTIME_DATA import RealTimeData
from EVENT_BAR_NEWS_ADD import RedisEventAddNewsSymbol


def ThreadRun():
    # multi threading class
    time.sleep(5)  # give the initial connection time to be established
    EventBarCandidate.run()
    StudyThreeBarsCandidates.run()
    RedisStack.run()
    RedisTradeSubscription.run()
    TradeNewStock.run()
    EventTradeSave.run()
    EventTradeScoreProcess.run()
    EventTradeScore.run()

    while 1:
        time.sleep(1)


def run(isCreateTable=True):
    if (isCreateTable):
        tables = TimeseriesTable()
        tables.run()
    p01 = Process(target=RealTimeData)
    p01.start()
    p02 = Process(target=ThreadRun)
    p02.start()

    while 1:
        time.sleep(1)


def main(isCreateTable=True):
    if (isCreateTable):
        tables = TimeseriesTable()
        tables.run()
    p01 = Process(target=RealTimeData)
    p01.start()
    time.sleep(5)  # give the initial connection time to be established
    p02 = Process(target=EventBarCandidate.run)
    p02.start()
    p03 = Process(target=StudyThreeBarsCandidates.run)
    p03.start()
    p04 = Process(target=RedisStack.run)
    p04.start()
    p05 = Process(target=RedisTradeSubscription.run)
    p05.start()
    p07 = Process(target=TradeNewStock.run)
    p07.start()
    p08 = Process(target=EventTradeSave.run)
    p08.start()
    p09 = Process(target=EventTradeScoreProcess.run)
    p09.start()
    p10 = Process(target=EventTradeScore.run)
    p10.start()
    # p11 = Process(target=RedisEventAddNewsSymbol.run)
    # p11.start()
    while 1:
        time.sleep(1)


if __name__ == "__main__":
    formatter = '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s'
    logging.basicConfig(level=logging.WARN, format=formatter,
                        datefmt='%d-%b-%y %H:%M:%S', filename="three-bar.log")
    logging.warning("ThreeBar.py Started")
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        main()
    else:
        main(False)
