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


def main(isCreateTable=True):
    if (isCreateTable):
        tables = TimeseriesTable()
        tables.run()
    # p01 = Process(target=RealTimeData)
    # p01.start()
    # time.sleep(5)  # give the initial connection time to be established
    logging.info("Function called......")

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

    while 1:
        pass


if __name__ == "__main__":
    formatter = '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s'
    logging.basicConfig(level=logging.INFO, format=formatter,
                        datefmt='%d-%b-%y %H:%M:%S', filename="three-bar.log")
    logging.info("ThreeBar.py Started")
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        print('coming in if')
        main(False)
    else:
        print('coming in else')
        main()
