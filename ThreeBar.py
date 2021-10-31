import logging
import sys
from multiprocessing import Process
from RedisTimeseriesTable import TimeseriesTable
from EVENT_BAR import MinuteBarStream
from EVENT_BAR_CANDIDATE import EventBarCandidate
from EVENT_BAR_CANDIDATE_CHECK import StudyThreeBarsCandidates
from EVENT_BAR_STACK_ADD import RedisStack
from EVENT_BAR_TRADE_ADD import RedisTradeSubscription
from EVENT_TRADE import StreamTradeRun
from EVENT_TRADE_NEW import TradeNewStock
from EVENT_TRADE_SAVE import EventTradeSave
from EVENT_TRADE_PROCESS import EventTradeScoreProcess
from EVENT_TRADE_SCORE import EventTradeScore


def main(isCreateTable=True):
    if (isCreateTable):
        tables = TimeseriesTable()
        tables.run()
    p01 = Process(target=MinuteBarStream.run)
    p01.start()
    p02 = Process(target=EventBarCandidate.run)
    p02.start()
    p03 = Process(target=StudyThreeBarsCandidates.run)
    p03.start()
    p04 = Process(target=RedisStack.run)
    p04.start()
    p05 = Process(target=RedisTradeSubscription.run)
    p05.start()
    p06 = Process(target=StreamTradeRun)
    p06.start()
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
        main(False)
    else:
        main()
