import logging
import sys
from multiprocessing import Process
from redisTimeseriesTable import TimeseriesTable
from EVENT_BAR import MinuteBarStream
from EVENT_BAR_CANDIDATE import EventBarCandidate
from EVENT_BAR_CANDIDATE_CHECK import StudyThreeBarsCandidates
from EVENT_BAR_STACK_ADD import RedisStack
from EVENT_BAR_TRADE_ADD import RedisTradeSubscription
from EVENT_TRADE import TradeStreamRun


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
    while 1:
        pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S', filename="three-bar.log")
    logging.info("ThreeBar.py Started")
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        main(False)
    else:
        main()
