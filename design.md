# FILE

RealtimeDataTable.py

- Create realtime data tables. \_0, \_1MIN, \_2MIN, \_5MIN
- redisTSCreateTable.CreateRedisStockTimeSeriesKeys()

# FILE

RealtimeBarData.py

- MinuteBarStream.MinuteBarStream()
- Handle 1 Minute Bar
- EVENT_BAR
  - bar Bar({ 'c': 136.02, 'h': 136.06, 'l': 136.0, 'o': 136.04, 'S': 'ALLE', 't': 1627493640000000000, 'v': 712})
  - Save data to 1 Minute Bar
  - publih, EVENT_BAR_CANDIDATE_CHECK

# FILE

redisTSBars.py -> TimeSeriesData.py
GetData() - return combined data.
AddData() - add combined data
ResetData() - remove all data

ThreeBarCandidate.py

- EVENT_BAR_CANDIDATE_CHECK (subscribe)
  {
  "symbol": "ALLE",
  "period": "1MIN",
  "data": [
  { 'timestamp': 000010, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
  { 'timestamp': 000009, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
  { 'timestamp': 000008, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
  { 'timestamp': 000007, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
  { 'timestamp': 000006, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 }
  ]
  }
  - Run the 3 bar candiate testing (3, 4 bar testing)
  - EVENT_STACK_ADD (publish)
  - {
    "type": "threebars",
    "symbol": "ALLE",
    "period": "1MIN",
    "data": [
    { 'timestamp': 000010, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
    { 'timestamp': 000009, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
    { 'timestamp': 000008, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
    { 'timestamp': 000007, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
    { 'timestamp': 000006, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 }
    ]
    }
  - EVENT_BAR_CANDIDATE_SUBSCRIBE (publish)
    {
    "symbol": "ALLE",
    "period": "1MIN,
    }

# FILE

ThreeBarCandidateSubscribe.py

- EVENT_CANDIDATE_SUBSCRIBE (subscribe)
- Add to subscribe list
- EVENT_CANDIDATE_SUBSCRIBE_NEW (publish)
  {
  "symbol": "ALLE"
  }

# FILE

RealtimeTradeData.py

- EVENT_CANDIDATE_SUBSCRIBE_NEW (subscribe)
- Add symbol to Alpaca Stream Trade List.
-
- Subscribe trade to event.
- EVENT_TRADE (publish)
  {
  'symbol': "ALLE",
  'close': 10.45,
  'volume': 100,
  'timestamp': 1234567890
  }

# FILE

ThreeBarStudy.py

- EVENT_TRADE (subscribe)
- Get data from Stack
- Process realtime event
- save the data to EVENT_CANDIDATE_SCORE
  {
  "type": "threebars",
  "symbol": "ALLE",
  "period": "1MIN,
  "price": 5,
  "volume": 1.5
  }

# REDIS

## timeseris

data_close_1MIN:FANG
data_open_1MIN:FANG
data_high_1MIN:FANG
data_low_1MIN:FANG
data_volume_1MIN:FANG

(_\_2MIN:_)
(_\_5MIN:_)

## Hash Key

# stack_trade_sub

-- key: symbol
-- field:
{
"type": "threebars",
"symbol": "ALLE",
"isSubscribed": false,
}

# stack_threebar

-- key: symbol
-- field:
{
"type": "threebars",
"symbol": "ALLE",
"period": "1MIN",
"data": [
{ 'timestamp': 000010, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
{ 'timestamp': 000009, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
{ 'timestamp': 000008, 'close': 136.02, 'high': 136.06, 'low': 136.0, 'open': 136.04, 'volume': 712 },
]
}

# score_threebar

-- key: symbol
-- field:
{
"type": "threebars",
"symbol": "ALLE",
"period": "1MIN,
"price": 5,
"volume": 1.5
}
