### RedisTimeSeriesTable.py

- Create realtime data tables. \_0, \_1MIN, \_2MIN, \_5MIN
- redisTSCreateTable.CreateRedisStockTimeSeriesKeys()

#

### EVENT_BAR (pub/sub)

- (from) RealtimeBarData.py
- (to) EVENT_BAR.py
- MinuteBarStream.MinuteBarStream()
- Handle 1 Minute Bar
- EVENT_BAR (subscribe)
  - bar Bar({ 'c': 136.02, 'h': 136.06, 'l': 136.0, 'o': 136.04, 'S': 'ALLE', 't': 1627493640000000000, 'v': 712})
  - publih, EVENT_BAR_CANDIDATE_CHECK

# EVENT_BAR_CANDIDATE_CHECK (publish)

#

### EVENT-BAR-CANDIDATE-CHECK--SAVE (subscribe)

EVENT_BAR_CANDIATE_CHECK (subscribe)

- Save data to 1 Minute Bar

#

### EVENT_BAR_CANDIDATE_CHECK (subscribe)

- (to) EVENT_BAR_CANDIDATE_CHECK (subscribe)
- (from) ThreeBarCandidate.py
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

# EVENT_TRADE_ADD (publish)

#

### EVENT_STACK_ADD (subscribe)

- ThreeBarStack.py
  {
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

# EVENT_TRADE_ADD (publish)

#

### EVENT_TRADE (subscribe)

EVENT_TRADE_ADD (subscribe)
{
"symbol": "ALLE",
"period": "1MIN,
}

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

# EVENT_TRADE_ADD (publish)

#

### EVENT_TRADE_ADD (subscribe)

- Data
  {
  stacks: {
  {
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
  },
  trade: {
  'symbol': "ALLE",
  'close': 10.45,
  'volume': 100,
  'timestamp': 1234567890
  },
  realtime: []
  }

# EVENT_TRADE_PROCESS (publish)

#

### EVENT_TRADE_PROCESS (subscribe)

- Process the data and score the stock

# EVENT_TRADE_SAVE (publish)

- Save to Score Hash Key

### EVENT_TRADE_SAVE (subscribe)

- Score the trade
  {
  "symbol": "ALLE",
  "period": "2MIN",
  "score": 7,
  "entry": 14.20,
  "exit": 14.45
  }
