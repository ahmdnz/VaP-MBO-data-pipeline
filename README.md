# VaP & MBO Data Pipeline

A real-time async data pipeline for collecting **Volume at Price (VaP)** and **Market-By-Order (MBO)** live data from the [OKX](https://okx.com) exchange via WebSocket, and storing it in compressed Parquet files for later strategy analysis.

## What this pipeline does

Most retail traders work with OHLCV candles. This pipeline goes deeper — it captures the full **order book state** and **trade-level volume distribution** at every price level in real time.

Two separate pipelines run independently:

| File | Data type | What it captures |
|---|---|---|
| `VaP.py` | Volume at Price | Per-candle trade volume split by buy/sell at each price level |
| `MBO.py` | Market By Order | Full limit order book (bids + asks) with CRC32 checksum validation |

---

## VaP.py — Volume at Price

Subscribes to OKX `trades` and `candle1m` WebSocket channels simultaneously. For each 1-minute candle, it accumulates all trades and groups them by price level — tracking buy volume and sell volume separately at every price.

When a candle closes (detected by timestamp change), the pipeline writes a row containing:

```
time | open | high | low | close | vap
```

The `vap` column encodes the full volume profile as a pipe-separated string:
```
price,ask_vol,bid_vol | price,ask_vol,bid_vol | ...
```

**Output:** `{SYMBOL}VAP{n}.parquet` — rotates to a new file every 24 hours.

---

## MBO.py — Market By Order (Limit Order Book)

Subscribes to OKX `books` (full order book) and `tickers` channels. Maintains a live, incrementally-updated limit order book for each instrument using snapshot + delta merging.

**Checksum validation:** After every update, the pipeline computes a CRC32 checksum over the top 25 bid/ask levels and validates it against OKX's provided checksum. If they don't match, it automatically resubscribes and resets.

On each price change (or every 500ms minimum), it saves a row:

```
time | last | askSz | bidSz | asks_p | bids_p
```

The `asks_p` and `bids_p` columns encode order book depth as pipe-separated strings:
```
price,size,order_count | price,size,order_count | ...
```

**Output:** `{SYMBOL}LOB{n}.parquet` — rotates to a new file every 12 hours.

---

## Supported instruments

Both pipelines currently track:
- `BTC-USDT-SWAP`
- `ETH-USDT-SWAP`
- `XRP-USDT-SWAP`
- `ETC-USDT-SWAP`

To add or change instruments, edit the `channels` list at the bottom of each file.

---

## Installation

```bash
pip install websockets pandas pyarrow
```

## Usage

Run each pipeline independently in separate terminals:

```bash
# Volume at Price pipeline
python VaP.py

# Market By Order (limit order book) pipeline
python MBO.py
```

**Note:** Output path is currently hardcoded to `E:/data/`. Change this in `write_data()` (VaP.py) and `subscribe_without_login()` (MBO.py) to match your setup.

---

## Output format

Both pipelines write **gzip-compressed Parquet** files via `pyarrow`. This keeps file sizes small while allowing fast columnar reads for strategy analysis later.

```python
import pandas as pd

# Read VaP data
df = pd.read_parquet("BTCVAP1.parquet")

# Parse volume profile for a row
vap_levels = [level.split(',') for level in df['vap'][0].split('|')]
# → [['price', 'ask_vol', 'bid_vol'], ...]
```

---

## Connection handling

Both pipelines include automatic reconnection logic:
- 25-second WebSocket timeout with ping/pong keepalive
- On timeout or disconnect: reconnects and resubscribes automatically
- MBO pipeline resubscribes on checksum mismatch

---

## Tech stack

- Python 3.9+
- `asyncio` + `websockets` — async WebSocket client
- `pandas` — DataFrame construction
- `pyarrow` — Parquet writing with gzip compression
- `zlib` — CRC32 checksum validation (MBO only)

---

## License

MIT
