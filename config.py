"""Configuration for Hyperliquid Orderbook Tracker."""

import os
import platform

# Websocket
WS_URI = "wss://api.hyperliquid.xyz/ws"

# Coins to track
COINS = ["HYPE", "BTC", "ETH", "ZEC", "SOL", "XRP", "PUMP", "FARTCOIN"]

# Database - same twap.db as your TWAP tracker
if platform.system() == "Windows":
    DB_PATH = os.path.join(os.path.expanduser("~"), "PycharmProjects", "Hyperliquid_TWAP_Analyzer", "data", "twap.db")
else:
    DB_PATH = os.path.expanduser("~/bots/Hyperliquid_TWAP_Analyzer/data/twap.db")

# Aggregation interval in seconds
FLUSH_INTERVAL = 60

# Large trade threshold (in coin units)
# NOTE: aggregator.py falls back to .get(coin, 1000) — a coin added to COINS
# without an entry here gets a meaningless threshold and fails silently.
LARGE_TRADE_THRESHOLD = {
    # unchanged: preserves continuity of the whale_* series since March 2026
    "HYPE": 1000,
    "BTC": 0.5,
    # added Sep 13 2026, sized to ~$50k notional at that day's marks
    "ETH": 20,
    "ZEC": 45,
    "SOL": 500,
    "XRP": 37000,
    "PUMP": 13500000,
    "FARTCOIN": 355000,
}

# Reconnect settings
RECONNECT_DELAY_INITIAL = 1
RECONNECT_DELAY_MAX = 60

TAPE_FLUSH_INTERVAL = 5