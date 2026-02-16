"""Configuration for Hyperliquid Orderbook Tracker."""

import os
import platform

# Websocket
WS_URI = "wss://api.hyperliquid.xyz/ws"

# Coins to track
COINS = ["HYPE", "BTC"]

# Database - same twap.db as your TWAP tracker
if platform.system() == "Windows":
    DB_PATH = os.path.join(os.path.expanduser("~"), "PycharmProjects", "Hyperliquid_TWAP_Analyzer", "data", "twap.db")
else:
    DB_PATH = os.path.expanduser("~/Hyperliquid_TWAP_Tracker/data/twap.db")

# Aggregation interval in seconds
FLUSH_INTERVAL = 60

# Large trade threshold (in coin units)
LARGE_TRADE_THRESHOLD = {
    "HYPE": 1000,
    "BTC": 0.5,
}

# Reconnect settings
RECONNECT_DELAY_INITIAL = 1
RECONNECT_DELAY_MAX = 60