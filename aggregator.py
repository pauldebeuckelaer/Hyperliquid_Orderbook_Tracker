"""Aggregator - accumulates websocket data in memory and produces 1-min summaries."""

import logging
from datetime import datetime, timezone
from config import LARGE_TRADE_THRESHOLD

logger = logging.getLogger(__name__)

# noinspection PyAttributeOutsideInit
class CoinAggregator:
    """Aggregates trades and book updates for a single coin."""

    def __init__(self, coin: str, whale_addresses: set):
        self.coin = coin
        self.whale_addresses = whale_addresses
        self.large_threshold = LARGE_TRADE_THRESHOLD.get(coin, 1000)
        self._prints: list[tuple] = []  # raw tape, drained by main.flush_tape
        self.reset()

    def reset(self):
        """Clear all accumulated state for next interval."""
        # Trade accumulators
        self.buy_aggressor_vol = 0.0
        self.sell_aggressor_vol = 0.0
        self.whale_buy_vol = 0.0
        self.whale_sell_vol = 0.0
        self.whale_trade_count = 0
        self.large_trade_count = 0
        self.num_trades = 0
        self.total_volume = 0.0

        # Book state (latest snapshot)
        self.bid_volume = 0.0
        self.ask_volume = 0.0
        self.best_bid = 0.0
        self.best_ask = 0.0

        # Book running averages
        self._spread_sum = 0.0
        self._imbalance_sum = 0.0
        self.book_updates = 0


    def process_book(self, book_data: dict):
        """Process an L2 book update from the websocket."""
        try:
            levels = book_data["levels"]
            bids = levels[0]
            asks = levels[1]

            # Compute volumes
            self.bid_volume = sum(float(b["sz"]) for b in bids)
            self.ask_volume = sum(float(a["sz"]) for a in asks)

            # Best bid/ask
            if bids:
                self.best_bid = float(bids[0]["px"])
            if asks:
                self.best_ask = float(asks[0]["px"])

            # Running averages for spread and imbalance
            if self.best_bid > 0 and self.best_ask > 0:
                self._spread_sum += self.best_ask - self.best_bid
                total = self.bid_volume + self.ask_volume
                if total > 0:
                    self._imbalance_sum += self.bid_volume / total
                self.book_updates += 1

        except (KeyError, ValueError, IndexError) as e:
            logger.debug(f"Skipping malformed book update: {e}")

    def process_trade(self, trade: dict):
        """Process a single trade from the websocket."""
        try:
            size = float(trade["sz"])
            px = float(trade["px"])
            side = trade["side"]  # "B" or "A"
            buyer = trade["users"][0].lower()
            seller = trade["users"][1].lower()
            tid = int(trade["tid"])
            ts = int(trade["time"])
            raw_hash = bytes.fromhex(trade["hash"][2:])
            order_id = raw_hash if any(raw_hash) else None  # all-zero -> NULL

            self.num_trades += 1
            self.total_volume += size

            # Aggressor flow: side "B" means buyer is aggressor (market buy)
            if side == "B":
                self.buy_aggressor_vol += size
            else:
                self.sell_aggressor_vol += size

            # Whale detection
            is_whale_buyer = buyer in self.whale_addresses
            is_whale_seller = seller in self.whale_addresses

            if is_whale_buyer:
                self.whale_buy_vol += size
                self.whale_trade_count += 1
            if is_whale_seller:
                self.whale_sell_vol += size
                self.whale_trade_count += 1

            # Large trade detection
            if size >= self.large_threshold:
                self.large_trade_count += 1

            # Raw tape (drained on its own cadence by main.flush_tape)
            self._prints.append(
                (tid, ts, self.coin, px, size, px * size, side, buyer, seller, order_id)
            )

        except (KeyError, ValueError, IndexError) as e:
            logger.debug(f"Skipping malformed trade: {e}")

    def flush(self) -> dict:
        """Produce a summary dict and reset state."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        spread_avg = (self._spread_sum / self.book_updates) if self.book_updates > 0 else 0.0
        imbalance_avg = (self._imbalance_sum / self.book_updates) if self.book_updates > 0 else 0.5

        snapshot = {
            "snapshot_time": now,
            "coin": self.coin,
            "bid_volume": round(self.bid_volume, 2),
            "ask_volume": round(self.ask_volume, 2),
            "book_imbalance": round(imbalance_avg, 6),
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread_avg": round(spread_avg, 6),
            "buy_aggressor_vol": round(self.buy_aggressor_vol, 2),
            "sell_aggressor_vol": round(self.sell_aggressor_vol, 2),
            "net_aggressor_flow": round(self.buy_aggressor_vol - self.sell_aggressor_vol, 2),
            "num_trades": self.num_trades,
            "total_volume": round(self.total_volume, 2),
            "whale_buy_vol": round(self.whale_buy_vol, 2),
            "whale_sell_vol": round(self.whale_sell_vol, 2),
            "whale_net_flow": round(self.whale_buy_vol - self.whale_sell_vol, 2),
            "whale_trade_count": self.whale_trade_count,
            "large_trade_count": self.large_trade_count,
            "book_updates": self.book_updates,
        }

        self.reset()
        return snapshot

    def drain_prints(self) -> list[tuple]:
        """Hand the buffered tape to the writer and start a fresh buffer."""
        out = self._prints
        self._prints = []
        return out