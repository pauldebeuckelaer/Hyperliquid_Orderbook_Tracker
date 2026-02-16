"""Hyperliquid Orderbook Tracker - Entry Point.

Connects to Hyperliquid websocket, aggregates trade and book data,
and writes 1-minute summaries to twap.db.

Usage:
    python main.py
"""

import asyncio
import logging
import signal
from datetime import datetime, timezone

from config import COINS, FLUSH_INTERVAL
from storage import init_db, load_whale_addresses, write_snapshot
from aggregator import CoinAggregator
from ws_handler import HyperliquidWS

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orderbook_tracker")


async def flush_loop(aggregators: dict[str, CoinAggregator]):
    """Flush aggregated data to database every FLUSH_INTERVAL seconds."""
    while True:
        # Sleep until next minute boundary
        now = datetime.now(timezone.utc)
        seconds_until_next = FLUSH_INTERVAL - (now.second % FLUSH_INTERVAL)
        await asyncio.sleep(seconds_until_next)

        for coin, agg in aggregators.items():
            snapshot = agg.flush()

            if snapshot["num_trades"] == 0 and snapshot["book_updates"] == 0:
                logger.debug(f"{coin}: No data to flush")
                continue

            write_snapshot(snapshot)
            logger.info(
                f"{coin}: trades={snapshot['num_trades']} "
                f"vol={snapshot['total_volume']:.0f} "
                f"aggressor={snapshot['net_aggressor_flow']:+.0f} "
                f"whale={snapshot['whale_net_flow']:+.0f} "
                f"imbalance={snapshot['book_imbalance']:.2%} "
                f"spread={snapshot['spread_avg']:.4f}"
            )


async def main():
    # Initialize
    init_db()
    whale_addresses = load_whale_addresses()

    # Create aggregators for each coin
    aggregators = {coin: CoinAggregator(coin, whale_addresses) for coin in COINS}

    # Create websocket handler
    ws = HyperliquidWS(aggregators)

    # Handle shutdown gracefully
    import platform
    if platform.system() != "Windows":
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: ws.stop())

    # Run both tasks
    logger.info(f"Starting Orderbook Tracker for {COINS}")
    await asyncio.gather(
        ws.run(),
        flush_loop(aggregators),
    )


if __name__ == "__main__":
    asyncio.run(main())