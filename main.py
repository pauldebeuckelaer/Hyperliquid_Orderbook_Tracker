"""Hyperliquid Orderbook Tracker - Entry Point.

Connects to Hyperliquid websocket, aggregates trade and book data,
and writes 1-minute summaries to twap.db (orderbook_snapshots).

Also stores every raw trade print (tape_prints) for per-wallet
absorption analysis, and logs websocket gaps (tape_gaps).

Usage:
    python main.py
"""

import asyncio
import logging
import platform
import signal
from datetime import datetime, timezone

from config import COINS, FLUSH_INTERVAL, TAPE_FLUSH_INTERVAL
from storage import (
    init_db,
    load_whale_addresses,
    write_snapshot,
    write_prints,
    record_restart_gap,
)
from aggregator import CoinAggregator
from ws_handler import HyperliquidWS

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orderbook_tracker")


# --------------------------------------------------------------------------
# Flush helpers (sync, so they can also run once at shutdown)
# --------------------------------------------------------------------------

def flush_snapshots(aggregators: dict[str, CoinAggregator]) -> None:
    """Write one 1-minute row per coin to orderbook_snapshots."""
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


def flush_tape(aggregators: dict[str, CoinAggregator]) -> None:
    """Drain buffered raw prints from every aggregator into tape_prints."""
    prints = []
    for agg in aggregators.values():
        prints.extend(agg.drain_prints())

    if not prints:
        return

    inserted = write_prints(prints)
    if inserted != len(prints):
        # tid is PRIMARY KEY and write_prints uses INSERT OR IGNORE,
        # so a shortfall means the feed replayed prints we already had.
        logger.info(f"tape: {len(prints)} buffered, {inserted} inserted")
    else:
        logger.debug(f"tape: {inserted} prints written")


# --------------------------------------------------------------------------
# Loops
# --------------------------------------------------------------------------

async def flush_loop(aggregators: dict[str, CoinAggregator], stop: asyncio.Event):
    """Flush 1-minute aggregates on the minute boundary until stop fires."""
    while not stop.is_set():
        now = datetime.now(timezone.utc)
        seconds_until_next = FLUSH_INTERVAL - (now.second % FLUSH_INTERVAL)
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds_until_next)
            break
        except asyncio.TimeoutError:
            flush_snapshots(aggregators)


async def tape_loop(aggregators: dict[str, CoinAggregator], stop: asyncio.Event):
    """Flush raw prints every TAPE_FLUSH_INTERVAL seconds.

    Shorter than FLUSH_INTERVAL on purpose: the tape cannot be backfilled,
    so a crash should cost seconds of prints, not a minute.
    """
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=TAPE_FLUSH_INTERVAL)
            break
        except asyncio.TimeoutError:
            flush_tape(aggregators)


# --------------------------------------------------------------------------
# Entry
# --------------------------------------------------------------------------

async def main():
    init_db()
    record_restart_gap()  # tape_gaps row: last stored print -> now, reason='restart'
    whale_addresses = load_whale_addresses()

    aggregators = {coin: CoinAggregator(coin, whale_addresses) for coin in COINS}
    ws = HyperliquidWS(aggregators)

    stop = asyncio.Event()

    def shutdown():
        logger.info("shutdown signal received")
        stop.set()
        ws.stop()

    if platform.system() != "Windows":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown)

    logger.info(
        f"Starting Orderbook Tracker for {COINS} "
        f"(snapshots every {FLUSH_INTERVAL}s, tape every {TAPE_FLUSH_INTERVAL}s)"
    )
    try:
        await asyncio.gather(
            ws.run(),
            flush_loop(aggregators, stop),
            tape_loop(aggregators, stop),
        )
    finally:
        # Runs on SIGTERM, on Ctrl-C (Windows), and if ws.run() dies.
        flush_tape(aggregators)
        flush_snapshots(aggregators)
        logger.info("shutdown: buffers flushed")


if __name__ == "__main__":
    asyncio.run(main())