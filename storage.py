"""Storage module - writes aggregated orderbook data to twap.db."""

import sqlite3
import logging
from config import DB_PATH

logger = logging.getLogger(__name__)


def init_db():
    """Create the orderbook_snapshots table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT NOT NULL,
            coin TEXT NOT NULL,
            -- Book state
            bid_volume REAL,
            ask_volume REAL,
            book_imbalance REAL,
            best_bid REAL,
            best_ask REAL,
            spread_avg REAL,
            -- Trade flow
            buy_aggressor_vol REAL,
            sell_aggressor_vol REAL,
            net_aggressor_flow REAL,
            num_trades INTEGER,
            total_volume REAL,
            -- Whale activity
            whale_buy_vol REAL,
            whale_sell_vol REAL,
            whale_net_flow REAL,
            whale_trade_count INTEGER,
            -- Structural
            large_trade_count INTEGER,
            book_updates INTEGER,
            UNIQUE(snapshot_time, coin)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ob_coin_time 
        ON orderbook_snapshots(coin, snapshot_time)
    """)
    conn.commit()
    conn.close()
    logger.info(f"Database initialized: {DB_PATH}")


def load_whale_addresses():
    """Load whale addresses from the existing whale_addresses table."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cursor = conn.execute("SELECT address FROM whale_addresses")
        addresses = {row[0].lower() for row in cursor.fetchall()}
        conn.close()
        logger.info(f"Loaded {len(addresses)} whale addresses")
        return addresses
    except Exception as e:
        logger.warning(f"Could not load whale addresses: {e}")
        return set()


def write_snapshot(snapshot: dict):
    """Write a single aggregated snapshot to the database."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO orderbook_snapshots (
                snapshot_time, coin,
                bid_volume, ask_volume, book_imbalance,
                best_bid, best_ask, spread_avg,
                buy_aggressor_vol, sell_aggressor_vol, net_aggressor_flow,
                num_trades, total_volume,
                whale_buy_vol, whale_sell_vol, whale_net_flow, whale_trade_count,
                large_trade_count, book_updates
            ) VALUES (
                :snapshot_time, :coin,
                :bid_volume, :ask_volume, :book_imbalance,
                :best_bid, :best_ask, :spread_avg,
                :buy_aggressor_vol, :sell_aggressor_vol, :net_aggressor_flow,
                :num_trades, :total_volume,
                :whale_buy_vol, :whale_sell_vol, :whale_net_flow, :whale_trade_count,
                :large_trade_count, :book_updates
            )
        """, snapshot)
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to write snapshot: {e}")
    finally:
        conn.close()