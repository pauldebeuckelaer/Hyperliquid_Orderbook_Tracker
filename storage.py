"""Storage module - writes aggregated orderbook data to twap.db."""

import time
import sqlite3
import logging
from config import DB_PATH, COINS

logger = logging.getLogger(__name__)

# hex address -> addresses.id. Ids never change, so this only grows.
_addr_ids: dict[str, int] = {}

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

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tape_prints (
            tid      INTEGER PRIMARY KEY,   -- rowid alias: no extra index, cheapest possible
            ts       INTEGER NOT NULL,      -- ms since epoch, payload 'time'
            coin     TEXT    NOT NULL,
            px       REAL    NOT NULL,
            sz       REAL    NOT NULL,
            notional REAL    NOT NULL,      -- px * sz
            side     TEXT    NOT NULL,      -- 'B' / 'A' as sent: the aggressor's side
            buyer    INTEGER NOT NULL,      -- addresses.id  (users[0])
            seller   INTEGER NOT NULL,      -- addresses.id  (users[1])
            order_id BLOB                   -- 32-byte hash; NULL when all-zero (HIP-2)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tape_gaps (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            disconnected INTEGER NOT NULL,  -- ms since epoch
            reconnected  INTEGER NOT NULL,
            coins        TEXT    NOT NULL,
            reason       TEXT
        )
    """)

    conn.execute("""
            CREATE TABLE IF NOT EXISTS tape_addresses (
                id      INTEGER PRIMARY KEY,
                address TEXT NOT NULL UNIQUE
            )
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


def _ensure_address_ids(conn, addresses: set[str]) -> dict[str, int]:
    """Insert any address not yet cached and return {address: id} for those.
    Does NOT touch _addr_ids: the caller merges after a successful commit,
    so a rolled-back transaction can never leave stale ids in the cache."""
    missing = [a for a in addresses if a not in _addr_ids]
    if not missing:
        return {}
    conn.executemany("INSERT OR IGNORE INTO tape_addresses (address) VALUES (?)",
                     [(a,) for a in missing])
    placeholders = ",".join("?" * len(missing))
    return {addr: id_ for id_, addr in conn.execute(
        f"SELECT id, address FROM tape_addresses WHERE address IN ({placeholders})", missing
    )}


def write_prints(prints: list[tuple]) -> int:
    """Insert raw prints in one transaction. Returns rows actually inserted;
    replays are dropped on tid.

    Each print: (tid, ts, coin, px, sz, notional, side, buyer_hex, seller_hex, order_id)
    with buyer/seller as lowercase hex and order_id as bytes or None.
    """
    if not prints:
        return 0
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        new_ids = _ensure_address_ids(conn, {p[7] for p in prints} | {p[8] for p in prints})
        ids = {**_addr_ids, **new_ids}
        rows = [
            (tid, ts, coin, px, sz, notional, side,
             ids[buyer], ids[seller], order_id)
            for tid, ts, coin, px, sz, notional, side, buyer, seller, order_id in prints
        ]
        before = conn.total_changes
        conn.executemany("""
            INSERT OR IGNORE INTO tape_prints
                (tid, ts, coin, px, sz, notional, side, buyer, seller, order_id)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)
        inserted = conn.total_changes - before
        conn.commit()
        _addr_ids.update(new_ids)   # only after the commit succeeded
        return inserted
    except Exception as e:
        logger.error(f"Failed to write {len(prints)} prints: {e}")
        return 0
    finally:
        conn.close()

def record_gap(disconnected_ms: int, reconnected_ms: int, coins, reason: str) -> None:
    """One row per loss of coverage. Called by ws_handler on reconnect and by main on start."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        conn.execute(
            "INSERT INTO tape_gaps (disconnected, reconnected, coins, reason) VALUES (?,?,?,?)",
            (disconnected_ms, reconnected_ms, ",".join(coins), reason),
        )
        conn.commit()
        logger.warning(f"tape gap: {(reconnected_ms - disconnected_ms) / 1000:.1f}s ({reason})")
    except Exception as e:
        logger.error(f"Failed to record gap: {e}")
    finally:
        conn.close()


def record_restart_gap() -> None:
    """Gap from the last stored print to now. No row on a first-ever run."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        last = conn.execute("SELECT MAX(ts) FROM tape_prints").fetchone()[0]
    finally:
        conn.close()
    if last is None:
        return
    record_gap(last, int(time.time() * 1000), COINS, "restart")


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