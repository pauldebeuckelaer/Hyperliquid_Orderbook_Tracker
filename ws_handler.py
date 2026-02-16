"""Websocket handler - connects to Hyperliquid, manages subscriptions and reconnection."""

import json
import logging
import asyncio
import websockets
from config import WS_URI, COINS, RECONNECT_DELAY_INITIAL, RECONNECT_DELAY_MAX
from aggregator import CoinAggregator

logger = logging.getLogger(__name__)


class HyperliquidWS:
    """Manages websocket connection to Hyperliquid."""

    def __init__(self, aggregators: dict[str, CoinAggregator]):
        self.aggregators = aggregators
        self.ws = None
        self._running = False

    async def _subscribe(self):
        """Subscribe to trades and l2Book for all configured coins."""
        for coin in COINS:
            # Trades feed
            await self.ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": coin}
            }))
            logger.info(f"Subscribed to {coin} trades")

            # L2 order book
            await self.ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin}
            }))
            logger.info(f"Subscribed to {coin} l2Book")

    async def _handle_message(self, raw: str):
        """Route incoming message to the appropriate aggregator."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        channel = msg.get("channel", "")
        data = msg.get("data", {})

        if channel == "trades":
            for trade in data:
                coin = trade.get("coin")
                if coin in self.aggregators:
                    self.aggregators[coin].process_trade(trade)

        elif channel == "l2Book":
            coin = data.get("coin")
            if coin in self.aggregators:
                self.aggregators[coin].process_book(data)

    async def run(self):
        """Connect and listen with automatic reconnection."""
        self._running = True
        delay = RECONNECT_DELAY_INITIAL

        while self._running:
            try:
                async with websockets.connect(WS_URI, ping_interval=20, ping_timeout=10) as ws:
                    self.ws = ws
                    logger.info(f"Connected to {WS_URI}")
                    delay = RECONNECT_DELAY_INITIAL

                    await self._subscribe()

                    async for raw in ws:
                        await self._handle_message(raw)

            except websockets.ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}. Reconnecting in {delay}s...")
            except Exception as e:
                logger.error(f"Websocket error: {e}. Reconnecting in {delay}s...")

            if self._running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_DELAY_MAX)

    def stop(self):
        """Signal the handler to stop."""
        self._running = False