#!/usr/bin/env python3
"""
Financial Recovery Engine - BTCUSDT Real-Time Price Monitor
============================================================

Institutional-grade WebSocket client for Binance market data.
Features:
- Asynchronous WebSocket connection with automatic reconnection.
- Exponential backoff on disconnects.
- Health check module logging system uptime.
- Secure API key management via environment variables.
- Rotating file + console logging with detailed formatting.
- Graceful shutdown on SIGINT/SIGTERM.

Requirements:
    pip install websockets python-dotenv

Environment variables (.env file or system):
    BINANCE_API_KEY=<optional, for private streams>
    BINANCE_API_SECRET=<optional>
    LOG_LEVEL=INFO  # optional, default INFO
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional, Callable

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from dotenv import load_dotenv

# Load environment variables from .env file (if present)
load_dotenv()


class LoggerConfig:
    """Centralized logging configuration for the entire application."""

    @staticmethod
    def setup_logger(
        name: str = "FinancialRecoveryEngine",
        log_level: str = None,
        log_file: str = "financial_recovery.log"
    ) -> logging.Logger:
        """
        Configure and return a logger with rotating file and console handlers.

        Args:
            name: Logger name.
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
            log_file: Path to the log file.

        Returns:
            Configured logger instance.
        """
        log_level = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, log_level, logging.INFO)

        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Avoid duplicate handlers if called multiple times
        if logger.handlers:
            return logger

        # Formatter with timestamp, level, module, and message
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Console handler (stderr for errors, stdout for others)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Rotating file handler (max 10 MB, keep 5 backups)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10_485_760, backupCount=5
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger


class HealthCheck:
    """
    Health check module that logs the system uptime at regular intervals.
    Also monitors the number of reconnections performed.
    """

    def __init__(self, logger: logging.Logger, check_interval: int = 60):
        """
        Initialize the health check monitor.

        Args:
            logger: Logger instance for health messages.
            check_interval: Seconds between health status logs.
        """
        self.logger = logger
        self.interval = check_interval
        self.start_time = time.monotonic()
        self.reconnection_count = 0
        self._task: Optional[asyncio.Task] = None

    def increment_reconnections(self) -> None:
        """Increment the reconnection counter (thread-safe via asyncio)."""
        self.reconnection_count += 1

    def get_uptime(self) -> float:
        """Return system uptime in seconds (monotonic)."""
        return time.monotonic() - self.start_time

    async def _health_loop(self) -> None:
        """Asynchronous loop that periodically logs health status."""
        while True:
            await asyncio.sleep(self.interval)
            uptime_sec = self.get_uptime()
            uptime_str = str(datetime.timedelta(seconds=int(uptime_sec)))
            self.logger.info(
                f"Health Check | Uptime: {uptime_str} | "
                f"Reconnections: {self.reconnection_count}"
            )

    async def start(self) -> None:
        """Start the health check background task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._health_loop())
            self.logger.debug("Health check task started.")

    async def stop(self) -> None:
        """Cancel the health check background task."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self.logger.debug("Health check task stopped.")


class BinanceWebSocketClient:
    """
    WebSocket client for Binance public market data streams.
    Implements auto-reconnect with exponential backoff and high-level error handling.
    """

    # Binance public WebSocket base URL (no API key required for trade streams)
    BASE_WS_URL = "wss://stream.binance.com:9443/ws"

    def __init__(
        self,
        symbol: str,
        logger: logging.Logger,
        health_check: HealthCheck,
        on_message_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        max_reconnect_attempts: int = 10,
        base_delay: float = 1.0,
        max_delay: float = 60.0
    ):
        """
        Initialize the WebSocket client.

        Args:
            symbol: Trading pair (e.g., "btcusdt").
            logger: Logger instance.
            health_check: HealthCheck instance to track reconnections.
            on_message_callback: Optional async callback for processing messages.
            max_reconnect_attempts: Maximum reconnect attempts (None for infinite).
            base_delay: Initial delay in seconds for exponential backoff.
            max_delay: Maximum delay in seconds.
        """
        self.symbol = symbol.lower()
        self.stream_name = f"{self.symbol}@trade"
        self.ws_url = f"{self.BASE_WS_URL}/{self.stream_name}"
        self.logger = logger
        self.health_check = health_check
        self.on_message_callback = on_message_callback
        self.max_reconnect_attempts = max_reconnect_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay

        self._running = False
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._reconnect_task: Optional[asyncio.Task] = None

    async def _handle_messages(self) -> None:
        """
        Continuously receive and process WebSocket messages.
        Raises ConnectionClosed or other exceptions to trigger reconnection.
        """
        async for message in self._websocket:
            try:
                data = json.loads(message)
                self.logger.debug(f"Received: {data}")

                # Basic price extraction (trade stream: data['p'] is price)
                if 'p' in data:
                    price = float(data['p'])
                    self.logger.info(f"BTCUSDT Price: ${price:,.2f} | Time: {datetime.fromtimestamp(data['T']/1000)}")

                # Call user callback if provided
                if self.on_message_callback:
                    # If callback is async, await it; otherwise call it
                    if asyncio.iscoroutinefunction(self.on_message_callback):
                        await self.on_message_callback(data)
                    else:
                        self.on_message_callback(data)

            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to decode JSON message: {e} | Raw: {message[:200]}")
            except Exception as e:
                self.logger.exception(f"Unexpected error in message handler: {e}")

    async def _connect_and_listen(self) -> None:
        """
        Establish the WebSocket connection and start listening for messages.
        Will stay connected until an irrecoverable error or stop signal.
        """
        reconnect_attempt = 0
        delay = self.base_delay

        while self._running:
            try:
                self.logger.info(f"Connecting to {self.ws_url} (attempt {reconnect_attempt + 1})")
                self._websocket = await websockets.connect(
                    self.ws_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10
                )
                self.logger.info("WebSocket connected successfully.")
                reconnect_attempt = 0  # Reset counter on successful connect
                delay = self.base_delay

                # Start processing messages (this blocks until connection closes)
                await self._handle_messages()

            except (ConnectionClosed, WebSocketException, OSError, asyncio.TimeoutError) as e:
                self.logger.warning(f"WebSocket connection lost: {type(e).__name__} - {e}")
                if not self._running:
                    break

                # Increment reconnection counter in health check
                self.health_check.increment_reconnections()

                reconnect_attempt += 1
                if self.max_reconnect_attempts and reconnect_attempt > self.max_reconnect_attempts:
                    self.logger.critical(f"Max reconnect attempts ({self.max_reconnect_attempts}) reached. Giving up.")
                    break

                # Exponential backoff with jitter
                delay = min(delay * 2, self.max_delay)
                jitter = delay * 0.1 * (time.monotonic() % 1)  # simple jitter
                wait_time = delay + jitter
                self.logger.info(f"Reconnecting in {wait_time:.2f} seconds (attempt {reconnect_attempt})")
                await asyncio.sleep(wait_time)

            except Exception as e:
                self.logger.exception(f"Unhandled exception in connection loop: {e}")
                if self._running:
                    await asyncio.sleep(5)  # brief pause before retry

        self.logger.info("WebSocket client stopped.")

    async def start(self) -> None:
        """Start the WebSocket client (connection + auto-reconnect loop)."""
        if self._running:
            self.logger.warning("Client already running.")
            return

        self._running = True
        self._reconnect_task = asyncio.create_task(self._connect_and_listen())

    async def stop(self) -> None:
        """Gracefully stop the WebSocket client and close the connection."""
        self.logger.info("Stopping WebSocket client...")
        self._running = False

        if self._websocket:
            await self._websocket.close()
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass

        self.logger.info("WebSocket client stopped.")


class FinancialRecoveryEngine:
    """
    Main engine orchestrating the WebSocket client, health check, and graceful shutdown.
    """

    def __init__(self, symbol: str = "BTCUSDT"):
        """
        Initialize the engine.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT").
        """
        self.logger = LoggerConfig.setup_logger()
        self.symbol = symbol
        self.health_check = HealthCheck(self.logger)
        self.ws_client = BinanceWebSocketClient(
            symbol=symbol,
            logger=self.logger,
            health_check=self.health_check,
            on_message_callback=self._on_price_update
        )
        self._shutdown_event = asyncio.Event()

    def _on_price_update(self, data: Dict[str, Any]) -> None:
        """
        Callback for price updates. Can be extended for financial recovery logic.
        """
        # Example: additional processing (e.g., store price, trigger alerts)
        # Currently only logs at INFO level inside the client.
        pass

    async def _shutdown_cleanup(self, sig: signal.Signals) -> None:
        """
        Cleanup tasks on system signal.
        """
        self.logger.info(f"Received shutdown signal: {sig.name}. Cleaning up...")
        self._shutdown_event.set()
        await self.ws_client.stop()
        await self.health_check.stop()
        self.logger.info("Shutdown complete. Exiting.")
        sys.exit(0)

    async def run(self) -> None:
        """
        Run the main engine: start health check and WebSocket client,
        then wait for shutdown.
        """
        self.logger.info(f"Starting Financial Recovery Engine for {self.symbol}")

        # Start health check module
        await self.health_check.start()

        # Start WebSocket client (auto-reconnect enabled)
        await self.ws_client.start()

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self._shutdown_cleanup(s))
            )

        # Wait until shutdown event is set
        await self._shutdown_event.wait()


async def main():
    """Entry point for the Financial Recovery Engine."""
    engine = FinancialRecoveryEngine(symbol="BTCUSDT")
    await engine.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # In case asyncio.run doesn't catch the interrupt cleanly
        print("\nEngine terminated by user.")
        sys.exit(0)
