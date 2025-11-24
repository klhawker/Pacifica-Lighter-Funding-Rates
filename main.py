#!/usr/bin/env python3
"""
dex_funding_dashboard.py
========================

(...)
(keeping your docstring unchanged for brevity)
(...)
"""

import asyncio
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import os
import pandas as pd
import psycopg2
import requests
import websockets

# Lighter and Pacifica endpoints
LIGHTER_API_BASE = "https://mainnet.zklighter.elliot.ai/api/v1"
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

PACIFICA_API_BASE = "https://api.pacifica.fi/api/v1"
PACIFICA_WS_URL = "wss://ws.pacifica.fi/ws"

# Funding epochs per day (24 hourly epochs)
FUNDING_EPOCHS_PER_DAY = 24
HOURS_PER_YEAR = FUNDING_EPOCHS_PER_DAY * 365


def hourly_rate_to_apr_percent(rate: Optional[float]) -> Optional[float]:
    """Convert an hourly decimal funding rate to APR percentage."""
    if rate is None:
        return None
    return rate * HOURS_PER_YEAR * 100


def _changed(old: Optional[float], new: Optional[float], *, eps: float = 1e-12) -> bool:
    """
    Return True if `new` should be considered different from `old`.
    Handles None and small float jitter.
    """
    if new is None:
        return False
    if old is None:
        return True
    return abs(new - old) > eps


@dataclass
class MarketData:
    """Holds the latest funding and price data for a trading symbol."""

    symbol: str
    market_id: int
    lighter_funding_rate: Optional[float] = None
    lighter_current_funding_rate: Optional[float] = None
    lighter_mark_price: Optional[float] = None
    pacifica_funding_rate: Optional[float] = None
    pacifica_next_funding_rate: Optional[float] = None
    pacifica_mark_price: Optional[float] = None
    last_update_lighter: Optional[datetime] = None
    last_update_pacifica: Optional[datetime] = None

    def compute_24h_funding_lighter(self) -> Optional[float]:
        if self.lighter_funding_rate is None:
            return None
        return self.lighter_funding_rate * FUNDING_EPOCHS_PER_DAY

    def compute_apr_lighter(self) -> Optional[float]:
        return hourly_rate_to_apr_percent(self.lighter_funding_rate)

    def compute_24h_funding_pacifica(self) -> Optional[float]:
        if self.pacifica_funding_rate is None:
            return None
        return self.pacifica_funding_rate * FUNDING_EPOCHS_PER_DAY

    def compute_apr_pacifica(self) -> Optional[float]:
        return hourly_rate_to_apr_percent(self.pacifica_funding_rate)

    def compute_spread_percentage(self) -> Optional[float]:
        if self.lighter_mark_price is None or self.pacifica_mark_price is None:
            return None
        try:
            return (
                (self.lighter_mark_price - self.pacifica_mark_price)
                / self.pacifica_mark_price
                * 100
            )
        except ZeroDivisionError:
            return None

    def compute_net_funding_long_lighter_short_pacifica_24h(self) -> Optional[float]:
        lighter_daily = self.compute_24h_funding_lighter()
        pacifica_daily = self.compute_24h_funding_pacifica()
        if lighter_daily is None or pacifica_daily is None:
            return None
        # Long Lighter: pay lighter_daily; Short Pacifica: receive pacifica_daily
        return pacifica_daily - lighter_daily

    def compute_net_funding_long_pacifica_short_lighter_24h(self) -> Optional[float]:
        lighter_daily = self.compute_24h_funding_lighter()
        pacifica_daily = self.compute_24h_funding_pacifica()
        if lighter_daily is None or pacifica_daily is None:
            return None
        # Long Pacifica: pay pacifica_daily; Short Lighter: receive lighter_daily
        return lighter_daily - pacifica_daily

    def compute_best_funding_arb_24h(self) -> Optional[dict]:
        a = self.compute_net_funding_long_lighter_short_pacifica_24h()
        b = self.compute_net_funding_long_pacifica_short_lighter_24h()
        if a is None or b is None:
            return None

        if a > b:
            direction = "LONG Lighter / SHORT Pacifica"
            edge = a
        else:
            direction = "LONG Pacifica / SHORT Lighter"
            edge = b

        edge = max(edge, 0.0)
        return {"direction": direction, "edge_24h": edge}


# ---------- NEW: accumulator for per-minute aggregates ----------


@dataclass
class FundingAccumulator:
    sum_funding: float = 0.0
    sum_current: float = 0.0
    sum_next: float = 0.0
    count: int = 0
    last_ts: Optional[datetime] = None


class FundingDB:
    def __init__(self, dsn: Optional[str] = None):
        if dsn is None:
            dsn = os.environ.get("DATABASE_URL")
            if not dsn:
                raise RuntimeError("DATABASE_URL env var is not set")

        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True

        # aggregation / flush config
        self.flush_interval_sec = float(os.getenv("FUNDING_FLUSH_INTERVAL_SEC", "60.0"))
        # (venue, symbol) -> FundingAccumulator
        self._accumulators: Dict[Tuple[str, str], FundingAccumulator] = {}

        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS funding_rates (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    venue TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    funding_rate DOUBLE PRECISION,
                    current_funding_rate DOUBLE PRECISION,
                    next_funding_rate DOUBLE PRECISION
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_funding_rates_symbol_ts
                ON funding_rates (symbol, timestamp);
                """
            )

    # --------- changed semantics: buffer, don't insert immediately ---------

    def buffer_funding(
        self,
        *,
        venue: str,
        symbol: str,
        funding_rate: Optional[float],
        current_funding_rate: Optional[float] = None,
        next_funding_rate: Optional[float] = None,
        ts: Optional[datetime] = None,
    ) -> None:
        """
        Buffer a funding sample into an in-memory accumulator.
        Actual DB insert happens in flush_aggregates().
        """
        if ts is None:
            ts = datetime.utcnow()

        key = (venue, symbol)
        acc = self._accumulators.get(key)
        if acc is None:
            acc = FundingAccumulator()
            self._accumulators[key] = acc

        # accumulate (None values just don't contribute)
        if funding_rate is not None:
            acc.sum_funding += funding_rate
        if current_funding_rate is not None:
            acc.sum_current += current_funding_rate
        if next_funding_rate is not None:
            acc.sum_next += next_funding_rate
        acc.count += 1
        acc.last_ts = ts

    def flush_aggregates(self) -> None:
        """
        Insert one aggregated row per (venue, symbol) with avg funding
        over the last interval, then reset the accumulator.
        """
        if not self._accumulators:
            return

        now = datetime.utcnow()
        try:
            with self.conn.cursor() as cur:
                for (venue, symbol), acc in list(self._accumulators.items()):
                    if acc.count == 0 or acc.last_ts is None:
                        continue

                    # compute averages; if we never saw a non-None for a field, keep None
                    avg_funding = (
                        acc.sum_funding / acc.count if acc.sum_funding != 0.0 else None
                    )
                    avg_current = (
                        acc.sum_current / acc.count if acc.sum_current != 0.0 else None
                    )
                    avg_next = acc.sum_next / acc.count if acc.sum_next != 0.0 else None

                    ts = acc.last_ts or now

                    cur.execute(
                        """
                        INSERT INTO funding_rates (
                            timestamp, venue, symbol,
                            funding_rate, current_funding_rate, next_funding_rate
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (ts, venue, symbol, avg_funding, avg_current, avg_next),
                    )

                    # reset accumulator for next interval
                    acc.sum_funding = 0.0
                    acc.sum_current = 0.0
                    acc.sum_next = 0.0
                    acc.count = 0
                    acc.last_ts = None

        except psycopg2.Error as e:
            print(f"[DB ERROR] flush_aggregates failed: {e}", file=sys.stderr)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


def fetch_lighter_markets() -> Dict[str, int]:
    """Fetch active markets from Lighter and return a mapping symbol -> market_id."""
    url = f"{LIGHTER_API_BASE}/orderBooks"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        print(f"Error fetching Lighter markets: {exc}", file=sys.stderr)
        return {}
    data = resp.json()
    mapping: Dict[str, int] = {}
    for item in data.get("order_books", []):
        if item.get("status") == "active":
            symbol = item.get("symbol")
            market_id = item.get("market_id")
            if symbol is not None and market_id is not None:
                mapping[symbol] = market_id
    return mapping


def fetch_pacifica_markets() -> List[str]:
    """Fetch market information from Pacifica and return a list of symbols."""
    url = f"{PACIFICA_API_BASE}/info"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        print(f"Error fetching Pacifica markets: {exc}", file=sys.stderr)
        return []
    data = resp.json()
    symbols: List[str] = []
    for item in data.get("data", []):
        symbol = item.get("symbol")
        if symbol:
            symbols.append(symbol)
    return symbols


def build_market_data() -> Dict[str, MarketData]:
    """Compute the intersection of Lighter and Pacifica markets and initialise data structures."""
    lighter_markets = fetch_lighter_markets()
    pacifica_symbols = set(fetch_pacifica_markets())
    common_symbols = [s for s in lighter_markets.keys() if s in pacifica_symbols]
    market_data: Dict[str, MarketData] = {}
    for symbol in common_symbols:
        market_id = lighter_markets[symbol]
        market_data[symbol] = MarketData(symbol=symbol, market_id=market_id)
    if not market_data:
        print(
            "No common markets found between Lighter and Pacifica. Please check the symbol mappings.",
            file=sys.stderr,
        )
    else:
        print(
            f"Found {len(market_data)} common markets: {', '.join(sorted(market_data.keys()))}",
            file=sys.stderr,
        )
    return market_data


async def subscribe_lighter_market_stats(
    market_data: Dict[str, MarketData],
    db: FundingDB,
):
    """Subscribe to Lighter's market stats channel for each market in market_data."""
    subscription_messages = []
    for m in market_data.values():
        channel = f"market_stats/{m.market_id}"
        msg = {"type": "subscribe", "channel": channel}
        subscription_messages.append(msg)

    while True:
        try:
            async with websockets.connect(LIGHTER_WS_URL, ping_interval=None) as ws:
                for sub in subscription_messages:
                    await ws.send(json.dumps(sub))
                    await asyncio.sleep(0.1)  # small delay to avoid rate limits

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                    except Exception:
                        continue
                    if msg.get("type") != "update/market_stats":
                        continue

                    stats = msg.get("market_stats", {})
                    market_id = stats.get("market_id")
                    symbol = None
                    for s, mdata in market_data.items():
                        if mdata.market_id == market_id:
                            symbol = s
                            break
                    if symbol is None:
                        continue
                    mdata = market_data[symbol]

                    # Parse funding + mark
                    try:
                        fr = (
                            float(stats.get("funding_rate"))
                            if stats.get("funding_rate") is not None
                            else None
                        )
                    except (TypeError, ValueError):
                        fr = None
                    try:
                        cfr = (
                            float(stats.get("current_funding_rate"))
                            if stats.get("current_funding_rate") is not None
                            else None
                        )
                    except (TypeError, ValueError):
                        cfr = None
                    try:
                        mark = (
                            float(stats.get("mark_price"))
                            if stats.get("mark_price") is not None
                            else None
                        )
                    except (TypeError, ValueError):
                        mark = None

                    # Check if funding changed
                    fr_changed = _changed(mdata.lighter_funding_rate, fr)
                    cfr_changed = _changed(mdata.lighter_current_funding_rate, cfr)
                    funding_changed = fr_changed or cfr_changed

                    # Always update mark in memory if present
                    if mark is not None:
                        mdata.lighter_mark_price = mark

                    now = datetime.utcnow()
                    mdata.last_update_lighter = now

                    # Only update funding + buffer if changed
                    if funding_changed:
                        if fr_changed and fr is not None:
                            mdata.lighter_funding_rate = fr
                        if cfr_changed and cfr is not None:
                            mdata.lighter_current_funding_rate = cfr

                        db.buffer_funding(
                            venue="lighter",
                            symbol=symbol,
                            funding_rate=mdata.lighter_funding_rate,
                            current_funding_rate=mdata.lighter_current_funding_rate,
                            ts=now,
                        )

        except Exception as exc:
            print(f"Lighter WebSocket error: {exc}", file=sys.stderr)
            await asyncio.sleep(5)
            continue


async def subscribe_pacifica_prices(
    market_data: Dict[str, MarketData],
    db: FundingDB,
):
    """Subscribe to Pacifica's prices feed and update the market_data."""
    subscribe_msg = {"method": "subscribe", "params": {"source": "prices"}}
    while True:
        try:
            async with websockets.connect(PACIFICA_WS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps(subscribe_msg))
                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                    except Exception:
                        continue

                    if msg.get("channel") != "prices":
                        continue
                    data_list = msg.get("data", [])
                    if not isinstance(data_list, list):
                        continue

                    for entry in data_list:
                        symbol = entry.get("symbol")
                        if symbol is None or symbol not in market_data:
                            continue
                        mdata = market_data[symbol]

                        # Parse funding + mark
                        try:
                            fr = (
                                float(entry.get("funding"))
                                if entry.get("funding") is not None
                                else None
                            )
                        except (TypeError, ValueError):
                            fr = None
                        try:
                            nfr = (
                                float(entry.get("next_funding"))
                                if entry.get("next_funding") is not None
                                else None
                            )
                        except (TypeError, ValueError):
                            nfr = None
                        try:
                            mark = (
                                float(entry.get("mark"))
                                if entry.get("mark") is not None
                                else None
                            )
                        except (TypeError, ValueError):
                            mark = None

                        # Check for actual funding changes
                        fr_changed = _changed(mdata.pacifica_funding_rate, fr)
                        nfr_changed = _changed(mdata.pacifica_next_funding_rate, nfr)
                        funding_changed = fr_changed or nfr_changed

                        # Always update mark in memory if present
                        if mark is not None:
                            mdata.pacifica_mark_price = mark

                        now = datetime.utcnow()
                        mdata.last_update_pacifica = now

                        # Only update funding + buffer if changed
                        if funding_changed:
                            if fr_changed and fr is not None:
                                mdata.pacifica_funding_rate = fr
                            if nfr_changed and nfr is not None:
                                mdata.pacifica_next_funding_rate = nfr

                            db.buffer_funding(
                                venue="pacifica",
                                symbol=symbol,
                                funding_rate=mdata.pacifica_funding_rate,
                                next_funding_rate=mdata.pacifica_next_funding_rate,
                                ts=now,
                            )

        except Exception as exc:
            print(f"Pacifica WebSocket error: {exc}", file=sys.stderr)
            await asyncio.sleep(5)
            continue


async def print_market_table(
    market_data: Dict[str, MarketData], refresh_interval: float = 10.0
):
    """Periodically print a summary table of market metrics."""
    while True:
        rows = []
        for symbol, mdata in sorted(market_data.items()):
            lighter_fr = mdata.lighter_funding_rate
            lighter_daily = mdata.compute_24h_funding_lighter()
            lighter_apr = mdata.compute_apr_lighter()
            pacifica_fr = mdata.pacifica_funding_rate
            pacifica_daily = mdata.compute_24h_funding_pacifica()
            pacifica_apr = mdata.compute_apr_pacifica()
            arb_info = mdata.compute_best_funding_arb_24h()
            arb_direction = arb_info["direction"] if arb_info is not None else ""
            arb_edge_24h = arb_info["edge_24h"] if arb_info is not None else None

            diff_daily = None
            if lighter_daily is not None and pacifica_daily is not None:
                diff_daily = lighter_daily - pacifica_daily
            spread_pct = mdata.compute_spread_percentage()
            rows.append(
                {
                    "Symbol": symbol,
                    "Lighter FR (1h)": (
                        f"{lighter_fr:.6f}" if lighter_fr is not None else ""
                    ),
                    "Lighter FR (24h)": (
                        f"{lighter_daily:.6f}" if lighter_daily is not None else ""
                    ),
                    "Lighter APR (%)": (
                        f"{lighter_apr:.4f}" if lighter_apr is not None else ""
                    ),
                    "Pacifica FR (1h)": (
                        f"{pacifica_fr:.6f}" if pacifica_fr is not None else ""
                    ),
                    "Pacifica FR (24h)": (
                        f"{pacifica_daily:.6f}" if pacifica_daily is not None else ""
                    ),
                    "Pacifica APR (%)": (
                        f"{pacifica_apr:.4f}" if pacifica_apr is not None else ""
                    ),
                    "Funding Diff 24h": (
                        f"{diff_daily:.6f}" if diff_daily is not None else ""
                    ),
                    "Lighter Mark": (
                        f"{mdata.lighter_mark_price:.6f}"
                        if mdata.lighter_mark_price is not None
                        else ""
                    ),
                    "Pacifica Mark": (
                        f"{mdata.pacifica_mark_price:.6f}"
                        if mdata.pacifica_mark_price is not None
                        else ""
                    ),
                    "Spread (%)": f"{spread_pct:.4f}" if spread_pct is not None else "",
                    "Best FR Arb": arb_direction,
                    "FR Arb Edge 24h": (
                        f"{arb_edge_24h:.6f}" if arb_edge_24h is not None else ""
                    ),
                    "Update (Lighter)": (
                        mdata.last_update_lighter.strftime("%H:%M:%S")
                        if mdata.last_update_lighter
                        else ""
                    ),
                    "Update (Pacifica)": (
                        mdata.last_update_pacifica.strftime("%H:%M:%S")
                        if mdata.last_update_pacifica
                        else ""
                    ),
                }
            )
        df = pd.DataFrame(rows)
        print("\033[2J\033[H", end="")  # clear screen
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"Funding Rate & Price Spread Dashboard (updated {now})")
        if df.empty:
            print("No data yet...")
        else:
            print(df.to_string(index=False))
        await asyncio.sleep(refresh_interval)


async def flush_loop(db: FundingDB):
    """Periodically flush aggregated funding data to the database."""
    while True:
        await asyncio.sleep(db.flush_interval_sec)
        db.flush_aggregates()


async def main(db: FundingDB):
    market_data = build_market_data()
    if not market_data:
        return
    tasks = [
        subscribe_lighter_market_stats(market_data, db),
        subscribe_pacifica_prices(market_data, db),
        flush_loop(db),
        # uncomment if you want to see the live terminal view
        # print_market_table(market_data),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    db = FundingDB()  # Uses DATABASE_URL from env
    try:
        asyncio.run(main(db))
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        db.close()
