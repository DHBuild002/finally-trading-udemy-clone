# Market Data Backend — Detailed Design

Status: describes the **as-built** system in `backend/app/market/` (complete, tested, reviewed — see `MARKET_DATA_SUMMARY.md`). This document consolidates and supersedes the individual notes in `planning/archive/` (`MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md`, `MARKET_DATA_REVIEW.md`) into a single reference, with code drawn directly from the shipped source so it stays accurate as the rest of the platform is built on top of it.

## 1. Goals & Constraints

From `planning/PLAN.md` §6:

- One abstract interface, two interchangeable implementations: a **GBM simulator** (default, zero config) and a **Massive (Polygon.io) REST poller** (real data, opt-in via `MASSIVE_API_KEY`).
- A single shared, thread-safe **price cache** decouples producers (simulator/poller) from consumers (SSE stream, portfolio valuation, trade execution) — none of them know or care which data source is active.
- Streaming to the browser via **SSE** (`GET /api/stream/prices`), not WebSockets — one-way push is all that's needed.
- Runs as an in-process `asyncio` background task; no external services required for the default (simulator) path.

## 2. Architecture

```
                    ┌───────────────────────────┐
 MASSIVE_API_KEY?   │   create_market_data_source│
   set / unset  ──▶ │   (factory.py)             │
                    └──────────────┬─────────────┘
                                   │ returns
                    ┌──────────────┴─────────────┐
                    │      MarketDataSource (ABC)  │
                    │      interface.py            │
                    └──────────────┬─────────────┘
                     implements   ╱ ╲   implements
                                 ╱   ╲
              ┌─────────────────┘     └─────────────────┐
              ▼                                          ▼
   SimulatorDataSource                          MassiveDataSource
   (simulator.py)                               (massive_client.py)
   GBM engine, 500ms tick                       REST poll, 15s interval
              │                                          │
              └─────────────────┬────────────────────────┘
                                 ▼
                       PriceCache (cache.py)
                  thread-safe, version-counted,
                  in-memory dict[ticker -> PriceUpdate]
                                 │
                ┌────────────────┼────────────────────┐
                ▼                ▼                     ▼
      SSE stream endpoint   Portfolio valuation   Trade execution
      (stream.py)           (reads cache.get_all) (reads cache.get(ticker))
      GET /api/stream/prices
```

**Strategy pattern**: `SimulatorDataSource` and `MassiveDataSource` both implement `MarketDataSource`. Nothing downstream branches on which one is active — that decision is made once, at startup, by the factory.

**Single point of truth**: producers (the active data source) write to `PriceCache`; every consumer reads from it. This means the SSE stream, portfolio P&L math, and trade fills always agree on "the current price," regardless of data source.

### File layout

```
backend/app/market/
├── __init__.py          # Public exports
├── models.py            # PriceUpdate (immutable dataclass)
├── interface.py          # MarketDataSource (ABC)
├── cache.py              # PriceCache (thread-safe store)
├── seed_prices.py        # Seed prices, per-ticker GBM params, correlation groups
├── simulator.py           # GBMSimulator + SimulatorDataSource
├── massive_client.py      # MassiveDataSource (Polygon.io REST poller)
├── factory.py             # create_market_data_source()
└── stream.py              # create_stream_router() — SSE endpoint factory
```

## 3. Data Model — `PriceUpdate`

The **only** object that crosses the boundary out of the market data layer. Frozen and slotted for cheap, safe sharing across threads/tasks.

```python
# app/market/models.py
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

`change`, `change_percent`, and `direction` are computed properties rather than stored fields — there is exactly one source of truth (`price` vs `previous_price`), so they can never drift out of sync.

## 4. Unified Interface — `MarketDataSource`

Both implementations honor the same lifecycle contract:

```python
# app/market/interface.py
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources. Safe to call twice."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

Because `add_ticker` / `remove_ticker` are part of the interface, the watchlist API (`POST/DELETE /api/watchlist`) can mutate the live data feed without ever knowing whether it's talking to the simulator or Massive.

## 5. Price Cache — the shared point of truth

```python
# app/market/cache.py
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # bumped on every update — SSE change detection

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Design notes:

- **Why a `Lock` and not asyncio-only coordination**: `MassiveDataSource` runs its (synchronous) HTTP calls via `asyncio.to_thread`, so writes can happen from a worker thread, not just the event loop. A plain `threading.Lock` is correct for both the thread and the coroutine callers.
- **Why a `version` counter**: it lets the SSE endpoint skip re-sending an unchanged payload without diffing every ticker — O(1) change detection.
- **Bounded memory**: only the *latest* `PriceUpdate` per ticker is retained — O(number of tickers), not O(ticks). Historical accumulation (e.g., for sparklines) is a frontend concern per PLAN.md §2 — it builds its own client-side buffer from the SSE stream.

## 6. Demo Mode — GBM Simulator

Used whenever `MASSIVE_API_KEY` is unset (the default, zero-config path).

### 6.1 Model

Prices follow **Geometric Brownian Motion**, the standard continuous-time model behind Black-Scholes:

```
S(t+dt) = S(t) * exp((mu - sigma²/2) * dt + sigma * sqrt(dt) * Z)
```

- `S(t)` — current price
- `mu` — annualized drift (expected return)
- `sigma` — annualized volatility
- `dt` — time step as a fraction of a trading year
- `Z` — a (correlated) standard normal draw

This guarantees prices stay strictly positive (it's multiplicative via `exp`) and reproduces the lognormal return distribution seen in real markets.

At ~500ms per tick over a 252-day, 6.5-hour trading year:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick that accumulate into realistic intraday ranges over minutes of runtime.

### 6.2 Correlated moves via Cholesky decomposition

Real stocks don't move independently. A correlation matrix is built per pair of tickers, then decomposed (`L = cholesky(C)`) so that `L @ Z_independent` yields correlated draws:

```python
# app/market/seed_prices.py — correlation configuration
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6      # tech stocks move together
INTRA_FINANCE_CORR = 0.5   # finance stocks move together
CROSS_GROUP_CORR = 0.3     # between sectors / unknown tickers
TSLA_CORR = 0.3            # TSLA does its own thing
```

```python
# app/market/simulator.py — pairwise correlation + Cholesky rebuild
@staticmethod
def _pairwise_correlation(t1: str, t2: str) -> float:
    tech = CORRELATION_GROUPS["tech"]
    finance = CORRELATION_GROUPS["finance"]

    if t1 == "TSLA" or t2 == "TSLA":
        return TSLA_CORR
    if t1 in tech and t2 in tech:
        return INTRA_TECH_CORR
    if t1 in finance and t2 in finance:
        return INTRA_FINANCE_CORR
    return CROSS_GROUP_CORR

def _rebuild_cholesky(self) -> None:
    """O(n^2); called whenever a ticker is added or removed. n stays small (<50)."""
    n = len(self._tickers)
    if n <= 1:
        self._cholesky = None
        return

    corr = np.eye(n)
    for i in range(n):
        for j in range(i + 1, n):
            rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
            corr[i, j] = rho
            corr[j, i] = rho

    self._cholesky = np.linalg.cholesky(corr)
```

A well-formed correlation matrix (symmetric, 1s on the diagonal, entries in `[-1, 1]`, positive semi-definite) is guaranteed to Cholesky-decompose — the sector-block structure above is constructed to satisfy that by design.

### 6.3 Random shock events

Every tick, each ticker has a small independent chance of a sudden 2–5% move — enough to keep a demo dashboard visually interesting without destabilizing the price path:

```python
if random.random() < self._event_prob:            # default 0.001 (0.1%)
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    self._prices[ticker] *= 1 + shock_magnitude * shock_sign
```

With `event_probability=0.001` and 10 tickers at 2 ticks/sec, expect a visible event roughly every 50 seconds.

### 6.4 Seed prices & per-ticker parameters

```python
# app/market/seed_prices.py
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # high volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # high volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}
```

Tickers added at runtime that aren't in `SEED_PRICES`/`TICKER_PARAMS` (e.g., a user adds `PYPL` to their watchlist) fall back to a random seed price in `$50–$300` and `DEFAULT_PARAMS`.

### 6.5 The engine — `GBMSimulator`

```python
# app/market/simulator.py
class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR

    def __init__(self, tickers, dt=DEFAULT_DT, event_probability=0.001):
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Hot path — called every 500ms."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)
```

### 6.6 Wiring into the async lifecycle — `SimulatorDataSource`

```python
# app/market/simulator.py
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5,
                 event_probability: float = 0.001) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache immediately so the first SSE poll already has data
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Step the simulation, write to cache, sleep. Exceptions are caught so
        one bad tick never kills the background task."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Key behaviors: the cache is seeded synchronously in `start()`/`add_ticker()` before the ticker's first `step()`, so there's never a window where a watchlisted ticker has no price. `stop()` cancels the task and awaits it so shutdown is deterministic (no dangling tasks in tests or on app teardown).

## 7. Real Data Mode — Massive (Polygon.io) API

Used whenever `MASSIVE_API_KEY` is a non-empty string.

### 7.1 API reference

- Package: `massive` (declared as a core dependency in `pyproject.toml`, `massive>=1.0.0`)
- Base URL: `https://api.massive.com` (legacy `https://api.polygon.io` also supported)
- Auth: `RESTClient(api_key=...)` — sends `Authorization: Bearer <API_KEY>` automatically
- Rate limits: **Free tier 5 req/min** → poll every 15s (the default); paid tiers support 2–5s

Primary endpoint — snapshot for *all* watched tickers in one call:

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="...")
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)
for snap in snapshots:
    print(snap.ticker, snap.last_trade.price, snap.last_trade.timestamp)
```

Fields used: `last_trade.price` (current price) and `last_trade.timestamp` (Unix **milliseconds** — must be divided by 1000 before going into `PriceCache`, which expects seconds).

Fetching all tickers in a single call is what keeps polling within the free tier's 5 req/min limit regardless of watchlist size.

### 7.2 Implementation — `MassiveDataSource`

```python
# app/market/massive_client.py
from massive import RESTClient
from massive.rest.models import SnapshotMarketType


class MassiveDataSource(MarketDataSource):
    """Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache."""

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # immediate first poll — cache has data right away
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)  # appears on next poll

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # The Massive RESTClient is synchronous — run it in a worker thread
            # so it never blocks the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms -> seconds
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Not re-raised — next scheduled poll retries. Covers 401/429/network errors.

    def _fetch_snapshots(self) -> list:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

Design notes:

- **`asyncio.to_thread`** — the `massive` client is a synchronous `requests`-based client; wrapping the blocking call keeps the event loop free for SSE clients and other requests during the ~seconds a poll can take.
- **Immediate first poll in `start()`** mirrors the simulator's cache-seeding behavior — a client connecting to `/api/stream/prices` right after startup shouldn't see an empty payload for 15 seconds.
- **Per-snapshot error isolation** — a malformed or missing field on one ticker (`AttributeError`/`TypeError`) is logged and skipped rather than aborting the whole poll cycle.
- **Whole-poll error isolation** — any other exception (network failure, 401, 429) is caught, logged, and the loop simply retries on the next interval; it never crashes the background task.
- **Error codes to expect**: `401` invalid key, `403` plan doesn't include the endpoint, `429` rate limit exceeded (free tier), `5xx` transient server errors.

## 8. Factory — selecting the source

```python
# app/market/factory.py
import os


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty -> MassiveDataSource; otherwise -> SimulatorDataSource.

    Returns an unstarted source — caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

This is the single decision point in the whole codebase for "which data source." Everything else — SSE stream, portfolio math, trade execution, the LLM chat's portfolio context — is written against `MarketDataSource`/`PriceCache` only, so switching from demo to live data is a one-line environment variable change with zero code changes elsewhere.

## 9. SSE Streaming Endpoint

```python
# app/market/stream.py
router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Factory pattern injects the PriceCache without module-level globals."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache, request: Request, interval: float = 0.5
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"  # browser auto-reconnects after 1s if the connection drops

    last_version = -1
    try:
        while True:
            if await request.is_disconnected():
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
```

Wire format delivered to `EventSource`:

```
data: {"AAPL": {"ticker": "AAPL", "price": 190.52, "previous_price": 190.48, "timestamp": 1770000000.123, "change": 0.04, "change_percent": 0.021, "direction": "up"}, "GOOGL": {...}, ...}

```

Behaviors:

- **Version-gated sends** — nothing is written to the wire unless `PriceCache.version` changed since the last poll, avoiding redundant payloads when a ticker set is momentarily static.
- **Disconnect detection** — polls `request.is_disconnected()` every loop iteration so the generator (and its `asyncio.sleep`) exit promptly when the browser tab closes, instead of running forever.
- **`retry: 1000`** plus native `EventSource` reconnection gives the "yellow dot → reconnecting" behavior described in PLAN.md §2 for free — the client doesn't need custom retry logic.

## 10. Application Integration

The market data module is self-contained and does not yet own a `main.py` — it exposes exactly what the rest of the backend needs via `app/market/__init__.py`:

```python
# app/market/__init__.py
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = ["PriceUpdate", "PriceCache", "MarketDataSource",
           "create_market_data_source", "create_stream_router"]
```

Expected FastAPI wiring (for the app-assembly work described in PLAN.md §3/§8) — a lifespan handler owns the cache and the source, and the router is mounted normally:

```python
# app/main.py (illustrative — app-level wiring, not yet built)
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    tickers = load_watchlist_tickers()  # from SQLite, default: the 10 seed tickers
    source = create_market_data_source(price_cache)
    await source.start(tickers)
    app.state.market_source = source
    app.state.price_cache = price_cache
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

Downstream consumers only ever need the cache (and, for watchlist mutation, the source):

```python
# GET /api/watchlist — attach live prices
@router.get("/api/watchlist")
async def get_watchlist(request: Request):
    cache: PriceCache = request.app.state.price_cache
    tickers = fetch_watchlist_from_db()
    return [
        {"ticker": t, **(cache.get(t).to_dict() if cache.get(t) else {"price": None})}
        for t in tickers
    ]


# POST /api/watchlist — add a ticker to both DB and the live feed
@router.post("/api/watchlist")
async def add_to_watchlist(body: WatchlistAdd, request: Request):
    save_watchlist_entry_to_db(body.ticker)
    source: MarketDataSource = request.app.state.market_source
    await source.add_ticker(body.ticker)
    return {"ticker": body.ticker}


# POST /api/portfolio/trade — fill at the current cached price
@router.post("/api/portfolio/trade")
async def execute_trade(body: TradeRequest, request: Request):
    cache: PriceCache = request.app.state.price_cache
    price = cache.get_price(body.ticker)
    if price is None:
        raise HTTPException(400, f"No price available for {body.ticker}")
    return apply_trade(ticker=body.ticker, side=body.side, quantity=body.quantity, fill_price=price)
```

## 11. Usage Cheat Sheet

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)   # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                     "NVDA", "META", "JPM", "V", "NFLX"])

# Reads (used everywhere downstream)
update = cache.get("AAPL")          # PriceUpdate | None
price = cache.get_price("AAPL")     # float | None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist (mirrors SQLite watchlist table changes)
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```

Local demo (no FastAPI needed) — `backend/market_data_demo.py` runs a Rich terminal dashboard directly against `SimulatorDataSource` + `PriceCache`:

```bash
cd backend
uv run market_data_demo.py
```

## 12. Testing Strategy (as implemented)

73 tests across 6 modules in `backend/tests/market/`, 84% overall coverage:

| Module | Tests | Coverage | What's verified |
|---|---|---|---|
| `test_models.py` | 11 | 100% | `change`/`change_percent`/`direction` math, `to_dict()` shape, immutability |
| `test_cache.py` | 13 | 100% | update/get/get_all/remove, version bump, first-update flat direction, thread-safety of reads/writes |
| `test_simulator.py` | 17 | 98% | GBM math correctness, Cholesky rebuild on add/remove, price positivity, shock events, per-ticker params |
| `test_simulator_source.py` | 10 | — | `start`/`stop`/`add_ticker`/`remove_ticker` lifecycle, cache seeding, task cancellation |
| `test_factory.py` | 7 | 100% | env-var branching (`MASSIVE_API_KEY` set/unset/whitespace) |
| `test_massive.py` | 13 | 56%† | snapshot parsing, timestamp ms→s conversion, malformed-snapshot skip, poll-loop cancellation |

† Lower by design: real HTTP calls to Polygon.io are mocked, so the `massive` package's own network code is intentionally not exercised.

```bash
cd backend
uv run --extra dev pytest -v                    # all tests
uv run --extra dev pytest --cov=app             # with coverage
uv run --extra dev ruff check app/ tests/       # lint
```

Notable test-design choices carried over from the code review (`MARKET_DATA_REVIEW.md`):

- `massive` is a **core** dependency (not optional/lazy-imported) specifically so `patch("app.market.massive_client.RESTClient")` can target a real module-level name in tests, rather than needing `create=True`.
- `pyproject.toml` declares `[tool.hatch.build.targets.wheel] packages = ["app"]` — required for `uv sync` / Docker builds to locate the package; its absence was the one high-severity issue caught in review.
- Background-task tests always assert `stop()` cancels and awaits the task (no dangling `asyncio.Task` warnings between tests).

## 13. Design Decisions & Trade-offs

| Decision | Rationale |
|---|---|
| REST polling over WebSocket for Massive | Works on every Polygon.io tier including free; one call fetches all tickers, so watchlist size doesn't multiply request volume |
| GBM over a simpler random walk | Prices can't go negative, and the lognormal return distribution matches how real equities actually move |
| Cholesky-correlated shocks | Sector co-movement ("tech stocks move together") reads as far more realistic than independent per-ticker noise, at negligible extra cost (n < 50) |
| `PriceCache` holds only the *latest* price | Historical accumulation for sparklines/charts is a frontend concern (built from the SSE stream since page load, per PLAN.md §2); the backend cache stays O(tickers), not O(ticks) |
| Version counter instead of a diff/pub-sub | Simplest possible change-detection primitive; avoids building an event bus for a single SSE consumer type |
| Factory selects source once, at startup | Keeps the "which data source" branch in exactly one place; every other module (SSE, portfolio, chat context) is written only against the abstract interface |
| Errors caught and logged inside both background loops | A single bad tick or a transient Massive 5xx/429 must never kill the long-running task — it retries on the next interval |

## 14. Known Limitations / Future Considerations

- `MassiveDataSource` has no fallback to the simulator mid-run if the API key is later found to be invalid (e.g. 401 on every poll) — it will simply log errors indefinitely without prices ever updating for that source's tickers. An operator would need to unset `MASSIVE_API_KEY` and restart. Acceptable for a single-user course project; a production system might want automatic fallback or a health-check surfaced via `/api/health`.
- `PriceCache.version` is read outside the lock in the `version` property; on CPython's GIL this is safe (a single `int` read is atomic), but it's inconsistent with the rest of the class and would need attention if the project ever targets a no-GIL Python build.
- No dedicated historical OHLCV/aggregates endpoint is implemented yet (Massive's `list_aggs`/`get_previous_close_agg` are documented in the archived `MASSIVE_API.md` reference but unused) — the current design only needs live/last-trade prices. Add a `/api/history/{ticker}` route backed by `client.list_aggs(...)` if a candlestick chart is added later; it fits cleanly alongside the existing REST polling without touching `PriceCache` or the SSE path.
