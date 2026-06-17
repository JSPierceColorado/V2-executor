import asyncio
import contextlib
import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Set, Tuple

import gspread
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from fastapi import FastAPI, HTTPException

APP_VERSION = "0.1.0-dry-run-first-draft"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("executor")

app = FastAPI(title="Executor", version=APP_VERSION)

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
MANAGER_TAB_NAME = os.getenv("MANAGER_TAB_NAME", "Manager").strip() or "Manager"
LOG_TAB_NAME = os.getenv("EXECUTOR_LOG_TAB_NAME", "ExecutorLog").strip() or "ExecutorLog"

LOOP_ENABLED = os.getenv("LOOP_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
EXECUTOR_LOOP_INTERVAL_SECONDS = int(os.getenv("EXECUTOR_LOOP_INTERVAL_SECONDS", "300"))
EXECUTOR_LOOP_INITIAL_DELAY_SECONDS = int(os.getenv("EXECUTOR_LOOP_INITIAL_DELAY_SECONDS", "15"))
MIN_LOOP_INTERVAL_SECONDS = int(os.getenv("MIN_LOOP_INTERVAL_SECONDS", "60"))
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() not in {"0", "false", "no", "off"}

# V1 is stock-safe by default. Add us_option explicitly only after you are ready.
ALLOWED_ASSET_CLASSES = {
    item.strip().lower()
    for item in os.getenv("EXECUTOR_ALLOWED_ASSET_CLASSES", "us_equity").split(",")
    if item.strip()
}

MAX_ORDERS_PER_CYCLE = int(os.getenv("MAX_ORDERS_PER_CYCLE", "10"))
MAX_QTY_DECIMALS = int(os.getenv("MAX_QTY_DECIMALS", "6"))
REQUIRE_DATA_STATUS_OK = os.getenv("REQUIRE_DATA_STATUS_OK", "true").strip().lower() not in {"0", "false", "no", "off"}
REQUIRE_STILL_RED = os.getenv("REQUIRE_STILL_RED", "true").strip().lower() not in {"0", "false", "no", "off"}

MANAGER_REQUIRED_HEADERS = [
    "symbol",
    "side",
    "asset_class",
    "qty",
    "unrealized_pct",
    "action",
    "reduce_pct",
    "reason",
    "data_status",
]

LOG_HEADERS = [
    "timestamp",
    "symbol",
    "action",
    "reduce_pct",
    "qty_before",
    "qty_to_sell",
    "asset_class",
    "unrealized_pct",
    "data_status",
    "status",
    "order_id",
    "reason",
    "error",
    "dry_run",
]

state_lock = threading.Lock()
cycle_lock = asyncio.Lock()
loop_task: Optional[asyncio.Task] = None
app_state: Dict[str, Any] = {
    "version": APP_VERSION,
    "last_refresh_started_at": None,
    "last_refresh_finished_at": None,
    "last_refresh_result": None,
    "last_refresh_error": None,
    "last_cycle_source": None,
    "running": False,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds")


def as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    text = as_str(value).replace("%", "")
    if text == "":
        return default
    try:
        number = float(text)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    # Manager stores unrealized_pct as decimal form, not percentage points.
    return number


def as_decimal(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    text = as_str(value).replace(",", "")
    if text == "":
        return default
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return default


def get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def alpaca_trading_client() -> TradingClient:
    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY")

    paper = os.getenv("ALPACA_PAPER", "true").strip().lower() not in {"0", "false", "no", "off"}
    return TradingClient(api_key, secret_key, paper=paper)


def gspread_client() -> gspread.Client:
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_json:
        try:
            info = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from exc
        return gspread.service_account_from_dict(info)

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    return gspread.service_account(filename=credentials_path)


def get_or_create_ws(spreadsheet: gspread.Spreadsheet, title: str, rows: int = 100, cols: int = 20) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        logger.info("Creating worksheet tab: %s", title)
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def load_manager_rows(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    missing = [h for h in MANAGER_REQUIRED_HEADERS if h not in headers]
    if missing:
        raise RuntimeError(f"Manager tab missing required columns: {missing}")

    rows: List[Dict[str, Any]] = []
    for row_number, raw_row in enumerate(values[1:], start=2):
        if not any(as_str(v) for v in raw_row):
            continue
        item = {headers[i]: raw_row[i] if i < len(raw_row) else "" for i in range(len(headers))}
        item["_row_number"] = row_number
        rows.append(item)
    return rows


def append_log_rows(ws: gspread.Worksheet, log_rows: List[List[Any]]) -> None:
    if not log_rows:
        return

    existing = ws.get_all_values()
    if not existing:
        ws.update(values=[LOG_HEADERS], range_name="A1", value_input_option="USER_ENTERED")
        try:
            ws.freeze(rows=1)
            ws.format("A1:N1", {"textFormat": {"bold": True}})
        except Exception:
            logger.info("Skipping optional ExecutorLog formatting", exc_info=True)
    elif existing[0][: len(LOG_HEADERS)] != LOG_HEADERS:
        # Do not destroy user data; append a header marker instead.
        log_rows = [["HEADER_MISMATCH", *[""] * (len(LOG_HEADERS) - 1)]] + log_rows

    ws.append_rows(log_rows, value_input_option="USER_ENTERED")


def current_positions_by_symbol(trading: TradingClient) -> Dict[str, Any]:
    positions = trading.get_all_positions()
    result: Dict[str, Any] = {}
    for pos in positions:
        symbol = as_str(get_field(pos, "symbol")).upper()
        if symbol:
            result[symbol] = pos
    return result


def open_sell_order_symbols(trading: TradingClient) -> Set[str]:
    try:
        request = GetOrdersRequest(status=QueryOrderStatus.OPEN)
        orders = trading.get_orders(filter=request)
    except Exception:
        logger.exception("Could not fetch open orders; failing closed for this cycle")
        raise

    result: Set[str] = set()
    for order in orders:
        symbol = as_str(get_field(order, "symbol")).upper()
        side = as_str(get_field(order, "side")).lower()
        if symbol and side == "sell":
            result.add(symbol)
    return result


def position_unrealized_pct(pos: Any) -> Optional[float]:
    pct = as_float(get_field(pos, "unrealized_plpc"), None)
    if pct is not None:
        return pct

    avg_entry = as_float(get_field(pos, "avg_entry_price"), None)
    current = as_float(get_field(pos, "current_price"), None)
    if avg_entry and current:
        return (current - avg_entry) / avg_entry
    return None


def decimal_qty_from_position(pos: Any) -> Decimal:
    qty = as_decimal(get_field(pos, "qty"), Decimal("0"))
    if qty is None:
        return Decimal("0")
    return qty.copy_abs()


def quantize_qty(qty: Decimal) -> Decimal:
    if qty <= 0:
        return Decimal("0")
    quantum = Decimal("1") if MAX_QTY_DECIMALS <= 0 else Decimal("1").scaleb(-MAX_QTY_DECIMALS)
    return qty.quantize(quantum, rounding=ROUND_DOWN).normalize()


def compute_qty_to_sell(qty_before: Decimal, reduce_pct: float, action: str) -> Decimal:
    if action == "EXIT":
        return qty_before
    if action == "REDUCE":
        pct = max(0.0, min(100.0, reduce_pct)) / 100.0
        return quantize_qty(qty_before * Decimal(str(pct)))
    return Decimal("0")


def manager_asset_class(row: Dict[str, Any]) -> str:
    return as_str(row.get("asset_class")).lower()


def evaluate_row(row: Dict[str, Any], positions: Dict[str, Any], blocked_symbols: Set[str]) -> Tuple[str, str, Decimal, Optional[Any]]:
    """Return status, error/reason, qty_to_sell, position."""
    symbol = as_str(row.get("symbol")).upper()
    action = as_str(row.get("action")).upper()
    data_status = as_str(row.get("data_status")).upper()
    row_side = as_str(row.get("side")).lower()
    asset_class = manager_asset_class(row)
    reduce_pct = as_float(row.get("reduce_pct"), 0.0) or 0.0

    if not symbol:
        return "SKIP", "Missing symbol", Decimal("0"), None
    if action not in {"REDUCE", "EXIT"}:
        return "SKIP", f"Action is {action or 'blank'}", Decimal("0"), None
    if REQUIRE_DATA_STATUS_OK and data_status != "OK":
        return "SKIP", f"data_status is {data_status or 'blank'}", Decimal("0"), None
    if row_side and row_side != "long":
        return "SKIP", f"Unsupported side {row_side}", Decimal("0"), None
    if asset_class not in ALLOWED_ASSET_CLASSES:
        return "SKIP", f"asset_class {asset_class or 'blank'} not allowed", Decimal("0"), None
    if symbol in blocked_symbols:
        return "SKIP", "Open sell order already exists", Decimal("0"), None

    pos = positions.get(symbol)
    if pos is None:
        return "SKIP", "Position no longer exists", Decimal("0"), None

    alpaca_side = as_str(get_field(pos, "side")).lower()
    if alpaca_side and alpaca_side != "long":
        return "SKIP", f"Alpaca position side is {alpaca_side}", Decimal("0"), pos

    live_pct = position_unrealized_pct(pos)
    if REQUIRE_STILL_RED and (live_pct is None or live_pct >= 0):
        return "SKIP", f"Position is not currently red live_pct={live_pct}", Decimal("0"), pos

    qty_before = decimal_qty_from_position(pos)
    if qty_before <= 0:
        return "SKIP", "Position qty is zero", Decimal("0"), pos

    qty_to_sell = compute_qty_to_sell(qty_before, reduce_pct, action)
    if qty_to_sell <= 0:
        return "SKIP", "Computed sell qty is zero", Decimal("0"), pos
    if qty_to_sell > qty_before:
        qty_to_sell = qty_before

    return "READY", "Ready", qty_to_sell, pos


def submit_sell_order(trading: TradingClient, symbol: str, qty_to_sell: Decimal) -> str:
    request = MarketOrderRequest(
        symbol=symbol,
        qty=str(qty_to_sell),
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
    )
    order = trading.submit_order(order_data=request)
    return as_str(get_field(order, "id"))


def run_executor_cycle(source: str = "manual") -> Dict[str, Any]:
    started = iso_now()
    with state_lock:
        app_state.update(
            {
                "last_refresh_started_at": started,
                "last_refresh_finished_at": None,
                "last_refresh_error": None,
                "last_cycle_source": source,
                "running": True,
            }
        )

    logger.info("Starting Executor cycle from %s dry_run=%s", source, DRY_RUN)

    log_rows: List[List[Any]] = []
    summary = {
        "status": "ok",
        "dry_run": DRY_RUN,
        "manager_tab": MANAGER_TAB_NAME,
        "log_tab": LOG_TAB_NAME,
        "allowed_asset_classes": sorted(ALLOWED_ASSET_CLASSES),
        "rows_read": 0,
        "ready": 0,
        "orders_submitted": 0,
        "dry_run_orders": 0,
        "skipped": 0,
        "errors": 0,
        "symbols": [],
        "started_at": started,
        "finished_at": None,
    }

    try:
        if not GOOGLE_SHEET_ID:
            raise RuntimeError("Missing GOOGLE_SHEET_ID")

        gc = gspread_client()
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        manager_ws = spreadsheet.worksheet(MANAGER_TAB_NAME)
        log_ws = get_or_create_ws(spreadsheet, LOG_TAB_NAME, rows=100, cols=len(LOG_HEADERS))

        rows = load_manager_rows(manager_ws)
        summary["rows_read"] = len(rows)

        trading = alpaca_trading_client()
        positions = current_positions_by_symbol(trading)
        sell_blocked = open_sell_order_symbols(trading)

        actions_taken = 0
        for row in rows:
            symbol = as_str(row.get("symbol")).upper()
            action = as_str(row.get("action")).upper()
            reduce_pct = as_float(row.get("reduce_pct"), 0.0) or 0.0
            asset_class = manager_asset_class(row)
            data_status = as_str(row.get("data_status")).upper()
            manager_pct = as_float(row.get("unrealized_pct"), None)
            row_reason = as_str(row.get("reason"))
            qty_before = Decimal("0")
            qty_to_sell = Decimal("0")
            order_id = ""
            error = ""

            try:
                status, reason, qty_to_sell, pos = evaluate_row(row, positions, sell_blocked)
                if pos is not None:
                    qty_before = decimal_qty_from_position(pos)
                    live_pct = position_unrealized_pct(pos)
                    if live_pct is not None:
                        manager_pct = live_pct

                if status == "READY":
                    summary["ready"] += 1
                    summary["symbols"].append(symbol)
                    if actions_taken >= MAX_ORDERS_PER_CYCLE:
                        status = "SKIP"
                        reason = "MAX_ORDERS_PER_CYCLE reached"
                        summary["skipped"] += 1
                    elif DRY_RUN:
                        status = "DRY_RUN"
                        reason = f"Would {action} sell qty {qty_to_sell}"
                        summary["dry_run_orders"] += 1
                        actions_taken += 1
                    else:
                        order_id = submit_sell_order(trading, symbol, qty_to_sell)
                        status = "ORDER_SUBMITTED"
                        reason = f"Submitted {action} sell order"
                        summary["orders_submitted"] += 1
                        actions_taken += 1
                        sell_blocked.add(symbol)
                elif status == "SKIP":
                    summary["skipped"] += 1

            except Exception as exc:
                logger.exception("Executor row failed for symbol=%s", symbol)
                status = "ERROR"
                reason = row_reason or "Row failed"
                error = str(exc)
                summary["errors"] += 1

            log_rows.append(
                [
                    iso_now(),
                    symbol,
                    action,
                    reduce_pct,
                    str(qty_before),
                    str(qty_to_sell),
                    asset_class,
                    manager_pct if manager_pct is not None else "",
                    data_status,
                    status,
                    order_id,
                    reason,
                    error,
                    DRY_RUN,
                ]
            )

        append_log_rows(log_ws, log_rows)
        summary["finished_at"] = iso_now()
        logger.info(
            "Finished Executor cycle from %s: rows=%s ready=%s submitted=%s dry_run=%s skipped=%s errors=%s",
            source,
            summary["rows_read"],
            summary["ready"],
            summary["orders_submitted"],
            summary["dry_run_orders"],
            summary["skipped"],
            summary["errors"],
        )

        with state_lock:
            app_state.update(
                {
                    "last_refresh_finished_at": summary["finished_at"],
                    "last_refresh_result": summary,
                    "last_refresh_error": None,
                    "running": False,
                }
            )
        return summary

    except Exception as exc:
        finished = iso_now()
        logger.exception("Executor cycle failed from %s", source)
        with state_lock:
            app_state.update(
                {
                    "last_refresh_finished_at": finished,
                    "last_refresh_error": str(exc),
                    "last_refresh_result": None,
                    "running": False,
                }
            )
        raise


async def run_cycle_guarded(source: str) -> Dict[str, Any]:
    if cycle_lock.locked():
        return {"status": "busy", "message": "Executor cycle already running"}
    async with cycle_lock:
        return await asyncio.to_thread(run_executor_cycle, source)


async def executor_loop() -> None:
    if EXECUTOR_LOOP_INITIAL_DELAY_SECONDS > 0:
        logger.info("Executor loop initial delay: %s seconds", EXECUTOR_LOOP_INITIAL_DELAY_SECONDS)
        await asyncio.sleep(EXECUTOR_LOOP_INITIAL_DELAY_SECONDS)

    while True:
        started_monotonic = time.monotonic()
        try:
            await run_cycle_guarded("loop")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Executor loop cycle failed; continuing after throttle")

        elapsed = time.monotonic() - started_monotonic
        interval = max(EXECUTOR_LOOP_INTERVAL_SECONDS, MIN_LOOP_INTERVAL_SECONDS)
        sleep_seconds = max(interval, MIN_LOOP_INTERVAL_SECONDS)
        logger.info("Executor loop sleeping for %s seconds after %.2f-second cycle", sleep_seconds, elapsed)
        await asyncio.sleep(sleep_seconds)


@app.on_event("startup")
async def startup_event() -> None:
    global loop_task
    logger.warning(
        "Loaded Executor version=%s loop_enabled=%s interval=%s initial_delay=%s dry_run=%s allowed_asset_classes=%s manager_tab=%s log_tab=%s",
        APP_VERSION,
        LOOP_ENABLED,
        EXECUTOR_LOOP_INTERVAL_SECONDS,
        EXECUTOR_LOOP_INITIAL_DELAY_SECONDS,
        DRY_RUN,
        sorted(ALLOWED_ASSET_CLASSES),
        MANAGER_TAB_NAME,
        LOG_TAB_NAME,
    )
    if LOOP_ENABLED:
        interval = max(EXECUTOR_LOOP_INTERVAL_SECONDS, MIN_LOOP_INTERVAL_SECONDS)
        logger.info("Starting perpetual Executor loop: interval=%s seconds, minimum_interval=%s seconds", interval, MIN_LOOP_INTERVAL_SECONDS)
        loop_task = asyncio.create_task(executor_loop())
    else:
        logger.warning("Perpetual Executor loop disabled by LOOP_ENABLED=false")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global loop_task
    if loop_task:
        loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop_task


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "service": "executor",
        "version": APP_VERSION,
        "loop_enabled": LOOP_ENABLED,
        "loop_interval_seconds": max(EXECUTOR_LOOP_INTERVAL_SECONDS, MIN_LOOP_INTERVAL_SECONDS),
        "dry_run": DRY_RUN,
        "allowed_asset_classes": sorted(ALLOWED_ASSET_CLASSES),
        "manager_tab": MANAGER_TAB_NAME,
        "log_tab": LOG_TAB_NAME,
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/status")
def status() -> Dict[str, Any]:
    with state_lock:
        return dict(app_state)


@app.api_route("/run", methods=["GET", "POST"])
async def run_now() -> Dict[str, Any]:
    try:
        return await run_cycle_guarded("manual")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
