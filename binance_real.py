"""
binance_real.py - zai-jin 真盘下单 API 客户端 (USDⓈ-M Futures)

设计原则:
  1. 只暴露幂等的薄封装,业务逻辑全在 trade_logic.py
  2. 所有错误抛 BinanceAPIError(code, msg, http_status),never silent fail
  3. -2011 (撤单时单已不存在) 视为成功
  4. -1021 timestamp 漂移: 启动 + 每 30min 重新对时
  5. STOP_MARKET 走 /fapi/v1/algoOrder (Algo Service, 2025-12-09 后强制)
  6. One-Way 模式: 下单不传 positionSide
"""
import os
import time
import hmac
import hashlib
import threading
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

load_dotenv("/root/zai-jin/.env")

BASE = "https://fapi.binance.com"
RECV_WINDOW = 5000

_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()

if not _API_KEY or not _API_SECRET:
    raise RuntimeError(
        "BINANCE_API_KEY / BINANCE_API_SECRET 未在 /root/zai-jin/.env 设置"
    )

_session = requests.Session()
_session.headers.update({"X-MBX-APIKEY": _API_KEY})

_time_offset_ms = 0
_time_offset_lock = threading.Lock()
_last_sync_ts = 0.0

_filters_cache = {}
_filters_cache_ts = 0.0


class BinanceAPIError(Exception):
    pass


def _make_api_error(code, msg, http_status=None):
    e = BinanceAPIError(f"[{http_status}] code={code} msg={msg}")
    e.code = code
    e.msg = msg
    e.http_status = http_status
    return e


def _now_ms():
    return int(time.time() * 1000)


def sync_server_time(force=False):
    """对时,30 分钟内默认复用。失败抛异常。"""
    global _time_offset_ms, _last_sync_ts
    now = time.time()
    if not force and (now - _last_sync_ts) < 1800:
        return _time_offset_ms
    r = _session.get(f"{BASE}/fapi/v1/time", timeout=5)
    if r.status_code != 200:
        raise _make_api_error(-1, f"sync time http {r.status_code}", r.status_code)
    server = int(r.json()["serverTime"])
    local = _now_ms()
    with _time_offset_lock:
        _time_offset_ms = server - local
        _last_sync_ts = now
    print(f"[binance_real] time offset = {_time_offset_ms}ms", flush=True)
    return _time_offset_ms


def _sign(params):
    qs = urlencode(params, doseq=True)
    sig = hmac.new(_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def _request(method, path, params=None, signed=False, timeout=10):
    """统一请求入口。signed 自动加 timestamp + signature。"""
    params = dict(params or {})
    if signed:
        if _last_sync_ts == 0:
            sync_server_time(force=True)
        params["timestamp"] = _now_ms() + _time_offset_ms
        params["recvWindow"] = RECV_WINDOW

    url = f"{BASE}{path}"
    try:
        if method == "GET":
            if signed:
                r = _session.get(url + "?" + _sign(params), timeout=timeout)
            else:
                r = _session.get(url, params=params, timeout=timeout)
        elif method == "POST":
            if signed:
                r = _session.post(url + "?" + _sign(params), timeout=timeout)
            else:
                r = _session.post(url, params=params, timeout=timeout)
        elif method == "DELETE":
            if signed:
                r = _session.delete(url + "?" + _sign(params), timeout=timeout)
            else:
                r = _session.delete(url, params=params, timeout=timeout)
        else:
            raise ValueError(f"bad method {method}")
    except requests.RequestException as e:
        raise _make_api_error(-2, f"network err: {e}", None)

    if r.status_code != 200:
        try:
            j = r.json()
            code = j.get("code", -1)
            msg = j.get("msg", r.text[:200])
        except Exception:
            code = -1
            msg = r.text[:200]
        if code == -1021:
            sync_server_time(force=True)
        raise _make_api_error(code, msg, r.status_code)

    return r.json()


# ---------- 账户 / 仓位模式 ----------

def get_account_balance():
    """返回 USDT 可用余额 (float)。"""
    j = _request("GET", "/fapi/v2/balance", signed=True)
    for x in j:
        if x.get("asset") == "USDT":
            return float(x.get("availableBalance", 0))
    return 0.0


def assert_one_way_mode():
    """断言 One-Way 模式。否则切换并验证;失败抛异常。"""
    j = _request("GET", "/fapi/v1/positionSide/dual", signed=True)
    if str(j.get("dualSidePosition")).lower() == "false":
        return True
    print("[binance_real] WARN: Hedge Mode, switching to One-Way", flush=True)
    try:
        _request("POST", "/fapi/v1/positionSide/dual",
                params={"dualSidePosition": "false"}, signed=True)
    except BinanceAPIError as e:
        if e.code != -4059:
            raise
    j2 = _request("GET", "/fapi/v1/positionSide/dual", signed=True)
    if str(j2.get("dualSidePosition")).lower() != "false":
        raise _make_api_error(-9, "failed to switch to One-Way", None)
    return True


def set_leverage(symbol, leverage):
    return _request("POST", "/fapi/v1/leverage",
                params={"symbol": symbol, "leverage": int(leverage)},
                signed=True)


def set_isolated(symbol):
    """切到逐仓。已经是逐仓返回 -4046,吞掉。"""
    try:
        return _request("POST", "/fapi/v1/marginType",
                params={"symbol": symbol, "marginType": "ISOLATED"},
                signed=True)
    except BinanceAPIError as e:
        if e.code == -4046:
            return {"ok": True, "msg": "already isolated"}
        raise


# ---------- exchangeInfo / 精度 ----------

def _refresh_filters(force=False):
    """拉 exchangeInfo,缓存 1 小时。"""
    global _filters_cache, _filters_cache_ts
    now = time.time()
    if not force and _filters_cache and (now - _filters_cache_ts) < 3600:
        return _filters_cache
    j = _request("GET", "/fapi/v1/exchangeInfo")
    cache = {}
    for s in j.get("symbols", []):
        sym = s.get("symbol")
        if not sym or s.get("status") != "TRADING":
            continue
        tick = step = min_qty = min_notional = None
        for f in s.get("filters", []):
            ft = f.get("filterType")
            if ft == "PRICE_FILTER":
                tick = float(f.get("tickSize"))
            elif ft == "LOT_SIZE":
                step = float(f.get("stepSize"))
                min_qty = float(f.get("minQty"))
            elif ft == "MIN_NOTIONAL":
                min_notional = float(f.get("notional"))
        cache[sym] = {
            "tickSize": tick,
            "stepSize": step,
            "minQty": min_qty,
            "minNotional": min_notional or 5.0,
            "pricePrecision": int(s.get("pricePrecision", 2)),
            "quantityPrecision": int(s.get("quantityPrecision", 3)),
        }
    _filters_cache = cache
    _filters_cache_ts = now
    print(f"[binance_real] exchangeInfo refreshed, symbols={len(cache)}", flush=True)
    return cache


def get_filters(symbol):
    """返回 {tickSize, stepSize, minQty, minNotional, pricePrecision, quantityPrecision}。"""
    cache = _refresh_filters()
    f = cache.get(symbol)
    if not f:
        cache = _refresh_filters(force=True)
        f = cache.get(symbol)
    if not f:
        raise _make_api_error(-3, f"symbol {symbol} not found in exchangeInfo")
    return f


def _round_step(value, step):
    """按 step 向下取整,避免浮点误差。"""
    if step <= 0:
        return value
    n = int(value / step + 1e-9)
    return n * step


def round_qty(symbol, qty):
    f = get_filters(symbol)
    rounded = _round_step(qty, f["stepSize"])
    prec = f["quantityPrecision"]
    return float(f"{rounded:.{prec}f}")


def round_price(symbol, price):
    f = get_filters(symbol)
    rounded = _round_step(price, f["tickSize"])
    prec = f["pricePrecision"]
    return float(f"{rounded:.{prec}f}")


# ---------- 下单 / 撤单 ----------

def place_market(symbol, side, qty, reduce_only=False, client_order_id=None):
    """市价单。side=BUY/SELL。reduce_only=True 用于止盈/平仓。"""
    qty_r = round_qty(symbol, qty)
    if qty_r <= 0:
        raise _make_api_error(-4, f"qty {qty} rounded to 0 for {symbol}")
    f = get_filters(symbol)
    if not reduce_only:
        mark = get_mark_price(symbol)
        notional = qty_r * mark
        if notional < f["minNotional"]:
            raise _make_api_error(-5, f"notional {notional:.4f} < min {f['minNotional']}")
    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty_r,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if client_order_id:
        params["newClientOrderId"] = client_order_id
    r = _request("POST", "/fapi/v1/order", params=params, signed=True)
    print(f"[binance_real] MARKET {side} {symbol} qty={qty_r} -> orderId={r.get('orderId')}", flush=True)
    return r


def place_algo_stop_close(symbol, side, stop_price, client_algo_id=None):
    """硬止损 (Algo Service, 2025-12-09 后强制路径)。
       side = 平仓方向 (LONG 持仓→SELL, SHORT 持仓→BUY)。
       closePosition=true 不需要 quantity, workingType=MARK_PRICE 防针刺。"""
    sp_r = round_price(symbol, stop_price)
    params = {
        "symbol": symbol,
        "side": side,
        "algoType": "CONDITIONAL",
        "type": "STOP_MARKET",
        "stopPrice": sp_r,
        "closePosition": "true",
        "workingType": "MARK_PRICE",
        "priceProtect": "true",
    }
    if client_algo_id:
        params["newClientAlgoId"] = client_algo_id
    r = _request("POST", "/fapi/v1/algoOrder", params=params, signed=True)
    print(f"[binance_real] STOP_MARKET {side} {symbol} stop={sp_r} -> algoId={r.get('algoId') or r.get('orderId')}", flush=True)
    return r


def cancel_algo_order(symbol, algo_id=None, client_algo_id=None):
    """撤 Algo 单。-2011 视为成功。"""
    params = {"symbol": symbol}
    if algo_id:
        params["algoId"] = algo_id
    elif client_algo_id:
        params["origClientAlgoId"] = client_algo_id
    else:
        raise _make_api_error(-6, "need algo_id or client_algo_id")
    try:
        return _request("DELETE", "/fapi/v1/algoOrder", params=params, signed=True)
    except BinanceAPIError as e:
        if e.code == -2011:
            return {"ok": True, "msg": "already gone (-2011 idempotent)"}
        raise


def list_open_algo_orders(symbol=None):
    params = {}
    if symbol:
        params["symbol"] = symbol
    return _request("GET", "/fapi/v1/algoOrder/open-orders", params=params, signed=True)


def cancel_all_orders(symbol):
    """撤普通单 + 撤所有 algo 单。两步分别执行,任一 -2011 视为成功。"""
    results = {}
    try:
        results["regular"] = _request("DELETE", "/fapi/v1/allOpenOrders",
                params={"symbol": symbol}, signed=True)
    except BinanceAPIError as e:
        if e.code != -2011:
            raise
        results["regular"] = {"ok": True, "msg": "-2011"}
    try:
        opens = list_open_algo_orders(symbol)
        algo_results = []
        for o in opens.get("orders", opens if isinstance(opens, list) else []):
            aid = o.get("algoId") or o.get("orderId")
            if aid:
                algo_results.append(cancel_algo_order(symbol, algo_id=aid))
        results["algo"] = algo_results
    except BinanceAPIError as e:
        results["algo_err"] = str(e)
    return results


# ---------- 仓位查询 ----------

def get_position(symbol):
    """返回单个 symbol 的持仓 dict。无持仓时 positionAmt=0。"""
    j = _request("GET", "/fapi/v2/positionRisk",
                params={"symbol": symbol}, signed=True)
    for p in j:
        if p.get("symbol") == symbol:
            return {
                "symbol": symbol,
                "positionAmt": float(p.get("positionAmt", 0)),
                "entryPrice": float(p.get("entryPrice", 0)),
                "markPrice": float(p.get("markPrice", 0)),
                "unRealizedProfit": float(p.get("unRealizedProfit", 0)),
                "liquidationPrice": float(p.get("liquidationPrice", 0)),
                "leverage": int(p.get("leverage", 1)),
                "marginType": p.get("marginType"),
            }
    return {"symbol": symbol, "positionAmt": 0.0}


def get_all_positions():
    """返回所有非零持仓。"""
    j = _request("GET", "/fapi/v2/positionRisk", signed=True)
    out = []
    for p in j:
        amt = float(p.get("positionAmt", 0))
        if abs(amt) > 0:
            out.append({
                "symbol": p.get("symbol"),
                "positionAmt": amt,
                "entryPrice": float(p.get("entryPrice", 0)),
                "markPrice": float(p.get("markPrice", 0)),
                "unRealizedProfit": float(p.get("unRealizedProfit", 0)),
                "leverage": int(p.get("leverage", 1)),
            })
    return out


def get_mark_price(symbol):
    """返回 mark price (float)。无签名公开接口。"""
    j = _request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol})
    return float(j.get("markPrice", 0))

