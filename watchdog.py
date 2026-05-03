#!/usr/bin/env python3
"""zaijin watchdog: cron 每 1min 跑.
检查:
  1. auto_trader systemd active
  2. 兜底硬止损: binance mark vs DB sl_price 直接比较, 触达 -> SELL reduce_only
  3. watcher 卡死检测: updated_at > 180s 警告
触发动作: alert log + wall 广播
"""
import sys, os, subprocess
from datetime import datetime, timezone

os.chdir('/root/zai-jin')
sys.path.insert(0, '/root/zai-jin')

import binance_real, storage

ALERT_DIR = "/root/zai-jin/logs/alerts"
os.makedirs(ALERT_DIR, exist_ok=True)
ALERT_LOG = f"{ALERT_DIR}/{datetime.now().strftime('%Y-%m-%d')}.log"

def alert(level, msg):
    line = f"[{datetime.now().isoformat(timespec='seconds')}] [{level}] {msg}"
    print(line, flush=True)
    with open(ALERT_LOG, 'a') as f:
        f.write(line + "\n")
    if level in ("CRITICAL", "ERROR"):
        try: subprocess.run(["wall", line], timeout=3, check=False)
        except Exception: pass

def emergency_close(symbol, qty, side_long, reason):
    try:
        side = "SELL" if side_long else "BUY"
        r = binance_real.place_market(symbol, side, abs(qty), reduce_only=True)
        alert("CRITICAL", f"WATCHDOG 强行平 {symbol} {side} qty={qty} reason={reason} orderId={r.get('orderId')}")
        try:
            binance_real._request("DELETE", "/fapi/v1/allOpenOrders",
                                  params={"symbol": symbol}, signed=True)
        except Exception: pass
    except Exception as e:
        alert("CRITICAL", f"WATCHDOG 平仓失败 {symbol}: {e}")


def sweep_dust_positions(threshold_usdt=1.0):
    """P12-D: 扫 binance 端 < $1 USDT 但 DB 无 OPEN/PARTIAL 记录的 dust 残留，reduce_only 平掉。"""
    try:
        positions = binance_real.get_all_positions()
    except Exception as e:
        alert("ERROR", f"sweep_dust get_all_positions failed: {e}")
        return
    try:
        with storage.get_conn() as c:
            db_open = {r['symbol'] for r in c.cursor().execute(
                "SELECT symbol FROM trade_positions WHERE status IN ('OPEN','PARTIAL') AND mode='live'"
            )}
    except Exception as e:
        alert("ERROR", f"sweep_dust db read failed: {e}")
        return

    swept = 0
    for p in positions:
        try:
            qty = abs(float(p.get('positionAmt') or 0))
            if qty < 1e-12:
                continue
            sym = p['symbol']
            if sym in db_open:
                continue
            mark = float(p.get('markPrice') or 0)
            notional = qty * mark
            if notional >= threshold_usdt:
                # 安全网：≥1U 但不在 DB → 异常，告警不自动平
                alert("WARNING", f"orphan position {sym} qty={qty} notional=${notional:.2f} not in DB - manual check")
                continue
            # 是 dust，reduce_only 平
            side = 'SELL' if float(p['positionAmt']) > 0 else 'BUY'
            try:
                r = binance_real.place_market(sym, side, qty, reduce_only=True)
                oid = r.get('orderId') if isinstance(r, dict) else r
                alert("INFO", f"dust sweep {sym} {side} qty={qty} ~${notional:.4f} orderId={oid}")
                swept += 1
            except Exception as e:
                alert("ERROR", f"dust sweep {sym} place_market failed: {e}")
            try:
                binance_real.cancel_all_orders(sym)
            except Exception:
                pass
        except Exception as e:
            alert("ERROR", f"sweep_dust loop exc on {p}: {e}")
    if swept > 0:
        alert("INFO", f"sweep_dust: {swept} dust positions cleaned")


def main():
    storage.init_db()

    # P12-D 2026-05-03: 独立 dust sweep（不依赖 DB 有 OPEN 仓位）
    try:
        sweep_dust_positions()
    except Exception as e:
        alert("ERROR", f"sweep_dust_positions exception: {e}")

    
    rc = subprocess.run(["systemctl", "is-active", "--quiet", "zaijin-auto_trader"]).returncode
    if rc != 0:
        alert("CRITICAL", "auto_trader systemd inactive!")
    
    with storage.get_conn() as conn:
        rows = conn.execute("""
            SELECT id, token, symbol, side, quantity, entry_price, stop_loss_price,
                   COALESCE(closed_qty, 0) as closed_qty,
                   COALESCE(updated_at, created_at) as last_update
            FROM trade_positions
            WHERE mode='live' AND status IN ('OPEN','PARTIAL')
        """).fetchall()
    
    if not rows:
        alert("INFO", "no live position (idle OK)")
        return
    
    try:
        positions = {p['symbol']: p for p in binance_real.get_all_positions()}
    except Exception as e:
        alert("ERROR", f"get_all_positions fail: {e}")
        return
    
    now = datetime.now(timezone.utc)
    summary = []
    for r in rows:
        d = dict(r)
        sym = d['symbol']
        sl = float(d['stop_loss_price'] or 0)
        side_long = (d['side'] == 'LONG')
        
        bp = positions.get(sym)
        if not bp:
            alert("WARNING", f"{sym} DB OPEN 但 binance 无, status 异常")
            continue
        actual_qty = abs(float(bp.get('positionAmt') or 0))
        if actual_qty < 1e-10:
            alert("WARNING", f"{sym} binance qty=0, DB 不同步")
            continue
        mark = float(bp.get('markPrice') or 0)
        
        # === INVARIANT WATCHDOG P13 2026-05-03 ===
        # 主程序 invariant 漏网时兜底. 仅校验首次开仓 (closed_qty=0). TP1+ 跳过.
        _closed_qty = float(d.get('closed_qty') or 0)
        _entry_p = float(d.get('entry_price') or 0)
        if sl > 0 and _entry_p > 0 and _closed_qty < 1e-10 and side_long:
            _inv_pct = (sl / _entry_p - 1) * 100
            if not (-5.5 <= _inv_pct <= -0.7):
                alert('CRITICAL', f"{sym} INVARIANT FAIL stop_dist={_inv_pct:.2f}% entry={_entry_p} sl={sl}")
                emergency_close(sym, actual_qty, side_long, f"invariant_fail dist={_inv_pct:.2f}%")
                continue

        # 兜底硬止损 (独立判断, 不信 watcher)
        breach = (sl > 0 and side_long and mark <= sl) or (sl > 0 and not side_long and mark >= sl)
        if breach:
            alert("CRITICAL", f"{sym} mark=${mark} {'≤' if side_long else '≥'} sl=${sl} watcher 未触发硬止损")
            emergency_close(sym, actual_qty, side_long, f"sl_breach mark={mark} sl={sl}")
            continue
        
        # 卡死检测
        try:
            lu = d['last_update']
            lu_dt = datetime.strptime(lu, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age = (now - lu_dt).total_seconds()
            if age > 180:
                dist = (mark - sl) / sl * 100 if sl else 0
                alert("WARNING", f"{sym} watcher stale ({age:.0f}s ago) mark=${mark} sl=${sl} dist={dist:+.2f}%")
            summary.append(f"{sym}:age{age:.0f}s,dist{(mark-sl)/sl*100:+.2f}%" if sl else sym)
        except Exception as e:
            alert("WARNING", f"{sym} parse last_update fail: {e}")
    
    # === SNAPSHOT FRESHNESS P13 2026-05-03 ===
    try:
        with storage.get_conn() as _c:
            _stale = _c.execute("SELECT token, updated_at FROM market_snapshots WHERE updated_at < datetime('now', '-300 seconds') ORDER BY updated_at ASC LIMIT 5").fetchall()
            if _stale:
                _detail = ', '.join('{}({})'.format(r[0], r[1]) for r in _stale)
                alert('WARNING', 'market_snapshots 陈旧 {} 个: {}'.format(len(_stale), _detail))
    except Exception as _e:
        alert('WARNING', 'snapshot freshness check fail: ' + str(_e))

    alert("INFO", f"watchdog OK, {len(rows)} pos: " + ", ".join(summary))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        alert("ERROR", f"watchdog crash: {type(e).__name__}: {e}\n{traceback.format_exc()[:500]}")
