#!/usr/bin/env python3
"""hedge_guard.py — Layer B hedge mode monitor (read-only, 5min cron)."""
import sys, os
from datetime import datetime, timezone
sys.path.insert(0, "/root/zai-jin")
import binance_real

ALERT_DIR = "/root/zai-jin/logs/alerts"
LOG_PATH = "/root/zai-jin/logs/hedge_guard.log"

def _alert(level, msg):
    os.makedirs(ALERT_DIR, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    line = f"[{datetime.now(timezone.utc).isoformat()}] [{level}] hedge_guard: {msg}\n"
    for p in (f"{ALERT_DIR}/{today}.log", LOG_PATH):
        with open(p, "a") as f:
            f.write(line)

def main():
    try:
        r = binance_real._request("GET", "/fapi/v1/positionSide/dual", signed=True)
    except Exception as e:
        _alert("ERROR", f"GET positionSide/dual failed: {e}")
        sys.exit(2)
    if str(r.get("dualSidePosition")).lower() == "true":
        msg = "HEDGE MODE DETECTED -- Layer A tripwire will switch on next open, but active OPEN positions will block POST switch. SSH required."
        _alert("CRITICAL", msg)
        os.system(f'echo "{msg}" | wall')
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
