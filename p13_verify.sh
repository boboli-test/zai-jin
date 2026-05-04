#!/bin/bash
DATE=$(date +%F)
LOG="/root/zai-jin/logs/p13_verify/${DATE}.log"
ALERT_LOG="/root/zai-jin/logs/alerts/${DATE}.log"
TRADER_LOG="/root/zai-jin/logs/auto_trader.systemd.log"
DB="/root/zai-jin/binance_square.db"
mkdir -p "$(dirname "$LOG")"

{
echo "===== P13 verify $(date) ====="
echo
echo "--- A. invariant 越界 (今日, 应=0) ---"
INV_COUNT=$(grep "INVARIANT" "$ALERT_LOG" 2>/dev/null | wc -l)
echo "count: $INV_COUNT"
[ "$INV_COUNT" -gt 0 ] && grep "INVARIANT" "$ALERT_LOG" 2>/dev/null | head -10

echo
echo "--- B. close cooldown 拒单 (今日, 信息量) ---"
CD_COUNT=$(grep -E "close_cooldown|平仓冷却" "$TRADER_LOG" 2>/dev/null | grep "$DATE" | wc -l)
echo "count: $CD_COUNT"
grep -E "close_cooldown|平仓冷却" "$TRADER_LOG" 2>/dev/null | grep "$DATE" | tail -5

echo
echo "--- C. stale market_snapshots (分桶,关注 5-30min 异常) ---"
sqlite3 -header -column "$DB" "SELECT '5-30min' AS bucket, COUNT(*) AS n FROM market_snapshots WHERE updated_at < datetime('now','-5 minutes') AND updated_at >= datetime('now','-30 minutes') UNION ALL SELECT '30min-2h', COUNT(*) FROM market_snapshots WHERE updated_at < datetime('now','-30 minutes') AND updated_at >= datetime('now','-2 hours') UNION ALL SELECT '2h-1d', COUNT(*) FROM market_snapshots WHERE updated_at < datetime('now','-2 hours') AND updated_at >= datetime('now','-1 days') UNION ALL SELECT '>1d', COUNT(*) FROM market_snapshots WHERE updated_at < datetime('now','-1 days')"

echo
echo "--- D. 今日新开仓 invariant 抽查 (全应 OK) ---"
sqlite3 -header -column "$DB" "SELECT token, mode, ROUND((stop_loss_price/entry_price-1)*100, 2) AS stop_pct, CASE WHEN (stop_loss_price/entry_price-1)*100 BETWEEN -5.5 AND -0.7 THEN 'OK' ELSE 'FAIL' END AS check_result, created_at AS opened FROM trade_positions WHERE mode='live' AND DATE(created_at) = DATE('now') ORDER BY id"

echo
echo "--- E. 当前 OPEN 仓 invariant 现状 ---"
sqlite3 -header -column "$DB" "SELECT token, ROUND((stop_loss_price/entry_price-1)*100, 2) AS stop_pct, CASE WHEN COALESCE(closed_qty,0) > 1e-10 THEN 'TP1+' WHEN (stop_loss_price/entry_price-1)*100 BETWEEN -5.5 AND -0.7 THEN 'OK' ELSE 'FAIL' END AS check_result FROM trade_positions WHERE status IN ('OPEN','PARTIAL') AND mode='live' ORDER BY id"

echo
echo "--- F. 服务状态 ---"
for s in zaijin-auto_trader zaijin-worker zaijin-market_realtime zaijin-web; do
    printf "%-30s %s\n" "$s" "$(systemctl is-active $s)"
done

echo
echo "===== 完成 $(date) ====="
} > "$LOG" 2>&1

INV=$(grep "INVARIANT" "$ALERT_LOG" 2>/dev/null | wc -l)
FAIL=$(grep "FAIL" "$LOG" 2>/dev/null | wc -l)
if [ "$INV" -gt 0 ] || [ "$FAIL" -gt 0 ]; then
    echo "[P13-VERIFY $(date '+%F %T')] ALERT: invariant=$INV fail=$FAIL — see $LOG" | wall 2>/dev/null
fi
