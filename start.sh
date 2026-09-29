#!/usr/bin/env bash
# start.sh — run ORBITAL: the live bot and the dashboard, together.
#
#   ./start.sh                 bot + dashboard (http://127.0.0.1:5050)
#   ./start.sh --dry-run       same, but the bot never sends Telegram messages
#   ./start.sh --no-web        bot only
#   ORBITAL_PORT=8080 ./start.sh
#
# Ctrl+C stops both. Output goes to the terminal and to logs/bot.log and
# logs/webapp.log. Any option other than --no-web is passed to main.py.
#
# If the bot exits on its own (market holiday, expired Dhan token), the
# dashboard keeps running so you can still look at the day; Ctrl+C to finish.

set -u
cd "$(dirname "$0")"

PY=".venv/bin/python"
PORT="${ORBITAL_PORT:-5050}"
WEB=1
BOT_ARGS=()

for arg in "$@"; do
    case "$arg" in
        --no-web) WEB=0 ;;
        -h|--help) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) BOT_ARGS+=("$arg") ;;
    esac
done

# ---- checks, so a missing piece fails here with a clear message
fail() { echo "❌ $1"; exit 1; }
[ -x "$PY" ] || fail "No virtualenv at .venv — run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
[ -f config.ini ] || [ -n "${DHAN_ACCESS_TOKEN:-}" ] || fail "No config.ini and no DHAN_ACCESS_TOKEN — see README 'Quick start'."
[ -f models/orbital_model.pkl ] || fail "No trained model — run: $PY walk_forward.py --train-live"
if [ "$WEB" = 1 ] && lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "Port $PORT is already in use (another dashboard?). Stop it, or use ORBITAL_PORT=<port> ./start.sh"
fi

mkdir -p logs
BOT_PID=""
WEB_PID=""

stop() {
    echo
    echo "⏹  Stopping ORBITAL..."
    [ -n "$BOT_PID" ] && kill "$BOT_PID" 2>/dev/null
    [ -n "$WEB_PID" ] && kill "$WEB_PID" 2>/dev/null
    wait 2>/dev/null
    echo "✅ Stopped."
    exit 0
}
trap stop INT TERM

# ---- start
if [ "$WEB" = 1 ]; then
    ORBITAL_PORT="$PORT" "$PY" -u webapp.py > >(tee -a logs/webapp.log | sed 's/^/[web] /') 2>&1 &
    WEB_PID=$!
fi

"$PY" -u main.py ${BOT_ARGS[@]+"${BOT_ARGS[@]}"} > >(tee -a logs/bot.log | sed 's/^/[bot] /') 2>&1 &
BOT_PID=$!

echo "🚀 ORBITAL started — bot pid $BOT_PID${WEB_PID:+, dashboard pid $WEB_PID}"
[ "$WEB" = 1 ] && echo "📊 Dashboard: http://127.0.0.1:$PORT"
echo "   Ctrl+C to stop."

# ---- supervise by PID (bash 3.2 has no `wait -n`)
while kill -0 "$BOT_PID" 2>/dev/null; do
    if [ -n "$WEB_PID" ] && ! kill -0 "$WEB_PID" 2>/dev/null; then
        echo "⚠️  Dashboard exited — see logs/webapp.log. The bot keeps running."
        WEB_PID=""
    fi
    sleep 2
done

wait "$BOT_PID"
CODE=$?
case "$CODE" in
    0) echo "ℹ️  Bot finished (outside the trading window, a holiday, or --once)." ;;
    2) echo "⛔ Bot stopped: Dhan token rejected. Put a new token in config.ini and run ./start.sh again." ;;
    *) echo "❌ Bot exited with code $CODE — see logs/bot.log." ;;
esac

if [ -n "$WEB_PID" ] && kill -0 "$WEB_PID" 2>/dev/null; then
    echo "📊 Dashboard still running at http://127.0.0.1:$PORT — Ctrl+C to stop."
    while kill -0 "$WEB_PID" 2>/dev/null; do sleep 2; done
fi
exit "$CODE"
