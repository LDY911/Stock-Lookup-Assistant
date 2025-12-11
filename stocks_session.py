# -*- coding: utf-8 -*-
"""
会话模式说明：
- 建议配合 Windows 任务计划程序：每天 09:30 ET 启动本脚本，加参数 --session
- 脚本会在当日 09:30–16:00 ET 内按 INTERVAL_MINUTES 轮询；到收盘自动退出
- 非交易时段不常驻进程，极省资源

历史说明（保留你的设定）：
- 取价来自 Finnhub（只保留这一家）
- 若拿到的价格时间戳超过 STRICT_REALTIME_SECONDS，整次计算放弃并打印原因
"""

import sys, os, time, signal, requests
from datetime import datetime, timedelta, time as dtime, timezone
from dateutil import tz
from urllib.parse import quote

# ========== 行为开关 ==========
ONLY_PUSH_ON_SELL = False                # False -> 任一条件（买/卖）满足就推送；True -> 仅卖出时推送
INTERVAL_MINUTES = 3                     # 轮询间隔（建议 15 更省资源；1 更灵敏）
STRICT_REALTIME_SECONDS = 600            # 行情时间戳必须在当前时间以内的秒数上限（如需更严格改为 60）

# 卖出触发的盈利金额（USD）与买入触发的亏损金额（USD）阈值保持不变
SELL_PROFIT_THRESHOLD = 1.0              # 卖出触发阈值：盈利金额（USD）
BUY_PROFIT_THRESHOLD  = -1.0             # 买入触发阈值：亏损金额（USD）


# === 差异化收益率阈值（按股票） ===
# 卖出（正向）：
SELL_YIELD_THRESHOLD_BY_TICKER = {
    "GOOGL": 0.06, "AAPL": 0.04, "BRK.B": 0.04,
    "NVDA": 0.06, "META": 0.06, "AMZN": 0.06, "AMD": 0.06,
    "TSLA": 0.08, "VST": 0.04, "TRMB": 0.05,
    "SNDX": 0.15, "BBAI": 0.20, "SENS": 0.20, "GCI": 0.20, "OPEN": 0.20, "ALDX": 0.20, "KURA": 0.20, "MVIS": 0.20, "VUZI": 0.20, "ARWR": 0.10,
}

# 买入（负向）：
BUY_YIELD_THRESHOLD_BY_TICKER = {
    "GOOGL": -0.06, "AAPL": -0.06, "BRK.B": -0.06,
    "NVDA": -0.09, "META": -0.09, "AMZN": -0.09, "AMD": -0.09,
    "TSLA": -0.09, "VST": -0.10, "TRMB": -0.06,
    "SNDX": -0.30, "BBAI": -0.50, "SENS": -0.50, "GCI": -0.50, "OPEN": -0.50, "ALDX": -0.50, "KURA": -0.50, "MVIS": -0.50, "VUZI": -0.50, "ARWR": -0.20,
}
# ====================================



LOG_TO_CSV = True                        # 如需落盘记录可改为 True
CSV_PATH = "holdings_monitor_log.csv"

# ========== Bark 通知配置 ==========
BARK_BASE_URL = "https://api.day.app/XXXXXXXXXXXXX/"  # 你的 Bark URL（含 device key）
BARK_CODE_BLOCK = True                   # 是否用代码块包裹正文，提升等宽渲染概率
# ====================================

# ========== 必填：你的 Finnhub API Key ==========
FINNHUB_API_KEY = "xxxxxxxxxxxxxxx"
# ==============================================

# 交易时段（仅常规：09:30–16:00 ET；会话模式下收盘自动退出）
MARKET_TZ = tz.gettz("America/New_York")  # 美东时区
MARKET_OPEN  = dtime(9, 30)               # 09:30
MARKET_CLOSE = dtime(16, 00)               # 16:00
REQUEST_TIMEOUT = 10

# 你的持仓（按你给定的“剩余股数”逐批跟踪）
HOLDINGS = {
    "NVDA": [
        {"buy_price": 185.27, "shares": 0.0538797587},
        {"buy_price": 184.77, "shares": 0.4847618057},
        {"buy_price": 186.34, "shares": 1.0731745371},
        {"buy_price": 193.51, "shares": 1.9952153110},
        {"buy_price": 189.46, "shares": 0.1583628042},
        {"buy_price": 187.15, "shares": 0.534337},
        {"buy_price": 181.90, "shares": 1.649257},
        {"buy_price": 181.94, "shares": 0.082447},
        {"buy_price": 180.19, "shares": 0.554969},
        {"buy_price": 179.56, "shares": 0.556916},
        {"buy_price": 177.80, "shares": 0.562434},
    ],
    "GOOGL": [
        {"buy_price": 246.59, "shares": 1.1792838410},
        {"buy_price": 240.91, "shares": 0.395096-0.02},
    ],
    "META": [
        {"buy_price": 700.47, "shares": 0.4078330141},
    ],
    "AMZN": [
        {"buy_price": 218.82, "shares": 0.4570506789},
    ],
    "AMD": [
        {"buy_price": 232.48, "shares": 2.1500316141},
    ],
    "AAPL": [
        {"buy_price": 255.03, "shares": 0.3918047149},
    ],
    "BRK.B": [
        {"buy_price": 488.67, "shares": 0.613916 - 0.20215},
    ],
    "TSLA": [
        {"buy_price": 431.06, "shares": 0.463977},
    ],
    "VST": [
        {"buy_price": 200.85, "shares": 1 + 0.268817},
    ],
    "TRMB": [
        {"buy_price": 78.45, "shares": 2},
    ],
    "SNDX": [
        {"buy_price": 15.70, "shares": 5},
    ],
    "BBAI": [
        {"buy_price": 7.31, "shares": 10},
    ],
    "SENS": [
        {"buy_price": 8.49, "shares": 6},
        {"buy_price": 6.74, "shares": 2.593471},
    ],
    "GCI": [
        {"buy_price": 3.55, "shares": 25},
    ],
    "OPEN": [
        {"buy_price": 7.14, "shares": 10},
    ],
    "ALDX": [
        {"buy_price": 5.42, "shares": 10},
    ],
    "KURA": [
        {"buy_price": 9.76, "shares": 5},
    ],
    "MVIS": [
        {"buy_price": 1.18, "shares": 50},
    ],
    "VUZI": [
        {"buy_price": 4.03, "shares": 10},
    ],
    "ARWR": [
        {"buy_price": 38.40, "shares": 3},
    ],
}

# 符号映射（黑白符号）
STATUS_SYMBOL = {"hold": "□", "buy": "▲", "sell": "★"}

# --------- 工具函数 ---------
def now_et():
    return datetime.now(MARKET_TZ)

def is_market_open(dt_et: datetime) -> bool:
    # 周一(0)~周五(4)，且时间在 [09:30, 16:00)
    return dt_et.weekday() < 5 and (MARKET_OPEN <= dt_et.time() < MARKET_CLOSE)

def ceil_to_next_interval(dt_et: datetime, minutes: int) -> datetime:
    discard = timedelta(minutes=dt_et.minute % minutes, seconds=dt_et.second, microseconds=dt_et.microsecond)
    dt2 = dt_et + (timedelta(minutes=minutes) - discard)
    return dt2.replace(second=0, microsecond=0)

def fmt_money(x: float) -> str:
    return f"${x:,.2f}"

def fmt_signed_pct(x: float) -> str:
    return f"{'+' if x>=0 else ''}{x*100:.2f}%"

def fmt_signed_money(x: float) -> str:
    return f"{'+' if x>=0 else '-'}${abs(x):,.2f}"

def hard_exit(msg: str):
    print(f"FAIL: {msg}")
    sys.exit(1)

# --------- Bark 推送 ----------
def _extract_bark_key(base_url: str) -> str:
    s = base_url.strip().rstrip("/")
    return s.split("/")[-1] if s else ""

def notify_bark(title: str, body: str, sound: str = None, is_archive: int = 1) -> bool:
    """优先 POST /push，更稳；失败回退 GET（自动 URL 编码）。返回是否成功。"""
    try:
        key = _extract_bark_key(BARK_BASE_URL)
        if key:
            payload = {"title": title, "body": body, "device_key": key}
            if sound: payload["sound"] = sound
            if is_archive is not None: payload["isArchive"] = str(is_archive)
            r = requests.post("https://api.day.app/push", json=payload, timeout=5)
            r.raise_for_status()
            return True
    except Exception:
        pass
    try:
        url = f"{BARK_BASE_URL}{quote(title)}/{quote(body)}"
        params = {}
        if sound: params["sound"] = sound
        if is_archive is not None: params["isArchive"] = str(is_archive)
        r = requests.get(url, params=params, timeout=5)
        r.raise_for_status()
        return True
    except Exception:
        return False

# --------- Finnhub 适配 ----------
class FinnhubProvider:
    name = "finnhub"
    def __init__(self, token: str):
        self.token = token
        # 个别符号的候选写法（按顺序尝试）
        self.symbol_candidates = {
            "BRK.B": ["BRK.B", "BRK-B"],
        }

    def _candidates_for(self, ticker: str):
        return self.symbol_candidates.get(ticker, [ticker])

    def fetch(self, tickers):
        out, errors = {}, {}
        for t in tickers:
            ok = False
            for sym in self._candidates_for(t):
                try:
                    r = requests.get(
                        "https://finnhub.io/api/v1/quote",
                        params={"symbol": sym, "token": self.token},
                        timeout=REQUEST_TIMEOUT
                    )
                    r.raise_for_status()
                    data = r.json()
                    # data: c(现价), t(UNIX秒), pc(昨收) 等
                    if "c" in data and "t" in data and data["c"] not in (None, 0) and data["t"]:
                        price = float(data["c"])
                        ts = datetime.fromtimestamp(int(data["t"]), tz=timezone.utc).astimezone(MARKET_TZ)
                        out[t] = {"price": price, "ts": ts, "symbol_used": sym}
                        ok = True
                        break
                except Exception as e:
                    errors[sym] = str(e)
            if not ok:
                raise RuntimeError(f"Finnhub 获取 {t} 失败（尝试 {self._candidates_for(t)}）。错误：{errors}")
        # 严格实时性校验
        now = now_et()
        stale = [(t, v["symbol_used"], v["ts"].strftime('%H:%M:%S')) for t, v in out.items()
                 if (now - v["ts"]).total_seconds() > STRICT_REALTIME_SECONDS]
        if stale:
            raise RuntimeError(
                "以下标的的最新成交时间超过 "
                f"{STRICT_REALTIME_SECONDS} 秒，已放弃本次计算：" +
                ", ".join([f"{t}(用 {sym}, ts {ts})" for t, sym, ts in stale])
            )
        return out  # {ticker: {"price": x, "ts": dt_et, "symbol_used": sym}}

# --------- 业务逻辑 ----------
def compute_batches(prices_map):
    """
    返回：
      rows: 所有批次的明细（含 status 与 symbol）
      sell_triggers: 触发“卖出”的批次
      buy_triggers:  触发“买入”的批次
      counts: {"hold":X1,"sell":X2,"buy":X3}
    """
    rows, sell_triggers, buy_triggers = [], [], []
    for ticker, batches in HOLDINGS.items():
        price = prices_map[ticker]["price"]

        # 按股票取差异化收益率阈值
        sell_yield_thr = SELL_YIELD_THRESHOLD_BY_TICKER[ticker]
        buy_yield_thr  = BUY_YIELD_THRESHOLD_BY_TICKER[ticker]

        for idx, b in enumerate(batches, start=1):
            buy = float(b["buy_price"])
            sh  = float(b["shares"])
            yld = price / buy - 1.0
            profit = (price - buy) * sh

            # 卖出触发（收益率>个股阈值 且 盈利>$阈值）
            sell_hit = (yld > sell_yield_thr) and (profit > SELL_PROFIT_THRESHOLD)
            # 买入触发（收益率<个股阈值 且 浮亏<$阈值(负数)）
            buy_hit  = (yld < buy_yield_thr) and (profit < BUY_PROFIT_THRESHOLD)

            if sell_hit:
                status = "sell"
            elif buy_hit:
                status = "buy"
            else:
                status = "hold"

            row = {
                "ticker": ticker,
                "batch": idx,
                "yield_pct": yld,
                "profit_usd": profit,
                "buy_value": buy * sh,
                "cur_value": price * sh,
                "buy_price": buy,
                "cur_price": price,
                "shares": sh,
                "status": status,
                "symbol": STATUS_SYMBOL[status],
            }
            rows.append(row)

            if status == "sell":
                sell_triggers.append(row)
            elif status == "buy":
                buy_triggers.append(row)

    total_batches = len(rows)
    x2 = len(sell_triggers)
    x3 = len(buy_triggers)
    x1 = total_batches - x2 - x3
    counts = {"hold": x1, "sell": x2, "buy": x3}
    return rows, sell_triggers, buy_triggers, counts

# === Bark 等宽表格正文 ===
def _format_rows_as_table(rows):
    """将每票每批渲染为等宽'表格'；用🟢/🔴区分涨跌；新增“操作”列显示状态符号"""
    headers = [("Ticker",6), ("批",2), ("收益率",8), ("盈亏",13), ("操作",2)]
    def rpad(s, w): return str(s).rjust(w)
    def lpad(s, w): return str(s).ljust(w)

    # 表头
    line = " ".join([lpad(h, w) for h, w in headers])
    sep  = "-" * len(line)
    out  = [line, sep]

    order = {"NVDA":0,"GOOGL":1,"META":2,"AMZN":3,"AMD":4,"AAPL":5,"BRK.B":7,"TSLA":8,"VST":9,"TRMB":10, "SNDX":11, "BBAI":12, "SENS":13, "GCI":14, "OPEN":15, "ALDX":16, "KURA":17, "MVIS":18, "VUZI":19, "ARWR":20}
    rows_sorted = sorted(rows, key=lambda r: (order.get(r["ticker"], 999), r["batch"]))

    for r in rows_sorted:
        mark = "🟢" if r["profit_usd"] >= 0 else "🔴"
        out.append(" ".join([
            lpad(r["ticker"], 6),
            rpad(r["batch"], 2),
            rpad(fmt_signed_pct(r["yield_pct"]), 8),
            rpad(mark + fmt_signed_money(r["profit_usd"]), 13),
            rpad(r["symbol"], 2),
        ]))
    table = "\n".join(out)
    return f"\n{table}\n" if BARK_CODE_BLOCK else table

def _compose_bark(now_ts_et, rows, counts):
    """
    标题（第一行）：持有X1份□，卖出X2份★，买入X3份▲
    正文（第二行开始）：总持仓 $... ；随后为时间戳与原表格
    """
    total_cur_value = sum(r["cur_value"] for r in rows)
    title = f"持有{counts['hold']}份{STATUS_SYMBOL['hold']}，卖出{counts['sell']}份{STATUS_SYMBOL['sell']}，买入{counts['buy']}份{STATUS_SYMBOL['buy']}"
    ts = now_ts_et.strftime("%Y-%m-%d %H:%M:%S %Z")
    total_line = f"总持仓 {fmt_money(total_cur_value)}"
    body = total_line + "\n" + ts + "\n" + _format_rows_as_table(rows)
    return title, body
# === Bark 等宽表格正文 ===

def maybe_log_csv(ts_et, rows):
    if not LOG_TO_CSV: return
    header = ["美东时间","股票","批次","买入价格","持股数量","当前价格","收益率","盈利金额","买入市值","当前市值","状态"]
    need_header = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", encoding="utf-8") as f:
        if need_header: f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join([
                ts_et.strftime("%Y-%m-%d %H:%M:%S"),
                r["ticker"], str(r["batch"]),
                f"{r['buy_price']:.6f}", f"{r['shares']:.6f}", f"{r['cur_price']:.6f}",
                f"{r['yield_pct'] * 100:.2f}%", f"{r['profit_usd']:.6f}",
                f"{r['buy_value']:.6f}", f"{r['cur_value']:.6f}",
                r["status"],
            ]) + "\n")

def run_once(provider: 'FinnhubProvider'):
    """单次运行；控制台仅打印 OK 或 FAIL: <原因>"""
    try:
        now_ts = now_et()
        if not is_market_open(now_ts):
            print("OK"); return

        tickers = ["NVDA","GOOGL","META","AMZN","AMD","AAPL","BRK.B","TSLA","VST","TRMB", "SNDX", "BBAI", "SENS", "GCI", "OPEN", "ALDX", "KURA", "MVIS", "VUZI", "ARWR"]
         # 获取最新价格
        prices = provider.fetch(tickers)  # 若超时/过期会抛异常
        rows, sell_triggers, buy_triggers, counts = compute_batches(prices)

        maybe_log_csv(now_ts, rows)

        # 推送策略
        has_sell = len(sell_triggers) > 0
        has_buy  = len(buy_triggers)  > 0
        if ONLY_PUSH_ON_SELL:
            need_push = has_sell
        else:
            # 任一条件满足就发消息
            need_push = has_sell or has_buy

        if need_push:
            title, body = _compose_bark(now_ts, rows, counts)
            ok = notify_bark(title, body, sound=None, is_archive=1)
            if not ok:
                print("FAIL: Bark 推送失败"); return

        print("OK", now_ts.strftime("%Y-%m-%d %H:%M:%S ET"),
              f"(持有{counts['hold']}份, 卖出{counts['sell']}份, 买入{counts['buy']}份, 持仓{len(rows)}票)")
    except Exception as e:
        print(f"FAIL: {e}")

# ========== 新增：会话模式（当日 09:30–16:00 ET 内循环，收盘自动退出） ==========
SESSION_END_GRACE_MIN = 0  # 可设 0~1，表示收盘后宽限几分钟再退出

def session_loop_run(interval_minutes: int = INTERVAL_MINUTES):
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "PUT_YOUR_FINNHUB_API_KEY_HERE":
        hard_exit("请先在脚本顶部填写你的 FINNHUB_API_KEY。")

    provider = FinnhubProvider(FINNHUB_API_KEY)
    tickers = ["NVDA","GOOGL","META","AMZN","AMD","AAPL","BRK.B","TSLA","VST","TRMB", "SNDX", "BBAI", "SENS", "GCI", "OPEN", "ALDX", "KURA", "MVIS", "VUZI", "ARWR"]

    stop = {"flag": False}
    def _sig(_a,_b): stop["flag"] = True
    for sig in (signal.SIGINT, signal.SIGTERM): signal.signal(sig, _sig)

    now = now_et()
    # 若被提前拉起（<09:30），先等到开盘
    if now.weekday() < 5 and now.time() < MARKET_OPEN:
        wake = datetime.combine(now.date(), MARKET_OPEN, tzinfo=MARKET_TZ)
        while not stop["flag"] and (now_et() < wake):
            time.sleep(10)

    # 首轮探测：若是假日/停市或数据过旧，直接结束会话
    try:
        _ = provider.fetch(tickers)
    except Exception as e:
        print(f"首轮探测失败，可能是假日/停市，退出会话：{e}")
        return

    # 先跑一轮
    run_once(provider)

    # 主循环直到收盘
    while not stop["flag"]:
        now = now_et()
        end_barrier = (datetime.combine(now.date(), MARKET_CLOSE, tzinfo=MARKET_TZ)
                       + timedelta(minutes=SESSION_END_GRACE_MIN))
        if now >= end_barrier or now.weekday() >= 5:
            print(f"到达收盘，退出：{now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            break

        next_tick = ceil_to_next_interval(now, interval_minutes)
        time.sleep(max(0, (next_tick - now).total_seconds()))
        if stop["flag"]: break
        run_once(provider)
# ======================================================================

def loop_run():
    # 仍保留老的“长期守候”模式（不推荐），以兼容旧用法
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "PUT_YOUR_FINNHUB_API_KEY_HERE":
        hard_exit("请先在脚本顶部填写你的 FINNHUB_API_KEY。")

    provider = FinnhubProvider(FINNHUB_API_KEY)
    stop = {"flag": False}
    def _sig(_a,_b): stop["flag"] = True
    for sig in (signal.SIGINT, signal.SIGTERM): signal.signal(sig, _sig)

    while not stop["flag"]:
        now = now_et()
        next_tick = ceil_to_next_interval(now, INTERVAL_MINUTES)
        time.sleep(max(0, (next_tick - now).total_seconds()))
        if stop["flag"]: break
        run_once(provider)

if __name__ == "__main__":
    if "--once" in sys.argv:
        if not FINNHUB_API_KEY or FINNHUB_API_KEY == "PUT_YOUR_FINNHUB_API_KEY_HERE":
            hard_exit("请先在脚本顶部填写你的 FINNHUB_API_KEY。")
        run_once(FinnhubProvider(FINNHUB_API_KEY))
    elif "--session" in sys.argv:
        session_loop_run()
    else:
        # 若直接双击运行，会进入旧的常驻模式（不推荐）；建议使用 --session
        loop_run()
