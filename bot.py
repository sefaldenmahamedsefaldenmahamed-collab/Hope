"""
═══════════════════════════════════════════════════════════════════════
  🧭 البوت — استراتيجية Triple Confluence Navigator
═══════════════════════════════════════════════════════════════════════
  تنفيذ كامل لاستراتيجية Pine Script:
    "Triple Confluence Navigator [MarkitTick]" © MarkitTick (CC BY-NC-SA 4.0)

  الاستراتيجية تجمع 3 منظومات (Confluence) خلال نافذة 5 شموع:
    1) Cardwell RSI   — تقاطع MA السريع (Kalman/RMA/LLAMA) فوق البطيء على RSI
    2) Adaptive ARSI  — تقاطع ARSI فوق/تحت Supertrend (دمج ARSI + Supertrend)
    3) Structure BOS  — كسر آخر قمة/قاع سوينغ (Break of Structure)

  عند اجتماع الإشارات الثلاث خلال نافذة confluence_window:
    ↗ long  : دخول شراء  | SL = entry - riskDist | TP1=1R, TP2=2R, TP3=3R
    ↘ short : دخول بيع   | SL = entry + riskDist | TP1=1R, TP2=2R, TP3=3R

  حساب riskDist:
    - LONG : min(structDist, atrDist)
        structDist = entry - (lastSwingLow - atr * slStructBuf)
        atrDist    = atr * atrMultSl
    - SHORT: min(structDist, atrDist)
        structDist = (lastSwingHigh + atr * slStructBuf) - entry
        atrDist    = atr * atrMultSl

  الإعدادات الافتراضية (مطابقة لـ Pine Script):
    rsiLen=14, rmaFastLen=9, rmaSlowLen=45, maType=Kalman
    arsiLen=14, arsiSigLen=14, arsiFiltType=Kalman Filter
    kalmanProcN=0.01, kalmanMeasN=0.10
    llamaAlphaMin=0.05, llamaAlphaMax=0.50
    stAtrLen=14, stFactor=2.0
    swingLen=5
    atrLen=14, atrMultSl=1.5, slStructBuf=0.25
    rr1=1.0, rr2=2.0, rr3=3.0
    confluenceWindow=5
    useHtfFilter=false, htfTimeframe=240
    useChopFilter=false, adxLen=14, adxMinLevel=20.0

  إعدادات التداول (مطلوبة من المستخدم):
    الهامش TRADE_SIZE_USDT = 50
    الرافعة LEVERAGE = 10
    المتزامنة MAX_OPEN_TRADES = 1

  التشغيل: python bot.py  (المتغيرات عبر البيئة:
    BINANCE_API_KEY, BINANCE_API_SECRET, TRADE_ENABLED, TESTNET,
    TRADE_SIZE_USDT, MAX_OPEN_TRADES, LEVERAGE,
    DB_FILE / DATA_DIR, PORT)
═══════════════════════════════════════════════════════════════════════
"""

import asyncio, aiohttp, json, math, os, sys, time, logging
import collections
import pandas as pd
import numpy as np
import hmac
import hashlib
import signal as sys_signal
from urllib.parse import urlencode
from logging.handlers import RotatingFileHandler
import websockets
import aiosqlite
from web_panel import start_web_panel, notify as panel_notify

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

logger = logging.getLogger('TripleBot')
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = RotatingFileHandler(os.path.join('/tmp', 'bot.log'),
                         maxBytes=2 * 1024 * 1024, backupCount=2, encoding='utf-8')
fh.setFormatter(fmt); logger.addHandler(fh)
ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)

try:
    import requests
except Exception:
    requests = None


# ═══════════════════════ مسار قاعدة البيانات ═══════════════════════
RAILWAY_VOLUME_PATH = "/var/lib/containers/railwayapp/bind-mounts/54c0d898-c1f2-44de-a173-2a711d30a4bc/vol_w3cxm5jyou5b58yw"
BASE_DIR = "/tmp"


def resolve_db_path():
    """يختار أول مسار قابل للاستخدام لقاعدة البيانات بترتيب آمن."""
    candidates = []
    env_file = (os.environ.get('DB_FILE') or '').strip()
    if env_file:
        candidates.append(('DB_FILE', env_file))
    data_dir = (os.environ.get('DATA_DIR') or '').strip()
    if data_dir:
        candidates.append(('DATA_DIR', os.path.join(data_dir, "triple_v1.db")))
    if RAILWAY_VOLUME_PATH:
        candidates.append(('RAILWAY_VOLUME', os.path.join(RAILWAY_VOLUME_PATH, "triple_v1.db")))
    candidates.append(('DEFAULT', os.path.join(BASE_DIR, "triple_v1.db")))
    for src, path in candidates:
        folder = os.path.dirname(os.path.abspath(path)) or '.'
        try:
            os.makedirs(folder, exist_ok=True)
            probe = os.path.join(folder, '.db_probe')
            with open(probe, 'w', encoding='utf-8') as f:
                f.write('ok')
            os.remove(probe)
            if src != 'DEFAULT':
                logger.info(f"💾 قاعدة البيانات: {path} (من المتغير {src})")
            return path
        except Exception as e:
            logger.warning(f"⚠️ مسار قاعدة البيانات غير قابل للاستخدام: {path} — السبب: {e}")
    return os.path.join(BASE_DIR, "triple_v1.db")


db_file = resolve_db_path()


# ═══════════════════════ قاعدة البيانات المشتركة ═══════════════════════
class DatabaseManager:
    """قاعدة البيانات: الصفقات الحية + أرباح الأيام + سجل الإشارات + الإعدادات."""

    def __init__(self, db_name=None):
        self.db_name = db_name or db_file

    async def init_db(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS active_trades (
                symbol TEXT PRIMARY KEY, side TEXT, entry_price REAL, quantity REAL,
                sl REAL, tp REAL, tp2 REAL, tp3 REAL, trailing_active INTEGER,
                highest_price REAL, lowest_price REAL, entry_time REAL,
                partial_closed INTEGER, partial2_closed INTEGER,
                realized_pnl REAL DEFAULT 0, stop_order_id INTEGER, tp_order_id INTEGER,
                strategy TEXT, margin_usdt REAL DEFAULT 0)""")
            for col_def in ('realized_pnl REAL DEFAULT 0', 'stop_order_id INTEGER',
                            'tp2 REAL', 'tp3 REAL', 'tp_order_id INTEGER', 'strategy TEXT',
                            'margin_usdt REAL DEFAULT 0', 'partial2_closed INTEGER DEFAULT 0'):
                try:
                    await db.execute(f"ALTER TABLE active_trades ADD COLUMN {col_def}")
                except Exception:
                    pass
            await db.execute("""CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY, realized_pnl REAL, wins INTEGER, losses INTEGER)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS bot_params (
                key TEXT PRIMARY KEY, value TEXT)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, direction TEXT,
                entry_ts REAL, score REAL, components TEXT,
                result INTEGER DEFAULT -1, pnl REAL)""")
            for col_def in ('close_ts REAL', 'close_reason TEXT',
                            'duration_s REAL', 'pnl_pct REAL'):
                try:
                    await db.execute(f"ALTER TABLE signal_log ADD COLUMN {col_def}")
                except Exception:
                    pass
            await db.commit()

    async def get_param(self, key):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                async with db.execute("SELECT value FROM bot_params WHERE key=?", (key,)) as cur:
                    row = await cur.fetchone()
                    return row[0] if row else None
        except Exception:
            return None

    async def set_param(self, key, value):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("INSERT OR REPLACE INTO bot_params (key, value) VALUES (?,?)",
                             (key, value))
            await db.commit()

    async def log_signal(self, symbol, direction, entry_ts, score, components):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute(
                    "INSERT INTO signal_log (symbol, direction, entry_ts, score, components) VALUES (?,?,?,?,?)",
                    (symbol, direction, entry_ts, score, json.dumps(components, ensure_ascii=False)))
                await db.commit()
        except Exception as e:
            logger.warning(f"تعذر تسجيل الإشارة: {e}")

    async def set_signal_result(self, symbol, entry_ts, win, pnl,
                                close_ts=None, close_reason=None,
                                duration_s=None, pnl_pct=None):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute(
                    """UPDATE signal_log SET result=?, pnl=?, close_ts=?, close_reason=?,
                       duration_s=?, pnl_pct=? WHERE symbol=? AND entry_ts=? AND result=-1""",
                    (int(win), pnl, close_ts, close_reason, duration_s, pnl_pct,
                     symbol, entry_ts))
                await db.commit()
        except Exception as e:
            logger.warning(f"تعذر تحديث نتيجة الإشارة: {e}")

    async def save_trade(self, t):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""INSERT OR REPLACE INTO active_trades
                (symbol, side, entry_price, quantity, sl, tp, tp2, tp3, trailing_active,
                 highest_price, lowest_price, entry_time, partial_closed, partial2_closed,
                 realized_pnl, stop_order_id, tp_order_id, strategy, margin_usdt)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t['symbol'], t['side'], t['entry_price'], t['quantity'],
                 t['sl'], t['tp'], t.get('tp2'), t.get('tp3'),
                 int(t.get('trailing_active', False)),
                 t.get('highest_price', 0), t.get('lowest_price', 999999),
                 t['entry_time'], int(t.get('partial_closed', 0)),
                 int(t.get('partial2_closed', 0)),
                 float(t.get('realized_pnl', 0.0)),
                 t.get('stop_order_id'), t.get('tp_order_id'),
                 t.get('strategy') or 'TRIPLE', float(t.get('margin_usdt') or 0)))
            await db.commit()

    async def load_active_trades(self):
        trades = {}
        try:
            async with aiosqlite.connect(self.db_name) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""SELECT symbol, side, entry_price, quantity, sl, tp, tp2, tp3,
                    trailing_active, highest_price, lowest_price, entry_time,
                    partial_closed, partial2_closed, realized_pnl, stop_order_id, tp_order_id,
                    strategy, margin_usdt FROM active_trades""") as cur:
                    async for row in cur:
                        trades[row['symbol']] = {
                            'symbol': row['symbol'], 'side': row['side'],
                            'entry_price': row['entry_price'], 'quantity': row['quantity'],
                            'sl': row['sl'], 'tp': row['tp'], 'tp2': row['tp2'], 'tp3': row['tp3'],
                            'trailing_active': bool(row['trailing_active']),
                            'highest_price': row['highest_price'],
                            'lowest_price': row['lowest_price'],
                            'entry_time': row['entry_time'],
                            'partial_closed': bool(row['partial_closed']),
                            'partial2_closed': bool(row['partial2_closed'] or 0),
                            'realized_pnl': row['realized_pnl'] if row['realized_pnl'] is not None else 0.0,
                            'stop_order_id': row['stop_order_id'],
                            'tp_order_id': row['tp_order_id'],
                            'strategy': row['strategy'] or 'TRIPLE',
                            'margin_usdt': row['margin_usdt'] or 0,
                            'stop_synced_price': None, 'tp_skipped': False,
                        }
        except Exception as e:
            logger.error(f"❌ فشل تحميل الصفقات من القاعدة: {e}")
        return trades

    async def remove_trade(self, symbol):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("DELETE FROM active_trades WHERE symbol=?", (symbol,))
            await db.commit()

    async def update_daily_pnl(self, pnl, is_win=None):
        today = time.strftime("%Y-%m-%d")
        w = 1 if is_win is True else 0
        l = 1 if is_win is False else 0
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute("SELECT * FROM daily_stats WHERE date=?", (today,)) as cur:
                row = await cur.fetchone()
            if row:
                await db.execute("UPDATE daily_stats SET realized_pnl=?, wins=?, losses=? WHERE date=?",
                                (row[1] + pnl, row[2] + w, row[3] + l, today))
            else:
                await db.execute("INSERT INTO daily_stats VALUES (?,?,?,?)",
                                 (today, pnl, w, l))
            await db.commit()

    async def get_day_stats(self, day):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                async with db.execute(
                        "SELECT realized_pnl, wins, losses FROM daily_stats WHERE date=?", (day,)) as cur:
                    return await cur.fetchone()
        except Exception:
            return None


# ══════════════════════════════════════════════════════════════════════
#  🧭 Triple Confluence Navigator — تنفيذ Pine Script حرفياً
# ══════════════════════════════════════════════════════════════════════
TRIPLE_CONFIG = {
    "name": "TRIPLE",
    "label": "Triple Confluence Navigator",
    "enabled": True,
    "settings_version": 1,
    # ── Cardwell RSI ──
    "rsiLen": 14,
    "rmaFastLen": 9,
    "rmaSlowLen": 45,
    "maType": "Kalman",           # Kalman / RMA / LLAMA
    # ── Adaptive ARSI ──
    "arsiLen": 14,
    "arsiSigLen": 14,
    "arsiFiltType": "Kalman Filter",  # RMA / Kalman Filter / LLAMA
    "kalmanProcN": 0.01,
    "kalmanMeasN": 0.10,
    "llamaAlphaMin": 0.05,
    "llamaAlphaMax": 0.50,
    # ── Supertrend ──
    "stAtrLen": 14,
    "stFactor": 2.0,
    # ── Structure ──
    "swingLen": 5,
    # ── Trade Setup ──
    "atrLen": 14,
    "atrMultSl": 1.5,
    "slStructBuf": 0.25,
    "rr1": 1.0, "rr2": 2.0, "rr3": 3.0,
    "confluenceWindow": 5,
    # ── HTF Filter (معطّل افتراضياً مطابق Pine) ──
    "useHtfFilter": False,
    "htfTimeframe": 240,
    # ── ADX/Choppiness Filter (معطّل افتراضياً مطابق Pine) ──
    "useChopFilter": False,
    "adxLen": 14,
    "adxMinLevel": 20.0,
    # ── التشغيل ──
    "max_open_trades": 1,         # ✅ المتزامنة = 1
    "reentry_cooldown": 3600,
    "scan_limit": 60,
}


# ─── دوال المساعدة الحسابية (مطابقة لـ ta.* في Pine) ───
def ema(a, n):
    a = np.asarray(a, dtype=float)
    out = np.empty_like(a)
    out[0] = a[0]
    k = 2.0 / (n + 1.0)
    for i in range(1, len(a)):
        out[i] = out[i - 1] + k * (a[i] - out[i - 1])
    return out


def rma(a, n):
    """Pine ta.rma = Wilder's smoothing = EMA with alpha=1/n."""
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    if len(a) < n:
        return out
    out[n - 1] = np.mean(a[:n])
    alpha = 1.0 / n
    for i in range(n, len(a)):
        out[i] = alpha * a[i] + (1 - alpha) * out[i - 1]
    return out


def rsi_wilder(close, n):
    """Pine ta.rsi باستخدام متوسط Wilder."""
    close = np.asarray(close, dtype=float)
    if len(close) < n + 1:
        return np.full_like(close, np.nan)
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = rma(gain, n)
    avg_loss = rma(loss, n)
    rs = np.where(avg_loss > 0, avg_gain / avg_loss, 100.0)
    rsi_arr = 100.0 - (100.0 / (1.0 + rs))
    rsi_arr[np.isnan(avg_loss) | np.isnan(avg_gain)] = np.nan
    return rsi_arr


def atr_func(h, l, c, n=14):
    """Pine ta.atr باستخدام Wilder smoothing."""
    h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float); c = np.asarray(c, dtype=float)
    if len(c) < n + 1:
        return np.full_like(c, np.nan)
    pc = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
    return rma(tr, n)


def dmi_func(h, l, c, n):
    """Pine ta.dmi -> returns (plus_di, minus_di, adx)."""
    h = np.asarray(h, dtype=float); l = np.asarray(l, dtype=float); c = np.asarray(c, dtype=float)
    if len(c) < 2 * n:
        return (np.full_like(c, np.nan), np.full_like(c, np.nan), np.full_like(c, np.nan))
    up = np.diff(h, prepend=h[0])
    down = -np.diff(l, prepend=l[0])
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    pc = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
    atr_s = rma(tr, n)
    plus_di = np.where(atr_s > 0, 100 * rma(plus_dm, n) / atr_s, np.nan)
    minus_di = np.where(atr_s > 0, 100 * rma(minus_dm, n) / atr_s, np.nan)
    dx = np.where((plus_di + minus_di) > 0,
                  100 * np.abs(plus_di - minus_di) / (plus_di + minus_di), 0.0)
    adx = rma(dx, n)
    return (plus_di, minus_di, adx)


def linreg(src, n, offset=0):
    """Pine ta.linreg."""
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) < n:
        return out
    x = np.arange(1, n + 1, dtype=float)
    x_mean = x.mean()
    x_var = ((x - x_mean) ** 2).sum()
    for i in range(n - 1, len(src)):
        y = src[i - n + 1: i + 1]
        if np.isnan(y).any():
            continue
        y_mean = y.mean()
        cov = ((x - x_mean) * (y - y_mean)).sum()
        slope = cov / x_var if x_var > 0 else 0.0
        intercept = y_mean - slope * x_mean
        out[i] = intercept + slope * (n - offset)
    return out


def kalman_filter(src, proc_n, meas_n):
    """Pine f_kalman — فلتر كالمان على متجه كامل."""
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) == 0:
        return out
    est = src[0]
    cov = 1.0
    out[0] = est
    for i in range(1, len(src)):
        if np.isnan(src[i]):
            out[i] = est
            continue
        cov = cov + proc_n
        gain = cov / (cov + meas_n)
        est = est + gain * (src[i] - est)
        cov = (1 - gain) * cov
        out[i] = est
    return out


def llama_filter(src, n, a_min, a_max):
    """Pine f_llama — LLAMA adaptive filter."""
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) < n + 1:
        return out
    est = src[0]
    out[0] = est
    for i in range(1, len(src)):
        if np.isnan(src[i]):
            out[i] = est
            continue
        if i >= n:
            slope = (src[i] - src[i - n]) / n
            window = src[i - n + 1: i + 1]
            hh = np.nanmax(window); ll = np.nanmin(window)
            rng = hh - ll
            eff = min(abs(slope * n) / rng, 1.0) if rng != 0 else 0.0
        else:
            eff = 0.0
        alpha = a_min + eff * (a_max - a_min)
        est = est + alpha * (src[i] - est)
        out[i] = est
    return out


def rolling_max(a, n):
    """Pine ta.highest."""
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    for i in range(n - 1, len(a)):
        out[i] = np.nanmax(a[i - n + 1: i + 1])
    return out


def rolling_min(a, n):
    """Pine ta.lowest."""
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    for i in range(n - 1, len(a)):
        out[i] = np.nanmin(a[i - n + 1: i + 1])
    return out


def crossover(a, b):
    """Pine ta.crossover: a cross above b."""
    a = np.asarray(a, dtype=float); b = np.asarray(b, dtype=float)
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if i < 2:
            continue
        if np.isnan(a[i - 1]) or np.isnan(b[i - 1]) or np.isnan(a[i - 2]) or np.isnan(b[i - 2]):
            continue
        out[i] = (a[i - 1] > b[i - 1]) and (a[i - 2] <= b[i - 2])
    return out


def crossunder(a, b):
    """Pine ta.crossunder: a cross below b."""
    a = np.asarray(a, dtype=float); b = np.asarray(b, dtype=float)
    out = np.zeros(len(a), dtype=bool)
    for i in range(1, len(a)):
        if i < 2:
            continue
        if np.isnan(a[i - 1]) or np.isnan(b[i - 1]) or np.isnan(a[i - 2]) or np.isnan(b[i - 2]):
            continue
        out[i] = (a[i - 1] < b[i - 1]) and (a[i - 2] >= b[i - 2])
    return out


def pivot_high(h, n):
    """Pine f_pivotHigh: قمة سوينغ بطول n (شكل ضمني: قيمة عند i-n أو nan)."""
    h = np.asarray(h, dtype=float)
    out = np.full_like(h, np.nan)
    for i in range(n, len(h) - n):
        c = h[i]
        ok = True
        for j in range(i - n, i):
            if h[j] >= c:
                ok = False; break
        if ok:
            for j in range(i + 1, i + n + 1):
                if h[j] > c:
                    ok = False; break
        if ok:
            out[i + n] = c   # يظهر بعد n شموع (Lookback)
    return out


def pivot_low(l, n):
    """Pine f_pivotLow: قاع سوينغ بطول n."""
    l = np.asarray(l, dtype=float)
    out = np.full_like(l, np.nan)
    for i in range(n, len(l) - n):
        c = l[i]
        ok = True
        for j in range(i - n, i):
            if l[j] <= c:
                ok = False; break
        if ok:
            for j in range(i + 1, i + n + 1):
                if l[j] < c:
                    ok = False; break
        if ok:
            out[i + n] = c
    return out


def triple_compute_signal(df15, cfg=None):
    """يحسب إشارات Triple Confluence Navigator على فريم 15 دقيقة.

    المرتجع: dict يحوي آخر إشارة {direction, entry, sl, tp1, tp2, tp3,
             components, score, atr_ref, lastSwingHigh, lastSwingLow} أو None.
    """
    cfg = cfg or TRIPLE_CONFIG
    if df15 is None or len(df15) < max(cfg['rmaSlowLen'], cfg['arsiLen'],
                                       cfg['stAtrLen'], cfg['atrLen']) + cfg['confluenceWindow'] + 5:
        return None

    close = df15['close'].values.astype(float)
    high = df15['high'].values.astype(float)
    low = df15['low'].values.astype(float)
    n = len(close)

    # ── 1) Cardwell RSI ──
    rsi_value = rsi_wilder(close, cfg['rsiLen'])

    # Kalman fast/slow على RSI (مطابق Pine: _kfFastX / _kfSlowX)
    _kfQ = 0.001; _kfR = 0.01
    _kfFastP = np.full(n, 1.0); _kfSlowP = np.full(n, 1.0)
    _kfFastX = np.full(n, np.nan); _kfSlowX = np.full(n, np.nan)
    if not np.isnan(rsi_value[0]):
        _kfFastX[0] = rsi_value[0]; _kfSlowX[0] = rsi_value[0]
    for i in range(1, n):
        if np.isnan(rsi_value[i]):
            _kfFastX[i] = _kfFastX[i - 1] if not np.isnan(_kfFastX[i - 1]) else rsi_value[i]
            _kfSlowX[i] = _kfSlowX[i - 1] if not np.isnan(_kfSlowX[i - 1]) else rsi_value[i]
            _kfFastP[i] = _kfFastP[i - 1]; _kfSlowP[i] = _kfSlowP[i - 1]
            continue
        prev_f = _kfFastX[i - 1] if not np.isnan(_kfFastX[i - 1]) else rsi_value[i]
        k_fast = _kfFastP[i - 1] / (_kfFastP[i - 1] + _kfR)
        _kfFastX[i] = prev_f + k_fast * (rsi_value[i] - prev_f)
        _kfFastP[i] = (1.0 - k_fast) * _kfFastP[i - 1] + _kfQ
        prev_s = _kfSlowX[i - 1] if not np.isnan(_kfSlowX[i - 1]) else rsi_value[i]
        k_slow = _kfSlowP[i - 1] / (_kfSlowP[i - 1] + _kfR * (cfg['rmaSlowLen'] / cfg['rmaFastLen']))
        _kfSlowX[i] = prev_s + k_slow * (rsi_value[i] - prev_s)
        _kfSlowP[i] = (1.0 - k_slow) * _kfSlowP[i - 1] + _kfQ

    ma_type = cfg['maType']
    if ma_type == "Kalman":
        rma_fast = _kfFastX
        rma_slow = _kfSlowX
    elif ma_type == "LLAMA":
        rma_fast = linreg(rsi_value, cfg['rmaFastLen'], 0)
        rma_slow = linreg(rsi_value, cfg['rmaSlowLen'], 0)
    else:
        rma_fast = rma(rsi_value, cfg['rmaFastLen'])
        rma_slow = rma(rsi_value, cfg['rmaSlowLen'])

    # rawBullCross / rawBearCross (مع إزاحة 1)
    raw_bull_cross = np.zeros(n, dtype=bool)
    raw_bear_cross = np.zeros(n, dtype=bool)
    for i in range(2, n):
        if np.isnan(rma_fast[i - 1]) or np.isnan(rma_slow[i - 1]):
            continue
        if np.isnan(rma_fast[i - 2]) or np.isnan(rma_slow[i - 2]):
            continue
        raw_bull_cross[i] = (rma_fast[i - 1] > rma_slow[i - 1]) and (rma_fast[i - 2] <= rma_slow[i - 2])
        raw_bear_cross[i] = (rma_fast[i - 1] < rma_slow[i - 1]) and (rma_fast[i - 2] >= rma_slow[i - 2])

    # ── ADX (Chop filter) ──
    if cfg['useChopFilter']:
        _pdi, _mdi, adx_value = dmi_func(high, low, close, cfg['adxLen'])
        chop_ok = (~np.isnan(adx_value)) & (np.r_[np.nan, adx_value[:-1]] >= cfg['adxMinLevel'])
    else:
        adx_value = np.full(n, np.nan)
        chop_ok = np.ones(n, dtype=bool)

    # ── HTF filter (معطّل افتراضياً) ──
    # في Pine يستخدم request.security — هنا نعتبر htfOk=true دائماً عند التعطيل
    htf_ok_bull = np.ones(n, dtype=bool)
    htf_ok_bear = np.ones(n, dtype=bool)
    htf_bull_bias = np.ones(n, dtype=bool)
    htf_bear_bias = np.ones(n, dtype=bool)
    htf_arsi_bull = np.ones(n, dtype=bool)
    htf_arsi_bear = np.ones(n, dtype=bool)

    # إشارات 1
    bull_signal1 = raw_bull_cross & (~cfg['useHtfFilter'] | htf_bull_bias) & chop_ok
    bear_signal1 = raw_bear_cross & (~cfg['useHtfFilter'] | htf_bear_bias) & chop_ok

    # ── 2) Adaptive ARSI + Supertrend ──
    hh = rolling_max(close, cfg['arsiLen'])
    ll = rolling_min(close, cfg['arsiLen'])
    rng = hh - ll
    diff_d = close - np.r_[np.nan, close[:-1]]
    # _diff = u > u[1] ? _r : l < l[1] ? -_r : _d
    hh_prev = np.r_[np.nan, hh[:-1]]
    ll_prev = np.r_[np.nan, ll[:-1]]
    cond_up = hh > hh_prev
    cond_dn = ll < ll_prev
    arsi_diff = np.where(cond_up, rng, np.where(cond_dn, -rng, diff_d))
    arsi_diff = np.where(np.isnan(rng), 0.0, arsi_diff)
    arsi_num = rma(arsi_diff, cfg['arsiLen'])
    arsi_den = rma(np.abs(arsi_diff), cfg['arsiLen'])
    with np.errstate(invalid='ignore', divide='ignore'):
        arsi = np.where(arsi_den != 0, arsi_num / arsi_den * 50 + 50, 50.0)

    # ARSI signal (filter)
    filt_type = cfg['arsiFiltType']
    if filt_type == "RMA":
        arsi_signal = rma(arsi, cfg['arsiSigLen'])
    elif filt_type == "Kalman Filter":
        arsi_signal = kalman_filter(arsi, cfg['kalmanProcN'], cfg['kalmanMeasN'])
    else:  # LLAMA
        arsi_signal = llama_filter(arsi, cfg['arsiSigLen'],
                                   cfg['llamaAlphaMin'], cfg['llamaAlphaMax'])

    arsi_atr = rma(np.abs(arsi - np.r_[np.nan, arsi[:-1]]), cfg['stAtrLen'])
    upper_band = arsi + cfg['stFactor'] * arsi_atr
    lower_band = arsi - cfg['stFactor'] * arsi_atr

    # Supertrend state
    supertrend = np.full(n, np.nan)
    trend_up = np.ones(n, dtype=bool)
    if not np.isnan(upper_band[0]) and not np.isnan(arsi[0]):
        supertrend[0] = upper_band[0]; trend_up[0] = True
    for i in range(1, n):
        if np.isnan(arsi[i]) or np.isnan(upper_band[i]) or np.isnan(lower_band[i]):
            if i > 0 and not np.isnan(supertrend[i - 1]):
                supertrend[i] = supertrend[i - 1]; trend_up[i] = trend_up[i - 1]
            else:
                supertrend[i] = upper_band[i] if not np.isnan(upper_band[i]) else np.nan
                trend_up[i] = True
            continue
        prev_st = supertrend[i - 1] if not np.isnan(supertrend[i - 1]) else upper_band[i]
        prev_tu = trend_up[i - 1]
        if arsi[i] > prev_st:
            tu = True
        elif arsi[i] < prev_st:
            tu = False
        else:
            tu = prev_tu
        if tu:
            supertrend[i] = max(lower_band[i], prev_st) if not np.isnan(prev_st) else lower_band[i]
        else:
            supertrend[i] = min(upper_band[i], prev_st) if not np.isnan(prev_st) else upper_band[i]
        trend_up[i] = tu

    raw_merged = np.where(np.isnan(supertrend), arsi_signal, (arsi_signal + supertrend) / 2.0)
    # rawBullCross2 = ta.crossover(arsi[1], rawMerged[1])
    arsi_shift = np.r_[np.nan, arsi[:-1]]
    merged_shift = np.r_[np.nan, raw_merged[:-1]]
    raw_bull_cross2 = crossover(arsi_shift, merged_shift)
    raw_bear_cross2 = crossunder(arsi_shift, merged_shift)

    bull_signal2 = raw_bull_cross2 & (~cfg['useHtfFilter'] | htf_arsi_bull)
    bear_signal2 = raw_bear_cross2 & (~cfg['useHtfFilter'] | htf_arsi_bear)

    # ── 3) Structure (BOS) ──
    ph = pivot_high(high, cfg['swingLen'])
    pl = pivot_low(low, cfg['swingLen'])

    last_swing_high = None
    last_swing_low = None
    last_swing_high_bar = -1
    last_swing_low_bar = -1
    last_processed_high_bar = -1
    last_processed_low_bar = -1
    trend3 = 0
    bull_signal3 = np.zeros(n, dtype=bool)
    bear_signal3 = np.zeros(n, dtype=bool)
    bar_idx = np.arange(n)

    # نمر على الشموع بالترتيب الزمني لمحاكاة حالة Pine
    lsh = np.full(n, np.nan); lsl = np.full(n, np.nan)
    lshb = np.full(n, -1, dtype=int); lslb = np.full(n, -1, dtype=int)
    trend3_arr = np.zeros(n, dtype=int)

    for i in range(n):
        if not np.isnan(ph[i]):
            last_swing_high = ph[i]
            last_swing_high_bar = i - cfg['swingLen']
        if not np.isnan(pl[i]):
            last_swing_low = pl[i]
            last_swing_low_bar = i - cfg['swingLen']
        lsh[i] = last_swing_high if last_swing_high is not None else np.nan
        lsl[i] = last_swing_low if last_swing_low is not None else np.nan
        lshb[i] = last_swing_high_bar
        lslb[i] = last_swing_low_bar
        # breakUp / breakDown (على الشمعة المؤكدة = الحالية)
        break_up = (last_swing_high is not None and close[i] > last_swing_high
                    and htf_ok_bull[i]
                    and (last_processed_high_bar == -1 or last_swing_high_bar != last_processed_high_bar))
        break_dn = (last_swing_low is not None and close[i] < last_swing_low
                    and htf_ok_bear[i]
                    and (last_processed_low_bar == -1 or last_swing_low_bar != last_processed_low_bar))
        if break_up:
            last_processed_high_bar = last_swing_high_bar
            trend3 = 1
        if break_dn:
            last_processed_low_bar = last_swing_low_bar
            trend3 = -1
        bull_signal3[i] = break_up
        bear_signal3[i] = break_dn
        trend3_arr[i] = trend3

    # ── Confluence (نافذة 5 شموع) ──
    bars_since_bull1 = np.full(n, 9999, dtype=int)
    bars_since_bear1 = np.full(n, 9999, dtype=int)
    bars_since_bull2 = np.full(n, 9999, dtype=int)
    bars_since_bear2 = np.full(n, 9999, dtype=int)
    bars_since_bull3 = np.full(n, 9999, dtype=int)
    bars_since_bear3 = np.full(n, 9999, dtype=int)

    for i in range(1, n):
        bars_since_bull1[i] = 0 if bull_signal1[i] else bars_since_bull1[i - 1] + 1
        bars_since_bear1[i] = 0 if bear_signal1[i] else bars_since_bear1[i - 1] + 1
        bars_since_bull2[i] = 0 if bull_signal2[i] else bars_since_bull2[i - 1] + 1
        bars_since_bear2[i] = 0 if bear_signal2[i] else bars_since_bear2[i - 1] + 1
        bars_since_bull3[i] = 0 if bull_signal3[i] else bars_since_bull3[i - 1] + 1
        bars_since_bear3[i] = 0 if bear_signal3[i] else bars_since_bear3[i - 1] + 1

    cw = cfg['confluenceWindow']
    bull_confluence = ((bars_since_bull1 <= cw) & (bars_since_bull2 <= cw) & (bars_since_bull3 <= cw))
    bear_confluence = ((bars_since_bear1 <= cw) & (bars_since_bear2 <= cw) & (bars_since_bear3 <= cw))

    # longSignal / shortSignal: حافة التحوّل (الإشارة الحالية true والسابقة false)
    bull_conf_prev = np.r_[False, bull_confluence[:-1]]
    bear_conf_prev = np.r_[False, bear_confluence[:-1]]
    long_signal = bull_confluence & (~bull_conf_prev)
    short_signal = bear_confluence & (~bear_conf_prev)

    # ── حساب المستويات على آخر إشارة ──
    atr_val = atr_func(high, low, close, cfg['atrLen'])
    atr_prev = np.r_[atr_val[0] if not np.isnan(atr_val[0]) else 0.0, atr_val[:-1]]
    atr_val1 = np.where(np.isnan(atr_prev), atr_val, atr_prev)

    # نبحث عن آخر إشارة طويلة أو قصيرة
    long_indices = np.where(long_signal)[0]
    short_indices = np.where(short_signal)[0]
    last_long = long_indices[-1] if len(long_indices) else -1
    last_short = short_indices[-1] if len(short_indices) else -1

    if last_long < 0 and last_short < 0:
        return None

    if last_long >= last_short:
        i = last_long
        direction = 'BUY'
        entry_price = float(close[i - 1]) if i >= 1 else float(close[i])
        atr_cap_dist = float(atr_val1[i]) * cfg['atrMultSl']
        last_swing_low_at = float(lsl[i]) if not np.isnan(lsl[i]) else None
        if last_swing_low_at is not None:
            struct_dist = entry_price - (last_swing_low_at - float(atr_val1[i]) * cfg['slStructBuf'])
        else:
            struct_dist = None
        if struct_dist is None or struct_dist <= 0:
            risk_dist = atr_cap_dist
        else:
            risk_dist = min(struct_dist, atr_cap_dist)
        sl_price = entry_price - risk_dist
        tp1_price = entry_price + risk_dist * cfg['rr1']
        tp2_price = entry_price + risk_dist * cfg['rr2']
        tp3_price = entry_price + risk_dist * cfg['rr3']
        last_swing_high_at = float(lsh[i]) if not np.isnan(lsh[i]) else None
    else:
        i = last_short
        direction = 'SELL'
        entry_price = float(close[i - 1]) if i >= 1 else float(close[i])
        atr_cap_dist = float(atr_val1[i]) * cfg['atrMultSl']
        last_swing_high_at = float(lsh[i]) if not np.isnan(lsh[i]) else None
        if last_swing_high_at is not None:
            struct_dist = (last_swing_high_at + float(atr_val1[i]) * cfg['slStructBuf']) - entry_price
        else:
            struct_dist = None
        if struct_dist is None or struct_dist <= 0:
            risk_dist = atr_cap_dist
        else:
            risk_dist = min(struct_dist, atr_cap_dist)
        sl_price = entry_price + risk_dist
        tp1_price = entry_price - risk_dist * cfg['rr1']
        tp2_price = entry_price - risk_dist * cfg['rr2']
        tp3_price = entry_price - risk_dist * cfg['rr3']
        last_swing_low_at = float(lsl[i]) if not np.isnan(lsl[i]) else None

    return {
        'direction': direction,
        'price': entry_price,
        'entry': entry_price,
        'sl': float(sl_price),
        'tp1': float(tp1_price),
        'tp2': float(tp2_price),
        'tp3': float(tp3_price),
        'risk_dist': float(risk_dist),
        'atr_ref': float(atr_val1[i]),
        'lastSwingHigh': last_swing_high_at,
        'lastSwingLow': last_swing_low_at,
        'signal_bar_idx': int(i),
        'components': {
            'trigger': 'Triple Confluence Navigator',
            'bull_signal1': bool(bull_signal1[i]),
            'bear_signal1': bool(bear_signal1[i]),
            'bull_signal2': bool(bull_signal2[i]),
            'bear_signal2': bool(bear_signal2[i]),
            'bull_signal3': bool(bull_signal3[i]),
            'bear_signal3': bool(bear_signal3[i]),
            'trend3': int(trend3_arr[i]),
            'rsi': float(rsi_value[i]) if not np.isnan(rsi_value[i]) else None,
            'arsi': float(arsi[i]) if not np.isnan(arsi[i]) else None,
        },
        'score': 8,
    }


# ══════════════════════════════════════════════════════════════════════
#  فئة الاستراتيجية + مدير الخروج + قاعدة بياناتها
# ══════════════════════════════════════════════════════════════════════
class TripleStrategy:
    def __init__(self, cfg=None, log=None):
        self.cfg = dict(TRIPLE_CONFIG, **(cfg or {}))
        self.last_scan = {}
        self._log = log or (lambda x: None)

    async def scan(self, bot, candidates, active_symbols, cooldowns):
        """يفحص المرشّحين على فريم 15 دقيقة ويُرجع إشارات Triple Confluence."""
        sigs = []
        scanned = 0
        for sym in candidates:
            if sym in active_symbols:
                continue
            if sym in cooldowns:
                continue
            try:
                df15 = await bot.get_klines(sym, '15m', 300)
                if df15 is None or len(df15) < 60:
                    continue
                scanned += 1
                sig = triple_compute_signal(df15, self.cfg)
                if not sig:
                    continue
                # نأخذ الإشارة فقط إذا كانت على آخر شمعة مغلقة (ضمن نافذة confluence)
                # آخر شمعة في DataFrame هي الأحدث — signal_bar_idx يجب أن يكون قريباً من النهاية
                last_idx = len(df15) - 1
                if sig['signal_bar_idx'] < last_idx - self.cfg['confluenceWindow']:
                    continue
                sig['symbol'] = sym
                sig['strategy'] = 'TRIPLE'
                sig['tp'] = sig['tp1']
                sig['signals_smc'] = ['Triple Confluence Navigator',
                                      f"RSI={sig['components'].get('rsi')}",
                                      f"ARSI={sig['components'].get('arsi')}"]
                sigs.append(sig)
            except Exception as e:
                logger.warning(f"⚠️ خطأ فحص {sym}: {e}")
        self.last_scan = {'scanned': scanned, 'signals': len(sigs), 'ts': time.time()}
        return sigs


class TripleExitManager:
    """إدارة الخروج: تنفيذ TP1/TP2/TP3 و SL للأهداف الجزئية."""
    def __init__(self, cfg=None, log=None):
        self.cfg = dict(TRIPLE_CONFIG, **(cfg or {}))
        self._log = log or (lambda x: None)

    async def manage(self, bot, trade):
        """فحص صفقة واحدة — يُرجع True إذا أُغلقت بالكامل."""
        sym = trade['symbol']
        if sym not in bot.active_trades:
            return False
        prices = bot.get_price(sym)
        if not prices:
            return False
        is_buy = trade['side'] == 'BUY'
        cur = prices['bid'] if is_buy else prices['ask']
        # تحديث أعلى/أدنى سعر
        if is_buy:
            if trade.get('highest_price') is None or cur > trade['highest_price']:
                trade['highest_price'] = cur
        else:
            if trade.get('lowest_price') is None or cur < trade['lowest_price']:
                trade['lowest_price'] = cur
        # فحص SL
        sl = trade.get('sl')
        if sl is not None:
            if (is_buy and cur <= sl) or (not is_buy and cur >= sl):
                await bot.close_trade(trade, reason='SL', exit_price=sl)
                return True
        # فحص TP3 (إغلاق كامل)
        tp3 = trade.get('tp3')
        if tp3 is not None and not trade.get('partial2_closed'):
            if (is_buy and cur >= tp3) or (not is_buy and cur <= tp3):
                # إغلاق 30% المتبقية
                await bot.close_trade_partial(trade, ratio=1.0, reason='TP3', exit_price=tp3)
                await bot.close_trade(trade, reason='TP3_DONE', exit_price=tp3, full_close=True)
                return True
        # فحص TP2 (إغلاق 30%)
        tp2 = trade.get('tp2')
        if tp2 is not None and not trade.get('partial_closed'):
            if (is_buy and cur >= tp2) or (not is_buy and cur <= tp2):
                await bot.close_trade_partial(trade, ratio=0.30, reason='TP2', exit_price=tp2)
                trade['partial_closed'] = True
                return False
        # فحص TP1 (إغلاق 40%)
        tp1 = trade.get('tp') or trade.get('tp1')
        if tp1 is not None and not trade.get('partial_closed'):
            if (is_buy and cur >= tp1) or (not is_buy and cur <= tp1):
                await bot.close_trade_partial(trade, ratio=0.40, reason='TP1', exit_price=tp1)
                trade['partial_closed'] = True
                return False
        await bot.db.save_trade(trade)
        return False


class TripleDB:
    """قاعدة بيانات مستقلة لاستراتيجية Triple — صفقات مغلقة + أرباح يومية."""
    def __init__(self, db_name=None):
        self.db_name = db_name or db_file

    async def init(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS triple_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, side TEXT, entry_ts REAL, exit_ts REAL,
                entry REAL, exit REAL, qty REAL, pnl REAL, pnl_pct REAL,
                win INTEGER, reason TEXT, components TEXT, margin REAL)""")
            for col_def in ('tp1 REAL', 'tp2 REAL', 'tp3 REAL', 'sl REAL'):
                try:
                    await db.execute(f"ALTER TABLE triple_trades ADD COLUMN {col_def}")
                except Exception:
                    pass
            await db.execute("""CREATE TABLE IF NOT EXISTS triple_daily (
                date TEXT PRIMARY KEY, pnl REAL, wins INTEGER, losses INTEGER)""")
            await db.commit()

    async def open(self, trade):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""INSERT INTO triple_trades
                (symbol, side, entry_ts, entry, qty, sl, tp1, tp2, tp3, components, margin)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (trade['symbol'], trade['side'], trade['entry_time'],
                 trade['entry_price'], trade['quantity'], trade.get('sl'),
                 trade.get('tp') or trade.get('tp1'), trade.get('tp2'), trade.get('tp3'),
                 json.dumps(trade.get('components', {}), ensure_ascii=False),
                 trade.get('margin_usdt', 0)))
            await db.commit()

    async def close(self, symbol, entry_ts, exit_ts, exit_price, pnl, pnl_pct, win, reason, components):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute("""UPDATE triple_trades
                    SET exit_ts=?, exit=?, pnl=?, pnl_pct=?, win=?, reason=?
                    WHERE symbol=? AND entry_ts=? AND exit_ts IS NULL""",
                    (exit_ts, exit_price, pnl, pnl_pct, int(win), reason, symbol, entry_ts))
                await db.commit()
        except Exception as e:
            logger.warning(f"تعذر تحديث صفقة Triple المغلقة: {e}")

    async def today_stats(self):
        today = time.strftime('%Y-%m-%d')
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute(
                "SELECT COALESCE(SUM(pnl),0), COUNT(*) FROM triple_trades "
                "WHERE date(exit_ts,'unixepoch')=? AND exit_ts IS NOT NULL", (today,)) as cur:
                row = await cur.fetchone()
                return {'pnl': row[0] or 0.0, 'count': row[1] or 0}

    async def history(self, limit=500):
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM triple_trades WHERE exit_ts IS NOT NULL "
                "ORDER BY exit_ts DESC LIMIT ?", (limit,)) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def daily_stats_all(self):
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM triple_daily ORDER BY date DESC LIMIT 60") as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def update_daily(self, pnl, win):
        today = time.strftime('%Y-%m-%d')
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute("SELECT * FROM triple_daily WHERE date=?", (today,)) as cur:
                row = await cur.fetchone()
            w = 1 if win else 0; l = 0 if win else 1
            if row:
                await db.execute("UPDATE triple_daily SET pnl=?, wins=?, losses=? WHERE date=?",
                                 (row[1] + pnl, row[2] + w, row[3] + l, today))
            else:
                await db.execute("INSERT INTO triple_daily VALUES (?,?,?,?)", (today, pnl, w, l))
            await db.commit()


# ══════════════════════════════════════════════════════════════════════
#  البوت الرئيسي
# ══════════════════════════════════════════════════════════════════════
class TripleConfluenceBot:
    STRATEGY_LABELS = {'TRIPLE': '🧭 Triple Confluence Navigator'}

    def __init__(self):
        self.binance_api_key = os.environ.get('BINANCE_API_KEY', '')
        self.binance_api_secret = os.environ.get('BINANCE_API_SECRET', '')

        self.TRADE_ENABLED = os.environ.get('TRADE_ENABLED', 'false').lower() == 'true'
        # ✅ الرافعة 10 (مطلوب المستخدم)
        self.LEVERAGE = int(os.environ.get('LEVERAGE', '10'))
        # ✅ الهامش 50$ (مطلوب المستخدم)
        self.TRADE_SIZE_USDT = float(os.environ.get('TRADE_SIZE_USDT', '50'))
        self.TESTNET = os.environ.get('TESTNET', 'false').lower() == 'true'

        # ✅ الصفقات المتزامنة: 1 فقط (مطلوب المستخدم)
        self.max_open_triple = int(os.environ.get('MAX_OPEN_TRADES', '1'))

        problems = []
        if self.TRADE_SIZE_USDT <= 0:
            problems.append("TRADE_SIZE_USDT يجب أن يكون أكبر من صفر")
        if not (1 <= self.max_open_triple <= 20):
            problems.append("MAX_OPEN_TRADES يجب أن يكون بين 1 و 20")
        if not (1 <= self.LEVERAGE <= 125):
            problems.append("LEVERAGE يجب أن يكون بين 1 و 125")
        if self.TESTNET:
            logger.info("🧪 وضع Testnet التجريبي مفعّل")
        if problems:
            raise SystemExit("❌ إعدادات خاطئة:\n- " + "\n- ".join(problems))

        if self.TRADE_ENABLED and (not self.binance_api_key or not self.binance_api_secret):
            self.TRADE_ENABLED = False
            logger.error("❌ التداول مفعّل لكن مفاتيح API ناقصة! التحويل للمراقبة فقط")

        # ثوابت حماية تشغيلية
        self.PRICE_MAX_AGE = 30
        self.REENTRY_COOLDOWN = 900
        self.BALANCE_CACHE_SECONDS = 60

        self.data_url = "https://fapi.binance.com"
        self.trade_url = ("https://testnet.binancefuture.com" if self.TESTNET
                          else "https://fapi.binance.com")
        self.time_offset_ms = 0

        self.db = DatabaseManager(db_file)
        self.triple = TripleStrategy(None, logger.warning)
        self.triple_exit = TripleExitManager(None, logger.warning)
        self.triple_db = TripleDB(db_file)
        self.port = int(os.environ.get('PORT', '10000'))

        self.session = None
        self.ws_task = None
        self.shutdown_requested = False
        self.EMERGENCY_MODE = False

        self.active_trades = {}
        self.all_futures_pairs = {}
        self.symbol_info_cache = {}
        self.min_qty_cache = {}
        self.max_qty_cache = {}
        self.min_notional_cache = {}
        self.price_precision_cache = {}
        self.qty_precision_cache = {}
        self._price_cache = {}
        self.reentry_cooldown = {}
        self.trend_pending = {}  # غير مستخدم لكن نبقيه للتوافق
        self.stats = {'trades_executed': 0, 'trades_closed': 0}
        self.stats_triple = {'trades': 0, 'wins': 0, 'losses': 0, 'net_pnl': 0.0}
        self.triple_events = collections.deque(maxlen=200)
        self.error_counts = {}
        self.entry_history = collections.deque(maxlen=100)
        self.last_scan_info = {'ts': 0}
        self._balance_cache = None
        self._balance_ts = 0
        self.server_ts = int(time.time())
        self.triple_capacity = self.max_open_triple

    # ─── الوصول للاستراتيجية (مطابق واجهة web_panel) ───
    def strategy_margin(self, key='TRIPLE'):
        return self.TRADE_SIZE_USDT

    def strategy_capacity(self, key='TRIPLE'):
        return self.triple_capacity

    def strategy_cfg(self, key='TRIPLE'):
        return self.triple.cfg

    def strategy_events(self, key='TRIPLE'):
        return self.triple_events

    def strategy_stats(self, key='TRIPLE'):
        return self.stats_triple

    @staticmethod
    def other_strategy(key):
        return None  # استراتيجية واحدة فقط

    def count_open(self, key='TRIPLE'):
        return sum(1 for t in self.active_trades.values() if t.get('strategy') == 'TRIPLE')

    def _log_event(self, key, kind, msg):
        try:
            self.triple_events.append({"ts": time.time(), "kind": kind, "msg": msg})
        except Exception:
            pass

    # ─── مكافحة الصمت ───
    def _notify_error(self, source, exc=None, context=""):
        count = self.error_counts.get(source, 0) + 1
        self.error_counts[source] = count
        txt = f"⚠️ {source}" + (f" — {context}" if context else "")
        if exc is not None:
            txt += f" | `{str(exc)[:100]}`"
        logger.warning(f"{txt} (تكرار #{count})")
        if count == 1 or count % 10 == 0:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.tg(f"{txt}\n🔢 مرات التكرار: {count}"))
            except RuntimeError:
                pass

    def _reset_error(self, source):
        self.error_counts.pop(source, None)

    def fmt_price(self, price):
        if price is None or price == 0:
            return "$0"
        if price < 0.00001:
            return f"${price:.10f}"
        elif price < 0.001:
            return f"${price:.6f}"
        elif price < 1:
            return f"${price:.4f}"
        elif price < 100:
            return f"${price:.2f}"
        return f"${price:,.2f}"

    def fmt_pct(self, pct):
        if pct is None:
            return "—"
        return f"{pct:+.2f}%"

    # ─── Telegram ───
    async def tg(self, msg):
        token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')
        if not token or not chat_id or not self.session:
            return
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'}
            async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.warning(f"Telegram status {r.status}")
        except Exception as e:
            logger.warning(f"تعذر إرسال Telegram: {e}")

    # ─── طلبات Binance ───
    async def _fapi_request(self, method, path, params=None, signed=False, quiet=(), soft=()):
        params = dict(params or {})
        if signed:
            ts = int(time.time() * 1000) + self.time_offset_ms
            params['timestamp'] = ts
            query = urlencode(params)
            sig = hmac.new(self.binance_api_secret.encode(),
                           query.encode(), hashlib.sha256).hexdigest()
            params['signature'] = sig
        url = self.trade_url + path
        headers = {'X-MBX-APIKEY': self.binance_api_key}
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self.session.request(method, url, params=params,
                                            headers=headers, timeout=timeout) as r:
                txt = await r.text()
                try:
                    data = json.loads(txt)
                except Exception:
                    data = {'raw': txt}
                if r.status == 200:
                    return data
                code = data.get('code') if isinstance(data, dict) else None
                if code in quiet:
                    return None
                if code in soft:
                    return {'_soft_code': code}
                logger.warning(f"Binance API {method} {path} -> {r.status} {txt[:200]}")
                return data if isinstance(data, dict) else None
        except Exception as e:
            logger.warning(f"استثناء Binance API {method} {path}: {e}")
            return None

    async def _public_get(self, path, params=None):
        url = self.data_url + path
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self.session.get(url, params=params or {}, timeout=timeout) as r:
                if r.status != 200:
                    return None
                return await r.json()
        except Exception as e:
            logger.warning(f"استثناء public GET {path}: {e}")
            return None

    # ─── بيانات السوق ───
    async def load_market_data(self):
        try:
            info = await self._public_get('/fapi/v1/exchangeInfo')
            if not info or 'symbols' not in info:
                return False
            self.all_futures_pairs = {}
            for s in info['symbols']:
                sym = s['symbol']
                if s.get('contractType') == 'PERPETUAL' and s.get('status') == 'TRADING':
                    self.all_futures_pairs[sym] = s
                    self.symbol_info_cache[sym] = s
                    pp = s.get('pricePrecision', 2)
                    qp = s.get('quantityPrecision', 3)
                    self.price_precision_cache[sym] = pp
                    self.qty_precision_cache[sym] = qp
                    for f in s.get('filters', []):
                        if f.get('filterType') == 'PRICE_FILTER':
                            pass
                        if f.get('filterType') == 'MARKET_LOT_SIZE':
                            self.min_qty_cache[sym] = float(f.get('minQty', 0.001))
                            self.max_qty_cache[sym] = float(f.get('maxQty', 1e9))
                        if f.get('filterType') == 'MIN_NOTIONAL':
                            self.min_notional_cache[sym] = float(f.get('notional', 5.0))
            logger.info(f"📊 تم تحميل {len(self.all_futures_pairs)} زوجاً دائماً")
            return True
        except Exception as e:
            logger.error(f"فشل تحميل بيانات السوق: {e}")
            return False

    async def get_klines(self, symbol, interval='15m', limit=300):
        try:
            data = await self._public_get('/fapi/v1/klines',
                                          {'symbol': symbol, 'interval': interval, 'limit': limit})
            if not data or not isinstance(data, list):
                return None
            df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume',
                                             'close_time', 'quote_vol', 'trades',
                                             'taker_buy_base', 'taker_buy_quote', 'ignore'])
            for c in ['open', 'high', 'low', 'close', 'volume']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            df['time'] = pd.to_numeric(df['time'], errors='coerce')
            df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
            return df
        except Exception as e:
            logger.warning(f"تعذر جلب klines {symbol} {interval}: {e}")
            return None

    def get_price(self, symbol):
        return self._price_cache.get(symbol)

    async def _update_prices_ws(self):
        """تحديث الأسعار عبر WebSocket."""
        if not self.all_futures_pairs:
            return
        # نبسّط: نستخدم REST كل دقيقة للأسعار (أكثر استقراراً على Railway)
        try:
            data = await self._public_get('/fapi/v1/ticker/bookTicker')
            if data and isinstance(data, list):
                for t in data:
                    sym = t.get('symbol')
                    if sym and sym in self.all_futures_pairs:
                        try:
                            self._price_cache[sym] = {
                                'bid': float(t.get('bidPrice', 0)),
                                'ask': float(t.get('askPrice', 0)),
                            }
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"تعذر تحديث الأسعار: {e}")

    async def sync_server_time(self):
        try:
            data = await self._public_get('/fapi/v1/time')
            if data and 'serverTime' in data:
                local = int(time.time() * 1000)
                self.time_offset_ms = int(data['serverTime'] - local)
                self.server_ts = int(data['serverTime'] / 1000)
                logger.info(f"⏱️ وقت الخادم مُزامن — فرق {self.time_offset_ms}ms")
        except Exception as e:
            logger.warning(f"تعذر مزامنة وقت الخادم: {e}")

    # ─── صياغة الكمية/السعر ───
    def format_quantity(self, symbol, qty):
        qp = self.qty_precision_cache.get(symbol, 3)
        return round(float(qty), qp)

    def qty_to_str(self, symbol, qty):
        qp = self.qty_precision_cache.get(symbol, 3)
        return f"{float(qty):.{qp}f}"

    def price_to_str(self, symbol, price):
        pp = self.price_precision_cache.get(symbol, 2)
        return f"{float(price):.{pp}f}"

    # ─── الرصيد ───
    async def get_available_usdt(self):
        now = time.time()
        if self._balance_cache and now - self._balance_ts < self.BALANCE_CACHE_SECONDS:
            return self._balance_cache
        try:
            data = await self._fapi_request('GET', '/fapi/v2/balance', signed=True)
            if isinstance(data, list):
                for b in data:
                    if b.get('asset') == 'USDT':
                        self._balance_cache = float(b.get('availableBalance', 0))
                        self._balance_ts = now
                        return self._balance_cache
        except Exception as e:
            logger.warning(f"تعذر جلب الرصيد: {e}")
        return self._balance_cache or 0.0

    # ─── تبريد إعادة الدخول ───
    def is_in_cooldown(self, symbol):
        exp = self.reentry_cooldown.get(symbol)
        return bool(exp and time.time() < exp)

    # ─── دوران المرشّحين ───
    def _rotation_candidates(self, cursor_attr, limit):
        all_pairs = sorted(self.all_futures_pairs.keys())
        if not all_pairs:
            return []
        cursor = getattr(self, cursor_attr, 0)
        selected = []
        for i in range(limit):
            idx = (cursor + i) % len(all_pairs)
            selected.append(all_pairs[idx])
        setattr(self, cursor_attr, (cursor + limit) % len(all_pairs))
        return selected

    # ─── المسح ───
    async def _scan_strategy(self, key='TRIPLE'):
        strat = self.triple
        if not strat.cfg.get('enabled'):
            return
        if self.count_open(key) >= self.strategy_capacity(key):
            return
        try:
            candidates = self._rotation_candidates('_triple_cursor', int(strat.cfg['scan_limit']))
            if not candidates:
                return
            sigs = await strat.scan(self, candidates, set(self.active_trades),
                                    self.reentry_cooldown)
            if sigs:
                self._log_event(key, 'SCAN',
                                f"🔍 {len(sigs)} إشارة من {len(candidates)} زوجاً مفحوصاً")
            for a in sigs:
                await self.execute_trade(a)
                await asyncio.sleep(1)
        except Exception as e:
            self._notify_error("خطأ مسح استراتيجية Triple", exc=e)

    async def scan_volatile_coins(self):
        if self.EMERGENCY_MODE or not self.TRADE_ENABLED:
            return
        await self._scan_strategy('TRIPLE')
        self.last_scan_info = {
            'ts': time.time(),
            'triple': dict(getattr(self.triple, 'last_scan', {}) or {}),
        }

    # ─── التنفيذ ───
    async def execute_trade(self, analysis):
        if not self.TRADE_ENABLED:
            return
        if self.EMERGENCY_MODE:
            return
        strategy = analysis.get('strategy', 'TRIPLE')
        symbol = analysis['symbol']
        if symbol in self.active_trades:
            return
        if self.is_in_cooldown(symbol):
            return
        margin = self.strategy_margin(strategy)
        if self.count_open(strategy) >= self.strategy_capacity(strategy):
            return

        available = await self.get_available_usdt()
        if available < margin * 1.2:
            self._notify_error("رصيد غير كافٍ للدخول",
                               context=f"المتاح {available:.2f}$ < المطلوب {margin * 1.2:.2f}$")
            return

        notional = margin * self.LEVERAGE
        quantity = self.format_quantity(symbol, notional / analysis['price'])
        if quantity <= 0:
            return
        if quantity < self.min_qty_cache.get(symbol, 0.0):
            return
        if quantity > self.max_qty_cache.get(symbol, 1e12):
            return
        if quantity * analysis['price'] < self.min_notional_cache.get(symbol, 5.0) * 1.02:
            return

        await self._fapi_request('POST', '/fapi/v1/leverage',
            {'symbol': symbol, 'leverage': self.LEVERAGE}, signed=True)
        await self._fapi_request('POST', '/fapi/v1/marginType',
            {'symbol': symbol, 'marginType': 'ISOLATED'}, signed=True, quiet=(-4046,))

        side = 'BUY' if analysis['direction'] == 'BUY' else 'SELL'
        result = await self._fapi_request('POST', '/fapi/v1/order', {
            'symbol': symbol, 'side': side, 'type': 'MARKET',
            'quantity': self.qty_to_str(symbol, quantity)
        }, signed=True)

        if not result or result.get('status') not in ['FILLED', 'NEW']:
            await self.tg(f"❌ *فشل تنفيذ {side} (`{symbol}`)*")
            return

        avg = float(result.get('avgPrice') or 0)
        entry_price = avg if avg > 0 else analysis['price']

        trade_data = {
            'symbol': symbol, 'side': side, 'entry_price': entry_price,
            'quantity': quantity, 'sl': analysis['sl'], 'tp': analysis['tp1'],
            'tp2': analysis.get('tp2'), 'tp3': analysis.get('tp3'),
            'score': analysis.get('score', 8),
            'trailing_active': False, 'highest_price': entry_price,
            'lowest_price': entry_price, 'entry_time': time.time(),
            'partial_closed': False, 'partial2_closed': False,
            'realized_pnl': 0.0, 'stop_order_id': None, 'tp_order_id': None,
            'stop_is_algo': False, 'tp_is_algo': False,
            'stop_synced_price': None, 'tp_skipped': False,
            'strategy': 'TRIPLE', 'margin_usdt': margin,
            'atr_ref': analysis.get('atr_ref'),
            'components': analysis.get('components', {}),
        }
        self.active_trades[symbol] = trade_data
        await self.db.save_trade(trade_data)
        self.stats['trades_executed'] += 1
        self.stats_triple['trades'] += 1
        self.entry_history.append((time.time(), time.strftime("%Y-%m-%d")))
        await self.db.log_signal(symbol, side, trade_data['entry_time'],
                                 analysis.get('score', 8), analysis.get('components', {}))

        try:
            await self.triple_db.open(trade_data)
        except Exception as e:
            logger.warning(f"تعذر تسجيل صفقة Triple: {e}")

        try:
            cd = int(self.triple.cfg.get('reentry_cooldown', 3600))
        except Exception:
            cd = 3600
        self.reentry_cooldown[symbol] = time.time() + cd

        risk = abs(analysis['price'] - analysis['sl'])
        self._log_event('TRIPLE', 'ENTRY',
                        f"{symbol} {analysis['direction']} @ {self.fmt_price(analysis['price'])} | "
                        f"وقف {self.fmt_price(analysis['sl'])} ({risk / analysis['price'] * 100:.2f}%) | "
                        f"TP1 {self.fmt_price(analysis['tp1'])} | TP2 {self.fmt_price(analysis.get('tp2'))} | "
                        f"TP3 {self.fmt_price(analysis.get('tp3'))} | مخاطرة {risk * (quantity or 0):.3f}$")
        await self.tg(f"🧭 *Triple Confluence — صفقة جديدة*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
                      f"🪙 `{symbol}` | رافعة {self.LEVERAGE}x | هامش {margin:.0f}$\n"
                      f"💵 الدخول: {self.fmt_price(entry_price)}\n"
                      f"🛑 SL: {self.fmt_price(analysis['sl'])}\n"
                      f"🎯 TP1: {self.fmt_price(analysis['tp1'])}\n"
                      f"🎯 TP2: {self.fmt_price(analysis.get('tp2'))}\n"
                      f"🎯 TP3: {self.fmt_price(analysis.get('tp3'))}")

    async def close_trade_partial(self, trade, ratio, reason, exit_price):
        """إغلاق جزئي بنسبة ratio من الكمية الأصلية."""
        if ratio <= 0 or ratio >= 1:
            return
        symbol = trade['symbol']
        side = trade['side']
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        qty = trade['quantity'] * ratio
        qty = self.format_quantity(symbol, qty)
        if qty <= 0:
            return
        try:
            result = await self._fapi_request('POST', '/fapi/v1/order', {
                'symbol': symbol, 'side': close_side, 'type': 'MARKET',
                'quantity': self.qty_to_str(symbol, qty),
                'reduceOnly': 'true'
            }, signed=True)
            if not result or result.get('status') not in ['FILLED', 'NEW']:
                return
            # حساب الربح الجزئي
            entry = trade['entry_price']
            if side == 'BUY':
                pnl = (exit_price - entry) * qty
            else:
                pnl = (entry - exit_price) * qty
            trade['realized_pnl'] = trade.get('realized_pnl', 0) + pnl
            trade['quantity'] = trade['quantity'] - qty
            await self.db.save_trade(trade)
            await self.db.update_daily_pnl(pnl, is_win=(pnl > 0))
            try:
                await self.triple_db.update_daily(pnl, win=(pnl > 0))
            except Exception:
                pass
            self._log_event('TRIPLE', 'PARTIAL',
                            f"{symbol} إغلاق {ratio*100:.0f}% @ {self.fmt_price(exit_price)} | "
                            f"ربح {pnl:+.2f}$")
        except Exception as e:
            logger.warning(f"تعذر الإغلاق الجزئي {symbol}: {e}")

    async def close_trade(self, trade, reason='CLOSE', exit_price=None, full_close=False):
        """إغلاق الصفقة بالكامل."""
        symbol = trade['symbol']
        if symbol not in self.active_trades:
            return
        side = trade['side']
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        qty = trade['quantity']
        if qty <= 0:
            del self.active_trades[symbol]
            await self.db.remove_trade(symbol)
            return
        try:
            result = await self._fapi_request('POST', '/fapi/v1/order', {
                'symbol': symbol, 'side': close_side, 'type': 'MARKET',
                'quantity': self.qty_to_str(symbol, qty),
                'reduceOnly': 'true'
            }, signed=True)
            if not result or result.get('status') not in ['FILLED', 'NEW']:
                return
            entry = trade['entry_price']
            actual_exit = exit_price or (self.get_price(symbol) or {}).get('bid' if side == 'BUY' else 'ask') or entry
            if side == 'BUY':
                pnl = (actual_exit - entry) * qty
            else:
                pnl = (entry - actual_exit) * qty
            pnl += trade.get('realized_pnl', 0.0)
            win = pnl > 0
            await self.db.update_daily_pnl(pnl, is_win=win)
            try:
                await self.triple_db.update_daily(pnl, win=win)
            except Exception:
                pass
            try:
                await self.triple_db.close(symbol, trade['entry_time'], time.time(),
                                           actual_exit, pnl, pnl / trade.get('margin_usdt', margin := 50) * 100,
                                           win, reason, trade.get('components', {}))
            except Exception:
                pass
            await self.db.set_signal_result(symbol, trade['entry_time'], win, pnl,
                                            close_ts=time.time(), close_reason=reason,
                                            duration_s=time.time() - trade['entry_time'],
                                            pnl_pct=pnl / max(trade.get('margin_usdt', 50), 1) * 100)
            self.stats['trades_closed'] += 1
            if win:
                self.stats_triple['wins'] += 1
            else:
                self.stats_triple['losses'] += 1
            self.stats_triple['net_pnl'] += pnl
            del self.active_trades[symbol]
            await self.db.remove_trade(symbol)
            self._log_event('TRIPLE', 'CLOSE',
                            f"{symbol} أُغلقت ({reason}) @ {self.fmt_price(actual_exit)} | "
                            f"صافي {pnl:+.2f}$")
            await self.tg(f"{'✅' if win else '❌'} *Triple — صفقة مغلقة*\n"
                          f"🪙 `{symbol}` | السبب: {reason}\n"
                          f"💵 الدخول: {self.fmt_price(entry)} | الخروج: {self.fmt_price(actual_exit)}\n"
                          f"💰 الصافي: {pnl:+.2f}$")
        except Exception as e:
            logger.error(f"خطأ إغلاق {symbol}: {e}")

    # ─── إغلاق طارئ ───
    async def close_all_trades(self):
        closed = []
        for sym in list(self.active_trades.keys()):
            try:
                await self.close_trade(self.active_trades[sym], reason='CLOSE_ALL')
                closed.append(sym)
            except Exception as e:
                logger.error(f"تعذر إغلاق {sym}: {e}")
        return closed

    # ─── التحكم الخارجي (من اللوحة) ───
    async def request_shutdown(self, source='external'):
        logger.info(f"🛑 طلب إيقاف من {source}")
        self.shutdown_requested = True

    async def pause_trading(self):
        self.TRADE_ENABLED = False
        logger.info("⏸️ التداول موقوف")

    async def resume_trading(self):
        if self.binance_api_key and self.binance_api_secret:
            self.TRADE_ENABLED = True
            logger.info("▶️ التداول مستأنف")
            return True
        return False

    # ─── إدارة الصفقات (فحص دوري) ───
    async def manage_positions(self):
        if not self.active_trades:
            return
        for sym in list(self.active_trades.keys()):
            trade = self.active_trades.get(sym)
            if not trade:
                continue
            try:
                await self.triple_exit.manage(self, trade)
            except Exception as e:
                logger.warning(f"خطأ إدارة {sym}: {e}")

    # ─── اللوب الرئيسي ───
    async def main_loop(self):
        self.session = aiohttp.ClientSession()
        try:
            # اللوحة تبدأ أولاً — مهم لفحص صحة Railway
            await start_web_panel(self)
            loop = asyncio.get_running_loop()
            for sig_name in ('SIGINT', 'SIGTERM'):
                try:
                    loop.add_signal_handler(getattr(sys_signal, sig_name),
                        lambda: asyncio.ensure_future(self.request_shutdown('signal')))
                except (NotImplementedError, ValueError, AttributeError, RuntimeError):
                    pass

            await self.db.init_db()
            try:
                await self.triple_db.init()
            except Exception as e:
                logger.error(f"تعذر تهيئة قاعدة بيانات Triple: {e}")

            # تحميل الإعدادات المحفوظة
            try:
                raw = await self.db.get_param('triple_config')
                if raw:
                    saved = json.loads(raw)
                    if int(saved.get('settings_version', 1) or 1) == \
                            int(self.triple.cfg.get('settings_version', 1) or 1):
                        self.triple.cfg.update({k: v for k, v in saved.items() if k in self.triple.cfg})
                        self.triple_exit.cfg.update(self.triple.cfg)
                    else:
                        logger.info("⚙️ triple_config: إعدادات قديمة — اعتماد الجديدة")
                        await self.db.set_param('triple_config', json.dumps(dict(self.triple.cfg)))
            except Exception as e:
                logger.warning(f"تعذر تحميل إعدادات محفوظة (triple_config): {e}")
            self.triple_capacity = int(self.triple.cfg.get('max_open_trades', self.triple_capacity))
            logger.info(f"⚙️ السعة — Triple: {self.triple_capacity} | "
                        f"هامش: {self.TRADE_SIZE_USDT}$ | رافعة: {self.LEVERAGE}x | "
                        f"الحالة: {'مفعّل' if self.triple.cfg.get('enabled') else 'متوقف'}")

            market_tries = 0
            while not self.all_futures_pairs:
                market_tries += 1
                await self.load_market_data()
                if self.all_futures_pairs:
                    break
                if market_tries == 1 or market_tries % 5 == 0:
                    await self.tg(f"❌ *فشل تحميل بيانات السوق* (محاولة {market_tries})\n"
                                  f"⏳ إعادة خلال 30 ثانية...")
                logger.error(f"فشل تحميل بيانات السوق (محاولة {market_tries})")
                await asyncio.sleep(30)

            self.active_trades = await self.db.load_active_trades()
            await self.sync_server_time()

            try:
                dual = await self._fapi_request('GET', '/fapi/v1/positionSide/dual', signed=True)
                if dual and dual.get('dualSidePosition'):
                    if self.TRADE_ENABLED:
                        self.TRADE_ENABLED = False
                        logger.error("❌ الحساب في وضع Hedge Mode — التحويل للمراقبة فقط")
                        await self.tg("⚠️ *تنبيه:* Hedge Mode مفعّل — البوت لوضع One-way فقط.")
            except Exception as e:
                logger.warning(f"تعذر فحص Hedge Mode: {e}")

            if self.TRADE_ENABLED and self.binance_api_key:
                try:
                    await self._fapi_request('GET', '/fapi/v2/account', signed=True)
                    logger.info("✅ مفاتيح API صالحة")
                except Exception as e:
                    logger.warning(f"تعذر التحقق من المفاتيح: {e}")

            # إشعار بدء التشغيل
            await self.tg("🧭 *Triple Confluence Navigator جاهز*\n"
                          f"⚙️ هامش {self.TRADE_SIZE_USDT}$ | رافعة {self.LEVERAGE}x | "
                          f"متزامنة {self.triple_capacity}\n"
                          f"📊 الأزواج المحملة: {len(self.all_futures_pairs)}\n"
                          f"📡 التداول: {'مفعّل' if self.TRADE_ENABLED else 'متوقف (مراقبة فقط)'}")

            # اللوب الرئيسي
            last_scan = 0
            last_price_update = 0
            last_manage = 0
            while not self.shutdown_requested:
                now = time.time()
                # تحديث الأسعار كل 5 ثوانٍ
                if now - last_price_update > 5:
                    await self._update_prices_ws()
                    last_price_update = now
                # إدارة الصفقات كل 3 ثوانٍ
                if now - last_manage > 3:
                    await self.manage_positions()
                    last_manage = now
                # المسح كل 60 ثانية
                if now - last_scan > 60:
                    await self.scan_volatile_coins()
                    last_scan = now
                await asyncio.sleep(1)

            # إيقاف نظيف
            logger.info("🛑 إيقاف البوت...")
            if self.active_trades:
                logger.info(f"⏳ {len(self.active_trades)} صفقة مفتوحة — تبقى محفوظة في القاعدة")
        finally:
            if self.session and not self.session.closed:
                await self.session.close()

    def start(self):
        asyncio.run(self.main_loop())


if __name__ == "__main__":
    bot = TripleConfluenceBot()
    bot.start()
