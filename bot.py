"""
═══════════════════════════════════════════════════════════════════════
  🤖 البوت — استراتيجية Triple Confluence Navigator (MarkitTick)
═══════════════════════════════════════════════════════════════════════
  هذا المحرك يطبّق حرفياً استراتيجية «Triple Confluence Navigator»
  المأخوذة من مؤشر TradingView © MarkitTick. وهي تجمع ثلاثة أنظمة:
    1) ⚙️ Cardwell RSI (RSI + RMA/Kalman/LLAMA على RSI)
    2) 🌊 Adaptive RSI (ARSI + Supertrend على ARSI)
    3) 🧱 Structure (Pivot BOS/CHoCH على آخر سوينق)
  تتأكد الإشارة فقط عند «تزامن» الثلاثة ضمن نافذة Confluence Window.
  وتحسب أهدافاً ثلاثة (TP1/TP2/TP3) من مضاعفات مسافة الوقف R:R.

  · دخول: طالما تحقق التزامن على آخر شمعة مكتملة (المسح على إطار 1h).
  · خروج: ضرب الوقف أو تحقق TP1، أو خروج زمني احترازي.
  · سعة قابلة للضبط (افتراضياً صفقة واحدة فقط متزامنة).

  · اللوحة: ملف web_panel.py واحد — /confluence (+ /health)
  · التشغيل: python bot.py (المتغيرات عبر البيئة:
      BINANCE_API_KEY, BINANCE_API_SECRET, TRADE_ENABLED, TESTNET,
      TRADE_SIZE_USDT, MAX_OPEN_TRADES,
      DB_FILE / DATA_DIR, PORT)
═══════════════════════════════════════════════════════════════════════
"""

import asyncio, aiohttp, json, math, os, sys, time, logging
import collections
import pandas as pd
import numpy as np
import hmac
import hashlib
import signal
from decimal import Decimal
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
fh = RotatingFileHandler(os.path.join('/tmp', 'bot.log'), maxBytes=2*1024*1024, backupCount=2, encoding='utf-8')
fh.setFormatter(fmt); logger.addHandler(fh)
ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)

import requests

def check_outbound_ip():
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=10)
        ip_address = response.json().get('ip')
        print(f"Current Outbound IP on Railway: {ip_address}")
        return ip_address
    except Exception as e:
        print(f"Failed to fetch IP: {e}")
        return None

if __name__ == "__main__":
    check_outbound_ip()

# ═══ مسار قاعدة البيانات — قرار واحد مضبوط عند الإقلاع (لا انهيار أبداً) ═══
RAILWAY_VOLUME_PATH = "/var/lib/containers/railwayapp/bind-mounts/54c0d898-c1f2-44de-a173-2a711d30a4bc/vol_w3cxm5jyou5b58yw"
BASE_DIR = "/tmp"


def resolve_db_path():
    """يختار أول مسار قابل للاستخدام فعلاً لقاعدة البيانات بالترتيب:
    1) DB_FILE من البيئة (تجاوز صريح — أعلى أولوية)
    2) DATA_DIR من البيئة (مجلد بيانات، يُضاف إليه triple_v1.db)
    3) قرص Railway الدائم (Volume Mount) — المسار المعتمد للنشر الحالي
    4) /tmp/triple_v1.db (مضمون الكتابة دائماً — احتياط أخير)"""
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
    """قاعدة مشتركة: الصفقات الحية + أرباح الأيام + سجل الإشارات + الإعدادات."""

    def __init__(self, db_name=None):
        self.db_name = db_name or db_file

    async def init_db(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS active_trades (
                symbol TEXT PRIMARY KEY, side TEXT, entry_price REAL, quantity REAL,
                sl REAL, tp REAL, tp2 REAL, tp3 REAL, trailing_active INTEGER, highest_price REAL,
                lowest_price REAL, entry_time REAL, partial_closed INTEGER,
                realized_pnl REAL DEFAULT 0, stop_order_id INTEGER, tp_order_id INTEGER)""")
            for col_def in ('realized_pnl REAL DEFAULT 0', 'stop_order_id INTEGER',
                            'tp2 REAL', 'tp3 REAL', 'tp_order_id INTEGER',
                            'strategy TEXT', 'margin_usdt REAL DEFAULT 0',
                            'atr_ref REAL DEFAULT 0'):
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
            await db.execute("INSERT OR REPLACE INTO bot_params (key, value) VALUES (?,?)", (key, value))
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
                                close_ts=None, close_reason=None, duration_s=None, pnl_pct=None):
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
                 highest_price, lowest_price, entry_time, partial_closed,
                 realized_pnl, stop_order_id, tp_order_id, strategy, margin_usdt, atr_ref)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t['symbol'], t['side'], t['entry_price'], t['quantity'],
                 t['sl'], t['tp'], t.get('tp2'), t.get('tp3'),
                 int(t.get('trailing_active', False)),
                 t.get('highest_price', 0), t.get('lowest_price', 999999),
                 t['entry_time'], int(t.get('partial_closed', 0)),
                 float(t.get('realized_pnl', 0.0)), t.get('stop_order_id'), t.get('tp_order_id'),
                 t.get('strategy') or '', float(t.get('margin_usdt') or 0),
                 float(t.get('atr_ref') or 0)))
            await db.commit()

    async def load_active_trades(self):
        trades = {}
        try:
            async with aiosqlite.connect(self.db_name) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""SELECT symbol, side, entry_price, quantity, sl, tp, tp2, tp3,
                    trailing_active, highest_price, lowest_price, entry_time, partial_closed,
                    realized_pnl, stop_order_id, tp_order_id, strategy, margin_usdt, atr_ref
                    FROM active_trades""") as cur:
                    async for row in cur:
                        trades[row['symbol']] = {
                            'symbol': row['symbol'], 'side': row['side'],
                            'entry_price': row['entry_price'], 'quantity': row['quantity'],
                            'sl': row['sl'], 'tp': row['tp'], 'tp2': row['tp2'],
                            'tp3': row['tp3'],
                            'trailing_active': bool(row['trailing_active']),
                            'highest_price': row['highest_price'],
                            'lowest_price': row['lowest_price'],
                            'entry_time': row['entry_time'],
                            'partial_closed': bool(row['partial_closed']),
                            'realized_pnl': row['realized_pnl'] if row['realized_pnl'] is not None else 0.0,
                            'stop_order_id': row['stop_order_id'],
                            'tp_order_id': row['tp_order_id'],
                            'strategy': row['strategy'] or None,
                            'margin_usdt': row['margin_usdt'] or 0,
                            'atr_ref': row['atr_ref'] or 0,
                            'stop_synced_price': None, 'tp_skipped': False
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
                    (row[1]+pnl, row[2]+w, row[3]+l, today))
            else:
                await db.execute("INSERT INTO daily_stats VALUES (?,?,?,?)", (today, pnl, w, l))
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
#  🧭 الاستراتيجية: Triple Confluence Navigator (MarkitTick)
#     مطبّقة حرفياً من مؤشر TradingView — ثلاث منظومات:
#        Cardwell RSI + Adaptive ARSI + Structure Break
#        إشارة الدخول = تزامن الثلاثة ضمن نافذة Confluence Window
#        الوقف من min(structure, ATR×Mult) | أهداف TP1/TP2/TP3 بـ R:R 1/2/3
# ══════════════════════════════════════════════════════════════════════
TRIPLE_CONFIG = {
    "name": "TRIPLE",
    "label": "Triple Confluence",
    "enabled": True,
    "settings_version": 1,
    # ── Cardwell RSI ──
    "rsiLen": 14,
    "rmaFastLen": 9,
    "rmaSlowLen": 45,
    "maType": "Kalman",          # RMA | Kalman | LLAMA
    # ── Adaptive RSI (ARSI) + Supertrend ──
    "arsiLen": 14,
    "arsiSigLen": 14,
    "arsiFiltType": "Kalman Filter",  # RMA | Kalman Filter | LLAMA
    "kalmanProcN": 0.01,
    "kalmanMeasN": 0.10,
    "llamaAlphaMin": 0.05,
    "llamaAlphaMax": 0.50,
    "stAtrLen": 14,
    "stFactor": 2.0,
    # ── Structure (Pivot / BOS) ──
    "swingLen": 5,
    # ── Trade Setup ──
    "atrLen": 14,
    "atrMultSl": 1.5,
    "slStructBuf": 0.25,          # ATR buffer under/above swing
    "rr1": 1.0, "rr2": 2.0, "rr3": 3.0,
    "confluenceWindow": 5,       # كم شمعة بين الإشارات الثلاث
    # ── فلاتر اختيارية (معطّلة افتراضياً كما في المؤشر) ──
    "useHtfFilter": False,
    "htfTimeframe": "240",       # 4h
    "useChopFilter": False,
    "adxLen": 14,
    "adxMinLevel": 20.0,
    # ── خروج ──
    "max_hold_minutes": 7 * 24 * 60,
    # ── السعة والتبريد ──
    "max_open_trades": 1,
    "reentry_cooldown": 3600,
    "scan_limit": 60,
    # ── الفلتر العام للإشارات الضعيفة ──
    "max_risk_pct": 8.0,
}


# ── أدوات المؤشرات (مطابقة لـ Pine Script حرفياً) ──

def _arr(df, col):
    return df[col].to_numpy(dtype=float)


def rma(a, n):
    """RMA = Wilder's moving average (EMA with alpha=1/n)."""
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    if len(a) < n:
        return out
    out[n - 1] = np.nanmean(a[:n])
    for i in range(n, len(a)):
        out[i] = out[i - 1] + (a[i] - out[i - 1]) / n
    return out


def rsi_wilder(close, n):
    """RSI بطريقة Wilder (RMA) — مطابق للمنصات."""
    close = np.asarray(close, dtype=float)
    d = np.diff(close, prepend=close[:1])
    up = np.where(d > 0, d, 0.0)
    dn = np.where(d < 0, -d, 0.0)
    au = pd.Series(up).ewm(alpha=1 / n, adjust=False, min_periods=n).mean().to_numpy()
    ad = pd.Series(dn).ewm(alpha=1 / n, adjust=False, min_periods=n).mean().to_numpy()
    rs = au / np.where(ad == 0, np.nan, ad)
    out = 100 - 100 / (1 + rs)
    return np.where(ad == 0, 100.0, np.where(au == 0, 0.0, out))


def atr_wilder(h, l, c, n=14):
    h = np.asarray(h, dtype=float)
    l = np.asarray(l, dtype=float)
    c = np.asarray(c, dtype=float)
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    return pd.Series(tr).ewm(alpha=1 / n, adjust=False, min_periods=n).mean().to_numpy()


def kalman_filter(src, procN, measN):
    """Kalman filter مطابق لـ f_kalman في Pine Script.

    في Pine: var float _est = na، ثم أول قيمة src صالحة تصبح البذرة
    (if na(_est): _est := src). القيم NaN قبل البذرة تبقى NaN في الخرج.
    """
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) == 0:
        return out
    cov = 1.0
    est = np.nan
    for i in range(len(src)):
        if np.isnan(est):
            if not np.isnan(src[i]):
                est = src[i]
                cov = 1.0
            out[i] = est
            continue
        # إذا صادفنا NaN (نادر بعد التسخين) نبقى على آخر تقدير
        if np.isnan(src[i]):
            out[i] = est
            continue
        cov = cov + procN
        gain = cov / (cov + measN)
        est = est + gain * (src[i] - est)
        cov = (1 - gain) * cov
        out[i] = est
    return out


def linreg(series, length, offset=0):
    """Linear regression curve — مطابق لـ ta.linreg في Pine."""
    s = pd.Series(series).astype(float)
    x = np.arange(length)
    x_mean = x.mean()
    out = np.full(len(series), np.nan)
    if len(series) < length:
        return out
    denom = ((x - x_mean) ** 2).sum()
    for i in range(length - 1, len(series)):
        y = s.iloc[i - length + 1: i + 1].to_numpy()
        if np.isnan(y).any():
            continue
        y_mean = y.mean()
        slope = ((x - x_mean) * (y - y_mean)).sum() / denom
        intercept = y_mean - slope * x_mean
        out[i] = intercept + slope * (length - 1 - offset)
    return out


def llama(src, length, aMin, aMax):
    """LLAMA filter — مطابق لـ f_llama في Pine Script.

    في Pine: var float _out = na، ثم أول قيمة src صالحة تصبح البذرة.
    """
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) == 0:
        return out
    est = np.nan
    for i in range(len(src)):
        if np.isnan(est):
            if not np.isnan(src[i]):
                est = src[i]
            out[i] = est
            continue
        if np.isnan(src[i]):
            out[i] = est
            continue
        prev = i - length
        if prev < 0 or np.isnan(src[prev]):
            # النافذة غير مكتملة بعد — نحاكي سلوك Pine التقريبي بتحديث بسيط
            est = est + aMin * (src[i] - est)
            out[i] = est
            continue
        slope = (src[i] - src[prev]) / length
        window = src[prev:i + 1]
        rng = np.max(window) - np.min(window)
        eff = np.min(np.abs(slope * length) / rng, 1.0) if rng != 0 else 0.0
        alpha = aMin + eff * (aMax - aMin)
        est = est + alpha * (src[i] - est)
        out[i] = est
    return out


def arsi_compute(close, length):
    """ARSI — مطابق لـ f_arsi في Pine."""
    close = np.asarray(close, dtype=float)
    u = pd.Series(close).rolling(length, min_periods=length).max().to_numpy()
    l = pd.Series(close).rolling(length, min_periods=length).min().to_numpy()
    r = u - l
    diff = np.zeros_like(close)
    diff[1:] = close[1:] - close[:-1]
    out = np.full_like(close, np.nan)
    if len(close) < length + 1:
        return out
    _d = np.zeros_like(close)
    for i in range(1, len(close)):
        if not np.isnan(u[i]) and not np.isnan(u[i - 1]) and u[i] > u[i - 1]:
            _d[i] = r[i]
        elif not np.isnan(l[i]) and not np.isnan(l[i - 1]) and l[i] < l[i - 1]:
            _d[i] = -r[i]
        else:
            _d[i] = diff[i]
    num = rma(_d, length)
    den = rma(np.abs(_d), length)
    for i in range(len(close)):
        if not np.isnan(den[i]) and den[i] != 0:
            out[i] = num[i] / den[i] * 50 + 50
        else:
            out[i] = 50.0
    return out


def arsi_filter(src, length, mode, procN, measN, aMin, aMax):
    """فلاتر ARSI: RMA | Kalman | LLAMA — مطابق لـ f_arsiFilter."""
    if mode == "RMA":
        return rma(src, length)
    if mode == "Kalman Filter":
        return kalman_filter(src, procN, measN)
    return llama(src, length, aMin, aMax)


def adx_compute(h, l, c, length):
    """ADX/DMI (Wilder) — مطابق لـ ta.dmi في Pine."""
    h = np.asarray(h, dtype=float)
    l = np.asarray(l, dtype=float)
    c = np.asarray(c, dtype=float)
    ph = np.roll(h, 1); ph[0] = h[0]
    pl = np.roll(l, 1); pl[0] = l[0]
    up = h - ph
    dn = pl - l
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr = np.maximum(h - l, np.maximum(np.abs(h - pl), np.abs(l - ph)))
    atr_ = pd.Series(tr).ewm(alpha=1 / length, adjust=False, min_periods=length).mean().to_numpy()
    plus_di_raw = pd.Series(plus_dm).ewm(alpha=1 / length, adjust=False, min_periods=length).mean().to_numpy()
    minus_di_raw = pd.Series(minus_dm).ewm(alpha=1 / length, adjust=False, min_periods=length).mean().to_numpy()
    plus_di = np.where(atr_ != 0, 100 * plus_di_raw / np.where(atr_ == 0, 1, atr_), 0)
    minus_di = np.where(atr_ != 0, 100 * minus_di_raw / np.where(atr_ == 0, 1, atr_), 0)
    dx = np.where((plus_di + minus_di) != 0,
                  np.abs(plus_di - minus_di) / np.where((plus_di + minus_di) == 0, 1, plus_di + minus_di) * 100,
                  0)
    adx = pd.Series(dx).ewm(alpha=1 / length, adjust=False, min_periods=length).mean().to_numpy()
    return plus_di, minus_di, adx


def _pivot_high(high, length):
    """مطابق لـ f_pivotHigh في Pine.

    في Pine، تُستدعى الدالة على كل شمعة (المؤشر 0 = الشمعة الحالية)، فتعيد
    قيمة high[length] (أي شمعة length قبل الحالية) إن كانت قمة محلية.
    عند تطبيقها على مصفوفة numpy، الشمعة الحالية هي الموضع i، فالـ pivot
    يقع عند الموضع i-length، والشمعات الحديثة بعده هي i-length+1..i (length شمعة)
    والشمعات الأقدم هي i-2*length..i-length-1 (length شمعة).
    الناتج يُكتب عند الموضع i (لحظة تأكيد الـ pivot)، تماماً كـ Pine.
    """
    high = np.asarray(high, dtype=float)
    n = len(high)
    out = np.full_like(high, np.nan)
    for i in range(2 * length, n):
        c_idx = i - length
        c = high[c_idx]
        ok = True
        # الشمعات الحديثة (length شمعة بعد القمة): يجب أن تكون أصغر صراحةً (<)
        for j in range(length):
            if high[c_idx + 1 + j] >= c:
                ok = False
                break
        if not ok:
            continue
        # الشمعات الأقدم (length شمعة قبل القمة): يجب أن تكون أصغر أو مساوية (<=)
        for j in range(length):
            if high[c_idx - 1 - j] > c:
                ok = False
                break
        if ok:
            out[i] = c
    return out


def _pivot_low(low, length):
    """مطابق لـ f_pivotLow في Pine."""
    low = np.asarray(low, dtype=float)
    n = len(low)
    out = np.full_like(low, np.nan)
    for i in range(2 * length, n):
        c_idx = i - length
        c = low[c_idx]
        ok = True
        for j in range(length):
            if low[c_idx + 1 + j] <= c:
                ok = False
                break
        if not ok:
            continue
        for j in range(length):
            if low[c_idx - 1 - j] < c:
                ok = False
                break
        if ok:
            out[i] = c
    return out


def _closed(df):
    """الشموع المغلقة فقط (يُستبعد آخر شمعة قيد التكوين)."""
    if df is None or len(df) < 60:
        return None
    return df.iloc[:-1].copy()


def triple_compute_signal(df1h, cfg=None):
    """يُعيد dict للتحليل إذا تحققت شروط الدخول على آخر شمعة مكتملة، وإلا None.

    المنطق مطابق للمؤشر: عند تحقق bullConfluence (أو bearConfluence) جديد
    على آخر شمعة مغلقة، تُولَّد إشارة دخول طويلة (أو قصيرة) مع وقف
    وأهداف TP1/TP2/TP3 محسوبة من مسافة الوقف.
    """
    cfg = dict(TRIPLE_CONFIG if cfg is None else cfg)
    h1 = _closed(df1h)
    if h1 is None or len(h1) < 220:
        return None

    c1, o1, hh1, ll1 = _arr(h1, "close"), _arr(h1, "open"), _arr(h1, "high"), _arr(h1, "low")
    n = len(c1)

    # ── 1) Cardwell RSI على إطار 1h ──
    rsiValue = rsi_wilder(c1, int(cfg["rsiLen"]))

    # Kalman filter على RSI للحصول على Fast/Slow كما في المؤشر.
    # في Pine: var float _kfFastX = na يُهيّأ فارغاً، ثم أول قيمة RSI صالحة تُصبح البذرة.
    # نطبّق نفس السلوك: نبدأ من أول موضع RSI صالح (غير NaN) ونترك ما قبله NaN.
    _kfFastX = np.full_like(c1, np.nan)
    _kfFastP = np.full_like(c1, 1.0)
    _kfSlowX = np.full_like(c1, np.nan)
    _kfSlowP = np.full_like(c1, 1.0)
    _kfQ = 0.001
    _kfR = 0.01
    # إيجاد أول قيمة RSI صالحة كبذرة (مطابق لـ nz(_kfFastX, rsiValue) في Pine)
    first_valid = -1
    for i in range(n):
        if not np.isnan(rsiValue[i]):
            first_valid = i
            break
    if first_valid >= 0:
        _kfFastX[first_valid] = rsiValue[first_valid]
        _kfSlowX[first_valid] = rsiValue[first_valid]
        for i in range(first_valid + 1, n):
            # إذا صادفنا NaN (لا يحدث عادةً بعد التسخين) نتجاهل التحديث
            if np.isnan(rsiValue[i]) or np.isnan(_kfFastX[i - 1]):
                if not np.isnan(rsiValue[i]):
                    _kfFastX[i] = rsiValue[i]
                    _kfSlowX[i] = rsiValue[i]
                else:
                    _kfFastX[i] = _kfFastX[i - 1]
                    _kfSlowX[i] = _kfSlowX[i - 1]
                continue
            k = _kfFastP[i - 1] / (_kfFastP[i - 1] + _kfR)
            _kfFastX[i] = _kfFastX[i - 1] + k * (rsiValue[i] - _kfFastX[i - 1])
            _kfFastP[i] = (1.0 - k) * _kfFastP[i - 1] + _kfQ
        slow_factor = float(cfg["rmaSlowLen"]) / float(cfg["rmaFastLen"])
        for i in range(first_valid + 1, n):
            if np.isnan(rsiValue[i]) or np.isnan(_kfSlowX[i - 1]):
                if not np.isnan(rsiValue[i]):
                    _kfSlowX[i] = rsiValue[i]
                else:
                    _kfSlowX[i] = _kfSlowX[i - 1]
                continue
            k = _kfSlowP[i - 1] / (_kfSlowP[i - 1] + _kfR * slow_factor)
            _kfSlowX[i] = _kfSlowX[i - 1] + k * (rsiValue[i] - _kfSlowX[i - 1])
            _kfSlowP[i] = (1.0 - k) * _kfSlowP[i - 1] + _kfQ

    if str(cfg["maType"]) == "Kalman":
        rmaFast = _kfFastX
        rmaSlow = _kfSlowX
    elif str(cfg["maType"]) == "LLAMA":
        rmaFast = linreg(rsiValue, int(cfg["rmaFastLen"]), 0)
        rmaSlow = linreg(rsiValue, int(cfg["rmaSlowLen"]), 0)
    else:  # RMA
        rmaFast = rma(rsiValue, int(cfg["rmaFastLen"]))
        rmaSlow = rma(rsiValue, int(cfg["rmaSlowLen"]))

    rawBullCross = np.zeros(n, dtype=bool)
    rawBearCross = np.zeros(n, dtype=bool)
    for i in range(2, n):
        if np.isnan(rmaFast[i - 1]) or np.isnan(rmaSlow[i - 1]) or \
           np.isnan(rmaFast[i - 2]) or np.isnan(rmaSlow[i - 2]):
            continue
        rawBullCross[i] = (rmaFast[i - 1] > rmaSlow[i - 1]) and (rmaFast[i - 2] <= rmaSlow[i - 2])
        rawBearCross[i] = (rmaFast[i - 1] < rmaSlow[i - 1]) and (rmaFast[i - 2] >= rmaSlow[i - 2])

    # ── ADX filter (اختياري) ──
    _, _, adxValue = adx_compute(hh1, ll1, c1, int(cfg["adxLen"]))
    chopOk = np.ones(n, dtype=bool)
    if bool(cfg["useChopFilter"]):
        for i in range(n):
            if i - 1 >= 0 and not np.isnan(adxValue[i - 1]):
                chopOk[i] = adxValue[i - 1] >= float(cfg["adxMinLevel"])
            else:
                chopOk[i] = False

    bullSignal1 = rawBullCross & chopOk
    bearSignal1 = rawBearCross & chopOk

    # ── 2) Adaptive ARSI + Supertrend على ARSI ──
    arsi = arsi_compute(c1, int(cfg["arsiLen"]))
    arsiSignal = arsi_filter(arsi, int(cfg["arsiSigLen"]), str(cfg["arsiFiltType"]),
                             float(cfg["kalmanProcN"]), float(cfg["kalmanMeasN"]),
                             float(cfg["llamaAlphaMin"]), float(cfg["llamaAlphaMax"]))
    arsi_diff = np.zeros_like(c1)
    arsi_diff[1:] = np.abs(arsi[1:] - arsi[:-1])
    arsi_diff[0] = np.nan
    arsiAtr = rma(arsi_diff, int(cfg["stAtrLen"]))
    upperBand = arsi + float(cfg["stFactor"]) * arsiAtr
    lowerBand = arsi - float(cfg["stFactor"]) * arsiAtr

    supertrend = np.full_like(c1, np.nan)
    trendUp = np.ones(n, dtype=bool)
    if not np.isnan(upperBand[0]):
        supertrend[0] = upperBand[0]
    for i in range(1, n):
        if np.isnan(supertrend[i - 1]) or np.isnan(arsi[i]):
            if not np.isnan(upperBand[i]):
                supertrend[i] = upperBand[i]
                trendUp[i] = True
            continue
        if arsi[i] > supertrend[i - 1]:
            trendUp[i] = True
        elif arsi[i] < supertrend[i - 1]:
            trendUp[i] = False
        else:
            trendUp[i] = trendUp[i - 1]
        if trendUp[i]:
            supertrend[i] = max(lowerBand[i], supertrend[i - 1]) if not np.isnan(lowerBand[i]) else supertrend[i - 1]
        else:
            supertrend[i] = min(upperBand[i], supertrend[i - 1]) if not np.isnan(upperBand[i]) else supertrend[i - 1]

    rawMerged = np.where(np.isnan(supertrend), arsiSignal, (arsiSignal + supertrend) / 2.0)

    rawBullCross2 = np.zeros(n, dtype=bool)
    rawBearCross2 = np.zeros(n, dtype=bool)
    for i in range(2, n):
        a0, a1 = arsi[i - 1], arsi[i - 2]
        m0, m1 = rawMerged[i - 1], rawMerged[i - 2]
        if np.isnan(a0) or np.isnan(a1) or np.isnan(m0) or np.isnan(m1):
            continue
        rawBullCross2[i] = (a0 > m0) and (a1 <= m1)
        rawBearCross2[i] = (a0 < m0) and (a1 >= m1)

    bullSignal2 = rawBullCross2.copy()
    bearSignal2 = rawBearCross2.copy()

    # ── 3) Structure (Pivot + BOS) ──
    ph = _pivot_high(hh1, int(cfg["swingLen"]))
    pl = _pivot_low(ll1, int(cfg["swingLen"]))

    lastSwingHigh = np.full_like(c1, np.nan)
    lastSwingLow = np.full_like(c1, np.nan)
    lastSwingHighBar = np.full(n, -1, dtype=int)
    lastSwingLowBar = np.full(n, -1, dtype=int)
    lastProcessedHighBar = np.full(n, -1, dtype=int)
    lastProcessedLowBar = np.full(n, -1, dtype=int)
    trend3 = np.zeros(n, dtype=int)

    cur_high = np.nan
    cur_high_bar = -1
    cur_low = np.nan
    cur_low_bar = -1
    cur_proc_high = -1
    cur_proc_low = -1
    cur_trend = 0
    swing = int(cfg["swingLen"])

    for i in range(n):
        # تحديث آخر سوينق عند ظهور pivot مؤكد على الشمعة الحالية i.
        # الـ pivot نفسه يقع فعلياً عند الموضع i-swing (قبل swing شمعة)،
        # لكنه لا يُؤكَّد إلا عند الشمعة i (يحتاج swing شمعة بعده للتأكيد).
        if not np.isnan(ph[i]):
            cur_high = ph[i]
            cur_high_bar = i - 2 * swing
        if not np.isnan(pl[i]):
            cur_low = pl[i]
            cur_low_bar = i - 2 * swing
        lastSwingHigh[i] = cur_high if not np.isnan(cur_high) else np.nan
        lastSwingLow[i] = cur_low if not np.isnan(cur_low) else np.nan
        lastSwingHighBar[i] = cur_high_bar
        lastSwingLowBar[i] = cur_low_bar
        lastProcessedHighBar[i] = cur_proc_high
        lastProcessedLowBar[i] = cur_proc_low
        trend3[i] = cur_trend

        # معالجة الاختراق على الشمعة الحالية
        if i >= 1 and not np.isnan(cur_high):
            if c1[i] > cur_high and \
               (cur_proc_high == -1 or cur_high_bar != cur_proc_high):
                cur_proc_high = cur_high_bar
                cur_trend = 1
        if i >= 1 and not np.isnan(cur_low):
            if c1[i] < cur_low and \
               (cur_proc_low == -1 or cur_low_bar != cur_proc_low):
                cur_proc_low = cur_low_bar
                cur_trend = -1

    bullSignal3 = np.zeros(n, dtype=bool)
    bearSignal3 = np.zeros(n, dtype=bool)
    for i in range(n):
        if i < 1:
            continue
        if not np.isnan(lastSwingHigh[i]) and c1[i] > lastSwingHigh[i] and \
           (lastProcessedHighBar[i] == -1 or lastSwingHighBar[i] != lastProcessedHighBar[i]):
            bullSignal3[i] = True
        if not np.isnan(lastSwingLow[i]) and c1[i] < lastSwingLow[i] and \
           (lastProcessedLowBar[i] == -1 or lastSwingLowBar[i] != lastProcessedLowBar[i]):
            bearSignal3[i] = True

    # ── تتبع عدد الشموع منذ كل إشارة ──
    barsSinceBull1 = np.full(n, 9999, dtype=int)
    barsSinceBear1 = np.full(n, 9999, dtype=int)
    barsSinceBull2 = np.full(n, 9999, dtype=int)
    barsSinceBear2 = np.full(n, 9999, dtype=int)
    barsSinceBull3 = np.full(n, 9999, dtype=int)
    barsSinceBear3 = np.full(n, 9999, dtype=int)
    b1, br1, b2, br2, b3, br3 = 9999, 9999, 9999, 9999, 9999, 9999
    for i in range(n):
        b1 = 0 if bullSignal1[i] else b1 + 1
        br1 = 0 if bearSignal1[i] else br1 + 1
        b2 = 0 if bullSignal2[i] else b2 + 1
        br2 = 0 if bearSignal2[i] else br2 + 1
        b3 = 0 if bullSignal3[i] else b3 + 1
        br3 = 0 if bearSignal3[i] else br3 + 1
        barsSinceBull1[i] = min(b1, 9999)
        barsSinceBear1[i] = min(br1, 9999)
        barsSinceBull2[i] = min(b2, 9999)
        barsSinceBear2[i] = min(br2, 9999)
        barsSinceBull3[i] = min(b3, 9999)
        barsSinceBear3[i] = min(br3, 9999)

    cw = int(cfg["confluenceWindow"])
    bullConfluence = (barsSinceBull1 <= cw) & (barsSinceBull2 <= cw) & (barsSinceBull3 <= cw)
    bearConfluence = (barsSinceBear1 <= cw) & (barsSinceBear2 <= cw) & (barsSinceBear3 <= cw)

    longSignal = np.zeros(n, dtype=bool)
    shortSignal = np.zeros(n, dtype=bool)
    for i in range(1, n):
        longSignal[i] = bool(bullConfluence[i]) and not bool(bullConfluence[i - 1])
        shortSignal[i] = bool(bearConfluence[i]) and not bool(bearConfluence[i - 1])

    i = n - 1
    if not longSignal[i] and not shortSignal[i]:
        return None

    # ── حساب الوقف والأهداف (مطابق للمؤشر) ──
    entry_price = float(c1[i - 1])
    atr_series = atr_wilder(hh1, ll1, c1, int(cfg["atrLen"]))
    atr_value1 = atr_series[i - 1] if not np.isnan(atr_series[i - 1]) else 0.0
    if atr_value1 <= 0:
        return None

    atr_mult = float(cfg["atrMultSl"])
    struct_buf = float(cfg["slStructBuf"])
    atr_cap_dist = atr_value1 * atr_mult
    last_sw = lastSwingLow[i] if longSignal[i] else lastSwingHigh[i]

    if longSignal[i]:
        if not np.isnan(last_sw):
            struct_dist = entry_price - (last_sw - atr_value1 * struct_buf)
        else:
            struct_dist = np.nan
        risk_dist = atr_cap_dist if (np.isnan(struct_dist) or struct_dist <= 0) \
            else min(struct_dist, atr_cap_dist)
        sl_price = entry_price - risk_dist
        tp1_price = entry_price + risk_dist * float(cfg["rr1"])
        tp2_price = entry_price + risk_dist * float(cfg["rr2"])
        tp3_price = entry_price + risk_dist * float(cfg["rr3"])
        side = "BUY"
    else:
        if not np.isnan(last_sw):
            struct_dist = (last_sw + atr_value1 * struct_buf) - entry_price
        else:
            struct_dist = np.nan
        risk_dist = atr_cap_dist if (np.isnan(struct_dist) or struct_dist <= 0) \
            else min(struct_dist, atr_cap_dist)
        sl_price = entry_price + risk_dist
        tp1_price = entry_price - risk_dist * float(cfg["rr1"])
        tp2_price = entry_price - risk_dist * float(cfg["rr2"])
        tp3_price = entry_price - risk_dist * float(cfg["rr3"])
        side = "SELL"

    if risk_dist <= 0:
        return None
    risk_pct = risk_dist / entry_price * 100.0
    if risk_pct > float(cfg["max_risk_pct"]):
        return None

    return {
        "side": side,
        "price_ref": entry_price,
        "atr": float(atr_value1),
        "atr_pct": round(atr_value1 / entry_price * 100.0, 2),
        "risk": float(risk_dist),
        "risk_pct": round(risk_pct, 2),
        "sl": float(sl_price),
        "tp1": float(tp1_price),
        "tp2": float(tp2_price),
        "tp3": float(tp3_price),
        "swing": float(last_sw) if not np.isnan(last_sw) else None,
        "rr1": float(cfg["rr1"]),
        "rr2": float(cfg["rr2"]),
        "rr3": float(cfg["rr3"]),
        "hour_open_ts": int(h1["time"].iloc[-1]),
        "hour_close_ts": int(h1["close_time"].iloc[-1]) if "close_time" in h1 else None,
    }


class TripleStrategy:
    """واجهة المحرك: analyze() تُعيد تحليلاً بصيغة التنفيذ نفسها."""

    def __init__(self, cfg=None, logger=None):
        self.cfg = dict(TRIPLE_CONFIG)
        if cfg:
            self.cfg.update(cfg)
        self.log = logger or (lambda *a, **k: None)
        self.last_scan = {}
        self.stats = {"scanned": 0, "signals": 0, "errors": 0}

    async def analyze(self, bot, symbol, now=None):
        try:
            k1h = await bot.get_klines(symbol, "1h", 260)
            if k1h is None:
                return None
            sig = triple_compute_signal(k1h, self.cfg)
            if not sig:
                return None

            px = float(sig["price_ref"])
            side = sig["side"]

            return {
                "symbol": symbol, "direction": side, "price": px,
                "sl": float(sig["sl"]), "tp1": float(sig["tp1"]),
                "tp2": float(sig["tp2"]), "tp3": float(sig["tp3"]),
                "score": 9, "strategy": "TRIPLE",
                "atr_ref": float(sig["atr"]),
                "components": {
                    "strategy": "TRIPLE",
                    "trigger": "Triple Confluence (Cardwell + ARSI + Structure)",
                    "side": side,
                    "atr_pct": sig["atr_pct"],
                    "risk_pct": sig["risk_pct"],
                    "swing": sig.get("swing"),
                    "rr1": sig["rr1"], "rr2": sig["rr2"], "rr3": sig["rr3"],
                },
                "signals_smc": [
                    f"🧭 تزامن ثلاثي (Cardwell RSI + Adaptive ARSI + Structure)",
                    f"🎯 دخول {side} | وقف {sig['risk_pct']}% | TP1={sig['rr1']}R · TP2={sig['rr2']}R · TP3={sig['rr3']}R",
                    f"🛡️ الوقف min(struct, {self.cfg['atrMultSl']}×ATR) | إطار 1h",
                ],
            }
        except Exception as e:
            self.stats["errors"] += 1
            self.log(f"خطأ تحليل Triple لـ {symbol}: {e}")
            return None

    async def scan(self, bot, candidates, active_symbols, cooldowns):
        found, scanned = [], 0
        now = time.time()
        for sym in candidates:
            if len(found) >= 2:
                break
            if sym in active_symbols or cooldowns.get(sym, 0) > now:
                continue
            if scanned >= int(self.cfg["scan_limit"]):
                break
            scanned += 1
            a = await self.analyze(bot, sym, now)
            if a:
                found.append(a)
        self.stats["scanned"] = scanned
        self.stats["signals"] = len(found)
        self.last_scan = {"ts": now, "scanned": scanned, "signals": len(found)}
        return found


class TripleExitManager:
    """خروج Triple: ضرب الوقف، أو تحقق TP1 (إغلاق كامل عند TP1)، أو خروج زمني.

    المطابق لمنطق المؤشر: الوقف ثابت من لحظة الدخول؛ والأهداف الثلاثة TP1/TP2/TP3
    تشكّل سلّماً تصاعدياً. هنا نُغلق الصفقة كاملة عند تحقق TP1 (الهدف الأساسي).
    """

    def __init__(self, cfg=None, logger=None):
        self.cfg = dict(TRIPLE_CONFIG)
        if cfg:
            self.cfg.update(cfg)
        self.log = logger or (lambda *a, **k: None)

    def decide(self, trade, price, atr_now=None):
        side = trade["side"]
        is_buy = side == "BUY"
        sl = float(trade["sl"])
        tp1 = float(trade.get("tp") or 0)

        # 1) ضرب الوقف
        if (is_buy and price <= sl) or ((not is_buy) and price >= sl):
            return {"action": "close", "reason": "🛑 ضرب الوقف (Triple)"}

        # 2) تحقق TP1 (نغلق كامل المركز عند TP1)
        if tp1 and ((is_buy and price >= tp1) or ((not is_buy) and price <= tp1)):
            return {"action": "close", "reason": f"🎯 تحقق TP1 ({self.cfg['rr1']}R)"}

        # 3) خروج زمني احترازي
        held_min = (time.time() - float(trade.get("entry_time") or time.time())) / 60.0
        if held_min >= float(self.cfg["max_hold_minutes"]):
            return {"action": "close", "reason": "⏳ خروج زمني احترازي (7 أيام)"}

        return {"action": "hold"}


# ✅ مفتاح id ذاتي بدل symbol — كل صفقة مغلقة سجل دائم لا يُمس
TRIPLE_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS triple_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, side TEXT, entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
    atr REAL, entry_ts REAL, exit_ts REAL, pnl REAL, pnl_pct REAL, win INTEGER DEFAULT -1,
    reason TEXT, components TEXT, margin REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_triple_sym_win ON triple_trades(symbol, win);
CREATE INDEX IF NOT EXISTS idx_triple_entry ON triple_trades(entry_ts);
"""

TRIPLE_SCHEMA = TRIPLE_TRADES_DDL + """
CREATE TABLE IF NOT EXISTS triple_daily (
    date TEXT PRIMARY KEY, pnl REAL, wins INTEGER, losses INTEGER
);
"""


async def _migrate_trades_keep_history(db, table, idx_old, new_ddl):
    """ترقية آمنة: id ذاتي بدل symbol للحفاظ على تاريخ كل صفقة."""
    async with db.execute(f"PRAGMA table_info({table})") as cur:
        cols = [r[1] for r in await cur.fetchall()]
    if not cols or 'id' in cols:
        return
    await db.execute(f"DROP INDEX IF EXISTS {idx_old}")
    await db.execute(f"ALTER TABLE {table} RENAME TO {table}_mig")
    await db.executescript(new_ddl)
    await db.execute(f"""INSERT INTO {table}
        (symbol, side, entry, sl, tp1, atr, entry_ts, exit_ts, pnl, pnl_pct,
         win, reason, components, margin)
        SELECT symbol, side, entry, sl, tp1, atr, entry_ts, exit_ts, pnl, pnl_pct,
               win, reason, components, margin FROM {table}_mig""")
    await db.execute(f"DROP TABLE {table}_mig")


class TripleDB:
    TABLE = 'triple_trades'

    def __init__(self, db_name=None):
        self.db_name = db_name or db_file

    async def init(self):
        async with aiosqlite.connect(self.db_name) as db:
            await _migrate_trades_keep_history(db, 'triple_trades', 'idx_triple_entry',
                                               TRIPLE_TRADES_DDL)
            await db.executescript(TRIPLE_SCHEMA)
            try:
                await db.execute("ALTER TABLE triple_trades ADD COLUMN margin REAL DEFAULT 0")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE triple_trades ADD COLUMN tp2 REAL")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE triple_trades ADD COLUMN tp3 REAL")
            except Exception:
                pass
            await db.commit()

    async def open(self, t):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute(f"DELETE FROM {self.TABLE} WHERE symbol=? AND win=-1",
                             (t["symbol"],))
            await db.execute(
                f"""INSERT INTO {self.TABLE}
                   (symbol, side, entry, sl, tp1, tp2, tp3, atr, entry_ts, margin, components)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (t["symbol"], t["side"], t["entry_price"], t["sl"], t.get("tp"),
                 t.get("tp2"), t.get("tp3"),
                 t.get("atr_ref") or 0, t["entry_time"],
                 float(t.get("margin_usdt") or 0),
                 json.dumps(t.get("components", {}), ensure_ascii=False)))
            await db.commit()

    async def close(self, symbol, exit_ts, pnl, pnl_pct, win, reason):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute(
                """UPDATE triple_trades SET exit_ts=?, pnl=?, pnl_pct=?, win=?, reason=?
                   WHERE symbol=? AND win=-1""",
                (exit_ts, pnl, pnl_pct, int(win), str(reason)[:200], symbol))
            day = time.strftime("%Y-%m-%d", time.localtime(exit_ts))
            async with db.execute("SELECT pnl, wins, losses FROM triple_daily WHERE date=?", (day,)) as cur:
                row = await cur.fetchone()
            w, l = (1, 0) if win else (0, 1)
            if row:
                await db.execute("UPDATE triple_daily SET pnl=?, wins=?, losses=? WHERE date=?",
                                 (row[0] + pnl, row[1] + w, row[2] + l, day))
            else:
                await db.execute("INSERT INTO triple_daily VALUES (?,?,?,?)", (day, pnl, w, l))
            await db.commit()

    async def update_sl(self, symbol, sl):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("UPDATE triple_trades SET sl=? WHERE symbol=? AND win=-1", (sl, symbol))
            await db.commit()

    async def open_symbols(self):
        """استعادة صفقات Triple المفتوحة بعد إعادة التشغيل."""
        out = {}
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute(
                    """SELECT symbol, side, entry, sl, tp1, tp2, tp3, atr, entry_ts, margin
                       FROM triple_trades WHERE win=-1""") as cur:
                for r in await cur.fetchall():
                    out[r[0]] = {"symbol": r[0], "side": r[1], "entry": r[2], "sl": r[3],
                                 "tp1": r[4], "tp2": r[5], "tp3": r[6], "atr": r[7],
                                 "entry_ts": r[8], "margin": r[9] or 0}
        return out

    async def summary(self, days=30):
        since = time.time() - days * 86400
        out = {"closed": 0, "wins": 0, "losses": 0, "winrate": 0.0, "net": 0.0,
               "pf": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "trades": [], "daily": []}
        async with aiosqlite.connect(self.db_name) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                    """SELECT COUNT(*) n, SUM(win=1) w, SUM(win=0) l, SUM(pnl) net,
                              AVG(CASE WHEN win=1 THEN pnl END) aw,
                              AVG(CASE WHEN win=0 THEN pnl END) al,
                              SUM(CASE WHEN win=1 THEN pnl ELSE 0 END) g,
                              SUM(CASE WHEN win=0 THEN -pnl ELSE 0 END) ls
                       FROM triple_trades WHERE win>=0 AND exit_ts>=?""", (since,)) as cur:
                r = await cur.fetchone()
            if r and r["n"]:
                n = r["n"]
                out.update({
                    "closed": n, "wins": int(r["w"] or 0), "losses": int(r["l"] or 0),
                    "winrate": round((r["w"] or 0) / n * 100, 2),
                    "net": round(r["net"] or 0.0, 2),
                    "pf": round((r["g"] or 0) / r["ls"], 3) if (r["ls"] or 0) > 0 else 999.0,
                    "avg_win": round(r["aw"] or 0.0, 2), "avg_loss": round(r["al"] or 0.0, 2),
                })
            async with db.execute(
                    """SELECT symbol, side, entry, exit_ts, pnl, pnl_pct, win, reason
                       FROM triple_trades WHERE win>=0 ORDER BY exit_ts DESC LIMIT 60""") as cur:
                out["trades"] = [dict(x) for x in await cur.fetchall()]
            async with db.execute("SELECT date, pnl, wins, losses FROM triple_daily ORDER BY date DESC LIMIT 30") as cur:
                out["daily"] = [{"date": x[0], "pnl": round(x[1] or 0, 2), "wins": x[2], "losses": x[3]}
                                for x in await cur.fetchall()]
        return out


# ══════════════════════════════════════════════════════════════════════
#  ⚙️ المحرك الأساسي — (تنفيذ، وقف على المنصة، مصالحة ذاتية، شفاء ذاتي)
# ══════════════════════════════════════════════════════════════════════
class TripleConfluenceBot:
    STRATEGY_LABELS = {'TRIPLE': '🧭 Triple Confluence'}

    def __init__(self):
        self.binance_api_key = os.environ.get('BINANCE_API_KEY', '')
        self.binance_api_secret = os.environ.get('BINANCE_API_SECRET', '')

        self.TRADE_ENABLED = os.environ.get('TRADE_ENABLED', 'false').lower() == 'true'
        self.LEVERAGE = 10
        # ✅ سعر الصفقة: 50$ (تم تغييره من 14 إلى 50)
        self.TRADE_SIZE_USDT = float(os.environ.get('TRADE_SIZE_USDT', '50'))
        self.TESTNET = os.environ.get('TESTNET', 'false').lower() == 'true'

        # ✅ الصفقات المتزامنة: 1 فقط (تم تغييرها من 5 إلى 1)
        self.max_open_triple = int(os.environ.get('MAX_OPEN_TRADES', '1'))

        # ✅ تحقق صارم من الإعدادات — فشل سريع وواضح
        problems = []
        if self.TRADE_SIZE_USDT <= 0:
            problems.append("TRADE_SIZE_USDT يجب أن يكون أكبر من صفر")
        if not (1 <= self.max_open_triple <= 20):
            problems.append("MAX_OPEN_TRADES يجب أن يكون بين 1 و 20")
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
        self.VERIFY_EVERY_N_LOOPS = 5
        self.WS_STALE_ALERT = 120
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
        self._shutdown_requested = False

        self.all_futures_pairs = []
        self.step_sizes_cache = {}
        self.tick_sizes_cache = {}
        self.min_qty_cache = {}
        self.max_qty_cache = {}
        self.min_notional_cache = {}
        self.live_prices = {}
        self.active_trades = {}
        self.reentry_cooldown = {}
        self.EMERGENCY_MODE = False
        self.error_counts = {}
        self.last_discovery_alert = {}
        self.entry_history = collections.deque(maxlen=200)
        self.last_scan_info = None
        self._last_heartbeat = 0
        self._report_day = time.strftime("%Y-%m-%d")
        self._balance_cache = (0.0, 0.0)

        self._triple_cursor = 0
        self.triple_events = collections.deque(maxlen=300)

        self.triple_capacity = self.max_open_triple

        self.stats = {'total_scans': 0, 'trades_executed': 0}
        self.stats_triple = {'wins': 0, 'losses': 0, 'trades': 0}

    # ═══ أدوات الاستراتيجية ═══
    def strategy_cfg(self, key='TRIPLE'):
        return self.triple.cfg

    def strategy_exit(self, key='TRIPLE'):
        return self.triple_exit

    def strategy_events(self, key='TRIPLE'):
        return self.triple_events

    def strategy_stats(self, key='TRIPLE'):
        return self.stats_triple

    def strategy_margin(self, key='TRIPLE'):
        return self.TRADE_SIZE_USDT

    def strategy_capacity(self, key='TRIPLE'):
        return self.triple_capacity

    def count_open(self, key='TRIPLE'):
        return sum(1 for t in self.active_trades.values() if t.get('strategy') == 'TRIPLE')

    def _log_event(self, key, kind, msg):
        try:
            self.strategy_events(key).append({"ts": time.time(), "kind": kind, "msg": msg})
        except Exception:
            pass

    # ═══ نظام مكافحة الصمت ═══
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
        if price is None or price == 0: return "$0"
        if price < 0.00001: return f"${price:.10f}"
        elif price < 0.001: return f"${price:.6f}"
        elif price < 1: return f"${price:.4f}"
        elif price < 100: return f"${price:.2f}"
        else: return f"${price:,.1f}"

    async def tg(self, msg):
        """كل إشعار يذهب للوحة (طبقة عرض)"""
        try:
            first_line = msg.split('\n')[0].replace('*', '').strip()
            kind = 'info'
            if msg.startswith('✅') or 'صفقة منفذة' in msg or 'صفقة جديدة' in msg: kind = 'trade'
            elif msg.startswith('🎯') or 'جني' in first_line: kind = 'partial'
            elif msg.startswith('🏁') or 'إغلاق' in first_line: kind = 'close'
            elif msg.startswith('🚨') or 'خطأ' in first_line or msg.startswith('⚠️'): kind = 'error'
            elif msg.startswith('🛑'): kind = 'system'
            await panel_notify(kind, first_line, msg, 'info')
        except Exception:
            pass

    # ═══ توقيت الخادم ═══
    async def sync_server_time(self):
        try:
            res = await self._fapi_request('GET', '/fapi/v1/time')
            if res and 'serverTime' in res:
                self.time_offset_ms = int(res['serverTime']) - int(time.time() * 1000)
                logger.info(f"🕐 مزامنة وقت الخادم: إزاحة {self.time_offset_ms}ms")
        except Exception as e:
            logger.warning(f"تعذر مزامنة وقت الخادم: {e}")

    def _sign(self, params):
        params['timestamp'] = int(time.time() * 1000) + self.time_offset_ms
        params['recvWindow'] = 5000
        query = urlencode(params)
        params['signature'] = hmac.new(
            self.binance_api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return params

    async def _fapi_request(self, method, endpoint, params=None, signed=False, retries=3, quiet=(), soft=()):
        for attempt in range(retries):
            try:
                url = f"{self.trade_url}{endpoint}"
                headers = {'X-MBX-APIKEY': self.binance_api_key} if signed else {}
                req_params = self._sign(dict(params) if params else {}) if signed else params

                async with self.session.request(method, url, params=req_params,
                                                headers=headers, timeout=15) as r:
                    if r.status == 200:
                        return await r.json()
                    body = await r.text()
                    if r.status in (429, 418):
                        await asyncio.sleep(10 * (attempt + 1))
                        continue
                    if r.status == 401:
                        self._notify_error("خطأ 401 من Binance", context="مفاتيح API خاطئة أو مقيدة بـIP")
                        return None
                    if '-1021' in body:
                        logger.warning("⚠️ خطأ توقيم -1021 — مزامنة الوقت وإعادة المحاولة")
                        await self.sync_server_time()
                        await asyncio.sleep(1)
                        continue
                    if r.status >= 500:
                        logger.warning(f"Futures API {r.status} — إعادة محاولة...")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    for c in soft:
                        if str(c) in body:
                            return {'_soft_code': c}
                    if any(str(c) in body for c in quiet):
                        return None
                    logger.warning(f"Futures API Error {r.status}: {body[:120]}")
                    return None
            except asyncio.TimeoutError:
                if attempt == retries - 1:
                    self._notify_error("انتهت مهلة طلب Binance", context=endpoint)
                    return None
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                if attempt == retries - 1:
                    self._notify_error("فشل طلب Binance", exc=e, context=endpoint)
                    return None
                await asyncio.sleep(2 ** attempt)
        return None

    async def setup_futures_account(self):
        msg = f"⚙️ *إعداد حساب العقود الآجلة (الرافعة {self.LEVERAGE}x)*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        setup_count = 0
        for pair in self.all_futures_pairs[:50]:
            symbol = pair['symbol']
            try:
                lev_res = await self._fapi_request('POST', '/fapi/v1/leverage',
                    {'symbol': symbol, 'leverage': self.LEVERAGE}, signed=True)
                await self._fapi_request('POST', '/fapi/v1/marginType',
                    {'symbol': symbol, 'marginType': 'ISOLATED'}, signed=True, quiet=(-4046,))
                if lev_res and lev_res.get('leverage') == self.LEVERAGE:
                    setup_count += 1
            except Exception as e:
                logger.debug(f"تخطي إعداد {symbol}: {e}")
            await asyncio.sleep(0.15)
        msg += f"✅ تم ضبط الرافعة والهامش المعزول لـ {setup_count} زوج"
        await self.tg(msg)

    async def load_market_data(self):
        data = await self._fapi_request('GET', '/fapi/v1/exchangeInfo')
        if not data: return
        new_pairs = []
        for s in data.get('symbols', []):
            if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING' and s['contractType'] == 'PERPETUAL':
                new_pairs.append({'symbol': s['symbol'], 'baseAsset': s['baseAsset']})
                for f in s.get('filters', []):
                    if f['filterType'] == 'LOT_SIZE':
                        self.step_sizes_cache[s['symbol']] = float(f['stepSize'])
                        self.min_qty_cache[s['symbol']] = float(f.get('minQty', f['stepSize']))
                        self.max_qty_cache[s['symbol']] = float(f.get('maxQty', 1e12))
                    elif f['filterType'] == 'PRICE_FILTER':
                        self.tick_sizes_cache[s['symbol']] = float(f['tickSize'])
                    elif f['filterType'] == 'MIN_NOTIONAL':
                        self.min_notional_cache[s['symbol']] = float(f.get('notional', f.get('minNotional', 5.0)))
        self.all_futures_pairs = new_pairs
        logger.info(f"📊 تم تحميل {len(self.all_futures_pairs)} زوج عقود آجلة")

    @staticmethod
    def _decimals_of(step):
        try:
            exp = Decimal(str(step)).normalize().as_tuple().exponent
            return max(0, -exp)
        except Exception:
            return 8

    def _step_qty(self, symbol, qty):
        step = self.step_sizes_cache.get(symbol) or 1.0
        if step <= 0: step = 1.0
        dec = self._decimals_of(step)
        return round(math.floor(round(qty / step, 9)) * step, dec)

    def format_quantity(self, symbol, qty):
        return self._step_qty(symbol, qty)

    def qty_to_str(self, symbol, qty):
        step = self.step_sizes_cache.get(symbol) or 1.0
        dec = self._decimals_of(step)
        return f"{self._step_qty(symbol, qty):.{dec}f}"

    def price_to_str(self, symbol, price):
        tick = self.tick_sizes_cache.get(symbol)
        if not tick or tick <= 0:
            if price >= 100: dec = 2
            elif price >= 1: dec = 4
            elif price >= 0.01: dec = 6
            else: dec = 8
            return f"{price:.{dec}f}"
        dec = self._decimals_of(tick)
        stepped = math.floor(round(price / tick, 9)) * tick
        return f"{round(stepped, dec):.{dec}f}"

    def get_price(self, symbol):
        p = self.live_prices.get(symbol)
        if p and (time.time() - p.get('ts', 0)) <= self.PRICE_MAX_AGE:
            return p
        return None

    def set_cooldown(self, symbol):
        self.reentry_cooldown[symbol] = time.time() + self.REENTRY_COOLDOWN

    def is_in_cooldown(self, symbol):
        expiry = self.reentry_cooldown.get(symbol, 0)
        if time.time() < expiry: return True
        self.reentry_cooldown.pop(symbol, None)
        return False

    async def get_available_usdt(self):
        now = time.time()
        cached_val, cached_ts = self._balance_cache
        if now - cached_ts < self.BALANCE_CACHE_SECONDS and cached_ts > 0:
            return cached_val
        res = await self._fapi_request('GET', '/fapi/v2/balance', signed=True)
        avail = 0.0
        if isinstance(res, list):
            for b in res:
                if b.get('asset') == 'USDT':
                    avail = float(b.get('availableBalance', 0))
                    break
        self._balance_cache = (avail, now)
        return avail

    # ═══ WebSocket ═══
    async def ws_manager(self):
        ws_url = ("wss://stream.binancefuture.com/ws/!bookTicker" if self.TESTNET
                  else "wss://fstream.binance.com/ws/!bookTicker")
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20) as ws:
                    logger.info("✅ Futures WebSocket متصل (Global Mode)!")
                    self._reset_error("انقطاع WebSocket")
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            if 's' in data and 'b' in data and 'a' in data:
                                self.live_prices[data['s']] = {
                                    'bid': float(data['b']), 'ask': float(data['a']),
                                    'ts': time.time()}
                        except (ValueError, KeyError, TypeError):
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._notify_error("انقطاع WebSocket", exc=e, context="إعادة الاتصال خلال 5 ثوانٍ")
                await asyncio.sleep(5)

    async def get_klines(self, symbol, interval='15m', limit=100):
        data = await self._fapi_request('GET', '/fapi/v1/klines',
            {'symbol': symbol, 'interval': interval, 'limit': limit})
        if data and len(data) > 20:
            df = pd.DataFrame(data, columns=[
                'time','open','high','low','close','volume',
                'close_time','quote_volume','trades','taker_buy_vol',
                'taker_buy_quote_vol','ignore'])
            for col in ['open','high','low','close','volume','quote_volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        return None

    @staticmethod
    def _compute_atr(df, period=14):
        hl = df['high'] - df['low']
        hc = (df['high'] - df['close'].shift()).abs()
        lc = (df['low'] - df['close'].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return float(tr.rolling(period).mean().iloc[-1])

    # ═══ المسح الدوران ═══
    def _rotation_candidates(self, cursor_attr, limit):
        pairs = [p['symbol'] for p in self.all_futures_pairs]
        if not pairs:
            return []
        n = len(pairs)
        start = getattr(self, cursor_attr) % n
        setattr(self, cursor_attr, (start + int(limit)) % n)
        out = []
        for k in range(min(int(limit), n)):
            sym = pairs[(start + k) % n]
            if sym not in self.active_trades:
                out.append(sym)
        return out

    async def _scan_strategy(self, key='TRIPLE'):
        strat = self.triple
        if not strat.cfg.get('enabled'):
            return
        if self.count_open(key) >= self.strategy_capacity(key):
            return
        try:
            cursor = '_triple_cursor'
            candidates = self._rotation_candidates(cursor, int(strat.cfg['scan_limit']))
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
            self._notify_error(f"خطأ مسح استراتيجية {self.STRATEGY_LABELS[key]}", exc=e)

    async def scan_volatile_coins(self):
        if self.EMERGENCY_MODE or not self.TRADE_ENABLED:
            return
        await self._scan_strategy('TRIPLE')
        self.last_scan_info = {
            'ts': time.time(),
            'triple': dict(getattr(self.triple, 'last_scan', {}) or {}),
        }

    # ═══ التنفيذ ═══
    async def execute_trade(self, analysis):
        if not self.TRADE_ENABLED: return
        if self.EMERGENCY_MODE: return
        strategy = analysis.get('strategy')
        if strategy != 'TRIPLE':
            return

        symbol = analysis['symbol']
        if symbol in self.active_trades: return
        if self.is_in_cooldown(symbol): return

        margin = self.strategy_margin(strategy)
        if self.count_open(strategy) >= self.strategy_capacity(strategy): return

        available = await self.get_available_usdt()
        if available < margin * 1.2:
            self._notify_error("رصيد غير كافٍ للدخول",
                               context=f"المتاح {available:.2f}$ < المطلوب {margin * 1.2:.2f}$")
            return

        notional = margin * self.LEVERAGE
        quantity = self.format_quantity(symbol, notional / analysis['price'])
        if quantity <= 0: return
        if quantity < self.min_qty_cache.get(symbol, 0.0): return
        if quantity > self.max_qty_cache.get(symbol, 1e12): return
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
            await self.tg(f"❌ *فشل تنفيذ {side} (`{symbol}`)*\n⚠️ تأكد من إعدادات العقود الآجلة وصلاحيات المفتاح")
            return

        avg = float(result.get('avgPrice') or 0)
        entry_price = avg if avg > 0 else analysis['price']

        trade_data = {
            'symbol': symbol, 'side': side, 'entry_price': entry_price,
            'quantity': quantity, 'sl': analysis['sl'], 'tp': analysis['tp1'],
            'tp2': analysis.get('tp2'), 'tp3': analysis.get('tp3'),
            'score': analysis.get('score', 9),
            'trailing_active': False, 'highest_price': entry_price,
            'lowest_price': entry_price, 'entry_time': time.time(),
            'partial_closed': False, 'realized_pnl': 0.0,
            'stop_order_id': None, 'tp_order_id': None,
            'stop_is_algo': False, 'tp_is_algo': False,
            'stop_synced_price': None, 'tp_skipped': False,
            'strategy': strategy, 'margin_usdt': margin,
            'atr_ref': analysis.get('atr_ref'),
            'components': analysis.get('components', {}),
            }
        self.active_trades[symbol] = trade_data
        await self.db.save_trade(trade_data)
        self.stats['trades_executed'] += 1
        self.strategy_stats(strategy)['trades'] += 1
        self.entry_history.append((time.time(), time.strftime("%Y-%m-%d")))
        await self.db.log_signal(symbol, side, trade_data['entry_time'],
                                 analysis.get('score', 9), analysis.get('components', {}))

        prices = self.get_price(symbol)
        ref_price = (prices['bid'] if side == 'BUY' else prices['ask']) if prices else entry_price
        await self._sync_stop_order(trade_data, ref_price)
        await self.db.save_trade(trade_data)

        try:
            cd = int(self.strategy_cfg(strategy).get('reentry_cooldown', 3600))
        except Exception:
            cd = 3600
        self.reentry_cooldown[symbol] = time.time() + cd

        try:
            await self.triple_db.open(trade_data)
        except Exception as e:
            logger.warning(f"تعذر تسجيل صفقة Triple: {e}")

        risk = abs(analysis['price'] - analysis['sl'])
        self._log_event(strategy, 'ENTRY',
                        f"{symbol} {analysis['direction']} @ {self.fmt_price(analysis['price'])} | "
                        f"وقف {self.fmt_price(analysis['sl'])} ({risk / analysis['price'] * 100:.2f}%) | "
                        f"هدف TP1 {self.fmt_price(analysis['tp1'])} | مخاطرة {risk * (quantity or 0):.3f}$")
        label = self.STRATEGY_LABELS[strategy]
        comp = analysis.get('components', {}) or {}
        await self.tg(f"{label} — *صفقة جديدة*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
                      f"🪙 `{symbol}` | رافعة {self.LEVERAGE}x | هامش {margin:.0f}$\n"
                      f"💵 الدخول: {self.fmt_price(entry_price)}\n"
                      f"🛑 SL: {self.fmt_price(analysis['sl'])}\n"
                      f"🎯 TP1: {self.fmt_price(analysis['tp1'])} ({comp.get('rr1')}R)\n"
                      f"🎯 TP2: {self.fmt_price(analysis.get('tp2'))} ({comp.get('rr2')}R)\n"
                      f"🎯 TP3: {self.fmt_price(analysis.get('tp3'))} ({comp.get('rr3')}R)\n"
                      f"📋 " + " | ".join(analysis.get('signals_smc', [])[:2]))

    # ═══ أوامر المنصة الشرطية ═══
    @staticmethod
    def _find_order_id(data):
        if isinstance(data, dict):
            for k in ('algoId', 'orderId', 'algoID', 'orderID'):
                v = data.get(k)
                if v:
                    return v
            for v in data.values():
                r = TripleConfluenceBot._find_order_id(v)
                if r:
                    return r
        elif isinstance(data, list):
            for v in data:
                r = TripleConfluenceBot._find_order_id(v)
                if r:
                    return r
        return None

    async def _place_algo_order(self, params):
        payload = {'algoType': 'CONDITIONAL'}
        payload.update(params)
        res = await self._fapi_request('POST', '/fapi/v1/algoOrder', payload,
                                       signed=True, soft=(-4130,))
        if isinstance(res, dict):
            if '_soft_code' in res:
                return 'EXISTS' if res['_soft_code'] == -4130 else None
            oid = self._find_order_id(res)
            if oid:
                return oid
            return 'UNKNOWN'
        return None

    async def _cancel_algo_order(self, symbol, algo_id):
        await self._fapi_request('DELETE', '/fapi/v1/algoOrder',
            {'symbol': symbol, 'algoId': algo_id}, signed=True, quiet=('-2011', '-2013'))

    async def _sync_stop_order(self, trade, current_price):
        symbol = trade['symbol']
        is_buy = trade['side'] == 'BUY'
        desired = trade['sl']
        if desired is None: return False

        if current_price and ((is_buy and desired >= current_price) or (not is_buy and desired <= current_price)):
            return False

        desired_str = self.price_to_str(symbol, desired)
        if trade.get('stop_order_id') and trade.get('stop_synced_price') is not None:
            if self.price_to_str(symbol, trade['stop_synced_price']) == desired_str:
                return False

        stop_side = 'SELL' if is_buy else 'BUY'
        new_id = await self._place_algo_order({
            'symbol': symbol, 'side': stop_side, 'type': 'STOP_MARKET',
            'triggerPrice': desired_str, 'closePosition': 'true',
            'workingType': 'CONTRACT_PRICE'})

        if new_id:
            old_id = trade.get('stop_order_id')
            if old_id:
                await self._cancel_algo_order(symbol, old_id)
            trade['stop_is_algo'] = True
            trade['stop_synced_price'] = desired
            if new_id in ('EXISTS', 'UNKNOWN'):
                trade['stop_order_id'] = None
                self._reset_error(f"ستوب المنصة {symbol}")
                logger.info(f"🛡️ ستوب {symbol} مؤكد على المنصة (معرف غير متاح من Testnet)")
                return True
            trade['stop_order_id'] = new_id
            self._reset_error(f"ستوب المنصة {symbol}")
            return True

        self._notify_error(f"تعذّر وضع ستوب المنصة لـ {symbol}",
                           context="سيُعاد — الوقف المحلي يعمل")
        return False

    async def _place_tp_order(self, trade, current_price, target=None, qty=None):
        symbol = trade['symbol']
        is_buy = trade['side'] == 'BUY'
        if trade.get('tp_order_id') or trade.get('tp_skipped') or trade.get('tp_unconfirmed'):
            return
        tp = target if target is not None else trade.get('tp2')
        if not tp: return
        if (is_buy and tp <= current_price) or (not is_buy and tp >= current_price):
            trade['tp_skipped'] = True
            return
        close_side = 'SELL' if is_buy else 'BUY'
        quantity = qty if qty is not None else trade['quantity']
        new_id = await self._place_algo_order({
            'symbol': symbol, 'side': close_side, 'type': 'TAKE_PROFIT_MARKET',
            'triggerPrice': self.price_to_str(symbol, tp),
            'quantity': self.qty_to_str(symbol, quantity),
            'reduceOnly': 'true',
            'workingType': 'CONTRACT_PRICE'})
        if new_id:
            trade['tp_is_algo'] = True
            if new_id in ('EXISTS', 'UNKNOWN'):
                trade['tp_order_id'] = None
                trade['tp_unconfirmed'] = True
            else:
                trade['tp_order_id'] = new_id
            await self.tg(f"🎯 *هدف TP1 مثبّت على المنصة* (`{symbol}`) عند {self.fmt_price(tp)}")
        else:
            trade['tp_skipped'] = True
            self._notify_error(f"تعذر تثبيت الهدف لـ {symbol}",
                               context="المراقبة المحلية ستدير الخروج بدلاً منه")

    # ═══ استعلامات المنصة ═══
    async def get_position_amt(self, symbol):
        try:
            res = await self._fapi_request('GET', '/fapi/v2/positionRisk',
                {'symbol': symbol}, signed=True)
            if isinstance(res, list) and res:
                return float(res[0].get('positionAmt', 0))
        except Exception as e:
            logger.warning(f"فشل استعلام المركز لـ {symbol}: {e}")
        return None

    async def get_last_realized_pnl(self, symbol, trade):
        try:
            res = await self._fapi_request('GET', '/fapi/v1/income',
                {'symbol': symbol, 'incomeType': 'REALIZED_PNL', 'limit': 20}, signed=True)
            if isinstance(res, list) and res:
                entry_ms = int(trade.get('entry_time', 0) * 1000)
                last = None
                for e in res:
                    if int(e.get('time', 0)) >= entry_ms - 5000:
                        last = e
                        break
                if last is not None:
                    return float(last.get('income', 0))
        except Exception as e:
            logger.warning(f"فشل جلب الأرباح المحققة لـ {symbol}: {e}")
        return None

    async def request_shutdown(self, source='panel'):
        if self._shutdown_requested:
            return False
        self._shutdown_requested = True
        logger.info(f"🛑 طلب إيقاف آمن من: {source}")
        try:
            await self.tg("🛑 *إيقاف آمن:* ستوب/أوامر الهدف على المنصة تبقى فعالة وتحمي المراكز.")
        except Exception:
            pass
        return True

    async def verify_positions(self):
        if not self.TRADE_ENABLED or not self.binance_api_key:
            return
        if not self.active_trades: return
        for symbol, trade in list(self.active_trades.items()):
            try:
                amt = await self.get_position_amt(symbol)
                if amt is None: continue
                step = self.step_sizes_cache.get(symbol) or 1.0
                if abs(amt) < step:
                    pnl = await self.get_last_realized_pnl(symbol, trade)
                    prices = self.get_price(symbol)
                    px = (prices['bid'] if trade['side'] == 'BUY' else prices['ask']) if prices else trade['entry_price']
                    await self._finalize_trade(trade, px, "🛑 أُغلق على المنصة (Stop/TP)", pnl)
            except Exception as e:
                self._notify_error(f"فشل التحقق من مركز {symbol}", exc=e)

    # ═══ إغلاق آمن بالكمية الفعلية على المنصة ═══
    async def _close_position(self, trade, current_price):
        symbol = trade['symbol']
        is_buy = trade['side'] == 'BUY'
        close_side = 'SELL' if is_buy else 'BUY'

        amt = await self.get_position_amt(symbol)
        if amt is None:
            self._notify_error(f"تعذر قراءة مركز {symbol}", context="إعادة المحاولة لاحقاً")
            return False, None, None
        step = self.step_sizes_cache.get(symbol) or 1.0
        if abs(amt) < step:
            pnl = await self.get_last_realized_pnl(symbol, trade)
            return True, current_price, pnl

        qty = self.format_quantity(symbol, min(trade['quantity'], abs(amt)))
        if qty <= 0:
            return True, current_price, await self.get_last_realized_pnl(symbol, trade)

        res = await self._fapi_request('POST', '/fapi/v1/order', {
            'symbol': symbol, 'side': close_side, 'type': 'MARKET',
            'quantity': self.qty_to_str(symbol, qty), 'reduceOnly': 'true'
        }, signed=True)

        if res and res.get('status') in ['FILLED', 'NEW']:
            fill = float(res.get('avgPrice') or 0)
            fill_price = fill if fill > 0 else current_price
            self._reset_error(f"إغلاق {symbol}")
            return True, fill_price, None

        amt2 = await self.get_position_amt(symbol)
        if amt2 is not None and abs(amt2) < step:
            return True, current_price, await self.get_last_realized_pnl(symbol, trade)

        self._notify_error(f"فشل إغلاق {symbol}",
                           context="ستُعاد المحاولة — ستوب المنصة يحمي المركز")
        return False, None, None

    # ═══ إنهاء الصفقة ═══
    async def _finalize_trade(self, trade, fill_price, reason, pnl_override=None):
        symbol = trade['symbol']
        is_buy = trade['side'] == 'BUY'
        strategy = 'TRIPLE'
        trade['strategy'] = strategy

        for oid_key, algo_key in (('stop_order_id', 'stop_is_algo'), ('tp_order_id', 'tp_is_algo')):
            oid = trade.get(oid_key)
            if not oid:
                continue
            if trade.get(algo_key):
                await self._cancel_algo_order(symbol, oid)
            else:
                await self._fapi_request('DELETE', '/fapi/v1/order',
                    {'symbol': symbol, 'orderId': oid}, signed=True, quiet=('-2011', '-2013'))
                await self._cancel_algo_order(symbol, oid)

        if pnl_override is not None:
            final_pnl = float(pnl_override)
        else:
            if is_buy: final_pnl = (fill_price - trade['entry_price']) * trade['quantity']
            else: final_pnl = (trade['entry_price'] - fill_price) * trade['quantity']

        total_pnl = trade.get('realized_pnl', 0.0) + final_pnl
        margin_used = float(trade.get('margin_usdt') or self.TRADE_SIZE_USDT or 0)
        margin_pnl_pct = (total_pnl / margin_used * 100) if margin_used > 0 else 0
        is_win = total_pnl > 0

        self.strategy_stats(strategy)['wins' if is_win else 'losses'] += 1

        try:
            await self.db.set_signal_result(
                symbol, trade['entry_time'], is_win, total_pnl,
                close_ts=time.time(), close_reason=str(reason)[:200],
                duration_s=round(time.time() - trade['entry_time'], 1),
                pnl_pct=round(margin_pnl_pct, 2))
        except Exception:
            pass
        await self.db.update_daily_pnl(total_pnl, is_win)
        await self.db.remove_trade(symbol)
        self.active_trades.pop(symbol, None)
        self.set_cooldown(symbol)

        try:
            await self.triple_db.close(symbol, time.time(), total_pnl,
                                       round(margin_pnl_pct, 2), is_win, reason)
        except Exception as e:
            logger.warning(f"تعذر إغلاق سجل الاستراتيجية: {e}")

        icon = "✅" if is_win else "❌"
        partial_note = (f"\n🧾 جني جزئي سابق: `{trade.get('realized_pnl', 0.0):.4f} USDT`"
                        if abs(trade.get('realized_pnl', 0.0)) > 1e-9 else "")
        label = self.STRATEGY_LABELS.get(strategy, '🧭')
        msg = (f"🏁 {icon} *إغلاق `{symbol}`* {label}\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
               f"{reason}\n"
               f"💵 النتيجة: `{total_pnl:.4f} USDT` ({margin_pnl_pct:.1f}% من الهامش)\n"
               f"🏆 {label.split(' ', 1)[1]}: "
               f"{self.strategy_stats(strategy)['wins']}W / {self.strategy_stats(strategy)['losses']}L"
               f"{partial_note}")
        await self.tg(msg)
        self._log_event(strategy, 'CLOSE',
                        f"{symbol} {trade['side']} أُغلقت {total_pnl:+.4f}$ ({margin_pnl_pct:+.1f}%) — {reason}")
        logger.info(f"🏁 [{strategy}] {symbol} {total_pnl:+.3f} USDT ({margin_pnl_pct:+.1f}%) — {reason}")

    # ═══ مراقبة الصفقات ═══
    async def monitor_trades(self):
        if not self.active_trades: return
        stale_symbols = []

        for symbol, trade in list(self.active_trades.items()):
            prices = self.get_price(symbol)
            if not prices:
                stale_symbols.append(symbol)
                continue

            is_buy = trade['side'] == 'BUY'
            current_price = prices['bid'] if is_buy else prices['ask']

            if is_buy and current_price > trade.get('highest_price', 0):
                trade['highest_price'] = current_price
            elif not is_buy and current_price < trade.get('lowest_price', 999999):
                trade['lowest_price'] = current_price

            strategy = 'TRIPLE'
            trade['strategy'] = strategy

            decision = self.triple_exit.decide(trade, current_price)
            act = decision.get('action')
            if act == 'close':
                ok, fill, pnl_over = await self._close_position(trade, current_price)
                if ok:
                    await self._finalize_trade(trade, fill, decision['reason'], pnl_over)
                continue
            await self._sync_stop_order(trade, current_price)
            await self.db.save_trade(trade)

        if stale_symbols:
            self._notify_error("أسعار مجمّدة لمراكز مفتوحة",
                               context=f"{', '.join(stale_symbols)} — ستوب المنصة يحميها")

    # ═══ الصيانة الذاتية ═══
    def _cleanup_memory(self):
        now = time.time()
        self.reentry_cooldown = {k: v for k, v in self.reentry_cooldown.items() if v > now}
        self.last_discovery_alert = {k: v for k, v in self.last_discovery_alert.items()
                                     if v > now - 7200}
        if len(self.error_counts) > 500:
            self.error_counts.clear()

    async def _send_daily_report(self, day):
        row = await self.db.get_day_stats(day)
        if not row: return
        pnl, w, l = row
        wr = (w / (w + l) * 100) if (w + l) > 0 else 0
        await self.tg(f"📊 *تقرير {day}* (Triple Confluence)\n💰 PnL: `{pnl:.4f} USDT`\n"
                      f"🏆 {w}W / {l}L (فوز {wr:.0f}%)")

    def _trades_today(self):
        today = time.strftime("%Y-%m-%d")
        return sum(1 for _, d in self.entry_history if d == today)

    async def _heartbeat_and_report(self, loop_count):
        now = time.time()
        today = time.strftime("%Y-%m-%d")
        if today != self._report_day:
            await self._send_daily_report(self._report_day)
            self._report_day = today
        if now - self._last_heartbeat >= 3600:
            self._last_heartbeat = now
            await self.tg(f"💓 *النبض:* حي ✓ | مسح #{loop_count} | صفقات اليوم: {self._trades_today()}\n"
                          f"🧭 Triple مفتوحة: {self.count_open('TRIPLE')}/{self.triple_capacity} "
                          f"({self.stats_triple['wins']}W/{self.stats_triple['losses']}L)")

    # ═══ تبنّي المراكز اليتيمة ═══
    async def resolve_strategy_for_symbol(self, symbol):
        """كل صفقة تُسند إلى TRIPLE حصراً."""
        return 'TRIPLE'

    async def reconcile(self):
        if not self.binance_api_key or not self.TRADE_ENABLED:
            return
        try:
            positions = await self._fapi_request('GET', '/fapi/v2/positionRisk', signed=True)
            open_pos = {}
            if isinstance(positions, list):
                for p in positions:
                    try:
                        amt = float(p.get('positionAmt', 0))
                    except (TypeError, ValueError):
                        continue
                    if abs(amt) > 0:
                        open_pos[p['symbol']] = {'amt': amt, 'entry': float(p.get('entryPrice', 0))}

            for symbol, trade in list(self.active_trades.items()):
                pos = open_pos.get(symbol)
                step = self.step_sizes_cache.get(symbol) or 1.0
                if not pos or abs(pos['amt']) < step:
                    pnl = await self.get_last_realized_pnl(symbol, trade)
                    await self._finalize_trade(trade, trade['entry_price'],
                                               "🔄 أُغلق أثناء توقف البوت", pnl)

            try:
                await self._adopt_strategy_orphans('TRIPLE', self.triple_db, open_pos)
            except Exception as e:
                logger.warning(f"⚠️ خطأ تبنّي صفقات TRIPLE: {e}")

            try:
                await self._adopt_unknown_orphans(open_pos)
            except Exception as e:
                logger.warning(f"⚠️ خطأ تبنّي المراكز المجهولة: {e}")

            try:
                await self._close_stale_strategy_rows('TRIPLE', self.triple_db, open_pos)
            except Exception as e:
                logger.warning(f"⚠️ خطأ تنظيف سجلات TRIPLE: {e}")

            orders = await self._fapi_request('GET', '/fapi/v1/openOrders', signed=True) or []
            try:
                orders += await self._fapi_request('GET', '/fapi/v1/algoOpenOrders',
                    signed=True, quiet=('-5000',)) or []
            except Exception:
                pass
            known_ids = set()
            for t in self.active_trades.values():
                if t.get('stop_order_id'): known_ids.add(t['stop_order_id'])
                if t.get('tp_order_id'): known_ids.add(t['tp_order_id'])
            cancelled = 0
            for o in orders:
                oid = o.get('orderId') or o.get('algoId')
                if oid in known_ids or o.get('type') not in ('STOP_MARKET', 'TAKE_PROFIT_MARKET'):
                    continue
                if o.get('algoId'):
                    await self._fapi_request('DELETE', '/fapi/v1/algoOrder',
                        {'symbol': o['symbol'], 'algoId': o['algoId']}, signed=True,
                        quiet=('-2011', '-2013'))
                else:
                    await self._fapi_request('DELETE', '/fapi/v1/order',
                        {'symbol': o['symbol'], 'orderId': o['orderId']}, signed=True,
                        quiet=('-2011', '-2013'))
                cancelled += 1
            if cancelled:
                await self.tg(f"🧹 *تنظيف ذاتي:* أُلغي {cancelled} أمر يتيم على المنصة")
            logger.info(f"🧾 المصالحة الذاتية: {len(open_pos)} مركز حي، {cancelled} أمر يتيم ملغى")
        except Exception as e:
            self._notify_error("فشل المصالحة الذاتية", exc=e, context="غير قاتل — تُعاد كل دورات")

    async def _adopt_strategy_orphans(self, key, sdb, open_pos):
        """تبنّي المراكز اليتيمة من قاعدة الاستراتيجية."""
        rows = await sdb.open_symbols()
        adopted = []
        cfg_s = self.strategy_cfg(key)
        risk_mult = float(cfg_s['atrMultSl'])
        for sym, row in rows.items():
            if sym in self.active_trades or sym not in open_pos:
                continue
            pos = open_pos[sym]
            step = self.step_sizes_cache.get(sym) or 1.0
            if abs(pos['amt']) < step:
                continue
            entry = float(row.get('entry') or 0) or float(pos.get('entry') or 0)
            if entry <= 0:
                continue
            side = 'BUY' if pos['amt'] > 0 else 'SELL'
            atr_v = float(row.get('atr') or 0)
            sl = float(row.get('sl') or 0)
            if sl <= 0 and atr_v > 0:
                sl = entry - risk_mult * atr_v if side == 'BUY' else entry + risk_mult * atr_v
            tp = float(row.get('tp1') or 0)
            tp2 = float(row.get('tp2') or 0)
            tp3 = float(row.get('tp3') or 0)
            trade = {
                'symbol': sym, 'side': side, 'entry_price': entry,
                'quantity': abs(pos['amt']), 'sl': sl, 'tp': tp or None,
                'tp2': tp2 or None, 'tp3': tp3 or None,
                'trailing_active': False, 'highest_price': entry, 'lowest_price': entry,
                'entry_time': float(row.get('entry_ts') or time.time()),
                'partial_closed': False, 'realized_pnl': 0.0,
                'stop_order_id': None, 'tp_order_id': None,
                'stop_is_algo': False, 'tp_is_algo': False,
                'stop_synced_price': None, 'tp_skipped': False,
                'strategy': key, 'atr_ref': atr_v,
                'margin_usdt': float(row.get('margin') or 0) or self.strategy_margin(key),
                'components': {'strategy': key, 'trigger': 'تبنّي بعد إعادة تشغيل'},
            }
            self.active_trades[sym] = trade
            await self.db.save_trade(trade)
            try:
                await self._sync_stop_order(trade, entry)
            except Exception:
                pass
            adopted.append(sym)
            logger.info(f"🔄 تبنّي {key}: {sym} {side} @ {self.fmt_price(entry)}")
        if adopted:
            label = self.STRATEGY_LABELS[key]
            await self.tg("🔄 *تبنّي " + label + "*\n"
                          + "، ".join(f"`{s}`" for s in adopted)
                          + "\nاستُعيدت بوقفها وأهدافها — ستظهر في لوحتها")

    async def _adopt_unknown_orphans(self, open_pos):
        """تبنّي أي مركز يتيم بلا سجل — يُسند إلى TRIPLE."""
        for symbol, pos in open_pos.items():
            if symbol in self.active_trades:
                continue
            side = 'BUY' if pos['amt'] > 0 else 'SELL'
            entry = pos['entry'] or (self.live_prices.get(symbol, {}).get('bid', 0))
            if not entry or entry <= 0:
                await self.tg(f"⚠️ *تعذّر تبنّي مركز يتيم* (`{symbol}`)\n"
                              f"سعر الدخول غير متاح بعد — سيُعاد في إعادة تشغيل قادمة")
                continue
            cfg = self.strategy_cfg('TRIPLE')
            df = await self.get_klines(symbol, '1h', 60)
            atr_v = self._compute_atr(df) if df is not None else None
            if not atr_v or atr_v <= 0 or math.isnan(atr_v):
                atr_v = entry * 0.01
            risk_mult = float(cfg['atrMultSl'])
            sl = entry - risk_mult * atr_v if side == 'BUY' else entry + risk_mult * atr_v
            tp1 = entry + float(cfg['rr1']) * risk_mult * atr_v if side == 'BUY' else entry - float(cfg['rr1']) * risk_mult * atr_v
            tp2 = entry + float(cfg['rr2']) * risk_mult * atr_v if side == 'BUY' else entry - float(cfg['rr2']) * risk_mult * atr_v
            tp3 = entry + float(cfg['rr3']) * risk_mult * atr_v if side == 'BUY' else entry - float(cfg['rr3']) * risk_mult * atr_v
            trade = {
                'symbol': symbol, 'side': side, 'entry_price': entry,
                'quantity': abs(pos['amt']), 'sl': sl, 'tp': tp1, 'tp2': tp2, 'tp3': tp3,
                'trailing_active': False, 'highest_price': entry, 'lowest_price': entry,
                'entry_time': time.time(), 'partial_closed': False,
                'realized_pnl': 0.0, 'stop_order_id': None, 'tp_order_id': None,
                'stop_is_algo': False, 'tp_is_algo': False,
                'stop_synced_price': None, 'tp_skipped': False,
                'strategy': 'TRIPLE', 'atr_ref': atr_v,
                'margin_usdt': self.strategy_margin('TRIPLE'),
                'components': {'strategy': 'TRIPLE', 'trigger': 'تبنّي مركز يتيم'},
            }
            self.active_trades[symbol] = trade
            await self.db.save_trade(trade)
            try:
                await self.triple_db.open(trade)
            except Exception as e:
                logger.warning(f"تعذر تسجيل المتبنّى في قاعدة TRIPLE: {e}")
            await self._sync_stop_order(trade, entry)
            await self.db.save_trade(trade)
            self.strategy_stats('TRIPLE')['trades'] += 1
            self.entry_history.append((time.time(), time.strftime("%Y-%m-%d")))
            self._log_event('TRIPLE', 'ADOPT',
                            f"{symbol} {side} تبنّاه بعد إعادة تشغيل — "
                            f"وقف {self.fmt_price(sl)} / TP1 {self.fmt_price(tp1)}")
            await self.tg(f"🔄 *تم تبنّي مركز يتيم* (`{symbol}`)\n"
                          f"أسند إلى {self.STRATEGY_LABELS['TRIPLE']} — وقف/هدف بقواعد الاستراتيجية")

    async def _close_stale_strategy_rows(self, key, sdb, open_pos):
        """سجل مفتوح بلا مركز على المنصة وبلا صفقة حية → انتهى فعلاً."""
        rows = await sdb.open_symbols()
        for sym, row in rows.items():
            if sym in self.active_trades:
                continue
            pos = open_pos.get(sym)
            step = self.step_sizes_cache.get(sym) or 1.0
            if pos and abs(pos['amt']) >= step:
                continue
            fake = {'symbol': sym, 'entry_time': float(row.get('entry_ts') or 0)}
            try:
                pnl = await self.get_last_realized_pnl(sym, fake)
            except Exception:
                pnl = None
            if pnl is None:
                pnl = 0.0
            await sdb.close(sym, time.time(), round(pnl, 4), 0.0,
                            pnl > 0, '🔄 أُغلق مركزه خارج البوت أثناء التوقف')
            logger.info(f"🧹 سجل {key} ميت نُظّف: {sym} ({pnl:+.4f}$)")

    # ═══ استعادة الصفقات ═══
    async def _load_trades_with_strategy(self):
        trades = await self.db.load_active_trades()
        try:
            rows = await self.triple_db.open_symbols()
        except Exception:
            rows = {}
        for sym, row in rows.items():
            if sym in trades and not trades[sym].get('strategy'):
                trades[sym]['strategy'] = 'TRIPLE'
                trades[sym]['atr_ref'] = row.get('atr') or 0.0
                m = float(row.get('margin') or 0)
                trades[sym]['margin_usdt'] = m if m > 0 else self.strategy_margin('TRIPLE')
                trades[sym]['tp2'] = row.get('tp2')
                trades[sym]['tp3'] = row.get('tp3')
                logger.info(f"♻️ استُعيدت صفقة TRIPLE مفتوحة: {sym}")
        for sym, t in trades.items():
            if t.get('strategy') not in ('TRIPLE',):
                t['strategy'] = 'TRIPLE'
                if not t.get('margin_usdt'):
                    t['margin_usdt'] = self.strategy_margin('TRIPLE')
                logger.info(f"♻️ صفقة بلا هوية أُسندت إلى TRIPLE: {sym}")
        return trades

    # ═══ اللوب الرئيسي ═══
    async def main_loop(self):
        self.session = aiohttp.ClientSession()
        try:
            await start_web_panel(self)
            loop = asyncio.get_running_loop()
            for sig_name in ('SIGINT', 'SIGTERM'):
                try:
                    loop.add_signal_handler(getattr(signal, sig_name),
                        lambda: asyncio.ensure_future(self.request_shutdown('signal')))
                except (NotImplementedError, ValueError, AttributeError, RuntimeError):
                    pass

            await self.db.init_db()
            try:
                await self.triple_db.init()
            except Exception as e:
                logger.error(f"تعذر تهيئة قاعدة بيانات Triple: {e}")

            # ✅ تحميل الإعدادات المحفوظة
            try:
                raw = await self.db.get_param('triple_config')
                if raw:
                    saved = json.loads(raw)
                    if int(saved.get('settings_version', 1) or 1) == \
                            int(self.triple.cfg.get('settings_version', 1) or 1):
                        self.triple.cfg.update({k: v for k, v in saved.items() if k in self.triple.cfg})
                        self.triple_exit.cfg.update(self.triple.cfg)
                    else:
                        logger.info(f"⚙️ triple_config: إعدادات محفوظة قديمة — اعتماد الإعدادات الجديدة")
                        await self.db.set_param('triple_config', json.dumps(dict(self.triple.cfg)))
            except Exception as e:
                logger.warning(f"تعذر تحميل إعدادات محفوظة (triple_config): {e}")
            self.triple_capacity = int(self.triple.cfg.get('max_open_trades', self.triple_capacity))
            logger.info(f"⚙️ السعة — Triple: {self.triple_capacity} | "
                        f"الحالة: Triple {'مفعّل' if self.triple.cfg.get('enabled') else 'متوقف'}")

            market_tries = 0
            while not self.all_futures_pairs:
                market_tries += 1
                await self.load_market_data()
                if self.all_futures_pairs: break
                if market_tries == 1 or market_tries % 5 == 0:
                    await self.tg(f"❌ *فشل تحميل بيانات السوق* (محاولة {market_tries})\n"
                                  f"⏳ إعادة خلال 30 ثانية...")
                logger.error(f"فشل تحميل بيانات السوق (محاولة {market_tries})")
                await asyncio.sleep(30)

            self.active_trades = await self._load_trades_with_strategy()
            await self.sync_server_time()

            try:
                dual = await self._fapi_request('GET', '/fapi/v1/positionSide/dual', signed=True)
                if dual and dual.get('dualSidePosition'):
                    if self.TRADE_ENABLED:
                        self.TRADE_ENABLED = False
                        logger.error("❌ الحساب في وضع Hedge Mode — التحويل للمراقبة فقط")
                        await self.tg("⚠️ *تنبيه:* Hedge Mode مفعّل — البوت لوضع One-way فقط. تم التحويل للمراقبة.")
            except Exception as e:
                logger.warning(f"تعذر فحص Hedge Mode: {e}")

            if self.TRADE_ENABLED and self.binance_api_key:
                await self.setup_futures_account()

            self.ws_task = asyncio.create_task(self.ws_manager())
            await asyncio.sleep(10)

            if self.TRADE_ENABLED and self.binance_api_key:
                await self.reconcile()
                await self.verify_positions()

            mode_trade = "⚔️ تداول تلقائي" + (" (TESTNET 🧪)" if self.TESTNET else "") \
                         if self.TRADE_ENABLED else "👁️ مراقبة فقط"
            msg = ("🤖 *البوت — Triple Confluence Navigator — انطلق!*\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
                   f"📡 الوضع: {mode_trade} | رافعة {self.LEVERAGE}x | هامش {self.TRADE_SIZE_USDT:.0f}$\n"
                   f"🧭 Triple Confluence: {'مفعّل' if self.triple.cfg.get('enabled') else 'متوقف'} — "
                   f"تزامن (Cardwell RSI + ARSI + Structure) | "
                   f"وقف min(struct, {self.triple.cfg.get('atrMultSl')}×ATR) | "
                   f"أهداف TP1/TP2/TP3 = {self.triple.cfg.get('rr1')}R/{self.triple.cfg.get('rr2')}R/{self.triple.cfg.get('rr3')}R | "
                   f"سعة {self.triple_capacity}\n"
                   "━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ بدء المسح...")
            await self.tg(msg)

            loop_count = 0
            while not self._shutdown_requested:
                try:
                    newest_ts = max((p.get('ts', 0) for p in self.live_prices.values()), default=0)
                    if newest_ts and (time.time() - newest_ts) > self.WS_STALE_ALERT:
                        self._notify_error("توقف تدفق الأسعار (WebSocket)",
                            context=f"لا تحديثات منذ {int(time.time() - newest_ts)} ثانية — ستوب المنصة يحمي المراكز")

                    await self.monitor_trades()
                    await self.scan_volatile_coins()

                    loop_count += 1
                    self.stats['total_scans'] = loop_count
                    if loop_count % self.VERIFY_EVERY_N_LOOPS == 0:
                        await self.verify_positions()
                    if loop_count % 10 == 0:
                        await self.reconcile()

                    self._cleanup_memory()
                    await self._heartbeat_and_report(loop_count)
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    raise
                except Exception as loop_err:
                    logger.error(f"خطأ في الحلقة: {loop_err}")
                    self._notify_error("خطأ في الحلقة الرئيسية", exc=loop_err)
                    await asyncio.sleep(15)
        finally:
            if self.ws_task:
                self.ws_task.cancel()
            if self.session and not self.session.closed:
                await self.session.close()

    def start(self):
        asyncio.run(self.main_loop())


if __name__ == "__main__":
    bot = TripleConfluenceBot()
    bot.start()
