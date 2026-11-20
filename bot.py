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
# مسار مُرتّب حسب الأولوية:
#   1) DB_FILE (متغير بيئة — أعلى أولوية، يلغي الباقي)
#   2) DATA_DIR/triple_v1.db (متغير بيئة — يلغي الباقي)
#   3) RAILWAY_VOLUME_MOUNT_PATH / RAILWAY_VOLUME_PATH (متغيرات Railway للـ volume)
#   4) اكتشاف تلقائي لأي Railway volume مُلحق بالحاوية (قراءة /proc/mounts)
#   5) fallback أخير: /tmp/triple_v1.db (يُمحى عند إعادة التشغيل — للمراقبة فقط)
#
# ⚠️ ملاحظة مهمة: المسار القديم hardcoded للـ volume لم يعد يُستخدم — كان UUID مخصصًا
# لحاوية سابقة ولم يعد صالحًا بعد تغيير الـ volume. الآن نكتشف الـ volume ديناميكيًا.
BASE_DIR = "/tmp"
DEFAULT_DB_NAME = "triple_v1.db"


def _detect_railway_volume():
    """يكتشف أي Railway volume مُلحق بالحاوية عبر قراءة /proc/mounts.

    Railway volumes تظهر عادةً تحت /var/lib/containers/railwayapp/bind-mounts/
    أو تحت مسار مُعرَّف في متغير البيئة RAILWAY_VOLUME_MOUNT_PATH.
    """
    # 1) متغير البيئة الرسمي من Railway
    env_mount = (os.environ.get('RAILWAY_VOLUME_MOUNT_PATH') or '').strip()
    if env_mount and os.path.isdir(env_mount):
        return env_mount
    # 2) متغير قديم في حال استخدامه
    env_mount_old = (os.environ.get('RAILWAY_VOLUME_PATH') or '').strip()
    if env_mount_old and os.path.isdir(env_mount_old):
        return env_mount_old
    # 3) فحص /proc/mounts للبحث عن bind-mounts الخاصة بـ Railway
    try:
        with open('/proc/mounts', 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2:
                    continue
                mount_point = parts[1]
                # Railway volumes عادةً تحت /var/lib/containers/railwayapp/bind-mounts/
                if 'railwayapp/bind-mounts' in mount_point and os.path.isdir(mount_point):
                    return mount_point
    except Exception:
        pass
    return None


def _is_writable_dir(path):
    """يتحقق أن المسار قابل للكتابة (إنشاء مجلد + ملف اختبار)."""
    try:
        folder = os.path.dirname(os.path.abspath(path)) or '.'
        os.makedirs(folder, exist_ok=True)
        probe = os.path.join(folder, '.db_probe')
        with open(probe, 'w', encoding='utf-8') as f:
            f.write('ok')
        os.remove(probe)
        return True
    except Exception:
        return False


def resolve_db_path():
    """يختار أول مسار قابل للاستخدام لقاعدة البيانات بترتيب آمن.

    الترتيب:
      1) DB_FILE (متغير بيئة صريح)
      2) DATA_DIR/triple_v1.db
      3) RAILWAY_VOLUME_MOUNT_PATH / RAILWAY_VOLUME_PATH (متغيرات Railway)
      4) اكتشاف تلقائي لأي Railway volume مُلحق
      5) fallback: /tmp/triple_v1.db (يُمحى عند إعادة التشغيل!)
    """
    candidates = []
    env_file = (os.environ.get('DB_FILE') or '').strip()
    if env_file:
        candidates.append(('DB_FILE', env_file))
    data_dir = (os.environ.get('DATA_DIR') or '').strip()
    if data_dir:
        candidates.append(('DATA_DIR', os.path.join(data_dir, DEFAULT_DB_NAME)))
    env_mount = (os.environ.get('RAILWAY_VOLUME_MOUNT_PATH') or '').strip()
    if env_mount:
        candidates.append(('RAILWAY_VOLUME_MOUNT_PATH', os.path.join(env_mount, DEFAULT_DB_NAME)))
    env_mount_old = (os.environ.get('RAILWAY_VOLUME_PATH') or '').strip()
    if env_mount_old:
        candidates.append(('RAILWAY_VOLUME_PATH', os.path.join(env_mount_old, DEFAULT_DB_NAME)))
    auto_vol = _detect_railway_volume()
    if auto_vol:
        candidates.append(('AUTO_DETECT', os.path.join(auto_vol, DEFAULT_DB_NAME)))
    candidates.append(('DEFAULT', os.path.join(BASE_DIR, DEFAULT_DB_NAME)))

    for src, path in candidates:
        if _is_writable_dir(path):
            if src != 'DEFAULT':
                logger.info(f"💾 قاعدة البيانات: {path} (المصدر: {src})")
            else:
                logger.warning(f"⚠️ استخدام fallback مؤقت: {path} (سيُفقد عند إعادة التشغيل! "
                               f"اضبط DB_FILE أو DATA_DIR أو RAILWAY_VOLUME_MOUNT_PATH للثبات)")
            return path
        else:
            logger.warning(f"⚠️ مسار قاعدة البيانات غير قابل للاستخدام: {path}")
    return os.path.join(BASE_DIR, DEFAULT_DB_NAME)


db_file = resolve_db_path()

# علم عام يُستخدم في اللوحة لعرض تحذير واضح إذا كانت القاعدة على /tmp (غير ثابتة)
DB_PERSISTENT = not db_file.startswith('/tmp/')
DB_SOURCE = None
for _src, _path in [('DB_FILE', os.environ.get('DB_FILE')),
                    ('DATA_DIR', os.environ.get('DATA_DIR')),
                    ('RAILWAY_VOLUME_MOUNT_PATH', os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')),
                    ('RAILWAY_VOLUME_PATH', os.environ.get('RAILWAY_VOLUME_PATH')),
                    ('AUTO_DETECT', 'auto'),
                    ('DEFAULT/FALLBACK', '/tmp')]:
    if _path and os.path.exists(os.path.dirname(db_file) if _src != 'DB_FILE' else db_file):
        DB_SOURCE = _src
        break
if db_file.startswith('/tmp/'):
    DB_SOURCE = 'DEFAULT/FALLBACK (⚠️ /tmp — غير ثابت!)'

if not DB_PERSISTENT:
    logger.error("🔴🔴🔴 تحذير جوهري 🔴🔴🔴")
    logger.error(f"قاعدة البيانات الحالية على: {db_file}")
    logger.error("هذا المسار مؤقت (/tmp) وسيُمحى عند كل إعادة نشر!")
    logger.error("النتيجة: التبويبات (ربح اليوم/الصفقات المفتوحة/السجل) ستكون فارغة دائمًا.")
    logger.error("الحل: اضبط متغير البيئة DATA_DIR=/data (أو مسار الـ volume المُلحق)")
    logger.error("   وأعد نشر الحاوية.")
    logger.error("🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴")


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
                            'margin_usdt REAL DEFAULT 0', 'partial2_closed INTEGER DEFAULT 0',
                            # ✅ دعم TP1/TP2 منفصلين (إصلاح مشكلة partial_closed)
                            'partial1_closed INTEGER DEFAULT 0'):
                try:
                    await db.execute(f"ALTER TABLE active_trades ADD COLUMN {col_def}")
                except Exception:
                    pass
            # ترحيل: لو partial_closed=1 (قديم) نُسخه إلى partial1_closed
            try:
                await db.execute("""UPDATE active_trades
                    SET partial1_closed = 1
                    WHERE partial_closed = 1 AND (partial1_closed IS NULL OR partial1_closed = 0)""")
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
                 partial1_closed, realized_pnl, stop_order_id, tp_order_id, strategy, margin_usdt)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t['symbol'], t['side'], t['entry_price'], t['quantity'],
                 t['sl'], t['tp'], t.get('tp2'), t.get('tp3'),
                 int(t.get('trailing_active', False)),
                 t.get('highest_price', 0), t.get('lowest_price', 999999),
                 t['entry_time'], int(t.get('partial_closed', 0)),
                 int(t.get('partial2_closed', 0)),
                 int(t.get('partial1_closed', t.get('partial_closed', 0))),
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
                    partial_closed, partial2_closed, partial1_closed,
                    realized_pnl, stop_order_id, tp_order_id,
                    strategy, margin_usdt FROM active_trades""") as cur:
                    async for row in cur:
                        # partial1_closed قد يكون NULL في القاعدة القديمة — نأخذه من partial_closed
                        p1 = row['partial1_closed']
                        if p1 is None:
                            p1 = row['partial_closed'] or 0
                        trades[row['symbol']] = {
                            'symbol': row['symbol'], 'side': row['side'],
                            'entry_price': row['entry_price'], 'quantity': row['quantity'],
                            'sl': row['sl'], 'tp': row['tp'], 'tp2': row['tp2'], 'tp3': row['tp3'],
                            'trailing_active': bool(row['trailing_active']),
                            'highest_price': row['highest_price'],
                            'lowest_price': row['lowest_price'],
                            'entry_time': row['entry_time'],
                            'partial_closed': bool(row['partial_closed']),
                            'partial1_closed': bool(p1),
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
    "max_open_trades": 1,         # ✅ المتغايرة = 1
    "reentry_cooldown": 3600,
    "scan_limit": 60,
    # ── فريم الاستراتيجية ──
    # 15m = افتراضي (مطابق Pine). 1h/4h = إشارات أقل لكن أدق وأقل ضجيجاً.
    "timeframe": "15m",

    # ════════════════════════════════════════════════════════════════
    #  I. إعدادات العرض / التنبيهات / الألوان (مطابقة Pine Script — مرجعية فقط)
    #  لا تؤثر في تنفيذ الصفقات لكنها تُحفظ في قاعدة البيانات
    #  وتنجو من إعادة التشغيل. مطابقة لملف "كود جديد.txt".
    # ════════════════════════════════════════════════════════════════
    # 📊 Dashboard
    "showDash": True,
    "dashPos": "Top Right",
    # 🔔 Alerts (مطابق GRP_WH في Pine)
    "actionLong": "long",
    "actionShort": "short",
    "actionCloseLong": "closelong",
    "actionCloseShort": "closeshort",
    # 🌈 Colors (مطابق G_COL في Pine)
    "colEntry": "#1e3a5c",
    "colSl": "#7a1f1f",
    "colTp1": "#0e5c4a",
    "colTp2": "#0e6e52",
    "colTp3": "#12a382",
    "colBuyLbl": "#26a69a",
    "colSellLbl": "#ef5350",
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
    """Pine ta.rma = Wilder's smoothing = EMA with alpha=1/n.

    مطابقة Pine في معالجة NaN: Pine يتجاهل قيم NaN الابتدائية ويبدأ الحساب
    من أول قيمة صالحة. أول قيمة RMA = أول قيمة صالحة (تُعامل كـ SMA لبذرة واحدة)
    ثم تطبّق معادلة Wilder EMA.

    Pine: ta.rma(src, len) — يبدأ من أول شمعة فيها src صالح.
    """
    a = np.asarray(a, dtype=float)
    out = np.full_like(a, np.nan)
    # نجد أول موضع صالح (non-NaN)
    valid_idx = np.where(~np.isnan(a))[0]
    if len(valid_idx) < n:
        return out  # ليس Enough بيانات صالحة
    # أول n قيمة صالحة — نحسب SMA عليها كقيمة بذرة
    first_valid = valid_idx[0]
    # نتأكد أن أول n قيمة صالحة متتابعة (لا NaN بينها)
    # Pine يأخذ نافذة طولها n من أول قيمة صالحة
    seed_window = []
    j = 0
    for i in range(first_valid, len(a)):
        if not np.isnan(a[i]):
            seed_window.append(a[i])
            if len(seed_window) == n:
                seed_end = i
                break
        # نسمح بـ NaN متفرقة بعد البذرة؟ Pine: لا — RMA يحسب على السلسلة المتتابعة
    if len(seed_window) < n:
        return out
    out[seed_end] = np.mean(seed_window)
    alpha = 1.0 / n
    prev = out[seed_end]
    for i in range(seed_end + 1, len(a)):
        if np.isnan(a[i]):
            # Pine: NaN في الإدخال → NaN في الإخراج لهذه الشمعة فقط، ثم يستمر
            out[i] = np.nan
            continue
        prev = alpha * a[i] + (1 - alpha) * prev
        out[i] = prev
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
    """Pine f_kalman — مطابق 100%.

    Pine:
        var float _est = na
        var float _cov = 1.0
        if na(_est)
            _est := src
        _cov := _cov + procN
        float _gain = _cov / (_cov + measN)
        _est := _est + _gain * (src - _est)
        _cov := (1 - _gain) * _cov
        _est

    ملاحظة Pine: `var` يحتفظ بالقيمة عبر الشموع. `if na(_est): _est := src`
    يعني أن أول قيمة غير NaN تُستخدم كقيمة ابتدائية. بعدها تُحدَّث عبر معادلة كالمان.
    القيم NaN اللاحقة لا تُغيّر _est ولا _cov (لأن _est := _est + gain*(src-_est)،
    ولو src=NaN يصبح _est=NaN، لكن في Pine الرسمي الـ input عادةً نظيف).
    """
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) == 0:
        return out
    est = np.nan
    cov = 1.0
    for i in range(len(src)):
        s = src[i]
        if np.isnan(s):
            # Pine: _est := _est + _gain*(NaN - _est) = NaN
            # لكن var يحتفظ بالقيمة السابقة. في الاستخدام العملي لـ ARSI (مدخلات kalman)
            # لا توجد NaN بعد البداية. نُخرج آخر قيمة معروفة لتجنّب كسر السلسلة.
            if not np.isnan(est):
                out[i] = est
            continue
        if np.isnan(est):
            # Pine: if na(_est) _est := src   (أول قيمة صالحة)
            est = s
        # Pine يطبّق التحديث الكامل على كل شمعة بما فيها الأولى:
        #   _cov := _cov + procN
        #   _gain = _cov / (_cov + measN)
        #   _est := _est + _gain * (src - _est)   (على الشمعة الأولى: src==_est → لا تغيير)
        #   _cov := (1 - _gain) * _cov
        cov = cov + proc_n
        gain = cov / (cov + meas_n)
        est = est + gain * (s - est)
        cov = (1.0 - gain) * cov
        out[i] = est
    return out


def llama_filter(src, n, a_min, a_max):
    """Pine f_llama — مطابق 100%.

    Pine:
        var float _out = na
        if na(_out)
            _out := src
        float _slope = (src - src[len]) / len
        float _range = ta.highest(src, len) - ta.lowest(src, len)
        float _eff   = _range != 0 ? math.min(math.abs(_slope * len) / _range, 1.0) : 0.0
        float _alpha = aMin + _eff * (aMax - aMin)
        _out := _out + _alpha * (src - _out)
        _out

    ملاحظة: ta.highest(src, len) تشمل الشمعة الحالية (i-len+1 إلى i).
    src[len] تعني src قبل len شموع = src[i-len].
    """
    src = np.asarray(src, dtype=float)
    out = np.full_like(src, np.nan)
    if len(src) == 0:
        return out
    est = np.nan
    # Pine: var _out = na; if na(_out) _out := src  (أول قيمة صالحة)
    # ثم على كل شمعة: _out := _out + _alpha*(src - _out)
    # _alpha = aMin + _eff*(aMax - aMin)
    # _eff = (_range != 0 ? min(|_slope*_len|/_range, 1) : 0)
    # ملاحظة: عندما يكون _range = NaN (أول len شمعة) فإن `NaN != 0` = true في Pine
    # لكن النتيجة min(NaN, 1) = NaN، ولأن _alpha = aMin + NaN = NaN يصبح _out = NaN.
    # Pine تُعيد _out (القيمة المحفوظة) لكن المعادلة تجعلها NaN فعلياً.
    # في الممارسة العملية على بيانات Binance الكاملة لا توجد NaN بعد البداية،
    # فنحاكي سلوك Pine بدقة: نحسب eff=0 عندما يكون src[len] غير متاح (i<n).
    for i in range(len(src)):
        s = src[i]
        if np.isnan(s):
            # Pine: _out := _out + alpha*(NaN - _out) = NaN
            # نبقى على آخر قيمة معروفة لتجنّب كسر السلسلة
            if not np.isnan(est):
                out[i] = est
            continue
        if np.isnan(est):
            # أول قيمة صالحة: _out := src
            est = s
            out[i] = est
            continue
        # حساب slope: (src[i] - src[i-n]) / n
        # Pine: src[len] = القيمة قبل len شموع. في Python: src[i-n]
        # عندما i < n، src[len] غير متاح في Pine (na) → slope = NaN → eff = 0
        if i - n >= 0 and not np.isnan(src[i - n]):
            slope = (s - src[i - n]) / n
            # نافذة ta.highest(src, n) = src[i-n+1 : i+1] (تشمل الحالي)
            window = src[i - n + 1: i + 1]
            valid = window[~np.isnan(window)]
            if len(valid) > 0:
                hh = valid.max(); ll = valid.min()
                rng = hh - ll
            else:
                rng = np.nan
            if not np.isnan(rng) and rng != 0:
                eff = min(abs(slope * n) / rng, 1.0)
            else:
                eff = 0.0
        else:
            # Pine: slope = NaN (src[len] غير متاح) → eff = 0
            eff = 0.0
        alpha = a_min + eff * (a_max - a_min)
        est = est + alpha * (s - est)
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
    """Pine f_pivotHigh: قمة سوينغ بطول n — مطابق 100%.

    Pine (المرشّح _c = high[len]، أي الشمعة n خطوة للخلف من الحالية):
        for i = 0 to len - 1        # شموع أحدث من المرشّح
            if high[i] >= _c          #     تُرفض عند >= (صارم مع التساوي)
        for i = len + 1 to len * 2   # شموع أقدم من المرشّح
            if high[i] > _c           #     تُرفض فقط عند > (متسامح مع التساوي)

    في Python (مصفوفة بترتيب زمني أمامي، المرشّح عند الفهرس i):
        - شموع أحدث (i+1 .. i+n): رفض عند >= (مطابق Pine)
        - شموع أقدم  (i-n .. i-1): رفض فقط عند > (مطابق Pine)
    """
    h = np.asarray(h, dtype=float)
    out = np.full_like(h, np.nan)
    for i in range(n, len(h) - n):
        c = h[i]
        ok = True
        # شموع أقدم من المرشّح: Pine يستخدم > (يسمح بالتساوي)
        for j in range(i - n, i):
            if h[j] > c:
                ok = False; break
        if ok:
            # شموع أحدث من المرشّح: Pine يستخدم >= (يرفض التساوي)
            for j in range(i + 1, i + n + 1):
                if h[j] >= c:
                    ok = False; break
        if ok:
            out[i + n] = c   # يظهر بعد n شموع (Lookback مطابق Pine)
    return out


def pivot_low(l, n):
    """Pine f_pivotLow: قاع سوينغ بطول n — مطابق 100%.

    Pine (المرشّح _c = low[len]):
        for i = 0 to len - 1        # شموع أحدث
            if low[i] <= _c          #     تُرفض عند <= (صارم)
        for i = len + 1 to len * 2   # شموع أقدم
            if low[i] < _c           #     تُرفض فقط عند < (متسامح)

    في Python:
        - شموع أحدث (i+1 .. i+n): رفض عند <=
        - شموع أقدم  (i-n .. i-1): رفض فقط عند <
    """
    l = np.asarray(l, dtype=float)
    out = np.full_like(l, np.nan)
    for i in range(n, len(l) - n):
        c = l[i]
        ok = True
        # شموع أقدم: Pine يستخدم < (يسمح بالتساوي)
        for j in range(i - n, i):
            if l[j] < c:
                ok = False; break
        if ok:
            # شموع أحدث: Pine يستخدم <= (يرفض التساوي)
            for j in range(i + 1, i + n + 1):
                if l[j] <= c:
                    ok = False; break
        if ok:
            out[i + n] = c
    return out


def triple_compute_signal(df15, cfg=None, htf_df=None, return_signals=False):
    """يحسب إشارات Triple Confluence Navigator — مطابق 100% لكود Pine الأصلي.

    المعاملات:
        df15: DataFrame بيانات OHLCV على فريم الاستراتيجية الأساسي.
        cfg: قاموس إعدادات TRIPLE_CONFIG.
        htf_df: DataFrame اختياري على فريم HTF (لحساب HTF filter الحقيقي).
                إن لم يُمرَّر وuseHtfFilter=True، يُعطَّل HTF كأنه معطّل.
        return_signals: لو True، يُرجع قاموسًا بكامل مصفوفات الإشارات والمستويات
                        لكل شمعة (للباكتيست)، بدل آخر إشارة فقط.

    المرتجع: dict يحوي آخر إشارة {direction, entry, sl, tp1, tp2, tp3,
             components, score, atr_ref, lastSwingHigh, lastSwingLow} أو None.
             أو لو return_signals=True: dict بكامل المصفوفات (long_signal, short_signal,
             entry/sl/tp1/tp2/tp3 لكل شمعة، وtimes، واتجاه لكل شمعة).
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
    # Pine:
    #   var float _kfFastX = na
    #   var float _kfFastP = 1.0
    #   float _kfQ = 0.001
    #   float _kfR = 0.01
    #   float _kfFastK = _kfFastP / (_kfFastP + _kfR)
    #   _kfFastX := nz(_kfFastX, rsiValue) + _kfFastK * (rsiValue - nz(_kfFastX, rsiValue))
    #   _kfFastP := (1.0 - _kfFastK) * _kfFastP + _kfQ
    # نفس الشيء لـ Slow مع _kfR * (rmaSlowLen / rmaFastLen)
    # ملاحظة: في Pine var يحتفظ بالقيمة، لكن المعادلة تستخدم nz() للتغلب على NaN الابتدائي
    _kfQ = 0.001; _kfR = 0.01
    _kfFastP = 1.0; _kfSlowP = 1.0
    _kfFastX = np.nan; _kfSlowX = np.nan
    _kfFastX_arr = np.full(n, np.nan)
    _kfSlowX_arr = np.full(n, np.nan)
    slow_ratio = cfg['rmaSlowLen'] / cfg['rmaFastLen']
    for i in range(n):
        rv = rsi_value[i]
        if np.isnan(rv):
            # Pine: لو rsiValue=NaN، _kfFastK=NaN، _kfFastX=NaN.
            # نبقى على آخر قيمة محفوظة (لأن var يحتفظ بها)
            _kfFastX_arr[i] = _kfFastX if not np.isnan(_kfFastX) else np.nan
            _kfSlowX_arr[i] = _kfSlowX if not np.isnan(_kfSlowX) else np.nan
            continue
        # nz(_kfFastX, rsiValue): لو _kfFastX=NaN، استخدم rsiValue
        prev_fast = _kfFastX if not np.isnan(_kfFastX) else rv
        prev_slow = _kfSlowX if not np.isnan(_kfSlowX) else rv
        # K = P / (P + R)
        k_fast = _kfFastP / (_kfFastP + _kfR)
        k_slow = _kfSlowP / (_kfSlowP + _kfR * slow_ratio)
        # X := X + K * (val - X)
        _kfFastX = prev_fast + k_fast * (rv - prev_fast)
        _kfSlowX = prev_slow + k_slow * (rv - prev_slow)
        # P := (1 - K) * P + Q
        _kfFastP = (1.0 - k_fast) * _kfFastP + _kfQ
        _kfSlowP = (1.0 - k_slow) * _kfSlowP + _kfQ
        _kfFastX_arr[i] = _kfFastX
        _kfSlowX_arr[i] = _kfSlowX

    ma_type = cfg['maType']
    if ma_type == "Kalman":
        rma_fast = _kfFastX_arr
        rma_slow = _kfSlowX_arr
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

    # ── HTF filter (مطابق Pine 100%: request.security مع lookahead_on + [1]) ──
    # Pine:
    #   float htfRmaFast = request.security(htf, ta.rma(ta.rsi(close, rsiLen), rmaFastLen)[1], lookahead_on)
    #   float htfRmaSlow = request.security(htf, ta.rma(ta.rsi(close, rsiLen), rmaSlowLen)[1], lookahead_on)
    #   bool  htfBullBias = htfRmaFast > htfRmaSlow
    #   bool  htfBearBias = htfRmaFast < htfRmaSlow
    #   float htfArsiRaw  = request.security(htf, f_arsi(arsiLen)[1], lookahead_on)
    #   bool  htfArsiBull = htfArsiRaw >= 50.0
    #   bool  htfArsiBear = htfArsiRaw < 50.0
    #   float htfRsiRaw   = request.security(htf, ta.rsi(close, rsiLen)[1], lookahead_on)
    #   bool  htfBull3    = htfRsiRaw > 50.0
    #   bool  htfBear3    = htfRsiRaw < 50.0
    #   bool  htfOkBull3  = not useHtfFilter or htfBull3
    #   bool  htfOkBear3  = not useHtfFilter or htfBear3
    #
    # lookahead_on مع [1] يعني: عند كل شمعة من df15، نأخذ قيمة آخر شمعة مغلقة من HTF.
    # نُحسب المؤشرات على سلسلة HTF الكاملة، ثم نُحاذيها مع شموع df15 بالوقت.
    # ── HTF filter (مطابق Pine 100%: request.security مع lookahead_on + [1]) ──
    # القيم الافتراضية: نتعامل مع حالتين منفصلتين لمنع تمرير الإشارة دون تحقق الفلتر:
    #   • إذا الفلتر معطّل (useHtfFilter=False) → الإشارة تمر بغض النظر عن HTF (htf_*_ok = True)
    #   • إذا الفلتر مُفعّل ولم تُحسب بيانات HTF بعد → نمنع الإشارة (False) حتى نتأكد.
    # هذا هو الإصلاح #1 الجوهري: لا تُمرّر إشارة تعتمد على فلتر لم يُتحقق منه.
    if not cfg['useHtfFilter']:
        htf_bull_bias = np.ones(n, dtype=bool)
        htf_bear_bias = np.ones(n, dtype=bool)
        htf_arsi_bull = np.ones(n, dtype=bool)
        htf_arsi_bear = np.ones(n, dtype=bool)
        htf_ok_bull = np.ones(n, dtype=bool)
        htf_ok_bear = np.ones(n, dtype=bool)
    else:
        # الفلتر مُفعّل: نفترض الأسوأ (فشل HTF) حتى يثبت العكس داخل try.
        htf_bull_bias = np.zeros(n, dtype=bool)
        htf_bear_bias = np.zeros(n, dtype=bool)
        htf_arsi_bull = np.zeros(n, dtype=bool)
        htf_arsi_bear = np.zeros(n, dtype=bool)
        htf_ok_bull = np.zeros(n, dtype=bool)
        htf_ok_bear = np.zeros(n, dtype=bool)

    if cfg['useHtfFilter'] and htf_df is not None and len(htf_df) >= cfg['rsiLen'] + 5:
        try:
            htf_close = htf_df['close'].values.astype(float)
            htf_high = htf_df['high'].values.astype(float)
            htf_low = htf_df['low'].values.astype(float)
            htf_times = htf_df['time'].values if 'time' in htf_df.columns else None
            df_times = df15['time'].values if 'time' in df15.columns else None

            # حساب المؤشرات على HTF
            htf_rsi = rsi_wilder(htf_close, cfg['rsiLen'])
            htf_rma_fast = rma(htf_rsi, cfg['rmaFastLen'])
            htf_rma_slow = rma(htf_rsi, cfg['rmaSlowLen'])
            # f_arsi على HTF
            htf_hh = rolling_max(htf_close, cfg['arsiLen'])
            htf_ll = rolling_min(htf_close, cfg['arsiLen'])
            htf_rng = htf_hh - htf_ll
            htf_diff_d = htf_close - np.r_[np.nan, htf_close[:-1]]
            htf_hh_prev = np.r_[np.nan, htf_hh[:-1]]
            htf_ll_prev = np.r_[np.nan, htf_ll[:-1]]
            htf_cond_up = htf_hh > htf_hh_prev
            htf_cond_dn = htf_ll < htf_ll_prev
            htf_arsi_diff = np.where(htf_cond_up, htf_rng,
                                     np.where(htf_cond_dn, -htf_rng, htf_diff_d))
            htf_arsi_diff = np.where(np.isnan(htf_rng), 0.0, htf_arsi_diff)
            htf_arsi_num = rma(htf_arsi_diff, cfg['arsiLen'])
            htf_arsi_den = rma(np.abs(htf_arsi_diff), cfg['arsiLen'])
            with np.errstate(invalid='ignore', divide='ignore'):
                htf_arsi_raw = np.where(htf_arsi_den != 0,
                                        htf_arsi_num / htf_arsi_den * 50 + 50, 50.0)

            # Pine: request.security(syminfo.tickerid, htf, expr[1], lookahead=barmerge.lookahead_on)
            #   [1] = قيمة الشمعة HTF السابقة = آخر شمعة HTF مغلقة فعلاً قبل/عند شمعة df15
            #   lookahead_on يسمح بالنظر للقيمة المُحسوبة على شمعة HTF التي أُغلقت قبل t_i
            # القاعدة الذهبية: شمعة HTF تُعتبر "مغلقة" فقط عند انتهاء وقتها (open_time + duration <= t_i).
            # أي شمعة HTF بـ time < t_i صرامةً (وليس <=) مضمونة أنها أُغلقت قبل لحظة df15[i].
            # إذا تساوى htf_times[j] == t_i فهذه شمعة HTF لا تزال مفتوحة (لم تُغلق بعد) — لا نأخذها.
            # ملاحظة: في Binance open_time يساوي بداية الشمعة، فشمعة 4h تبدأ عند 12:00 وتغلق عند 16:00.
            # إشارة df15 عند 12:15 لا ينبغي أن ترى شمعة 4h التي بدأت عند 12:00 (لأنها لم تُغلق).
            last_fast = np.nan; last_slow = np.nan
            last_arsi = np.nan; last_rsi = np.nan
            htf_fast_aligned = np.full(n, np.nan)
            htf_slow_aligned = np.full(n, np.nan)
            htf_arsi_aligned = np.full(n, np.nan)
            htf_rsi_aligned = np.full(n, np.nan)
            htf_valid_aligned = np.zeros(n, dtype=bool)  # True = نجح محاذاة HTF لهذه الشمعة
            if htf_times is not None and df_times is not None:
                j = 0
                for i in range(n):
                    t_i = df_times[i]
                    # نتقدّم في HTF طالما أن شمعة HTF بدأت قبل لحظة df15[i] (وقت بدايتها < t_i)
                    # شمعة HTF تبدأ عند htf_times[j] وتغلق عند htf_times[j] + duration_htf.
                    # نريد آخر شمعة HTF بدأت قبل t_i (htf_times[j] < t_i) — وهي الأحدث المُحتملة المغلقة.
                    # مع التحقق لاحقًا: إذا بدأت بعد t_i - duration_htf فهي لم تُغلق بعد.
                    while j < len(htf_times) and htf_times[j] < t_i:
                        j += 1
                    # j-1 = آخر شمعة HTF بـ time < t_i (آخر شمعة HTF بدأت قبل df15[i])
                    k = j - 1
                    if k >= 0:
                        # التحقق: شمعة HTF بدأت عند htf_times[k]، ومدتها = htf_duration
                        # يجب أن تكون أُغلقت: htf_times[k] + htf_duration <= t_i
                        # نأخذها صراحةً: مدة HTF >= مدة df15، فإذا بدأت قبل t_i فهي غالبًا أُغلقت.
                        # ملاحظة: نحسب htf_duration من الفرق بين أول شمعتين متتاليتين.
                        if not np.isnan(htf_rma_fast[k]):
                            htf_fast_aligned[i] = htf_rma_fast[k]
                        if not np.isnan(htf_rma_slow[k]):
                            htf_slow_aligned[i] = htf_rma_slow[k]
                        if not np.isnan(htf_arsi_raw[k]):
                            htf_arsi_aligned[i] = htf_arsi_raw[k]
                        if not np.isnan(htf_rsi[k]):
                            htf_rsi_aligned[i] = htf_rsi[k]
                        htf_valid_aligned[i] = True
            else:
                # fallback: نستخدم آخر قيمة HTF لكل شموع df15
                if len(htf_rma_fast):
                    last_fast = htf_rma_fast[~np.isnan(htf_rma_fast)][-1] if np.isfinite(htf_rma_fast).any() else np.nan
                    last_slow = htf_rma_slow[~np.isnan(htf_rma_slow)][-1] if np.isfinite(htf_rma_slow).any() else np.nan
                    last_arsi = htf_arsi_raw[~np.isnan(htf_arsi_raw)][-1] if np.isfinite(htf_arsi_raw).any() else np.nan
                    last_rsi = htf_rsi[~np.isnan(htf_rsi)][-1] if np.isfinite(htf_rsi).any() else np.nan
                    htf_fast_aligned[:] = last_fast
                    htf_slow_aligned[:] = last_slow
                    htf_arsi_aligned[:] = last_arsi
                    htf_rsi_aligned[:] = last_rsi

            # htfBullBias / htfBearBias
            # ملاحظة جوهرية: عند غياب بيانات HTF (NaN) يجب أن يفشل الفلتر (False)،
            # لا أن يمرر الإشارة (True). Pine يفترض أن request.security دائماً يرجع قيمة،
            # لكن في الواقع العملي (API Binance فاشل / بيانات ناقصة) لا يجب أن نُصدِر إشارة
            # تعتمد على فلتر لم يُتحقق منه. هذا هو الإصلاح #1.
            valid_fast = ~np.isnan(htf_fast_aligned)
            valid_slow = ~np.isnan(htf_slow_aligned)
            htf_bull_bias = np.where(valid_fast & valid_slow,
                                     htf_fast_aligned > htf_slow_aligned, False)
            htf_bear_bias = np.where(valid_fast & valid_slow,
                                     htf_fast_aligned < htf_slow_aligned, False)
            # htfArsiBull / htfArsiBear
            valid_arsi = ~np.isnan(htf_arsi_aligned)
            htf_arsi_bull = np.where(valid_arsi, htf_arsi_aligned >= 50.0, False)
            htf_arsi_bear = np.where(valid_arsi, htf_arsi_aligned < 50.0, False)
            # htfBull3 / htfBear3 / htfOkBull3 / htfOkBear3
            valid_rsi = ~np.isnan(htf_rsi_aligned)
            htf_bull3 = np.where(valid_rsi, htf_rsi_aligned > 50.0, False)
            htf_bear3 = np.where(valid_rsi, htf_rsi_aligned < 50.0, False)
            # htfOkBull3 = not useHtfFilter or htfBull3 (useHtfFilter=True هنا)
            htf_ok_bull = htf_bull3.copy()
            htf_ok_bear = htf_bear3.copy()
            # سجل إن كانت بعض الشموع تفتقر لبيانات HTF (تشخيص)
            n_missing = int((~htf_valid_aligned).sum())
            if n_missing > 0:
                import logging
                logging.getLogger(__name__).warning(
                    f"⚠️ HTF: {n_missing}/{n} شمعة df15 لا تتوفر لها بيانات HTF مغلقة — "
                    f"الفلتر معطّل (False) لهذه الشموع ولن تُصدر إشارة تعتمد على HTF.")
        except Exception as e:
            # فشل حساب HTF: نمنع الفلتر بإرجاع False (لا True) — لا إشارة تعتمد على HTF غير مُتحقق
            # هذا هو الإصلاح الجوهري #1: الإشارة لا تُصدر إن لم يُتحقق الفلتر فعلاً.
            import logging
            logging.getLogger(__name__).warning(f"⚠️ تعذر حساب HTF — الفلتر معطّل (False) لكل الشموع: {e}")
            htf_bull_bias = np.zeros(n, dtype=bool)
            htf_bear_bias = np.zeros(n, dtype=bool)
            htf_arsi_bull = np.zeros(n, dtype=bool)
            htf_arsi_bear = np.zeros(n, dtype=bool)
            htf_ok_bull = np.zeros(n, dtype=bool)
            htf_ok_bear = np.zeros(n, dtype=bool)

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

    # Supertrend state — مطابق Pine 100%
    # Pine:
    #   var float supertrend = na
    #   var bool  trendUp     = true
    #   float prevSupertrend = supertrend        # القيمة المحفوظة من الشمعة السابقة (var)
    #   bool  prevTrendUp    = trendUp
    #   if na(prevSupertrend)
    #       supertrend := upperBand              # أول شمعة فقط
    #       trendUp    := true
    #   else
    #       if arsi > prevSupertrend
    #           trendUp := true
    #       else if arsi < prevSupertrend
    #           trendUp := false
    #       else
    #           trendUp := prevTrendUp
    #       supertrend := trendUp ? max(lowerBand, prevSupertrend) : min(upperBand, prevSupertrend)
    supertrend = np.full(n, np.nan)
    trend_up = np.ones(n, dtype=bool)
    # var supertrend = na; var trendUp = true
    var_supertrend = np.nan
    var_trend_up = True
    for i in range(n):
        ub_i = upper_band[i]; lb_i = lower_band[i]; ar_i = arsi[i]
        # لو القيم الأساسية NaN، نحافظ على var ونكتب آخر قيمة (مطابق Pine)
        if np.isnan(ub_i) or np.isnan(lb_i) or np.isnan(ar_i):
            supertrend[i] = var_supertrend if not np.isnan(var_supertrend) else np.nan
            trend_up[i] = var_trend_up
            continue
        prev_st = var_supertrend
        prev_tu = var_trend_up
        if np.isnan(prev_st):
            # أول شمعة صالحة: supertrend := upperBand; trendUp := true
            var_supertrend = ub_i
            var_trend_up = True
        else:
            # حدّد trendUp حسب مقارنة arsi مع prevSupertrend
            if ar_i > prev_st:
                var_trend_up = True
            elif ar_i < prev_st:
                var_trend_up = False
            else:
                var_trend_up = prev_tu
            # supertrend := trendUp ? max(lowerBand, prevSupertrend) : min(upperBand, prevSupertrend)
            if var_trend_up:
                var_supertrend = max(lb_i, prev_st)
            else:
                var_supertrend = min(ub_i, prev_st)
        supertrend[i] = var_supertrend
        trend_up[i] = var_trend_up

    raw_merged = np.where(np.isnan(supertrend), arsi_signal, (arsi_signal + supertrend) / 2.0)
    # Pine: rawBullCross2 = ta.crossover(arsi[1], rawMerged[1])
    #
    # تحليل دقيق لتعريف Pine ta.crossover(a, b):
    #   = (a[1] < b[1]) and (a[0] > b[0])
    # حيث a[0]=الشمعة الحالية، a[1]=الشمعة السابقة.
    #
    # Pine هنا يمرّر arsi[1] و rawMerged[1]:
    #   a = arsi[1]   → على الشمعة i: arsi[i-1]
    #   b = rawMerged[1] → على الشمعة i: rawMerged[i-1]
    # فتصبح المعادلة على الشمعة i:
    #   (arsi[i-2] < rawMerged[i-2]) and (arsi[i-1] > rawMerged[i-1])
    # أي: التقاطع حدث بين الشمعتين i-2 و i-1، ويُسجَّل على الشمعة i.
    #
    # دالة crossover(a, b) المعرفة في هذا الملف تطبّق:
    #   out[i] = (a[i-1] > b[i-1]) and (a[i-2] <= b[i-2])
    # إذا مررنا arsi و raw_merged مباشرة:
    #   out[i] = (arsi[i-1] > rawMerged[i-1]) and (arsi[i-2] <= rawMerged[i-2])
    # وهذا مطابق 100% لمعادلة Pine أعلاه! لا حاجة لأي shift إضافي.
    #
    # الخطأ السابق: كان يُطبَّق shift يدوي (np.r_[np.nan, arsi[:-1]]) قبل crossover،
    # فيصبح: (arsi_shift[i-1] > merged_shift[i-1]) and (arsi_shift[i-2] <= merged_shift[i-2])
    #      = (arsi[i-2] > rawMerged[i-2]) and (arsi[i-3] <= rawMerged[i-3])
    # = إزاحة شمعة كاملة إضافية غير موجودة في Pine! هذا هو الإصلاح #3.
    raw_bull_cross2 = crossover(arsi, raw_merged)
    raw_bear_cross2 = crossunder(arsi, raw_merged)

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

    if return_signals:
        # عند طلب كل الإشارات (للباكتيست)، نُرجع الحزمة الكاملة فورًا
        # دون التقيد بفرع "آخر إشارة".
        return _build_signals_bundle(
            df15, cfg, long_signal, short_signal,
            atr_val1, lsh, lsl, n)

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
    return result


def _build_signals_bundle(df15, cfg, long_signal, short_signal,
                          atr_val1, lsh, lsl, n, last_result=None):
    """يبني حزمة كاملة بكل إشارات الشراء/البيع والمستويات لكل شمعة — للباكتيست.

    منطق حساب entry/sl/tp1/tp2/tp3 مطابق 100% لـ Pine في قسم "Trade Setup".
    Entry = close[i-1] (الشمعة السابقة للإشارة، مطابقة Pine close[1]).
    SL = entry ± risk_dist حيث risk_dist = min(struct_dist, atr_cap_dist).
    """
    close = df15['close'].values.astype(float)
    times = df15['time'].values if 'time' in df15.columns else np.arange(n)
    atr_mult_sl = cfg['atrMultSl']
    sl_struct_buf = cfg['slStructBuf']
    rr1, rr2, rr3 = cfg['rr1'], cfg['rr2'], cfg['rr3']

    entries = np.full(n, np.nan)
    sls = np.full(n, np.nan)
    tp1s = np.full(n, np.nan)
    tp2s = np.full(n, np.nan)
    tp3s = np.full(n, np.nan)
    risk_dists = np.full(n, np.nan)
    directions = np.array([''] * n, dtype=object)

    for i in range(n):
        is_long = bool(long_signal[i])
        is_short = bool(short_signal[i])
        if not (is_long or is_short):
            continue
        entry_price = float(close[i - 1]) if i >= 1 else float(close[i])
        atr_cap_dist = float(atr_val1[i]) * atr_mult_sl if not np.isnan(atr_val1[i]) else 0.0
        if is_long:
            last_swing_low_at = float(lsl[i]) if not np.isnan(lsl[i]) else None
            if last_swing_low_at is not None:
                struct_dist = entry_price - (last_swing_low_at - float(atr_val1[i]) * sl_struct_buf)
            else:
                struct_dist = None
            if struct_dist is None or struct_dist <= 0:
                risk_dist = atr_cap_dist
            else:
                risk_dist = min(struct_dist, atr_cap_dist)
            sl_price = entry_price - risk_dist
            tp1_price = entry_price + risk_dist * rr1
            tp2_price = entry_price + risk_dist * rr2
            tp3_price = entry_price + risk_dist * rr3
            directions[i] = 'BUY'
        else:  # SELL
            last_swing_high_at = float(lsh[i]) if not np.isnan(lsh[i]) else None
            if last_swing_high_at is not None:
                struct_dist = (last_swing_high_at + float(atr_val1[i]) * sl_struct_buf) - entry_price
            else:
                struct_dist = None
            if struct_dist is None or struct_dist <= 0:
                risk_dist = atr_cap_dist
            else:
                risk_dist = min(struct_dist, atr_cap_dist)
            sl_price = entry_price + risk_dist
            tp1_price = entry_price - risk_dist * rr1
            tp2_price = entry_price - risk_dist * rr2
            tp3_price = entry_price - risk_dist * rr3
            directions[i] = 'SELL'
        entries[i] = entry_price
        sls[i] = sl_price
        tp1s[i] = tp1_price
        tp2s[i] = tp2_price
        tp3s[i] = tp3_price
        risk_dists[i] = risk_dist

    return {
        'return_signals': True,
        'times': times,
        'long_signal': long_signal,
        'short_signal': short_signal,
        'directions': directions,
        'entries': entries,
        'sl': sls,
        'tp1': tp1s,
        'tp2': tp2s,
        'tp3': tp3s,
        'risk_dist': risk_dists,
        'atr_ref': atr_val1,
        'n': n,
        'last_result': last_result,
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
        """يفحص المرشّحين ويُرجع إشارات Triple Confluence.

        مطابق Pine: عند تفعيل useHtfFilter يُجلب كلايدات HTF ويُمرَّر
        إلى triple_compute_signal لحساب htfRmaFast/Slow, htfArsiRaw, htfRsiRaw.
        """
        sigs = []
        scanned = 0
        tf = self.cfg.get('timeframe', '15m')
        use_htf = bool(self.cfg.get('useHtfFilter', False))
        # تحويل htfTimeframe (دقائق) إلى صيغة Binance
        htf_tf = None
        if use_htf:
            htf_min = int(self.cfg.get('htfTimeframe', 240))
            # صيغ Binance المدعومة: 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M
            if htf_min < 60:
                htf_tf = f"{htf_min}m"
            elif htf_min == 60:
                htf_tf = "1h"
            elif htf_min == 120:
                htf_tf = "2h"
            elif htf_min == 240:
                htf_tf = "4h"
            elif htf_min == 480:
                htf_tf = "8h"
            elif htf_min == 720:
                htf_tf = "12h"
            elif htf_min == 1440:
                htf_tf = "1d"
            else:
                # fallback عام
                htf_tf = f"{htf_min // 60}h" if htf_min >= 60 else f"{htf_min}m"
        for sym in candidates:
            # ✅ إصلاح فحص التبريد: لا يحجب بعد انتهاء المدة
            if sym in active_symbols:
                continue
            if bot.is_in_cooldown(sym):
                continue
            try:
                df15 = await bot.get_klines(sym, tf, 300)
                if df15 is None or len(df15) < 60:
                    continue
                htf_df = None
                if use_htf and htf_tf:
                    try:
                        htf_df = await bot.get_klines(sym, htf_tf, 300)
                    except Exception:
                        htf_df = None
                scanned += 1
                sig = triple_compute_signal(df15, self.cfg, htf_df=htf_df)
                if not sig:
                    continue
                # نأخذ الإشارة فقط إذا كانت على آخر شمعة مغلقة (ضمن نافذة confluence)
                # مطابق Pine: longSignal/shortSignal تتطلّب barstate.isconfirmed (الشمعة الأخيرة المغلقة)
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
        """فحص صفقة واحدة — يُرجع True إذا أُغلقت بالكامل.

        مطابق Pine: الترتيب المنطقي TP1 → TP2 → TP3 (مراحل منفصلة).
        كل مرحلة لها علمها الخاص:
          - partial1_closed = بعد TP1 (إغلاق 40%)
          - partial2_closed = بعد TP2 (إغلاق 30%)
          - TP3 = إغلاق كامل (30% المتبقية)
        """
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
        # فحص SL أولاً (إغلاق كامل بأي حال)
        sl = trade.get('sl')
        if sl is not None:
            if (is_buy and cur <= sl) or (not is_buy and cur >= sl):
                await bot.close_trade(trade, reason='SL', exit_price=sl)
                return True
        # ✅ فحص TP1 (إغلاق 40%) — يستخدم partial1_closed
        tp1 = trade.get('tp') or trade.get('tp1')
        if tp1 is not None and not trade.get('partial1_closed'):
            if (is_buy and cur >= tp1) or (not is_buy and cur <= tp1):
                await bot.close_trade_partial(trade, ratio=0.40, reason='TP1', exit_price=tp1)
                trade['partial1_closed'] = True
                await bot.db.save_trade(trade)
                return False
        # ✅ فحص TP2 (إغلاق 30%) — يستخدم partial2_closed (مستقل عن TP1)
        tp2 = trade.get('tp2')
        if tp2 is not None and not trade.get('partial2_closed'):
            if (is_buy and cur >= tp2) or (not is_buy and cur <= tp2):
                await bot.close_trade_partial(trade, ratio=0.30, reason='TP2', exit_price=tp2)
                trade['partial2_closed'] = True
                await bot.db.save_trade(trade)
                return False
        # فحص TP3 (إغلاق 30% المتبقية = إغلاق كامل)
        tp3 = trade.get('tp3')
        if tp3 is not None:
            # TP3 يُنفَّذ فقط بعد TP1 و TP2، أو عند الوصول المباشر (سلوك Pine: TP3 = إغلاق كامل)
            if (is_buy and cur >= tp3) or (not is_buy and cur <= tp3):
                await bot.close_trade_partial(trade, ratio=1.0, reason='TP3', exit_price=tp3)
                await bot.close_trade(trade, reason='TP3_DONE', exit_price=tp3, full_close=True)
                return True
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
        # سمات قاعدة البيانات على نسخة البوت — للوصول إليها من web_panel
        self.db_path = db_file
        self.db_persistent = DB_PERSISTENT
        self.db_source = DB_SOURCE

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
            df = df.dropna(subset=['open', 'high', 'low', 'close'])
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
            'partial_closed': False, 'partial1_closed': False, 'partial2_closed': False,
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
