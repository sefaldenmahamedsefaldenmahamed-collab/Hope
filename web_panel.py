"""
═══════════════════════════════════════════════════════════════════════
  🧭 لوحة Triple Confluence Navigator — ملف واحد
═══════════════════════════════════════════════════════════════════════
  - مسار واحد: /confluence (الصفحة) + /health
  - API: /api/confluence/{panel, trades, today, history, closed, losses, chart}
  - شارت Lightweight Charts بأسلوب بينانس + خطوط الدخول/الوقف/الأهداف
  - الواجهة: تبويبات (الرئيسية، ربح اليوم، المفتوحة، الشارت، المغلقة، السجل، التحكم)
═══════════════════════════════════════════════════════════════════════
"""

import os, time, json, asyncio, html
import aiosqlite
from aiohttp import web

PANEL_VERSION = "V1-Triple"
PANEL_START = time.time()
PANEL_DB = None
PANEL_PORT = None

# ═══ استراتيجية واحدة فقط ═══
STRATEGY_META = {
    'confluence': {
        'key': 'confluence', 'attr': 'triple', 'exit_attr': 'triple_exit',
        'db_attr': 'triple_db', 'events_attr': 'triple_events', 'stats_attr': 'stats_triple',
        'name': '🧭 Triple Confluence', 'short': 'Triple', 'accent': '#14b5cb',
        'tables': ('triple_trades', 'triple_daily'), 'skey': 'TRIPLE',
        'persist': 'triple_config',
    },
}


def _num(obj, attr, default=0):
    try:
        v = getattr(obj, attr, None)
        return float(v) if v is not None else default
    except Exception:
        return default


def _day_start_ts():
    t = time.localtime()
    return time.mktime((t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 0, -1))


# ═══════════════════════ قاعدة بيانات اللوحة (الإشعارات) ═══════════════════════
class PanelDB:
    def __init__(self, db_name):
        self.db_name = db_name

    async def init(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS panel_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL, kind TEXT, title TEXT, body TEXT, level TEXT)""")
            await db.commit()

    async def add(self, kind, title, body='', level='info'):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                await db.execute(
                    "INSERT INTO panel_events (ts, kind, title, body, level) VALUES (?,?,?,?,?)",
                    (time.time(), kind, title, body, level))
                await db.commit()
        except Exception:
            pass

    async def get_after(self, after_id=0):
        try:
            async with aiosqlite.connect(self.db_name) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                        "SELECT id, ts, kind, title, body, level FROM panel_events "
                        "WHERE id > ? ORDER BY id ASC LIMIT 100", (after_id,)) as cur:
                    rows = await cur.fetchall()
                items = [{'id': r['id'], 'ts': r['ts'], 'kind': r['kind'],
                          'title': r['title'], 'body': r['body'], 'level': r['level']}
                         for r in rows]
                last_id = items[-1]['id'] if items else after_id
                return {'items': items, 'last_id': last_id}
        except Exception:
            return {'items': [], 'last_id': after_id}


PANEL_DB_DEFAULT = None


async def notify(kind, title, body='', level='info'):
    if PANEL_DB:
        await PANEL_DB.add(kind, title, body, level)


def esc(s):
    return html.escape(str(s) if s is not None else '')


def mono(s):
    return f'<span class="mono">{esc(s)}</span>'


def kv(k, v):
    return f'<div class="kv"><span>{esc(k)}</span><b>{v}</b></div>'


# ═══════════════════════ صفحة HTML ═══════════════════════
def tab_bar(active, accent):
    tabs = [
        ('s-dash', '🔴 الرئيسية'),
        ('s-today', '💰 ربح اليوم'),
        ('s-open', '📊 الصفقات المفتوحة'),
        ('s-chart', '📈 الشارت'),
        ('s-loss', '🧾 الصفقات المغلقة'),
        ('s-hist', '🗂 السجل'),
        ('s-ctrl', '🎛 التحكم'),
    ]
    out = '<div class="tabs">'
    for key, label in tabs:
        cls = 'tab on' if key == active else 'tab'
        out += f'<div class="{cls}" data-s="{key}" onclick="setTab(\'{key}\')">{label}</div>'
    out += '</div>'
    return out


HTML_PAGE = """<!DOCTYPE html>
<html lang="ar" dir="rtl"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>🧭 Triple Confluence Navigator</title>
<style>
:root{
  --bg:#0a0f1a; --card:#101726; --card2:#16202f; --tx:#e2e8f0; --mut:#64748b;
  --line:#1e293b; --green:#10b981; --red:#ef4444; --gold:#f59e0b; --blue:#3b82f6;
  --cyan:#14b5cb; --purple:#8b5cf6; --orange:#fb923c;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Tahoma,sans-serif;background:var(--bg);color:var(--tx);
     font-size:14px;line-height:1.6;padding-bottom:30px}
.wrap{max-width:1280px;margin:0 auto;padding:0 10px}
.hdr{display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--card);
    border-bottom:1px solid var(--line);position:sticky;top:0;z-index:50;flex-wrap:wrap}
.logo{font-weight:800;font-size:18px;display:flex;align-items:center;gap:8px}
.logo small{font-weight:400;color:var(--mut);font-size:11px}
.clock{font-family:monospace;font-size:17px;background:var(--card2);padding:6px 12px;
       border-radius:8px;border:1px solid var(--line);min-width:140px;text-align:center}
.clock small{display:block;font-size:10px;color:var(--mut);margin-top:2px}
.badge{padding:4px 10px;border-radius:6px;background:var(--card2);border:1px solid var(--line);
       font-size:11px;font-weight:600}
.tabs{display:flex;gap:4px;padding:8px 12px;flex-wrap:wrap;background:var(--card);border-bottom:1px solid var(--line);position:sticky;top:53px;z-index:40}
.tab{padding:8px 14px;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;
     transition:.15s;border:1px solid transparent}
.tab:hover{background:var(--card2)}
.tab.on{background:var(--card2);border-color:var(--cyan);color:var(--cyan)}
.sec{display:none;padding:12px}
.sec.on{display:block}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;margin-bottom:12px}
.card h3{font-size:14px;margin-bottom:10px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.kpi{display:flex;flex-direction:column;gap:2px}
.kpi .v{font-size:24px;font-weight:800;font-family:monospace}
.kpi .l{font-size:11px;color:var(--mut)}
.pos{color:var(--green)}.neg{color:var(--red)}.gold{color:var(--gold)}
.grid{display:grid;gap:12px}
.g2{grid-template-columns:repeat(auto-fit,minmax(280px,1fr))}
.g3{grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}
.g4{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
table{width:100%;border-collapse:collapse;font-size:12px}
th{background:var(--card2);padding:8px;text-align:right;font-weight:700;border-bottom:2px solid var(--line);white-space:nowrap}
td{padding:7px 8px;border-bottom:1px solid var(--line)}
.tblbox{max-height:500px;overflow:auto;border:1px solid var(--line);border-radius:8px}
.tag{padding:2px 8px;border-radius:6px;font-size:11px;font-weight:700}
.tag.buy{background:rgba(16,185,129,.15);color:var(--green)}
.tag.sell{background:rgba(239,68,68,.15);color:var(--red)}
.btn{padding:8px 14px;border-radius:8px;border:none;cursor:pointer;font-weight:700;font-size:13px;transition:.15s}
.btn:hover{opacity:.85}
.btn.b-blue{background:var(--blue);color:white}
.btn.b-green{background:var(--green);color:white}
.btn.b-red{background:var(--red);color:white}
.btn.b-gold{background:var(--gold);color:black}
.btn.b-gray{background:var(--card2);color:var(--tx);border:1px solid var(--line)}
.btnrow{display:flex;gap:10px;flex-wrap:wrap}
.empty{color:var(--mut);text-align:center;padding:26px;font-size:13px}
.note{font-size:11px;color:var(--mut);line-height:1.7;margin-top:8px}
.warnbox{background:rgba(239,68,68,.08);border:1px solid var(--red);border-radius:12px;padding:14px;margin-bottom:12px;color:#fca5a5;font-size:13px;line-height:1.8}
.big{font-size:32px;font-weight:800;font-family:monospace}
.med{font-size:17px;font-weight:700;font-family:monospace}
select,input{background:var(--card2);color:var(--tx);border:1px solid var(--line);border-radius:8px;padding:7px 10px;font-size:13px;font-family:inherit}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-left:5px}
.dot.g{background:var(--green);animation:pulse 1.5s infinite}
.dot.r{background:var(--red)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.mono{font-family:monospace}.mut{color:var(--mut)}
.kv{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px dashed var(--line);font-size:12px}
.kv b{font-family:monospace}
.feed{max-height:340px;overflow:auto}
.feed .it{padding:8px 4px;border-bottom:1px dashed var(--line);font-size:12px}
.feed .it .t{color:var(--mut);font-size:10.5px}
.feed .it .h{font-weight:700}
.kpi .v,.big,.med,.mono,.clock,td.mono{direction:ltr;unicode-bidi:isolate}
@media(max-width:700px){.clock{font-size:14px;min-width:110px}.big{font-size:24px}.tab{padding:6px 10px;font-size:12px}}
</style>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body>
__TABS__
<div class="hdr">
  <div class="logo">🧭 <small id="ver">لوحة V1 • Triple Confluence</small></div>
  <div class="clock" id="clock">--:--:--<small id="cdate">...</small></div>
  <span class="badge" id="syncBadge"><span class="dot r"></span>جاري الاتصال...</span>
  <span class="badge" id="modeBadge">—</span>
  <span class="badge" id="emgBadge" style="display:none">🚨 طوارئ</span>
  <span style="flex:1"></span>
  <span class="badge" id="hbBadge">نبض: —</span>
</div>
__TABS2__
<div class="wrap">

<!-- 🔴 الرئيسية -->
<div class="sec on" id="s-dash">
  <div class="grid g4">
    <div class="card"><div class="kpi"><span class="v" id="dBal">—</span><span class="l">💰 الرصيد المتاح (USDT)</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="dToday">—</span><span class="l">📅 صافي اليوم</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="dOpen">—</span><span class="l">📊 مفتوحة الآن / السعة</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="dTotal">—</span><span class="l">🏆 صافي الاستراتيجية الإجمالي</span></div></div>
  </div>
  <div class="grid g3" style="margin-top:12px">
    <div class="card">
      <h3>💓 <b>نبض البوت</b> — مزامنة لحظية</h3>
      <div id="dHeart"></div>
    </div>
    <div class="card">
      <h3>⚙️ <b>إعدادات Triple Confluence</b></h3>
      <div id="dCfg"></div>
    </div>
    <div class="card">
      <h3>🔔 <b>آخر الإشعارات</b></h3>
      <div class="feed" id="dFeed"><div class="empty">لا إشعارات</div></div>
    </div>
  </div>
  <div class="card" style="margin-top:12px">
    <h3>📊 <b>الصفقات المفتوحة الآن</b> <span class="mut" id="dOpenCount"></span></h3>
    <div class="tblbox" style="max-height:280px"><table id="dOpenTbl"></table></div>
  </div>
</div>

<!-- 💰 ربح اليوم -->
<div class="sec" id="s-today">
  <div class="grid g4">
    <div class="card"><div class="kpi"><span class="v" id="tPnl">—</span><span class="l">💰 صافي ربح اليوم</span></div></div>
    <div class="card"><div class="kpi"><span class="v pos" id="tWin">—</span><span class="l">✅ الصفقات الرابحة</span></div></div>
    <div class="card"><div class="kpi"><span class="v neg" id="tLoss">—</span><span class="l">❌ الصفقات الخاسرة</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="tWR">—</span><span class="l">📈 نسبة الفوز اليوم</span></div></div>
  </div>
  <div class="card" style="margin-top:12px">
    <h3>📋 <b>صفقات اليوم</b> — أُغلقت اليوم</h3>
    <div class="tblbox" style="max-height:400px"><table id="tTbl"></table></div>
  </div>
</div>

<!-- 📊 المفتوحة -->
<div class="sec" id="s-open">
  <div class="card">
    <h3>📊 <b>صفقات Triple المفتوحة الآن</b> — تحديث كل 2.5 ثانية <span id="oCount"></span></h3>
    <div class="tblbox"><table id="oTbl"></table></div>
    <div class="note">💡 الصفقات تُدار تلقائياً: SL, TP1 (40%), TP2 (30%), TP3 (30%).</div>
  </div>
</div>

<!-- 📈 الشارت -->
<div class="sec" id="s-chart">
  <div class="card">
    <h3>📈 <b>شارت مباشر</b> — شموع يابانية بأسلوب بينانس مع خطوط الدخول/الوقف/الأهداف</h3>
    <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:12px">
      <label style="font-size:12px;color:var(--mut)">الزوج:
        <select id="chSymbol" style="min-width:160px"></select>
      </label>
      <label style="font-size:12px;color:var(--mut)">الفريم:
        <select id="chInterval" style="min-width:90px">
          <option value="1m">1 دقيقة</option>
          <option value="5m">5 دقائق</option>
          <option value="15m" selected>15 دقيقة</option>
          <option value="1h">ساعة</option>
          <option value="4h">4 ساعات</option>
          <option value="1d">يوم</option>
        </select>
      </label>
      <label style="font-size:12px;color:var(--mut)">عدد الشموع:
        <select id="chLimit" style="min-width:90px">
          <option value="100">100</option>
          <option value="200" selected>200</option>
          <option value="500">500</option>
          <option value="1000">1000</option>
        </select>
      </label>
      <button class="btn b-blue" id="chRefreshBtn">🔄 تحديث</button>
      <span id="chStatus" class="mut" style="font-size:12px"></span>
    </div>
    <div id="chChart" style="width:100%;height:480px;background:var(--card2);border:1px solid var(--line);border-radius:10px"></div>
    <div class="grid g3" style="margin-top:12px">
      <div class="card" style="background:var(--card2)">
        <h3>📊 <b>معلومات الصفقة على الشارت</b></h3>
        <div id="chTradeInfo"></div>
      </div>
      <div class="card" style="background:var(--card2)">
        <h3>🎯 <b>تفسير الألوان</b></h3>
        <div style="font-size:12px;line-height:2">
          <div><span style="display:inline-block;width:14px;height:3px;background:#3b82f6;vertical-align:middle;margin-left:6px"></span> <b style="color:#3b82f6">الأزرق المتقطع</b> = سعر الدخول</div>
          <div><span style="display:inline-block;width:14px;height:3px;background:#ef4444;vertical-align:middle;margin-left:6px"></span> <b style="color:#ef4444">الأحمر الصلب</b> = وقف الخسارة (SL)</div>
          <div><span style="display:inline-block;width:14px;height:3px;background:#10b981;vertical-align:middle;margin-left:6px"></span> <b style="color:#10b981">الأخضر المتقطع</b> = TP1 (1R)</div>
          <div><span style="display:inline-block;width:14px;height:3px;background:#14b5cb;vertical-align:middle;margin-left:6px"></span> <b style="color:#14b5cb">السماوي المتقطع</b> = TP2 (2R)</div>
          <div><span style="display:inline-block;width:14px;height:3px;background:#8b5cf6;vertical-align:middle;margin-left:6px"></span> <b style="color:#8b5cf6">البنفسجي المتقطع</b> = TP3 (3R)</div>
          <div style="margin-top:8px">▲ سهم أخضر أسفل الشمعة = دخول شراء</div>
          <div>▼ سهم أحمر أعلى الشمعة = دخول بيع</div>
        </div>
      </div>
      <div class="card" style="background:var(--card2)">
        <h3>💡 <b>ملاحظات</b></h3>
        <div style="font-size:12px;line-height:2;color:var(--mut)">
          • الشموع تتحدث عند فتح التبويب أو الضغط على «تحديث».<br>
          • إذا كان للزوج صفقة مفتوحة، تُرسَم خطوط الدخول والوقف والأهداف تلقائياً.<br>
          • اسحب الشارت للتنقل، ومرّر عجلة الفأرة للتكبير.<br>
          • الشارت يعرض أسعار Binance Futures الحقيقية مباشرة.
        </div>
      </div>
    </div>
  </div>
</div>

<!-- 🧾 المغلقة -->
<div class="sec" id="s-loss">
  <div class="grid g4">
    <div class="card"><div class="kpi"><span class="v" id="lCount">—</span><span class="l">🧾 إجمالي الصفقات المغلقة</span></div></div>
    <div class="card"><div class="kpi"><span class="v pos" id="lWins">—</span><span class="l">✅ الصفقات الرابحة</span></div></div>
    <div class="card"><div class="kpi"><span class="v neg" id="lLosses">—</span><span class="l">❌ الصفقات الخاسرة</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="lNet">—</span><span class="l">💰 صافي الكل</span></div></div>
  </div>
  <div class="card" style="margin-top:12px">
    <h3>🧾 <b>كل الصفقات المغلقة</b>
      <select id="lFilter" style="margin-right:10px"><option value="">كل الأيام</option><option value="today">اليوم فقط</option><option value="7">آخر 7 أيام</option><option value="30">آخر 30 يوماً</option></select>
      <select id="lRes" style="margin-right:10px"><option value="">النتيجة: الكل</option><option value="win">✅ رابحة فقط</option><option value="loss">❌ خاسرة فقط</option></select>
      <span class="mut" id="lCountSub"></span></h3>
    <div class="tblbox"><table id="lTbl"></table></div>
  </div>
</div>

<!-- 🗂 السجل -->
<div class="sec" id="s-hist">
  <div class="grid g4">
    <div class="card"><div class="kpi"><span class="v" id="hTotal">—</span><span class="l">إجمالي الصفقات المغلقة</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="hWR">—</span><span class="l">نسبة الفوز الإجمالية</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="hPF">—</span><span class="l">عامل الربح (Profit Factor)</span></div></div>
    <div class="card"><div class="kpi"><span class="v" id="hNet">—</span><span class="l">الصافي التراكمي</span></div></div>
  </div>
  <div class="card" style="margin-top:12px">
    <h3>📅 <b>ملخص كل الأيام</b></h3>
    <div class="tblbox" style="max-height:300px"><table id="hDays"></table></div>
  </div>
  <div class="card" style="margin-top:12px">
    <h3>🗄 <b>كل الصفقات المغلقة</b> — بالتفصيل</h3>
    <div class="tblbox"><table id="hTbl"></table></div>
  </div>
</div>

<!-- 🎛 التحكم -->
<div class="sec" id="s-ctrl">
  <div id="cInfoBox"></div>
  <div class="grid g2">
    <div class="card">
      <h3>🔑 <b>تشغيل التداول</b></h3>
      <div class="btnrow">
        <button class="btn b-green" id="cOnBtn">▶️ تفعيل التداول</button>
        <button class="btn b-red" id="cOffBtn">⏸️ إيقاف التداول</button>
      </div>
      <div class="note" style="margin-top:8px">يطلب من البوت بدء/إيقاف قبول الصفقات الجديدة. الصفقات المفتوحة تبقى تُدار.</div>
    </div>
    <div class="card">
      <h3>💸 <b>الإجراءات الطارئة</b></h3>
      <div class="btnrow">
        <button class="btn b-gold" id="cCloseBtn">🚪 إغلاق كل الصفقات</button>
        <button class="btn b-red" id="cEmergencyBtn">🚨 طوارئ</button>
        <button class="btn b-gray" id="cEmgResetBtn">♻️ إلغاء الطوارئ</button>
      </div>
      <div class="note" style="margin-top:8px">«طوارئ» يوقف التداول فوراً ويغلق كل الصفقات المفتوحة.</div>
    </div>
    <div class="card">
      <h3>💰 <b>الرصيد</b></h3>
      <button class="btn b-blue" id="cBalanceBtn">🔄 تحديث الرصيد</button>
      <div id="cBalOut" style="margin-top:8px"></div>
    </div>
    <div class="card">
      <h3>📊 <b>معلومات النظام</b></h3>
      <div id="cSysInfo"></div>
    </div>
    <div class="card">
      <h3>🧹 <b>صيانة قاعدة البيانات</b></h3>
      <div class="btnrow">
        <button class="btn b-red" id="cWipeBtn">🗑 مسح كل البيانات</button>
      </div>
      <div class="note" style="margin-top:8px">يحذف كل الصفقات المغلقة والإشعارات. الصفقات المفتوحة لا تُمس.</div>
    </div>
    <div class="card">
      <h3>📡 <b>حالة الاتصال</b></h3>
      <div id="cConnInfo"></div>
    </div>
  </div>
</div>

</div><!-- /wrap -->

<script>
let CUR_TAB = 's-dash';
const $ = (id) => document.getElementById(id);
const api = async (url, opts) => {
  const r = await fetch(url, opts || {});
  return r.json();
};
const esc = (s) => (s==null?'':String(s)).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function setTab(t){
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('on'));
  document.querySelectorAll('.sec').forEach(x => x.classList.remove('on'));
  const tab = document.querySelector('.tab[data-s="'+t+'"]');
  if(tab) tab.classList.add('on');
  const sec = $(t); if(sec) sec.classList.add('on');
  CUR_TAB = t;
  if(t === 's-chart') setTimeout(chSwitchTab, 50);
}
let SRV_OFF = 0;
function tickClock(){
  if(typeof ov_server_ts === 'undefined') return;
  const d = new Date(ov_server_ts * 1000 + SRV_OFF + (Date.now() - ov_local_ts));
  $('clock').innerHTML = d.toLocaleTimeString('en-GB') + '<small>' + d.toLocaleDateString('ar-EG') + '</small>';
}
async function loadAll(){
  const [ov, tr, td] = await Promise.all([
    api('/api/confluence/panel'),
    api('/api/confluence/trades'),
    api('/api/confluence/today'),
  ]);
  ov_server_ts = ov.server_ts; ov_local_ts = Date.now();
  SRV_OFF = (ov.server_ts * 1000) - Date.now();
  tickClock();
  renderTop(ov); renderDash(ov, td); renderOpen(tr); renderToday(td, ov);
  if(CUR_TAB === 's-chart') chPopulateSymbolSelect(tr);
}
function renderTop(ov){
  const badge = $('syncBadge');
  if(ov.ok){
    badge.innerHTML = '<span class="dot g"></span>متصل';
    badge.style.borderColor = 'var(--green)';
  } else {
    badge.innerHTML = '<span class="dot r"></span>منقطع';
    badge.style.borderColor = 'var(--red)';
  }
  $('modeBadge').textContent = ov.trade_enabled ? '🟢 تداول مفعّل' : '🟡 مراقبة فقط';
  $('modeBadge').style.borderColor = ov.trade_enabled ? 'var(--green)' : 'var(--gold)';
  $('emgBadge').style.display = ov.emergency ? 'inline-block' : 'none';
  $('hbBadge').textContent = 'نبض: ' + (ov.uptime_s ? Math.floor(ov.uptime_s/60)+'د' : '—');
}
function renderDash(ov, td){
  $('dBal').textContent = (ov.balance||0).toFixed(2);
  $('dBal').className = 'v ' + ((ov.balance||0) >= 0 ? 'pos' : 'neg');
  $('dToday').textContent = (td && td.pnl != null ? td.pnl : 0).toFixed(2);
  $('dToday').className = 'v ' + ((td && td.pnl || 0) >= 0 ? 'pos' : 'neg');
  $('dOpen').textContent = (ov.open_count||0) + ' / ' + (ov.capacity||1);
  $('dTotal').textContent = (ov.total_net||0).toFixed(2);
  $('dTotal').className = 'v ' + ((ov.total_net||0) >= 0 ? 'pos' : 'neg');
  $('dHeart').innerHTML = (ov.heart||[]).map(h => '<div style="font-size:11px;padding:3px 0">' + esc(h) + '</div>').join('');
  if(ov.cfg){
    const c = ov.cfg;
    $('dCfg').innerHTML =
      kv('الهامش', '$' + (c.trade_size||50)) +
      kv('الرافعة', (c.leverage||10) + 'x') +
      kv('المتزامنة', (c.max_open||1)) +
      kv('RSI Len', c.rsiLen) +
      kv('MA Fast/Slow', c.rmaFastLen + '/' + c.rmaSlowLen) +
      kv('MA Type', c.maType) +
      kv('Confluence Window', c.confluenceWindow) +
      kv('ATR Mult (SL)', c.atrMultSl) +
      kv('R:R', c.rr1 + '/' + c.rr2 + '/' + c.rr3);
  }
  if(ov.feed){
    $('dFeed').innerHTML = ov.feed.length ? ov.feed.slice(0,10).map(f =>
      '<div class="it"><div class="t">' + new Date(f.ts*1000).toLocaleTimeString('en-GB') +
      ' • ' + esc(f.kind) + '</div><div class="h">' + esc(f.title) + '</div>' +
      (f.body ? '<div class="mut">' + esc(f.body) + '</div>' : '') + '</div>').join('') :
      '<div class="empty">لا إشعارات</div>';
  }
  if(ov.open_trades){
    $('dOpenCount').textContent = '(' + ov.open_trades.length + ' صفقة)';
    $('dOpenTbl').innerHTML = ov.open_trades.length ?
      '<thead><tr><th>الزوج</th><th>الاتجاه</th><th>الدخول</th><th>السعر الحالي</th><th>SL</th><th>TP1</th><th>الربح</th></tr></thead><tbody>' +
      ov.open_trades.map(t => '<tr><td class="mono">' + esc(t.symbol) + '</td>' +
        '<td><span class="tag ' + (t.side==='BUY'?'buy':'sell') + '">' + (t.side==='BUY'?'▲ شراء':'▼ بيع') + '</span></td>' +
        '<td class="mono">' + (t.entry||0).toFixed(4) + '</td>' +
        '<td class="mono">' + (t.current||0).toFixed(4) + '</td>' +
        '<td class="mono neg">' + (t.sl||0).toFixed(4) + '</td>' +
        '<td class="mono pos">' + (t.tp1||0).toFixed(4) + '</td>' +
        '<td class="mono ' + ((t.pnl||0)>=0?'pos':'neg') + '">' + (t.pnl||0).toFixed(2) + '$</td></tr>').join('') +
      '</tbody>' : '<tbody><tr><td colspan="7" class="empty">لا صفقات مفتوحة</td></tr></tbody>';
  }
}
function renderOpen(tr){
  $('oCount').textContent = '(' + tr.length + ' صفقة)';
  $('oTbl').innerHTML = tr.length ?
    '<thead><tr><th>الزوج</th><th>الاتجاه</th><th>الدخول</th><th>الحالي</th><th>SL</th><th>TP1</th><th>TP2</th><th>TP3</th><th>الربح</th><th>%</th><th>العمر</th></tr></thead><tbody>' +
    tr.map(t => '<tr><td class="mono">' + esc(t.symbol) + '</td>' +
      '<td><span class="tag ' + (t.side==='BUY'?'buy':'sell') + '">' + (t.side==='BUY'?'▲':'▼') + '</span></td>' +
      '<td class="mono">' + (t.entry||0).toFixed(4) + '</td>' +
      '<td class="mono">' + (t.current||0).toFixed(4) + '</td>' +
      '<td class="mono neg">' + (t.sl||0).toFixed(4) + '</td>' +
      '<td class="mono pos">' + (t.tp1||0).toFixed(4) + '</td>' +
      '<td class="mono" style="color:var(--cyan)">' + (t.tp2||0).toFixed(4) + '</td>' +
      '<td class="mono" style="color:var(--purple)">' + (t.tp3||0).toFixed(4) + '</td>' +
      '<td class="mono ' + ((t.pnl||0)>=0?'pos':'neg') + '">' + (t.pnl||0).toFixed(2) + '$</td>' +
      '<td class="mono ' + ((t.pnl_pct||0)>=0?'pos':'neg') + '">' + (t.pnl_pct||0).toFixed(1) + '%</td>' +
      '<td class="mono mut">' + Math.floor((t.age_s||0)/60) + 'د</td></tr>').join('') +
    '</tbody>' : '<tbody><tr><td colspan="11" class="empty">لا صفقات مفتوحة — البوت يراقب السوق</td></tr></tbody>';
}
function renderToday(td, ov){
  if(!td) return;
  $('tPnl').textContent = (td.pnl||0).toFixed(2);
  $('tPnl').className = 'v ' + ((td.pnl||0) >= 0 ? 'pos' : 'neg');
  $('tWin').textContent = td.wins || 0;
  $('tLoss').textContent = td.losses || 0;
  const total = (td.wins||0) + (td.losses||0);
  $('tWR').textContent = total ? Math.round((td.wins/total)*100) + '%' : '—';
  if(td.trades){
    $('tTbl').innerHTML = td.trades.length ?
      '<thead><tr><th>الزوج</th><th>الاتجاه</th><th>الدخول</th><th>الخروج</th><th>الربح</th><th>%</th><th>السبب</th><th>الوقت</th></tr></thead><tbody>' +
      td.trades.map(t => '<tr><td class="mono">' + esc(t.symbol) + '</td>' +
        '<td><span class="tag ' + (t.side==='BUY'?'buy':'sell') + '">' + (t.side==='BUY'?'▲':'▼') + '</span></td>' +
        '<td class="mono">' + (t.entry||0).toFixed(4) + '</td>' +
        '<td class="mono">' + (t.exit||0).toFixed(4) + '</td>' +
        '<td class="mono ' + ((t.pnl||0)>=0?'pos':'neg') + '">' + (t.pnl||0).toFixed(2) + '$</td>' +
        '<td class="mono ' + ((t.pnl_pct||0)>=0?'pos':'neg') + '">' + (t.pnl_pct||0).toFixed(1) + '%</td>' +
        '<td>' + esc(t.reason||'—') + '</td>' +
        '<td class="mono mut">' + new Date((t.exit_ts||0)*1000).toLocaleTimeString('en-GB') + '</td></tr>').join('') +
      '</tbody>' : '<tbody><tr><td colspan="8" class="empty">لا صفقات أُغلقت اليوم</td></tr></tbody>';
  }
}
setInterval(() => loadAll().catch(e => console.warn(e)), 2500);
setInterval(tickClock, 1000);

// ═══ شارت Lightweight Charts ═══
let CHART = null, CHART_SERIES = null, CHART_VOL = null;
let CHART_SYMBOL = '', CHART_INTERVAL = '15m', CHART_LIMIT = 200;
let CHART_TIMER = null;
function chStatus(msg, isErr){
  const e = $('chStatus'); e.textContent = msg || '';
  e.style.color = isErr ? 'var(--red)' : 'var(--gold)';
}
function chDestroyChart(){
  if(CHART){ try{ CHART.remove(); }catch(e){} CHART = null; CHART_SERIES = null; CHART_VOL = null; }
}
function chCreateChart(){
  chDestroyChart();
  const container = $('chChart'); if(!container) return;
  if(typeof LightweightCharts === 'undefined'){
    chStatus('⚠️ مكتبة الرسوم لم تُحمّل', true);
    container.innerHTML = '<div style="padding:30px;text-align:center;color:var(--mut)">تعذّر تحميل مكتبة الرسوم. تحقق من الإنترنت.</div>';
    return;
  }
  CHART = LightweightCharts.createChart(container, {
    width: container.clientWidth, height: 480,
    layout: { background: { type: 'solid', color: '#0d1320' }, textColor: '#e2e8f0', fontSize: 11, fontFamily: 'monospace', attributionLogo: false },
    grid: { vertLines: { color: '#16202f' }, horzLines: { color: '#16202f' } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: { color: '#3b82f6', labelBackgroundColor: '#1e3a8a' },
      horzLine: { color: '#3b82f6', labelBackgroundColor: '#1e3a8a' } },
    rightPriceScale: { borderColor: '#1e293b', scaleMargins: { top: 0.08, bottom: 0.28 } },
    timeScale: { borderColor: '#1e293b', timeVisible: true, secondsVisible: false, rightOffset: 6, barSpacing: 8 },
    handleScroll: true, handleScale: true,
  });
  CHART_SERIES = CHART.addCandlestickSeries({
    upColor: '#10b981', downColor: '#ef4444',
    borderUpColor: '#10b981', borderDownColor: '#ef4444',
    wickUpColor: '#10b981', wickDownColor: '#ef4444',
  });
  CHART_VOL = CHART.addHistogramSeries({
    priceFormat: { type: 'volume' }, priceScaleId: 'vol', color: '#334155',
  });
  CHART_VOL.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
  window.addEventListener('resize', chResize);
}
function chResize(){
  if(CHART && $('chChart')){ try{ CHART.applyOptions({ width: $('chChart').clientWidth }); }catch(e){} }
}
function chFmtPrice(p){
  if(p == null || isNaN(p)) return '—';
  if(p >= 1000) return p.toLocaleString('en-US', {maximumFractionDigits: 2});
  if(p >= 1) return p.toFixed(4);
  if(p >= 0.01) return p.toFixed(6);
  return p.toFixed(8);
}
async function chLoad(symbol, interval, limit){
  if(!symbol){ chStatus('⚠️ اختر زوجاً أولاً', true); return; }
  CHART_SYMBOL = symbol; CHART_INTERVAL = interval || CHART_INTERVAL; CHART_LIMIT = limit || CHART_LIMIT;
  chStatus('⏳ جاري تحميل الشموع...');
  if(!CHART) chCreateChart();
  try{
    const url = '/api/confluence/chart?symbol=' + encodeURIComponent(symbol) +
                '&interval=' + encodeURIComponent(CHART_INTERVAL) +
                '&limit=' + encodeURIComponent(CHART_LIMIT);
    const data = await api(url);
    if(!data.ok){ chStatus('❌ ' + (data.error||'فشل'), true); return; }
    if(!data.candles || !data.candles.length){
      chStatus('⚠️ لا توجد شموع', true); return;
    }
    const candles = data.candles.filter(c => c && typeof c.time === 'number').sort((a, b) => a.time - b.time);
    const seen = new Set(); const unique = [];
    for(const c of candles){ if(seen.has(c.time)) continue; seen.add(c.time); unique.push(c); }
    CHART_SERIES.setData(unique);
    CHART_VOL.setData(unique.map(c => ({
      time: c.time, value: c.volume,
      color: c.close >= c.open ? 'rgba(16,185,129,.35)' : 'rgba(239,68,68,.35)',
    })));
    try{ if(CHART_SERIES._priceLines){ for(const pl of CHART_SERIES._priceLines) CHART_SERIES.removePriceLine(pl); } }catch(e){}
    CHART_SERIES._priceLines = [];
    if(data.lines && data.lines.length){
      for(const ln of data.lines){
        try{
          const pl = CHART_SERIES.createPriceLine({
            price: ln.price, color: ln.color, lineWidth: ln.lineWidth || 1,
            lineStyle: ln.lineStyle || 0, axisLabelVisible: ln.axisLabelVisible !== false,
            title: ln.title || '',
          });
          CHART_SERIES._priceLines.push(pl);
        }catch(e){}
      }
    }
    try{
      if(CHART_SERIES.setMarkers){
        CHART_SERIES.setMarkers(data.markers && data.markers.length ? data.markers : []);
      }
    }catch(e){}
    try{ CHART.timeScale().fitContent(); }catch(e){}
    chRenderTradeInfo(data.trade, data.current);
    const last = candles[candles.length - 1];
    chStatus('✅ ' + symbol + ' • ' + CHART_INTERVAL + ' • ' + candles.length +
             ' شمعة • آخر إغلاق: ' + chFmtPrice(last.close) +
             (data.current ? ' • حي: ' + chFmtPrice(data.current) : ''));
  }catch(e){
    chStatus('❌ ' + (e.message||'فشل تحميل الشارت'), true);
  }
}
function chRenderTradeInfo(trade, current){
  const box = $('chTradeInfo');
  if(!trade){
    box.innerHTML = '<div class="empty">لا توجد صفقة مفتوحة على هذا الزوج حالياً — الشارت يعرض الشموع فقط.</div>';
    return;
  }
  const isBuy = trade.side === 'BUY';
  box.innerHTML =
    kv('الزوج', '<b>' + esc(trade.symbol||CHART_SYMBOL) + '</b>') +
    kv('الاتجاه', '<span class="tag ' + (isBuy?'buy':'sell') + '">' + (isBuy?'▲ شراء':'▼ بيع') + '</span>') +
    kv('سعر الدخول', '<span class="mono ' + (isBuy?'pos':'neg') + '">' + chFmtPrice(trade.entry) + '</span>') +
    kv('الوقف SL', '<span class="mono neg">' + chFmtPrice(trade.sl) + '</span>') +
    kv('TP1 (1R)', '<span class="mono pos">' + chFmtPrice(trade.tp1) + '</span>') +
    kv('TP2 (2R)', '<span class="mono" style="color:var(--cyan)">' + chFmtPrice(trade.tp2) + '</span>') +
    kv('TP3 (3R)', '<span class="mono" style="color:var(--purple)">' + chFmtPrice(trade.tp3) + '</span>') +
    (current ? kv('السعر الحالي', '<span class="mono gold">' + chFmtPrice(current) + '</span>') : '');
}
function chPopulateSymbolSelect(trades_list){
  const sel = $('chSymbol'); if(!sel) return;
  const current = sel.value;
  const opts = {};
  for(const t of (trades_list||[])) opts[t.symbol] = true;
  if(!opts['BTCUSDT']) opts['BTCUSDT'] = true;
  const items = Object.keys(opts).sort();
  sel.innerHTML = items.map(s => '<option value="'+esc(s)+'">'+esc(s)+'</option>').join('');
  if(items.includes(current)) sel.value = current;
  else sel.value = items[0];
}
async function chSwitchTab(){
  if(CUR_TAB !== 's-chart'){ if(CHART_TIMER){ clearInterval(CHART_TIMER); CHART_TIMER = null; } return; }
  if(!CHART) chCreateChart();
  try{
    const tr = await api('/api/confluence/trades');
    chPopulateSymbolSelect(tr);
    if(!CHART_SYMBOL) CHART_SYMBOL = $('chSymbol').value;
    await chLoad(CHART_SYMBOL, $('chInterval').value, parseInt($('chLimit').value));
    if(CHART_TIMER){ clearInterval(CHART_TIMER); }
    CHART_TIMER = setInterval(async () => {
      if(CUR_TAB !== 's-chart'){ clearInterval(CHART_TIMER); CHART_TIMER = null; return; }
      try{ await chLoad(CHART_SYMBOL, CHART_INTERVAL, CHART_LIMIT); }catch(e){}
    }, 30000);
  }catch(e){
    chStatus('❌ ' + (e.message||'فشل تهيئة الشارت'), true);
  }
}
if($('chSymbol')) $('chSymbol').onchange = () => chLoad($('chSymbol').value, $('chInterval').value, parseInt($('chLimit').value));
if($('chInterval')) $('chInterval').onchange = () => chLoad($('chSymbol').value, $('chInterval').value, parseInt($('chLimit').value));
if($('chLimit')) $('chLimit').onchange = () => chLoad($('chSymbol').value, $('chInterval').value, parseInt($('chLimit').value));
if($('chRefreshBtn')) $('chRefreshBtn').onclick = () => chLoad($('chSymbol').value, $('chInterval').value, parseInt($('chLimit').value));

// ═══ التحكم ═══
async function act(url){
  try{ const r = await fetch(url, {method:'POST'}); return await r.json(); }catch(e){ alert('فشل: '+e.message); return null; }
}
if($('cOnBtn')) $('cOnBtn').onclick = async () => { if(await act('/api/confluence/enable')) loadAll(); };
if($('cOffBtn')) $('cOffBtn').onclick = async () => { if(await act('/api/confluence/disable')) loadAll(); };
if($('cCloseBtn')) $('cCloseBtn').onclick = async () => {
  if(!confirm('إغلاق كل الصفقات المفتوحة؟')) return;
  const r = await act('/api/confluence/close-all');
  if(r){ alert('تم! أُغلق: ' + (r.closed && r.closed.length ? r.closed.join(', ') : 'لا صفقات')); loadAll(); }
};
if($('cEmergencyBtn')) $('cEmergencyBtn').onclick = async () => {
  if(!confirm('🚨 تفعيل وضع الطوارئ؟ سيُوقف التداول وتُغلق كل الصفقات!')) return;
  const r = await act('/api/emergency');
  if(r){ alert('وضع الطوارئ مفعّل. أُغلق: ' + (r.closed && r.closed.length ? r.closed.join(', ') : 'لا صفقات')); loadAll(); }
};
if($('cEmgResetBtn')) $('cEmgResetBtn').onclick = async () => {
  const r = await act('/api/emergency-reset');
  if(r && r.ok){ alert('أُلغي وضع الطوارئ — اضغط «استئناف» لتفعيل التداول'); loadAll(); }
};
if($('cBalanceBtn')) $('cBalanceBtn').onclick = async () => {
  const r = await act('/api/balance');
  if(r){ $('cBalOut').innerHTML = '💰 الرصيد المتاح: <b class="pos">' + (r.balance||0).toFixed(2) + ' $</b>'; loadAll(); }
};
if($('cWipeBtn')) $('cWipeBtn').onclick = async () => {
  const v = prompt('اكتب «مسح» لتأكيد حذف كل الصفقات المغلقة والإشعارات:');
  if(v !== 'مسح'){ alert('أُلغي'); return; }
  const r = await act('/api/confluence/wipe');
  if(r){ alert('تم المسح: ' + (r.deleted||'')); loadAll(); }
};

let ov_server_ts = Math.floor(Date.now()/1000), ov_local_ts = Date.now();
tickClock();
loadAll().catch(e => console.warn(e));
</script>
</body>
</html>"""


def render_page(key):
    m = STRATEGY_META[key]
    page = HTML_PAGE
    # أول __TABS__ = شريط التبويبات (لا نضعه في الأعلى — نعرض شريط منفصل)
    page = page.replace('__TABS__', '')
    page = page.replace('__TABS2__', tab_bar('s-dash', m['accent']))
    return page


# ═══════════════════════ معالجات الاستراتيجية ═══════════════════════
def _strategy_handlers(bot, m):
    key = m['key']
    strat = getattr(bot, m['attr'])
    tables = m['tables']
    persist_key = m['persist']

    def _own_open():
        return {s: t for s, t in getattr(bot, 'active_trades', {}).items()
                if t.get('strategy') == m['skey']}

    async def _persist_cfg():
        try:
            await bot.db.set_param(persist_key, json.dumps(dict(strat.cfg)))
        except Exception:
            pass

    async def page(request):
        return web.Response(text=render_page(key), content_type='text/html',
                            headers={'Cache-Control': 'no-store'})

    async def panel(request):
        try:
            today = time.strftime('%Y-%m-%d')
            try:
                day_row = await bot.db.get_day_stats(today)
                day_pnl = float(day_row[0]) if day_row and day_row[0] else 0.0
                day_wins = int(day_row[1]) if day_row else 0
                day_losses = int(day_row[2]) if day_row else 0
            except Exception:
                day_pnl, day_wins, day_losses = 0.0, 0, 0
            try:
                async with aiosqlite.connect(bot.db.db_name) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute(
                        f"SELECT COUNT(*) as c, COALESCE(SUM(pnl),0) as s FROM {tables[0]} "
                        f"WHERE exit_ts IS NOT NULL") as cur:
                        r = await cur.fetchone()
                    total_closed = r['c'] if r else 0
                    total_net = r['s'] if r else 0.0
            except Exception:
                total_closed, total_net = 0, 0.0

            try:
                feed_rows = []
                if PANEL_DB:
                    feed_data = await PANEL_DB.get_after(0)
                    feed_rows = feed_data.get('items', [])[-10:]
            except Exception:
                feed_rows = []

            events = list(getattr(bot, m['events_attr'], []))[-20:]
            cfg = dict(strat.cfg)
            cfg['trade_size'] = bot.TRADE_SIZE_USDT
            cfg['leverage'] = bot.LEVERAGE
            cfg['max_open'] = bot.strategy_capacity(m['skey'])

            open_trades = []
            for sym, t in _own_open().items():
                prices = bot.get_price(sym)
                cur = None
                if prices:
                    cur = (prices['bid'] + prices['ask']) / 2
                open_trades.append({
                    'symbol': sym, 'side': t['side'],
                    'entry': round(t['entry_price'], 6),
                    'current': round(cur, 6) if cur else None,
                    'sl': round(t['sl'], 6) if t.get('sl') else None,
                    'tp1': round(t.get('tp') or 0, 6),
                    'pnl': 0.0,
                })

            return web.json_response({
                'ok': True,
                'server_ts': bot.server_ts if hasattr(bot, 'server_ts') else int(time.time()),
                'uptime_s': int(time.time() - PANEL_START),
                'balance': getattr(bot, '_balance_cache', None) or 0,
                'trade_enabled': bot.TRADE_ENABLED,
                'emergency': getattr(bot, 'EMERGENCY_MODE', False),
                'open_count': len(open_trades),
                'capacity': bot.strategy_capacity(m['skey']),
                'total_net': total_net,
                'cfg': cfg,
                'feed': feed_rows,
                'heart': [e['msg'] for e in events] if events else [],
            }, headers={'Cache-Control': 'no-store'})
        except Exception as e:
            return web.json_response({'ok': False, 'error': str(e)[:200]}, status=500)

    async def trades(request):
        out = []
        default_margin = bot.strategy_margin(m['skey'])
        for symbol, t in list(_own_open().items()):
            prices = bot.get_price(symbol)
            cur = pnl = None
            if prices:
                cur = prices['bid'] if t['side'] == 'BUY' else prices['ask']
                diff = (cur - t['entry_price']) if t['side'] == 'BUY' else (t['entry_price'] - cur)
                pnl = diff * t['quantity'] + t.get('realized_pnl', 0.0)
            entry = t['entry_price']
            is_buy = t['side'] == 'BUY'
            margin = float(t.get('margin_usdt') or default_margin)
            out.append({
                'symbol': symbol, 'side': t['side'],
                'margin': round(margin, 2),
                'entry': round(entry, 8),
                'current': round(cur, 8) if cur else None,
                'qty': t['quantity'],
                'pnl': round(pnl, 4) if pnl is not None else None,
                'pnl_pct': round(pnl / margin * 100, 2) if pnl is not None and margin else None,
                'sl': round(t['sl'], 8) if t.get('sl') else None,
                'tp1': round(t.get('tp') or 0, 8),
                'tp2': round(t.get('tp2') or 0, 8),
                'tp3': round(t.get('tp3') or 0, 8),
                'age_s': int(time.time() - t.get('entry_time', time.time())),
                'kind': 'Triple Confluence',
            })
        out.sort(key=lambda x: x['age_s'])
        return web.json_response(out, headers={'Cache-Control': 'no-store'})

    async def today(request):
        day_ts = _day_start_ts()
        try:
            async with aiosqlite.connect(bot.db.db_name) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    f"SELECT pnl, wins, losses FROM {tables[1]} WHERE date=?",
                    (time.strftime('%Y-%m-%d'),)) as cur:
                    r = await cur.fetchone()
                if r:
                    db_pnl, wins, losses = r[0] or 0.0, r[1] or 0, r[2] or 0
                else:
                    db_pnl, wins, losses = 0.0, 0, 0
                async with db.execute(
                    f"SELECT symbol, side, entry_ts, exit_ts, entry, exit, pnl, pnl_pct, win, reason "
                    f"FROM {tables[0]} WHERE exit_ts IS NOT NULL AND exit_ts >= ? "
                    f"ORDER BY exit_ts DESC LIMIT 100", (day_ts,)) as cur:
                    rows = await cur.fetchall()
            trades_list = [{
                'symbol': r['symbol'], 'side': r['side'],
                'entry': r['entry'], 'exit': r['exit'],
                'pnl': r['pnl'], 'pnl_pct': r['pnl_pct'],
                'win': r['win'], 'reason': r['reason'],
                'exit_ts': r['exit_ts'],
            } for r in rows]
            return web.json_response({
                'pnl': db_pnl, 'wins': wins, 'losses': losses,
                'trades': trades_list,
            }, headers={'Cache-Control': 'no-store'})
        except Exception as e:
            return web.json_response({'pnl': 0, 'wins': 0, 'losses': 0, 'trades': [],
                                      'error': str(e)[:100]},
                                     headers={'Cache-Control': 'no-store'})

    async def history(request):
        try:
            async with aiosqlite.connect(bot.db.db_name) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    f"SELECT COUNT(*) as c, COALESCE(SUM(pnl),0) as s, "
                    f"COALESCE(SUM(CASE WHEN win=1 THEN pnl ELSE 0 END),0) as gw, "
                    f"COALESCE(SUM(CASE WHEN win=0 THEN ABS(pnl) ELSE 0 END),0) as gl, "
                    f"COALESCE(SUM(CASE WHEN win=1 THEN 1 ELSE 0 END),0) as w, "
                    f"COALESCE(SUM(CASE WHEN win=0 THEN 1 ELSE 0 END),0) as l "
                    f"FROM {tables[0]} WHERE exit_ts IS NOT NULL") as cur:
                    r = await cur.fetchone()
                async with db.execute(
                    f"SELECT * FROM {tables[0]} WHERE exit_ts IS NOT NULL "
                    f"ORDER BY exit_ts DESC LIMIT 200") as cur:
                    rows = await cur.fetchall()
                async with db.execute(
                    f"SELECT * FROM {tables[1]} ORDER BY date DESC LIMIT 60") as cur:
                    days = await cur.fetchall()
            total = r['c'] if r else 0
            wins = r['w'] if r else 0
            losses = r['l'] if r else 0
            wr = (wins / total * 100) if total else 0
            pf = (r['gw'] / r['gl']) if r and r['gl'] > 0 else (float('inf') if r and r['gw'] > 0 else 0)
            return web.json_response({
                'ok': True,
                'total': total, 'wins': wins, 'losses': losses,
                'wr': round(wr, 2),
                'pf': round(pf, 2) if pf != float('inf') else '∞',
                'net': r['s'] if r else 0,
                'trades': [dict(r) for r in rows],
                'days': [dict(d) for d in days],
            }, headers={'Cache-Control': 'no-store'})
        except Exception as e:
            return web.json_response({'ok': False, 'error': str(e)[:200]}, status=500)

    async def closed(request):
        return await history(request)

    async def losses(request):
        rng = (request.query.get('range', '') or '').strip()
        res = (request.query.get('result', '') or '').strip()
        out = []
        agg = {'count': 0, 'wins': 0, 'losses': 0, 'net': 0.0}
        try:
            async with aiosqlite.connect(bot.db.db_name) as db:
                db.row_factory = aiosqlite.Row
                sql = f"""SELECT symbol, side, entry_ts, exit_ts, pnl, pnl_pct, win, reason, components
                          FROM {tables[0]} WHERE win>=0"""
                args = []
                if res == 'win':
                    sql += " AND win=1 AND COALESCE(pnl,0) > 0"
                elif res == 'loss':
                    sql += " AND (win=0 OR COALESCE(pnl,0) < 0)"
                if rng == 'today':
                    sql += " AND exit_ts >= ?"; args.append(_day_start_ts())
                elif rng.isdigit():
                    sql += " AND exit_ts >= ?"; args.append(time.time() - int(rng) * 86400)
                sql += " ORDER BY exit_ts DESC LIMIT 1000"
                async with db.execute(sql, args) as cur:
                    rows = await cur.fetchall()
            for r in rows:
                out.append({
                    'symbol': r['symbol'], 'side': r['side'],
                    'entry_ts': r['entry_ts'], 'exit_ts': r['exit_ts'],
                    'pnl': r['pnl'], 'pnl_pct': r['pnl_pct'],
                    'win': r['win'], 'reason': r['reason'],
                })
                agg['count'] += 1
                if r['win']:
                    agg['wins'] += 1
                else:
                    agg['losses'] += 1
                agg['net'] += float(r['pnl'] or 0)
            return web.json_response({'ok': True, 'agg': agg, 'trades': out},
                                     headers={'Cache-Control': 'no-store'})
        except Exception as e:
            return web.json_response({'ok': False, 'error': str(e)[:200]}, status=500)

    async def chart(request):
        """يرجع بيانات شموع الزوج + خطوط الدخول/الوقف/الأهداف."""
        try:
            symbol = (request.query.get('symbol', '') or '').strip().upper()
            interval = (request.query.get('interval', '15m') or '15m').strip()
            limit = int(request.query.get('limit', '200') or 200)
            if not symbol:
                return web.json_response({'ok': False, 'error': 'symbol مطلوب'}, status=400)
            if not hasattr(bot, 'get_klines') or not bot.session:
                return web.json_response({'ok': False, 'error': 'البوت لم يبدأ بعد'}, status=503)
            limit = max(50, min(limit, 1000))
            try:
                df = await bot.get_klines(symbol, interval, limit)
            except Exception as e:
                return web.json_response({'ok': False, 'error': f'فشل جلب الشموع: {e}'}, status=502)
            if df is None or len(df) == 0:
                return web.json_response({'ok': False, 'error': 'لا بيانات شموع'}, status=404)

            candles = []
            for _, row in df.iterrows():
                candles.append({
                    'time': int(row['time'] // 1000),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row.get('volume', 0) or 0),
                })

            cur_price = None
            prices = bot.get_price(symbol)
            if prices:
                cur_price = (prices['bid'] + prices['ask']) / 2

            trade = _own_open().get(symbol)
            markers = []
            lines = []
            if trade:
                is_buy = trade['side'] == 'BUY'
                entry = float(trade['entry_price'])
                sl = float(trade['sl'] or 0) or None
                tp1 = float(trade.get('tp') or 0) or None
                tp2 = float(trade.get('tp2') or 0) or None
                tp3 = float(trade.get('tp3') or 0) or None
                entry_ts = int(float(trade.get('entry_time', time.time())))
                lines = [
                    {'id': 'entry', 'color': '#3b82f6', 'price': entry,
                     'lineStyle': 2, 'lineWidth': 2,
                     'title': f'دخول {trade["side"]}', 'axisLabelVisible': True},
                ]
                if sl:
                    lines.append({'id': 'sl', 'color': '#ef4444', 'price': sl,
                                  'lineStyle': 0, 'lineWidth': 2,
                                  'title': 'SL', 'axisLabelVisible': True})
                if tp1:
                    lines.append({'id': 'tp1', 'color': '#10b981', 'price': tp1,
                                  'lineStyle': 1, 'lineWidth': 2,
                                  'title': 'TP1 (1R)', 'axisLabelVisible': True})
                if tp2:
                    lines.append({'id': 'tp2', 'color': '#14b5cb', 'price': tp2,
                                  'lineStyle': 1, 'lineWidth': 1,
                                  'title': 'TP2 (2R)', 'axisLabelVisible': True})
                if tp3:
                    lines.append({'id': 'tp3', 'color': '#8b5cf6', 'price': tp3,
                                  'lineStyle': 1, 'lineWidth': 1,
                                  'title': 'TP3 (3R)', 'axisLabelVisible': True})
                markers.append({
                    'time': entry_ts,
                    'position': 'belowBar' if is_buy else 'aboveBar',
                    'color': '#10b981' if is_buy else '#ef4444',
                    'shape': 'arrowUp' if is_buy else 'arrowDown',
                    'text': ('شراء' if is_buy else 'بيع') + ' @ ' + f'{entry:.4g}',
                })

            return web.json_response({
                'ok': True, 'symbol': symbol, 'interval': interval,
                'candles': candles,
                'current': round(cur_price, 8) if cur_price else None,
                'lines': lines, 'markers': markers,
                'trade': {
                    'symbol': symbol, 'side': trade['side'],
                    'entry': float(trade['entry_price']),
                    'sl': float(trade['sl'] or 0) or None,
                    'tp1': float(trade.get('tp') or 0) or None,
                    'tp2': float(trade.get('tp2') or 0) or None,
                    'tp3': float(trade.get('tp3') or 0) or None,
                    'entry_ts': int(float(trade.get('entry_time', time.time()))),
                } if trade else None,
            }, headers={'Cache-Control': 'no-store'})
        except Exception as e:
            return web.json_response({'ok': False, 'error': str(e)[:200]}, status=500)

    async def enable(request):
        try:
            if await bot.resume_trading():
                await notify(m['skey'], '▶️ تفعيل التداول', 'Triple Confluence')
                return web.json_response({'ok': True, 'enabled': True},
                                         headers={'Cache-Control': 'no-store'})
            return web.json_response({'ok': False, 'error': 'تعذر التفعيل — تحقق من المفاتيح'},
                                     status=400)
        except Exception as e:
            return web.json_response({'ok': False, 'error': str(e)[:200]}, status=500)

    async def disable(request):
        try:
            await bot.pause_trading()
            await notify(m['skey'], '⏸️ إيقاف التداول', 'Triple Confluence')
            return web.json_response({'ok': True, 'enabled': False},
                                     headers={'Cache-Control': 'no-store'})
        except Exception as e:
            return web.json_response({'ok': False, 'error': str(e)[:200]}, status=500)

    async def close_all(request):
        try:
            closed_syms = await bot.close_all_trades()
            await notify(m['skey'], '🚪 إغلاق كل الصفقات',
                         ', '.join(closed_syms) if closed_syms else 'لا صفقات')
            return web.json_response({'ok': True, 'closed': closed_syms},
                                     headers={'Cache-Control': 'no-store'})
        except Exception as e:
            return web.json_response({'ok': False, 'error': str(e)[:200]}, status=500)

    async def wipe(request):
        deleted = []
        try:
            async with aiosqlite.connect(bot.db.db_name) as db:
                try:
                    cur = await db.execute(f"DELETE FROM {tables[0]} WHERE exit_ts IS NOT NULL")
                    deleted.append(f"{tables[0]}: {cur.rowcount}")
                    await cur.close()
                except Exception:
                    pass
                try:
                    cur = await db.execute(f"DELETE FROM {tables[1]}")
                    deleted.append(f"{tables[1]}: {cur.rowcount}")
                    await cur.close()
                except Exception:
                    pass
                try:
                    if PANEL_DB:
                        cur = await db.execute("DELETE FROM panel_events")
                        deleted.append(f"panel_events: {cur.rowcount}")
                        await cur.close()
                except Exception:
                    pass
                await db.commit()
        except Exception:
            pass
        await notify(m['skey'], f"🗑 مسح بيانات {m['name']}", '، '.join(deleted) or 'لا شيء')
        return web.json_response({'ok': True, 'deleted': '، '.join(deleted) or 'لا شيء'},
                                 headers={'Cache-Control': 'no-store'})

    return {'page': page, 'panel': panel, 'trades': trades, 'today': today,
            'history': history, 'closed': closed, 'losses': losses,
            'enable': enable, 'disable': disable, 'close_all': close_all,
            'wipe': wipe, 'chart': chart}


# ═══════════════════════ المعالجات المشتركة ═══════════════════════
def _shared_handlers(bot):
    def j(data, status=200):
        return web.json_response(data, status=status,
                                headers={'Cache-Control': 'no-store'})

    async def index(request):
        raise web.HTTPFound('/confluence')

    async def notifications(request):
        after = int(request.query.get('after', 0))
        if PANEL_DB:
            return j(await PANEL_DB.get_after(after))
        return j({'items': [], 'last_id': after})

    async def refresh_balance(request):
        try:
            bot._balance_cache = None
            bal = await bot.get_available_usdt()
            return j({'ok': True, 'balance': bal})
        except Exception as e:
            return j({'ok': False, 'error': str(e)[:200]}, status=500)

    async def pause(request):
        try:
            await bot.pause_trading()
            return j({'ok': True})
        except Exception as e:
            return j({'ok': False, 'error': str(e)[:200]})

    async def resume(request):
        try:
            ok = await bot.resume_trading()
            return j({'ok': ok} if ok else {'ok': False, 'error': 'تعذر الاستئناف'})
        except Exception as e:
            return j({'ok': False, 'error': str(e)[:200]})

    async def emergency(request):
        try:
            bot.EMERGENCY_MODE = True
            bot.TRADE_ENABLED = False
            closed = await bot.close_all_trades()
            return j({'ok': True, 'closed': closed})
        except Exception as e:
            return j({'ok': False, 'error': str(e)[:200]})

    async def emergency_reset(request):
        try:
            bot.EMERGENCY_MODE = False
            return j({'ok': True})
        except Exception as e:
            return j({'ok': False, 'error': str(e)[:200]})

    async def stop_bot(request):
        try:
            await bot.request_shutdown('panel')
            return j({'ok': True})
        except Exception as e:
            return j({'ok': False, 'error': str(e)[:200]})

    async def close_all(request):
        try:
            closed = await bot.close_all_trades()
            return j({'ok': True, 'closed': closed})
        except Exception as e:
            return j({'ok': False, 'error': str(e)[:200]})

    return {'index': index, 'notifications': notifications,
            'refresh_balance': refresh_balance, 'pause': pause, 'resume': resume,
            'close_all': close_all, 'emergency': emergency,
            'emergency_reset': emergency_reset, 'stop_bot': stop_bot}


# ═══════════════════════ الخادم ═══════════════════════
def create_app(bot):
    app = web.Application()
    shared = _shared_handlers(bot)

    async def health(request):
        """نقطة فحص صحة — Railway يستهدفها للتأكد من استجابة البوت."""
        return web.json_response({'ok': True, 'version': PANEL_VERSION,
                                  'uptime_s': int(time.time() - PANEL_START)})

    app.router.add_get('/health', health)
    app.router.add_get('/', shared['index'])
    app.router.add_get('/api/notifications', shared['notifications'])
    app.router.add_post('/api/balance', shared['refresh_balance'])
    app.router.add_post('/api/pause', shared['pause'])
    app.router.add_post('/api/resume', shared['resume'])
    app.router.add_post('/api/close-all', shared['close_all'])
    app.router.add_post('/api/emergency', shared['emergency'])
    app.router.add_post('/api/emergency-reset', shared['emergency_reset'])
    app.router.add_post('/api/stop-bot', shared['stop_bot'])

    for key in ('confluence',):
        h = _strategy_handlers(bot, STRATEGY_META[key])
        app.router.add_get(f'/{key}', h['page'])
        app.router.add_get(f'/api/{key}/panel', h['panel'])
        app.router.add_get(f'/api/{key}/trades', h['trades'])
        app.router.add_get(f'/api/{key}/today', h['today'])
        app.router.add_get(f'/api/{key}/history', h['history'])
        app.router.add_get(f'/api/{key}/closed', h['closed'])
        app.router.add_get(f'/api/{key}/losses', h['losses'])
        app.router.add_get(f'/api/{key}/chart', h['chart'])
        app.router.add_post(f'/api/{key}/enable', h['enable'])
        app.router.add_post(f'/api/{key}/disable', h['disable'])
        app.router.add_post(f'/api/{key}/close-all', h['close_all'])
        app.router.add_post(f'/api/{key}/wipe', h['wipe'])
    return app


async def start_web_panel(bot):
    """بدء اللوحة — مهم أن يحدث فوراً لفحص صحة Railway، حتى لو فشلت قاعدة البيانات."""
    global PANEL_DB, PANEL_PORT
    try:
        PANEL_DB = PanelDB(bot.db.db_name)
        await PANEL_DB.init()
    except Exception as e:
        print(f'⚠️ PanelDB فشل التهيئة — اللوحة تعمل بوضع محدود: {e}')
        PANEL_DB = None
    try:
        app = create_app(bot)
        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.environ.get('PORT', '10000'))
        PANEL_PORT = port
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        print(f'🖥️ اللوحة تعمل على المنفذ {port} — 🧭 /confluence و 💓 /health')
        return True
    except Exception as e:
        print(f'⚠️ فشل اللوحة (البوت يكمل عمله): {e}')
        return False
