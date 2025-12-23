import streamlit as st
import pandas as pd
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta, timezone, time as dt_time
from dataclasses import dataclass, field
import time, os, twstock, json, threading, sqlite3, concurrent.futures, requests, queue
from itertools import cycle

# ==========================================
# 1. Config & Domain Models
# ==========================================
st.set_page_config(page_title="Sniper v5.19 Classic", page_icon="🛡️", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "") 
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v60.db"
DEFAULT_WATCHLIST = "3035 3037 2368 2383 6274 8046 3189 3324 3017 3653 2421 3483 3081 3163 4979 4908 3363 4977 6442 2356 3231 2382 6669 2317 2330 2454 2303 6781 4931 3533"
DEFAULT_INVENTORY = """2330,800,1\n2317,105,5"""

@dataclass
class SniperEvent:
    code: str
    name: str
    scope: str
    event_kind: str
    event_label: str
    price: float
    pct: float
    vwap: float
    ratio: float
    net_10m: int
    net_1h: int
    net_day: int
    timestamp: float = field(default_factory=time.time)
    data_status: str = "DATA_OK"
    is_test: bool = False

# ==========================================
# 2. Market Session
# ==========================================
class MarketSession:
    MARKET_OPEN, MARKET_CLOSE = dt_time(9, 0), dt_time(13, 35)
    @staticmethod
    def is_market_open(now=None):
        if not now: now = datetime.now(timezone.utc) + timedelta(hours=8)
        return MarketSession.MARKET_OPEN <= now.time() <= MarketSession.MARKET_CLOSE

# ==========================================
# 3. Database (Async Write)
# ==========================================
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.write_queue = queue.Queue()
        self._init_db()
        threading.Thread(target=self._writer_loop, daemon=True).start()

    def _get_conn(self): 
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (code TEXT PRIMARY KEY, name TEXT, category TEXT, price REAL, pct REAL, vwap REAL, vol REAL, ratio REAL, net_1h REAL, net_10m REAL, net_day REAL, signal TEXT, update_time REAL, data_status TEXT DEFAULT 'DATA_OK', signal_level TEXT DEFAULT 'B', risk_status TEXT DEFAULT 'NORMAL')''')
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY, cost REAL, qty REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pinned (code TEXT PRIMARY KEY)''')
        # avg_vol now stores "Yesterday Volume"
        c.execute('''CREATE TABLE IF NOT EXISTS static_info (code TEXT PRIMARY KEY, win REAL, ret REAL, yoy REAL, eps REAL, pe REAL, avg_vol REAL DEFAULT 0)''')
        conn.commit(); conn.close()

    def _writer_loop(self):
        conn = self._get_conn(); cursor = conn.cursor()
        while True:
            try:
                tasks = []
                try: tasks.append(self.write_queue.get(timeout=0.1))
                except queue.Empty: continue
                while not self.write_queue.empty() and len(tasks) < 50: tasks.append(self.write_queue.get())
                for task_type, sql, args in tasks:
                    try:
                        if task_type == 'executemany': cursor.executemany(sql, args)
                        else: cursor.execute(sql, args)
                    except: pass
                conn.commit()
                for _ in tasks: self.write_queue.task_done()
            except: time.sleep(1)

    def upsert_realtime_batch(self, data_list):
        if not data_list: return
        sql = '''INSERT OR REPLACE INTO realtime (code, name, category, price, pct, vwap, vol, ratio, net_1h, net_day, signal, update_time, data_status, signal_level, risk_status, net_10m) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.write_queue.put(('executemany', sql, data_list))

    def upsert_static(self, data_list):
        sql = 'INSERT OR REPLACE INTO static_info (code, win, ret, yoy, eps, pe, avg_vol) VALUES (?, ?, ?, ?, ?, ?, ?)'
        self.write_queue.put(('executemany', sql, data_list))

    def update_pinned(self, code, is_pinned):
        if is_pinned: self.write_queue.put(('execute', 'INSERT OR IGNORE INTO pinned (code) VALUES (?)', (code,)))
        else: self.write_queue.put(('execute', 'DELETE FROM pinned WHERE code = ?', (code,)))

    def update_inventory_list(self, inventory_text):
        self.write_queue.put(('execute', 'DELETE FROM inventory', ()))
        for line in inventory_text.split('\n'):
            parts = line.split(',')
            if len(parts) >= 2:
                try: self.write_queue.put(('execute', 'INSERT OR REPLACE INTO inventory (code, cost, qty) VALUES (?, ?, ?)', (parts[0].strip(), float(parts[1].strip()), float(parts[2].strip()) if len(parts) > 2 else 1.0)))
                except: pass

    def update_watchlist(self, codes_text):
        self.write_queue.put(('execute', 'DELETE FROM watchlist', ()))
        targets = [t.strip() for t in codes_text.split() if t.strip()]
        for t in targets: self.write_queue.put(('execute', 'INSERT OR REPLACE INTO watchlist (code) VALUES (?)', (t,)))
        return targets

    def get_watchlist_view(self):
        conn = self._get_conn()
        query = '''SELECT w.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal as event_label, r.net_10m, r.net_1h, r.net_day, s.yoy, s.eps, s.pe, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM watchlist w LEFT JOIN realtime r ON w.code = r.code LEFT JOIN static_info s ON w.code = s.code LEFT JOIN pinned p ON w.code = p.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_inventory_view(self):
        conn = self._get_conn()
        query = '''SELECT i.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal as event_label, r.net_1h, r.net_day, s.yoy, s.eps, s.pe, i.cost, i.qty, (r.price - i.cost) * i.qty * 1000 as profit_val, (r.price - i.cost) / i.cost * 100 as profit_pct, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status FROM inventory i LEFT JOIN realtime r ON i.code = r.code LEFT JOIN static_info s ON i.code = s.code LEFT JOIN pinned p ON i.code = p.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_all_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code FROM inventory UNION SELECT code FROM watchlist UNION SELECT code FROM pinned')
        rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

    def get_inventory_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code FROM inventory')
        rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

    def get_volume_map(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code, avg_vol FROM static_info')
        rows = c.fetchall(); conn.close()
        return {r[0]: r[1] for r in rows if r[1] > 0}

db = Database(DB_PATH)

# ==========================================
# 4. Utilities (Classic Logic Restored)
# ==========================================
def format_number(x, decimals=2, *, pos_color="#ff4d4f", neg_color="#2ecc71", zero_color="#e0e0e0", threshold=None, threshold_color="#ff4d4f", suffix=""):
    try:
        if pd.isna(x) or x == "" or x is None: return ""
        if isinstance(x, str) and "<span" in x: return x
        val = float(str(x).replace(",", "").replace("%", ""))
        if decimals == 0: text = f"{int(val)}{suffix}"
        else: text = f"{val:.{decimals}f}{suffix}"
        if threshold is not None and val >= threshold: return f"<span style='color:{threshold_color}'>{text}</span>"
        if val > 0: return f"<span style='color:{pos_color}'>{text}</span>"
        elif val < 0: return f"<span style='color:{neg_color}'>{text}</span>"
        else: return f"<span style='color:{zero_color}'>{text}</span>"
    except: return str(x)

def fetch_yesterday_volume(client, code):
    """[CLASSIC LOGIC] Fetch Volume from Fugle Candles (D-1)"""
    try:
        # Fetch last 2 days to get yesterday
        candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
        if candles and 'data' in candles and len(candles['data']) >= 2:
            # Fugle returns shares, convert to lots (/1000)
            return int(candles['data'][-2]['volume']) // 1000
    except: pass
    return None

def fetch_fundamental_data(code):
    suffix = ".TW"
    try:
        if code in twstock.codes:
            if twstock.codes[code].market == '上櫃': suffix = ".TWO"
    except: pass
    try:
        ticker = yf.Ticker(f"{code}{suffix}")
        info = ticker.info
        if info and 'symbol' in info:
            yoy = (info.get('revenueGrowth', 0) or 0) * 100
            eps = info.get('trailingEps', 0) or 0
            pe = info.get('trailingPE', 0) or 0
            # [NOTE] We don't fetch avg_vol here anymore, we use Fugle in Engine
            return yoy, eps, pe
    except: pass
    return 0, 0, 0

def get_stock_name(symbol):
    try: return twstock.codes[symbol].name if symbol in twstock.codes else symbol
    except: return symbol

def get_dynamic_thresholds(price):
    if price < 50: return 3.5, 2.5
    elif price < 300: return 2.5, 1.5
    else: return 2.0, 1.2

def _calc_est_vol(current_vol):
    """
    [CLASSIC LOGIC] Linear Projection
    Raw volume (shares) -> Estimated daily volume (shares)
    """
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    
    if now < market_open: return 0 
    
    elapsed_minutes = (now - market_open).seconds / 60
    
    if elapsed_minutes <= 0: return 0
    if elapsed_minutes >= 270: return current_vol # Market closed

    # Simple Linear Projection (Old School)
    return int(current_vol * (270 / elapsed_minutes))

def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown, price, vwap, has_attacked):
    if pct >= 9.5: return "漲停"
    if ratio > 0:
        if (ratio >= 10.0) and (abs(price - vwap) / vwap <= 0.01) and (pct <= 2.0) and (net_1h > 0) and (not has_attacked): return "伏擊"
        if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: return "攻擊"
        if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200: return "量增"
        if is_breakdown and ratio >= tgt_ratio and net_1h < 0: return "出貨"
    if pct > 2.0 and net_1h < 0: return "誘多"
    if is_bullish and pct >= tgt_pct: return "價強"
    return "盤整"

# ==========================================
# 5. Notification
# ==========================================
class NotificationManager:
    COOLDOWN_SECONDS = 600
    RATE_LIMIT_DELAY = 1.0
    EMOJI_MAP = {"攻擊": "🚀", "伏擊": "💣", "量增": "👀", "出貨": "💀", "跌破": "⚠️", "漲停": "👑"}

    def __init__(self):
        self._queue = queue.Queue()
        self._cooldowns = {}
        threading.Thread(target=self._worker_loop, daemon=True).start()

    def reset_daily_state(self):
        self._cooldowns.clear()

    def should_notify(self, event: SniperEvent) -> bool:
        if event.is_test: return True
        if not MarketSession.is_market_open(): return False
        key = f"{event.code}_{event.scope}_{event.event_label}"
        if time.time() - self._cooldowns.get(key, 0) < self.COOLDOWN_SECONDS: return False
        return True

    def enqueue(self, event: SniperEvent):
        if self.should_notify(event):
            if not event.is_test: self._cooldowns[f"{event.code}_{event.scope}_{event.event_label}"] = time.time()
            self._queue.put(event)

    def _worker_loop(self):
        while True:
            event = self._queue.get()
            try:
                self._send_telegram(event)
                time.sleep(self.RATE_LIMIT_DELAY)
            except: pass
            finally: self._queue.task_done()

    def _send_telegram(self, event: SniperEvent):
        if not TG_BOT_TOKEN or not TG_CHAT_ID: return
        emoji = self.EMOJI_MAP.get(event.event_label, "📌")
        up_dn = "UP" if event.pct >= 0 else "DN"
        market_label = "上市"
        try:
            if event.code in twstock.codes: market_label = twstock.codes[event.code].market
        except: pass
        msg = (f"<b>{emoji} {event.event_label}｜{event.code} {event.name} ({market_label})</b>\n"
               f"現價：{event.price:.2f} ({event.pct:.2f}% {up_dn})　均價：{event.vwap:.2f}\n"
               f"大戶10M：{event.net_10m}　大戶1H：{event.net_1h}　大戶(日)：{event.net_day}")
        buttons = [[{"text": "📈 TradingView", "url": f"https://www.tradingview.com/chart/?symbol=TWSE%3A{event.code}&interval=1"},
                    {"text": "📊 Yahoo", "url": f"https://tw.stock.yahoo.com/quote/{event.code}.TW"}]]
        try: requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "reply_markup": json.dumps({"inline_keyboard": buttons})}, timeout=5)
        except: pass

notification_manager = NotificationManager()

# ==========================================
# 6. Engine (Classic Volume Logic)
# ==========================================
class SniperEngine:
    def __init__(self):
        self.running = False
        self.clients = [RestClient(api_key=k) for k in API_KEYS] if API_KEYS else []
        self.client_cycle = cycle(self.clients) if self.clients else None
        self.targets = []
        self.inventory_codes = []
        self.yesterday_vol = {} # Stores YESTERDAY'S volume
        self.daily_net = {}
        self.vol_queues = {}
        self.prev_data = {}
        self.active_flags = {}
        self.daily_risk_flags = {}
        self.last_reset = datetime.now().date()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)

    def update_targets(self):
        self.targets = db.get_all_codes()
        self.inventory_codes = db.get_inventory_codes()
        # Note: We fetch yesterday's vol dynamically in _fetch_stock, or pre-load here if needed.
        # For simplicity and robustness, we lazy load in _fetch_stock

    def start(self):
        if self.running: return
        self.update_targets()
        self.running = True
        threading.Thread(target=self._run_loop, daemon=True).start()

    def stop(self): self.running = False

    def _dispatch_event(self, ev: SniperEvent):
        notification_manager.enqueue(ev)

    def _fetch_stock(self, code):
        try:
            # 1. Get Client
            client = next(self.client_cycle) if self.client_cycle else None
            if not client: return None
            
            # 2. Lazy Load Yesterday's Volume (Classic Logic)
            if code not in self.yesterday_vol:
                y_vol = fetch_yesterday_volume(client, code)
                if y_vol: self.yesterday_vol[code] = y_vol
                else: self.yesterday_vol[code] = 1000 # Fallback 1000 if fails
            
            base_vol = self.yesterday_vol.get(code, 1000)

            # 3. Get Realtime Quote
            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', 0)
            if not price: return None
            
            pct = q.get('changePercent', 0)
            vol_shares = q.get('total', {}).get('tradeVolume', 0)
            vol = vol_shares * 1000 # To shares? No, volume is usually lots in Fugle quote? Wait. 
            # Correction: Fugle 'tradeVolume' is usually lots. 'tradeValue' is total value.
            # Let's align units:
            # base_vol is in LOTS (shares // 1000).
            # q['total']['tradeVolume'] is usually LOTS.
            # So ratio = est_lots / base_lots.
            
            # Re-read legacy code: 
            # daily_vol = row['Volume'] -> Yahoo daily is shares? Fugle candles is shares.
            # q.get('total').get('tradeVolume') is LOTS.
            
            current_lots = q.get('total', {}).get('tradeVolume', 0)
            est_lots = _calc_est_vol(current_lots)
            
            ratio = est_lots / base_vol if base_vol > 0 else 0
            
            # VWAP Calculation
            # tradeValue is total TWD. tradeVolume is lots.
            # VWAP = Value / (Volume * 1000)
            total_val = q.get('total', {}).get('tradeValue', 0)
            vwap = (total_val / (current_lots * 1000)) if current_lots > 0 else price
            
            # Big Player Logic (Legacy)
            delta_net = 0
            if code in self.prev_data:
                prev_v = self.prev_data[code]['vol']
                prev_p = self.prev_data[code]['price']
                # Delta Volume (Lots)
                delta_v = current_lots - prev_v
                
                threshold = int(400/price) if price > 0 else 5
                threshold = max(1, threshold)
                
                if delta_v >= threshold:
                    if price >= prev_p: delta_net = int(delta_v)
                    elif price < prev_p: delta_net = -int(delta_v)
            
            self.prev_data[code] = {'vol': current_lots, 'price': price}
            
            now_ts = time.time()
            if code not in self.vol_queues: self.vol_queues[code] = []
            if delta_net != 0: self.vol_queues[code].append((now_ts, delta_net))
            
            # Clean old queue
            self.daily_net[code] = self.daily_net.get(code, 0) + delta_net
            self.vol_queues[code] = [x for x in self.vol_queues[code] if x[0] > now_ts - 3600]
            net_1h = sum(x[1] for x in self.vol_queues[code])
            net_10m = sum(x[1] for x in self.vol_queues[code] if x[0] > now_ts - 600)
            net_day = self.daily_net.get(code, 0)
            
            # Signal Check
            tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
            raw_state = check_signal(pct, price >= vwap, net_day, net_1h, ratio, tgt_pct, tgt_ratio, price < vwap*0.99, price, vwap, code in self.active_flags)
            
            event_label = None
            scope = "inventory" if code in self.inventory_codes else "watchlist"
            
            if "攻擊" in raw_state and code not in self.active_flags: event_label = "攻擊"
            elif "伏擊" in raw_state: event_label = "伏擊"
            elif "出貨" in raw_state and code not in self.daily_risk_flags and scope == "inventory": event_label = "出貨"

            if event_label:
                self.active_flags[code] = True
                if "出貨" in event_label: self.daily_risk_flags[code] = True
                ev = SniperEvent(
                    code=code, name=get_stock_name(code), scope=scope, 
                    event_kind="STRATEGY", event_label=event_label, 
                    price=price, pct=pct, vwap=vwap, ratio=ratio, 
                    net_10m=net_10m, net_1h=net_1h, net_day=net_day
                )
                self._dispatch_event(ev)

            return (code, get_stock_name(code), "一般", price, pct, vwap, current_lots, ratio, net_1h, net_day, raw_state, now_ts, "DATA_OK", "B", "NORMAL", net_10m)
        except: return None

    def _run_loop(self):
        while self.running:
            now = datetime.now(timezone.utc) + timedelta(hours=8)
            if now.date() > self.last_reset:
                self.active_flags = {}
                self.daily_risk_flags = {}
                self.daily_net = {}
                self.prev_data = {}
                self.vol_queues = {}
                self.yesterday_vol = {} # Reset base vol daily
                notification_manager.reset_daily_state()
                self.last_reset = now.date()
            
            targets = db.get_all_codes()
            self.inventory_codes = db.get_inventory_codes()
            
            if not targets: time.sleep(2); continue
            
            batch = []
            futures = [self.executor.submit(self._fetch_stock, c) for c in targets]
            for f in concurrent.futures.as_completed(futures):
                if f.result(): batch.append(f.result())
            db.upsert_realtime_batch(batch)
            time.sleep(3 if MarketSession.is_market_open(now) else 10)

if "sniper_engine_core" not in st.session_state:
    st.session_state.sniper_engine_core = SniperEngine()
engine = st.session_state.sniper_engine_core

# ==========================================
# 7. UI (Layout & Fragments)
# ==========================================
class LegacyDispatcher:
    def dispatch(self, event_dict):
        ev = SniperEvent(
            code=event_dict['code'], name=event_dict['name'], scope=event_dict['scope'],
            event_kind=event_dict.get('event_kind', 'TEST'), event_label=event_dict['event_label'], 
            price=event_dict['price'], pct=event_dict['pct'], vwap=event_dict.get('vwap', 0), 
            ratio=event_dict['ratio'], net_1h=event_dict['net_1h'], net_10m=event_dict['net_10m'], 
            net_day=event_dict['net_day'], timestamp=event_dict['timestamp'], is_test=event_dict.get('is_test', False)
        )
        notification_manager.enqueue(ev)

dispatcher = LegacyDispatcher()

with st.sidebar:
    st.title("⚙️ 戰情室 v5.19")
    mode = st.radio("身分模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
    st.subheader("🔍 濾網設定")
    use_filter = st.checkbox("只看基本面良好")
    
    if mode == "👨‍✈️ 指揮官":
        with st.expander("📦 庫存管理 (Inventory)", expanded=False):
            inv_input = st.text_area("庫存清單 (代碼,成本,張數)", DEFAULT_INVENTORY, height=100)
            if st.button("更新庫存"):
                db.update_inventory_list(inv_input)
                time.sleep(0.5)
                engine.update_targets()
                st.toast("庫存已更新！")
                st.rerun()

        with st.expander("🔭 監控設定 (Watchlist)", expanded=True):
            raw_input = st.text_area("新選清單", DEFAULT_WATCHLIST, height=150)
            if st.button("1. 初始化並更新清單", type="primary"):
                if not API_KEYS: st.error("缺 API Key")
                else:
                    db.update_watchlist(raw_input)
                    time.sleep(0.5)
                    engine.update_targets()
                    targets = engine.targets
                    status = st.status("正在初始化數據 (基本面)...", expanded=True)
                    static_list = []
                    progress_bar = status.progress(0)
                    for i, code in enumerate(targets):
                        yoy, eps, pe = fetch_fundamental_data(code)
                        # Static info no longer holds avg_vol, set 0
                        static_list.append((code, 0, 0, yoy, eps, pe, 0))
                        progress_bar.progress((i + 1) / len(targets))
                    db.upsert_static(static_list)
                    status.update(label="初始化完成！", state="complete")
                    st.rerun()
            
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🟢 啟動監控", disabled=engine.running):
                    engine.start()
                    st.toast("核心已啟動")
                    st.rerun()
            with col_b:
                if st.button("🔴 停止監控", disabled=not engine.running):
                    engine.stop()
                    st.toast("核心已停止")
                    st.rerun()

    st.markdown("---")
    st.caption(f"Engine: {'🟢 RUNNING' if engine.running else '🔴 STOPPED'}")
    
    st.subheader("🧪 系統測試")
    if st.button("🔥 測試攻擊"):
        dispatcher.dispatch({
            "code": "2330", "name": "台積電 (測試)", "scope": "watchlist", 
            "event_kind": "TEST", "event_label": "攻擊",  
            "price": 888.0, "pct": 3.5, "vwap": 870.0, "ratio": 2.5, "net_10m": 150, "net_1h": 500, "net_day": 1200, 
            "timestamp": time.time(), "is_test": True
        })
        st.toast("測試訊號已發送")

# --- Safe Fragment Fallback ---
try:
    from streamlit import fragment
except ImportError:
    def fragment(run_every=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if run_every:
                    if "last_frag_run" not in st.session_state: st.session_state.last_frag_run = time.time()
                    if time.time() - st.session_state.last_frag_run >= run_every:
                        st.session_state.last_frag_run = time.time()
                        st.rerun()
                return func(*args, **kwargs)
            return wrapper
        return decorator

@fragment(run_every=1.5)
def render_live_dashboard():
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    st.caption(f"⚡ Live Refresh: {now.strftime('%H:%M:%S')} (Rate: 1.5s)")
    
    # --- Part 1: Inventory (Top - Editor Mode) ---
    st.subheader("📦 庫存損益")
    df_inv = db.get_inventory_view()
    if not df_inv.empty:
        df_inv = df_inv.rename(columns={'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 'code': '代碼', 'name': '名稱', 'event_label': '訊號', 'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE', 'cost': '成本', 'profit_val': '損益$', 'profit_pct': '報酬%', 'risk_status': '狀態'})
        cols = ['代碼', '名稱', '狀態', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE', '成本', '損益$', '報酬%']
        for c in cols: 
            if c not in df_inv.columns: df_inv[c] = 0
        df_inv_show = df_inv[cols].copy()
        
        for col in ['營收YoY', 'EPS', 'PE']:
            df_inv_show[col] = df_inv_show[col].fillna(0)

        st.data_editor(
            df_inv_show,
            column_config={
                "代碼": st.column_config.TextColumn("代碼", width="small", pinned=True),
                "名稱": st.column_config.TextColumn("名稱", pinned=True),
                "狀態": st.column_config.TextColumn("狀態", width="small"),
                "成本": st.column_config.NumberColumn("成本", format="%.2f"),
                "現價": st.column_config.NumberColumn("現價", format="%.2f"),
                "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                "均價": st.column_config.NumberColumn("均價", format="%.2f"),
                "量比": st.column_config.NumberColumn("量比", format="%.1f"),
                "損益$": st.column_config.NumberColumn("損益$", format="%d"),
                "報酬%": st.column_config.NumberColumn("報酬%", format="%.2f%%"),
                "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"),
                "EPS": st.column_config.NumberColumn("EPS", format="%.2f"),
                "PE": st.column_config.NumberColumn("PE", format="%.1f")
            },
            use_container_width=True, hide_index=True, disabled=True, key="inv_table_live"
        )
    else: st.info("尚無庫存資料")

    st.markdown("---")

    # --- Part 2: Watchlist (Bottom - HTML Render Mode) ---
    st.subheader("🔭 監控雷達")
    df_watch = db.get_watchlist_view()
    if not df_watch.empty:
        df_watch['Pinned'] = df_watch['is_pinned'].astype(bool)
        
        for col in ['net_10m', 'net_1h', 'net_day']:
            if col not in df_watch.columns: df_watch[col] = 0
            df_watch[col] = df_watch[col].fillna(0)

        df_watch['yoy'] = df_watch['yoy'].fillna(0)
        df_watch['eps'] = df_watch['eps'].fillna(0)
        
        if use_filter: 
            df_watch = df_watch[(df_watch['yoy'] > 0) & (df_watch['eps'] > 0) & (df_watch['pe'].notna()) & (df_watch['pe'] < 50)]
        
        df_watch = df_watch.rename(columns={'event_label': '訊號', 'code': '代碼', 'name': '名稱', 'price': '現價', 'pct': '漲跌%', 'vwap': '均價', 'ratio': '量比', 'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE', 'signal_level': '等級'})
        
        df_watch['現價'] = df_watch['現價'].apply(lambda x: format_number(x, decimals=2, pos_color="#e0e0e0"))
        df_watch['均價'] = df_watch['均價'].apply(lambda x: format_number(x, decimals=2, pos_color="#e0e0e0"))
        df_watch['漲跌%'] = df_watch['漲跌%'].apply(lambda x: format_number(x, decimals=2, suffix="%"))
        df_watch['量比'] = df_watch['量比'].apply(
            lambda x: format_number(x, decimals=1, threshold=10, pos_color="#cccccc") if float(str(x or 0).replace(',','')) > 0 else "<span style='color:#cccccc'>-</span>"
        )
        df_watch['PE'] = df_watch['PE'].apply(lambda x: format_number(x, decimals=1))
        
        def format_big_player(row):
            if row['net_10m'] == 0 and row['net_1h'] == 0 and row['net_day'] == 0:
                return "<span style='color:#e0e0e0'>-- / -- / --</span>"
            return f"{format_number(row['net_10m'], decimals=0)} / {format_number(row['net_1h'], decimals=0)} / {format_number(row['net_day'], decimals=0)}"
        
        df_watch['大戶'] = df_watch.apply(format_big_player, axis=1)
        df_watch['📌'] = df_watch['Pinned'].apply(lambda x: "📌" if x else "")

        cols_html = ['📌', '代碼', '名稱', '等級', '現價', '漲跌%', '均價', '量比', '訊號', '大戶', '營收YoY', 'EPS', 'PE']
        df_html = df_watch[cols_html].copy()
        
        st.markdown("""
            <style>
            table.custom-table { width: 100%; border-collapse: collapse; }
            table.custom-table th { text-align: left; background-color: #262730; color: white; padding: 8px; font-size: 14px; }
            table.custom-table td { padding: 8px; border-bottom: 1px solid #444; font-size: 14px; }
            table.custom-table tr:hover { background-color: #FFF3BF; }
            </style>
        """, unsafe_allow_html=True)

        html_str = df_html.to_html(escape=False, index=False, classes="custom-table")
        st.markdown(html_str, unsafe_allow_html=True)

    else: st.info("尚無監控資料")

# Render the fragment
render_live_dashboard()



