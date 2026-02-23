import streamlit as st
import pandas as pd
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta, timezone, time as dt_time
from dataclasses import dataclass, field
import time, os, twstock, json, threading, sqlite3, concurrent.futures, requests, queue
from itertools import cycle
import warnings
import logging
import math

# [LOG FIX]
warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 0. Global Config v7.0.1 (Fix Null Pointer & HTML Render)
# ==========================================
st.set_page_config(page_title="Sniper v7.0.1 FirstPrinciple", page_icon="⚔️", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v70.db"

DEFAULT_WATCHLIST = "3006 3037 1513 3189 1795 3491 8046 6274"
DEFAULT_INVENTORY = """2481,84.4,3,MOM\n3231,150.14,7,FUND"""

@dataclass
class SniperEvent:
    code: str
    name: str
    scope: str
    event_label: str
    price: float
    pct: float
    vwap: float
    divergence: float
    bpe: float
    ratio: float
    net_10m: int
    net_1h: int
    net_day: int
    stock_type: str
    vwap_test_passed: bool
    daily_res: bool
    timestamp: float = field(default_factory=time.time)

# ==========================================
# 1. Market Session & Utils
# ==========================================
class MarketSession:
    MARKET_OPEN, MARKET_CLOSE = dt_time(9, 0), dt_time(13, 35)
    @staticmethod
    def is_market_open(now=None):
        if not now: now = datetime.now(timezone.utc) + timedelta(hours=8)
        return MarketSession.MARKET_OPEN <= now.time() <= MarketSession.MARKET_CLOSE

def get_tick_size(price):
    if price < 10: return 0.01
    elif price < 50: return 0.05
    elif price < 100: return 0.1
    elif price < 500: return 0.5
    elif price < 1000: return 1.0
    else: return 5.0

def adjust_to_tick(price, method='floor'):
    tick = get_tick_size(price)
    if method == 'round': return round(price / tick) * tick
    else: return math.floor(price / tick) * tick

# ==========================================
# 2. Database v7.0
# ==========================================
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.write_queue = queue.Queue()
        self._init_db()
        threading.Thread(target=self._writer_loop, daemon=True).start()

    def _get_conn(self): return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (code TEXT PRIMARY KEY, name TEXT, price REAL, pct REAL, vwap REAL, vol REAL, est_vol REAL, ratio REAL, net_10m REAL, net_1h REAL, net_day REAL, signal TEXT, update_time REAL, ratio_yest REAL, divergence REAL DEFAULT 0, bpe REAL DEFAULT 0, morning_high REAL DEFAULT 0, vwap_test INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY, cost REAL, qty REAL, strategy TEXT DEFAULT 'MOM')''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS static_info (code TEXT PRIMARY KEY, vol_5ma REAL, vol_yest REAL, price_5ma REAL, stock_type TEXT DEFAULT 'MOM', daily_res INTEGER DEFAULT 0, prev_close REAL DEFAULT 0)''')
        
        for col, def_val in [('divergence', 'REAL DEFAULT 0'), ('bpe', 'REAL DEFAULT 0'), ('morning_high', 'REAL DEFAULT 0'), ('vwap_test', 'INTEGER DEFAULT 0')]:
            try: c.execute(f"ALTER TABLE realtime ADD COLUMN {col} {def_val}")
            except: pass
        for col, def_val in [('stock_type', "TEXT DEFAULT 'MOM'"), ('daily_res', 'INTEGER DEFAULT 0'), ('prev_close', 'REAL DEFAULT 0')]:
            try: c.execute(f"ALTER TABLE static_info ADD COLUMN {col} {def_val}")
            except: pass
        try: c.execute("ALTER TABLE inventory ADD COLUMN strategy TEXT DEFAULT 'MOM'")
        except: pass
        
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
        sql = '''INSERT OR REPLACE INTO realtime (code, name, price, pct, vwap, vol, est_vol, ratio, net_1h, net_day, signal, update_time, net_10m, ratio_yest, divergence, bpe, morning_high, vwap_test) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.write_queue.put(('executemany', sql, data_list))

    def upsert_static(self, data_list):
        sql = 'INSERT OR REPLACE INTO static_info (code, vol_5ma, vol_yest, price_5ma, stock_type, daily_res, prev_close) VALUES (?, ?, ?, ?, ?, ?, ?)'
        self.write_queue.put(('executemany', sql, data_list))

    def update_inventory_list(self, inventory_text):
        self.write_queue.put(('execute', 'DELETE FROM inventory', ()))
        for line in inventory_text.split('\n'):
            parts = line.split(',')
            if len(parts) >= 2:
                try:
                    code = parts[0].strip()
                    cost = float(parts[1].strip())
                    qty = float(parts[2].strip()) if len(parts) > 2 else 1.0
                    strat = parts[3].strip().upper() if len(parts) > 3 else "MOM"
                    self.write_queue.put(('execute', 'INSERT OR REPLACE INTO inventory (code, cost, qty, strategy) VALUES (?, ?, ?, ?)', (code, cost, qty, strat)))
                except: pass

    def update_watchlist(self, codes_text):
        self.write_queue.put(('execute', 'DELETE FROM watchlist', ()))
        targets = [t.strip() for t in codes_text.split() if t.strip()]
        for t in targets: self.write_queue.put(('execute', 'INSERT OR REPLACE INTO watchlist (code) VALUES (?)', (t,)))
        return targets

    def get_watchlist_view(self):
        conn = self._get_conn()
        query = '''SELECT w.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal, r.net_10m, r.net_1h, r.net_day, r.divergence, r.bpe, r.morning_high, r.vwap_test, s.stock_type, s.daily_res, s.prev_close FROM watchlist w LEFT JOIN realtime r ON w.code = r.code LEFT JOIN static_info s ON w.code = s.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_inventory_view(self):
        conn = self._get_conn()
        query = '''SELECT i.code, r.name, r.pct, r.price, r.vwap, i.cost, i.qty, i.strategy as stock_type, r.signal, r.divergence, r.bpe, (r.price - i.cost) * i.qty * 1000 as profit_val, (r.price - i.cost) / i.cost * 100 as profit_pct FROM inventory i LEFT JOIN realtime r ON i.code = r.code LEFT JOIN static_info s ON i.code = s.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_all_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code FROM inventory UNION SELECT code FROM watchlist')
        rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

    def get_inventory_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code, strategy FROM inventory')
        rows = c.fetchall(); conn.close(); return {r[0]: r[1] for r in rows}

    def get_static_map(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code, vol_5ma, vol_yest, stock_type, daily_res, prev_close FROM static_info')
        rows = c.fetchall(); conn.close()
        return {r[0]: {'vol_5ma': r[1], 'vol_yest': r[2], 'stock_type': r[3], 'daily_res': bool(r[4]), 'prev_close': r[5]} for r in rows}

db = Database(DB_PATH)

# ==========================================
# 3. Static Data & FinMind API (Fix NoneType)
# ==========================================
def fetch_finmind_fund_factor(code):
    stock_type = 'MOM'
    try:
        ym_start = (datetime.now() - timedelta(days=60)).strftime('%Y-%m')
        url_rev = f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockMonthRevenue&data_id={code}&start_date={ym_start}"
        res_rev = requests.get(url_rev, timeout=5).json()
        yoy_ok = False
        if res_rev.get('data'):
            latest_rev = res_rev['data'][-1]
            yoy_val = latest_rev.get('RevenueYearOnYear')
            if yoy_val is not None and float(yoy_val) > 15.0: 
                yoy_ok = True
        
        d_start = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
        url_inst = f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockInstitutionalInvestorsBuySell&data_id={code}&start_date={d_start}"
        res_inst = requests.get(url_inst, timeout=5).json()
        inst_ok = False
        if res_inst.get('data'):
            df_inst = pd.DataFrame(res_inst['data'])
            df_target = df_inst[df_inst['name'].isin(['Foreign_Investor', 'Investment_Trust'])]
            if not df_target.empty:
                daily_net = df_target.groupby('date')['buy'].sum() - df_target.groupby('date')['sell'].sum()
                buy_days = (daily_net.tail(5) > 0).sum()
                if buy_days >= 3: inst_ok = True

        if yoy_ok and inst_ok: stock_type = 'FUND'
    except: pass
    return stock_type

def fetch_static_stats(code):
    try:
        suffix = ".TW"
        if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
        hist = yf.Ticker(f"{code}{suffix}").history(period="15d", auto_adjust=True)
        
        vol_5ma, vol_yest, price_5ma, prev_close = 0, 0, 0, 0
        daily_res = False
        
        if not hist.empty and len(hist) >= 6:
            prev_close = float(hist['Close'].iloc[-1])
            vol_yest = int(hist['Volume'].iloc[-1]) // 1000
            last_5 = hist.iloc[-6:-1]
            vol_5ma = int(last_5['Volume'].mean()) // 1000
            price_5ma = float(last_5['Close'].mean())
            
            for _, row in last_5.iterrows():
                if row['Close'] < row['Open'] and (row['Volume'] // 1000) > (vol_5ma * 1.5):
                    daily_res = True; break

        stock_type = fetch_finmind_fund_factor(code)
        return vol_5ma, vol_yest, price_5ma, stock_type, int(daily_res), prev_close
    except: return 0, 0, 0, 'MOM', 0, 0

# ==========================================
# 4. Notification Manager
# ==========================================
class NotificationManager:
    COOLDOWN = 1800 

    def __init__(self):
        self._queue = queue.Queue()
        self._cooldowns = {}
        threading.Thread(target=self._worker_loop, daemon=True).start()

    def reset(self): self._cooldowns.clear()

    def should_notify(self, ev: SniperEvent) -> bool:
        if ev.event_label == "⚖️觀望": return False
        
        key = f"{ev.code}_{ev.event_label}"
        if time.time() - self._cooldowns.get(key, 0) < self.COOLDOWN: return False
        return True

    def enqueue(self, ev: SniperEvent):
        if self.should_notify(ev):
            self._cooldowns[f"{ev.code}_{ev.event_label}"] = time.time()
            self._queue.put(ev)

    def _worker_loop(self):
        while True:
            ev = self._queue.get()
            try: self._send_telegram(ev); time.sleep(1)
            except: pass
            finally: self._queue.task_done()

    def _send_telegram(self, ev: SniperEvent):
        if not TG_BOT_TOKEN or not TG_CHAT_ID: return
        icon = "🛡️FUND" if ev.stock_type == "FUND" else "⚡MOM"
        
        msg = (f"<b>{ev.event_label}｜{ev.code} {ev.name} ({icon})</b>\n"
               f"現價：{ev.price:.2f} ({ev.pct:+.2f}%)　均價：{ev.vwap:.2f}\n"
               f"🎯 乖離率：{ev.divergence:+.2f}%\n"
               f"⚡ 推升力：BPE {ev.bpe:.1f}\n"
               f"📊 狀態：引力{'✔️' if ev.vwap_test_passed else '❌'} / 套牢壓{'❌' if ev.daily_res else '✔️'}\n"
               f"💰 大戶(10m/1H/日)：{ev.net_10m:+} / {ev.net_1h:+} / {ev.net_day:+}")
        
        buttons = [[{"text": "📈 開啟 Yahoo", "url": f"https://tw.stock.yahoo.com/quote/{ev.code}.TW"}]]
        try: requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "reply_markup": json.dumps({"inline_keyboard": buttons})}, timeout=5)
        except: pass

nm = NotificationManager()

# ==========================================
# 5. Core Engine v7.0
# ==========================================
class SniperEngine:
    def __init__(self):
        self.running = False
        self.clients = [RestClient(api_key=k) for k in API_KEYS] if API_KEYS else []
        self.client_cycle = cycle(self.clients) if self.clients else None
        
        self.static_cache = {}
        self.inv_cache = {}
        self.prev_data = {}
        self.vol_queues = {}
        self.price_history_10m = {} 
        self.morning_high_cache = {}
        self.vwap_test_cache = {}
        self.daily_net = {}
        self.active_flags = {}
        self.last_reset = datetime.now().date()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=12)

    def update_targets(self):
        self.static_cache = db.get_static_map()
        self.inv_cache = db.get_inventory_codes()

    def start(self):
        if self.running: return
        self.update_targets()
        self.running = True
        threading.Thread(target=self._run_loop, daemon=True).start()

    def stop(self): self.running = False

    def _calc_est_vol(self, current_vol):
        now = datetime.now(timezone.utc) + timedelta(hours=8)
        elapsed = (now - now.replace(hour=9, minute=0, second=0, microsecond=0)).seconds / 60
        if elapsed <= 0: return 0
        if elapsed >= 270: return current_vol
        return int(current_vol * (270 / elapsed))

    def _fetch_stock(self, code, now_time):
        try:
            client = next(self.client_cycle) if self.client_cycle else None
            if not client: return None

            static = self.static_cache.get(code, {})
            stock_type = self.inv_cache.get(code, static.get('stock_type', 'MOM'))
            daily_res = static.get('daily_res', False)
            prev_close = static.get('prev_close', 0)

            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', 0)
            if not price: return None

            pct = q.get('changePercent', 0)
            vol_lots = q.get('total', {}).get('tradeVolume', 0)
            if vol_lots == 0 and code in self.prev_data: vol_lots = self.prev_data[code]['vol']
            
            vol = vol_lots * 1000
            est_lots = self._calc_est_vol(vol_lots)
            ratio_5ma = est_lots / static.get('vol_5ma', 1) if static.get('vol_5ma', 0) > 0 else 0
            vwap = (q.get('total', {}).get('tradeValue', 0) / vol) if vol > 0 else price
            divergence = ((price - vwap) / vwap * 100) if vwap > 0 else 0

            delta_net = 0
            if code in self.prev_data:
                prev_v = self.prev_data[code]['vol']; delta_v = vol_lots - prev_v
                if delta_v > 0:
                    prev_p = self.prev_data[code]['price']
                    if price >= prev_p: delta_net = int(delta_v)
                    else: delta_net = -int(delta_v)

            self.prev_data[code] = {'vol': vol_lots, 'price': price}
            now_ts = time.time()
            
            if code not in self.vol_queues: self.vol_queues[code] = []
            if code not in self.price_history_10m: self.price_history_10m[code] = []
            
            if delta_net != 0: self.vol_queues[code].append((now_ts, delta_net))
            self.price_history_10m[code].append((now_ts, price))
            
            self.vol_queues[code] = [x for x in self.vol_queues[code] if x[0] > now_ts - 3600]
            self.price_history_10m[code] = [x for x in self.price_history_10m[code] if x[0] > now_ts - 600]

            self.daily_net[code] = self.daily_net.get(code, 0) + delta_net
            net_10m = sum(x[1] for x in self.vol_queues[code] if x[0] > now_ts - 600)
            net_1h = sum(x[1] for x in self.vol_queues[code])

            t_obj = now_time.time()
            if t_obj <= dt_time(9, 30):
                if price > self.morning_high_cache.get(code, 0):
                    self.morning_high_cache[code] = price
            m_high = self.morning_high_cache.get(code, 0)

            if t_obj >= dt_time(10, 0):
                if abs(divergence) < 0.5 and ratio_5ma < 1.2:
                    self.vwap_test_cache[code] = True
            v_test = self.vwap_test_cache.get(code, False)

            bpe = 0.0
            if net_10m > 50 and len(self.price_history_10m[code]) > 0:
                price_10m_ago = self.price_history_10m[code][0][1]
                delta_ticks = (price - price_10m_ago) / get_tick_size(price)
                bpe = round(delta_ticks / (net_10m / 100), 2)

            signal = ""
            scope = "inventory" if code in self.inv_cache else "watchlist"
            
            if scope == "inventory":
                if stock_type == "MOM" and price < prev_close and t_obj > dt_time(9, 0):
                    signal = "🚨緊急撤退" 
                elif price < vwap and net_10m < 0:
                    signal = "🚨緊急撤退" 
                elif bpe < 0.2 and net_10m > 100 and pct > 2:
                    signal = "🚨緊急撤退" 
            
            if not signal and scope == "watchlist":
                if dt_time(10, 30) <= t_obj <= dt_time(13, 0):
                    if price > m_high and divergence < 3.0 and bpe > 0.5 and v_test and not daily_res and net_1h > 0:
                        signal = "📥狙擊建倉"
                
                elif dt_time(13, 0) <= t_obj <= dt_time(13, 25):
                    if price > vwap and net_1h > 0 and pct < 8.5 and not daily_res:
                        signal = "🔥尾盤留倉"
                
                if stock_type == "FUND" and pct < 0 and divergence < 0.5 and bpe > 0:
                    signal = "💣錯殺伏擊"

            if signal and signal != self.active_flags.get(code):
                self.active_flags[code] = signal
                ev = SniperEvent(code, get_stock_name(code), scope, signal, price, pct, vwap, divergence, bpe, ratio_5ma, net_10m, net_1h, self.daily_net.get(code,0), stock_type, v_test, daily_res)
                nm.enqueue(ev)

            return (code, get_stock_name(code), price, pct, vwap, vol_lots, est_lots, ratio_5ma, net_1h, self.daily_net.get(code,0), signal, now_ts, net_10m, 0, divergence, bpe, m_high, int(v_test))
        except Exception as e: return None

    def _run_loop(self):
        while self.running:
            try:
                now = datetime.now(timezone.utc) + timedelta(hours=8)
                if now.date() > self.last_reset:
                    self.active_flags.clear(); self.daily_net.clear(); self.prev_data.clear(); self.vol_queues.clear(); self.price_history_10m.clear(); self.morning_high_cache.clear(); self.vwap_test_cache.clear()
                    nm.reset(); self.last_reset = now.date()

                targets = db.get_all_codes()
                if not targets: time.sleep(2); continue

                batch = []
                futures = [self.executor.submit(self._fetch_stock, c, now) for c in targets]
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res: batch.append(res)
                
                db.upsert_realtime_batch(batch)
                time.sleep(1.5 if MarketSession.is_market_open(now) else 5)
            except: time.sleep(5)

def get_stock_name(symbol):
    try: return twstock.codes[symbol].name if symbol in twstock.codes else symbol
    except: return symbol

if "engine" not in st.session_state: st.session_state.engine = SniperEngine()
engine = st.session_state.engine

# ==========================================
# 6. Streamlit UI
# ==========================================
def render_ui():
    with st.sidebar:
        st.title("🛡️ Sniper v7.0.1 (First Principle)")
        st.markdown("---")
        with st.expander("📦 庫存管理 (Format: 代碼,成本,張數,FUND/MOM)"):
            inv_input = st.text_area("庫存", DEFAULT_INVENTORY, height=80)
            if st.button("更新庫存"):
                db.update_inventory_list(inv_input)
                time.sleep(0.5); engine.update_targets(); st.rerun()

        with st.expander("🔭 監控設定", expanded=True):
            raw_input = st.text_area("新選清單", DEFAULT_WATCHLIST, height=100)
            if st.button("1. 初始化清單 (含 FinMind 雙因子判定)", type="primary"):
                db.update_watchlist(raw_input)
                targets = db.get_all_codes()
                status = st.status("建立戰略數據 (FinMind 營收/籌碼 + 日K壓力)...", expanded=True)
                bar = status.progress(0)
                static_list = []
                for i, code in enumerate(targets):
                    status.write(f"正在分析 {code}...")
                    static_list.append((code,) + fetch_static_stats(code))
                    bar.progress((i + 1) / len(targets))
                    time.sleep(0.1)
                db.upsert_static(static_list)
                engine.update_targets()
                status.update(label="數據建立完成！", state="complete")
                st.rerun()

        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("🟢 啟動監控", disabled=engine.running): engine.start(); st.rerun()
        with col_b:
            if st.button("🔴 停止監控", disabled=not engine.running): engine.stop(); st.rerun()
        st.caption(f"Engine: {'🟢 RUNNING' if engine.running else '🔴 STOPPED'}")

    try: from streamlit import fragment
    except ImportError:
        def fragment(run_every=None):
            def decorator(func):
                def wrapper(*args, **kwargs):
                    if run_every:
                        if "lfr" not in st.session_state: st.session_state.lfr = time.time()
                        if time.time() - st.session_state.lfr >= run_every: st.session_state.lfr = time.time(); st.rerun()
                    return func(*args, **kwargs)
                return wrapper
            return decorator

    @fragment(run_every=1.5)
    def render_dashboard():
        st.subheader("📦 庫存戰況")
        df_inv = db.get_inventory_view()
        if not df_inv.empty: 
            for col in ['price', 'pct', 'profit_val', 'profit_pct', 'divergence', 'bpe']:
                if col in df_inv.columns: df_inv[col] = pd.to_numeric(df_inv[col], errors='coerce').fillna(0.0)
            st.dataframe(df_inv[['code', 'name', 'stock_type', 'price', 'pct', 'profit_val', 'signal']], hide_index=True)
        else: st.info("無庫存")

        st.markdown("---")
        st.subheader("⚔️ 精銳監控 (First Principles)")
        df_w = db.get_watchlist_view()
        if df_w.empty: return

        numeric_cols = ['price', 'pct', 'vwap', 'ratio', 'net_10m', 'net_1h', 'net_day', 'divergence', 'bpe', 'morning_high']
        for col in numeric_cols:
            if col in df_w.columns: df_w[col] = pd.to_numeric(df_w[col], errors='coerce').fillna(0.0)

        html_rows = []
        for _, r in df_w.iterrows():
            c_price = "#ff4d4f" if r['pct'] > 0 else "#2ecc71" if r['pct'] < 0 else "#aaa"
            type_icon = "🛡️FUND" if r.get('stock_type') == 'FUND' else "⚡MOM"
            
            div = r.get('divergence', 0.0)
            div_color = "#ff4d4f" if div > 3 else "#2ecc71" if div < 1 else "#e67e22"
            
            bpe = r.get('bpe', 0.0)
            bpe_str = f"🚀 {bpe:.1f}" if bpe > 1.5 else f"⚠️ {bpe:.1f}" if bpe < 0.2 else f"{bpe:.1f}"
            
            v_test = "✔️" if r.get('vwap_test') else "❌"
            res_test = "❌(有壓)" if r.get('daily_res') else "✔️(無壓)"
            
            signal = r.get('signal') if pd.notna(r.get('signal')) and r.get('signal') else '-'
            
            html_rows.append(
                f"<tr><td>{r['code']}</td>"
                f"<td><a href='https://tw.stock.yahoo.com/quote/{r['code']}.TW' target='_blank' style='text-decoration:none; color:#3498db;'>{r['name']}</a> <span style='font-size:0.8em;color:#888'>{type_icon}</span></td>"
                f"<td><b style='color:{c_price}'>{r['price']:.2f} ({r['pct']:+.2f}%)</b></td>"
                f"<td>{r['vwap']:.2f} <span style='color:{div_color}; font-size:0.9em'>(乖離: {div:+.2f}%)</span></td>"
                f"<td>{bpe_str}</td>"
                f"<td>引力:{v_test} | 日K:{res_test}</td>"
                f"<td>{r['net_10m']:+} / {r['net_1h']:+}</td>"
                f"<td style='font-weight:bold;'>{signal}</td></tr>"
            )

        st.markdown(f"""
<style>
table.st-table {{ width: 100%; border-collapse: collapse; font-family: 'Courier New', monospace; }}
table.st-table th {{ background-color: #262730; color: white; padding: 8px; text-align: left; }}
table.st-table td {{ padding: 8px; border-bottom: 1px solid #444; }}
table.st-table tr:hover {{ background-color: #333; }}
</style>
<table class="st-table">
<tr><th>代碼</th><th>名稱 (屬性)</th><th>現價 (漲跌)</th><th>均價 (乖離率)</th><th>BPE(推升力)</th><th>防禦檢測</th><th>大戶(10m/1H)</th><th>系統訊號</th></tr>
{"".join(html_rows)}
</table>
""", unsafe_allow_html=True)

    render_dashboard()

if __name__ == "__main__":
    render_ui()
