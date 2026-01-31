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
import numpy as np
import math

# [LOG FIX] Silence yfinance and other non-critical warnings
warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 1. Config & Domain Models
# ==========================================
st.set_page_config(page_title="Sniper v6.15 Elite", page_icon="🛡️", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v615.db"

# [01/29 Elite List] 預設清單
DEFAULT_WATCHLIST = "3006 3037 1513 3189 1795 3491 8046 6274"

# [User Inventory] 指揮官最新庫存狀態
DEFAULT_INVENTORY = """2481,84.4,3
3231,150.14,7
4566,54.94,2
8046,252.64,7"""

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
    ratio_yest: float
    net_10m: int
    net_1h: int
    net_day: int
    tp_price: float
    sl_price: float
    win_rate: float
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
# 3. Database
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
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (code TEXT PRIMARY KEY, name TEXT, category TEXT, price REAL, pct REAL, vwap REAL, vol REAL, est_vol REAL, ratio REAL, net_1h REAL, net_10m REAL, net_day REAL, signal TEXT, update_time REAL, data_status TEXT DEFAULT 'DATA_OK', signal_level TEXT DEFAULT 'B', risk_status TEXT DEFAULT 'NORMAL', situation TEXT, ratio_yest REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY, cost REAL, qty REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pinned (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS static_info (code TEXT PRIMARY KEY, vol_5ma REAL, vol_yest REAL, price_5ma REAL, win_rate REAL DEFAULT 0, avg_ret REAL DEFAULT 0)''')
        
        try:
            c.execute("ALTER TABLE static_info ADD COLUMN win_rate REAL DEFAULT 0")
            c.execute("ALTER TABLE static_info ADD COLUMN avg_ret REAL DEFAULT 0")
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
        sql = '''INSERT OR REPLACE INTO realtime (code, name, category, price, pct, vwap, vol, est_vol, ratio, net_1h, net_day, signal, update_time, data_status, signal_level, risk_status, net_10m, situation, ratio_yest) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.write_queue.put(('executemany', sql, data_list))

    def upsert_static(self, data_list):
        sql = 'INSERT OR REPLACE INTO static_info (code, vol_5ma, vol_yest, price_5ma, win_rate, avg_ret) VALUES (?, ?, ?, ?, ?, ?)'
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
        query = '''SELECT w.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.net_10m, r.net_1h, r.net_day, r.situation, s.price_5ma, s.win_rate, s.avg_ret, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM watchlist w LEFT JOIN realtime r ON w.code = r.code LEFT JOIN static_info s ON w.code = s.code LEFT JOIN pinned p ON w.code = p.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_inventory_view(self):
        conn = self._get_conn()
        query = '''SELECT i.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.net_1h, r.net_day, r.situation, s.price_5ma, i.cost, i.qty, (r.price - i.cost) * i.qty * 1000 as profit_val, (r.price - i.cost) / i.cost * 100 as profit_pct, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM inventory i LEFT JOIN realtime r ON i.code = r.code LEFT JOIN static_info s ON i.code = s.code LEFT JOIN pinned p ON i.code = p.code'''
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

    def get_pinned_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code FROM pinned')
        rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

    def get_volume_map(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code, vol_5ma, vol_yest, price_5ma, win_rate FROM static_info')
        rows = c.fetchall(); conn.close()
        return {r[0]: {'vol_5ma': r[1], 'vol_yest': r[2], 'price_5ma': r[3], 'win_rate': r[4]} for r in rows}

db = Database(DB_PATH)

# ==========================================
# 4. Utilities
# ==========================================

def adjust_to_tick(price, method='floor'):
    if price < 10: tick = 0.01
    elif price < 50: tick = 0.05
    elif price < 100: tick = 0.1
    elif price < 500: tick = 0.5
    elif price < 1000: tick = 1.0
    else: tick = 5.0
    
    if method == 'round':
        return round(price / tick) * tick
    else:
        return math.floor(price / tick) * tick

def fetch_static_stats(client, code):
    try:
        suffix = ".TW"
        if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
        
        hist_10d = yf.Ticker(f"{code}{suffix}").history(period="10d", auto_adjust=True)
        vol_5ma, vol_yest, price_5ma = 0, 0, 0
        if not hist_10d.empty and len(hist_10d) >= 5:
            vol_yest = int(hist_10d['Volume'].iloc[-2]) // 1000
            last_5_days = hist_10d.iloc[-6:-1]
            if last_5_days.empty: last_5_days = hist_10d.tail(5)
            vol_5ma = int(last_5_days['Volume'].mean()) // 1000
            price_5ma = float(last_5_days['Close'].mean())

        win_rate, avg_ret = _run_quick_backtest(f"{code}{suffix}")

        return vol_5ma, vol_yest, price_5ma, win_rate, avg_ret
    except: 
        return 0, 0, 0, 0, 0

def _run_quick_backtest(target_code):
    """
    [核心修正] V2.4.2 回測邏輯：
    1. 避開前 5 分鐘 (09:05 後開始)
    2. 乖離率限制：進場時股價不得超過 VWAP * 1.015 (避免追高)
    """
    try:
        df = yf.download(target_code, period="60d", interval="5m", progress=False, auto_adjust=True)
        if df.empty: return 0, 0
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df.index = pd.to_datetime(df.index)
        df['Date'] = df.index.date
        df['TP_Vol'] = df['Close'] * df['Volume']
        df['Cum_Vol'] = df.groupby('Date')['Volume'].cumsum()
        df['Cum_Val'] = df.groupby('Date')['TP_Vol'].cumsum()
        df['VWAP'] = df['Cum_Val'] / df['Cum_Vol']
        df = df.dropna()

        results = []
        unique_dates = sorted(list(set(df.index.date)))
        wins = 0
        losses = 0
        
        ENTRY_MIN = 1.005  # 地板: +0.5%
        ENTRY_MAX = 1.015  # 天花板: +1.5% (超過不買)
        TP = 1.02
        SL = 0.985

        for trade_date in unique_dates:
            mask = df['Date'] == trade_date
            day_data = df.loc[mask]
            if day_data.empty: continue

            in_pos = False
            entry_p = 0
            entry_v = 0
            
            for ts, row in day_data.iterrows():
                p = float(row['Close'])
                v = float(row['VWAP'])
                t = ts.time()
                
                if not in_pos:
                    # [修改] 09:05 前不動作
                    if t.hour==9 and t.minute<5: continue
                    if t.hour>=13: continue
                    
                    # [核心] 精準區間：0.5% ~ 1.5% 之間才出手
                    if (v * ENTRY_MIN) < p < (v * ENTRY_MAX):
                        in_pos = True; entry_p = p; entry_v = v
                        
                elif in_pos:
                    if p >= entry_p * TP:
                        results.append((p-entry_p)/entry_p); wins+=1; in_pos=False; break
                    elif p <= entry_v * SL:
                        results.append((p-entry_p)/entry_p); losses+=1; in_pos=False; break
                    elif t.hour==13 and t.minute>=25:
                        res = (p-entry_p)/entry_p
                        results.append(res)
                        if res>0: wins+=1
                        else: losses+=1
                        in_pos=False; break
                        
        total = wins + losses
        wr = (wins/total*100) if total > 0 else 0
        av = np.mean(results)*100 if results else 0
        return wr, av
    except: return 0, 0

def get_stock_name(symbol):
    try: return twstock.codes[symbol].name if symbol in twstock.codes else symbol
    except: return symbol

def get_dynamic_thresholds(price):
    if price >= 1000: return {"tgt_pct": 1.5, "tgt_ratio": 1.2, "ambush": 1.5, "overheat": 4.0}
    elif price >= 500: return {"tgt_pct": 2.0, "tgt_ratio": 1.5, "ambush": 2.5, "overheat": 5.0}
    elif price >= 150: return {"tgt_pct": 2.5, "tgt_ratio": 1.8, "ambush": 4.0, "overheat": 6.5}
    elif price >= 70: return {"tgt_pct": 3.0, "tgt_ratio": 2.2, "ambush": 6.0, "overheat": 8.0}
    else: return {"tgt_pct": 5.0, "tgt_ratio": 3.0, "ambush": 10.0, "overheat": 9.0}

def _calc_est_vol(current_vol):
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < market_open: return 0
    elapsed_minutes = (now - market_open).seconds / 60
    if elapsed_minutes <= 0: return 0
    if elapsed_minutes >= 270: return current_vol
    weight = 2.0 if elapsed_minutes < 15 else 1.0
    return int(current_vol * (270 / elapsed_minutes) / weight)

def check_signal(pct, is_bullish, net_day, net_1h, ratio, thresholds, is_breakdown, price, vwap, has_attacked, now_time, vol_lots):
    if is_breakdown: return "🚨撤退"
    if pct >= 9.5: return "👑漲停"
    
    # [修正] 09:05 前暖機
    if now_time.time() < dt_time(9, 5): return "⏳暖機"

    # [核心] 檢查是否在「黃金走廊」(0.5% ~ 1.5%)
    # is_bullish 已經代表 > 0.5%
    # 這裡額外檢查是否 > 1.5% (過熱)
    in_golden_zone = False
    if is_bullish and price <= (vwap * 1.015):
        in_golden_zone = True

    if ratio >= thresholds['tgt_ratio']:
        if in_golden_zone and net_1h > 0:
            if not has_attacked: return "🔥攻擊"
        elif net_1h < 0:
            return "💀出貨"
        
    if not is_bullish:
        return "📉線下"
        
    # 如果是牛市但超過 1.5%，顯示「追高風險」而不是攻擊訊號
    if is_bullish and not in_golden_zone:
         # 這裡可以選擇不回傳任何訊號，或者回傳一個警告
         return "⚠️追高"

    bias = ((price - vwap) / vwap) * 100 if vwap > 0 else 0
    if bias > thresholds['overheat']: return "⚠️過熱"

    if dt_time(13, 0) <= now_time.time() <= dt_time(13, 25):
        if (3.0 <= pct <= 9.0) and (net_1h > 0) and (net_day / (vol_lots+1) >= 0.05): return "🔥尾盤"
        
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    if pct >= thresholds['tgt_pct']: return "⚠️價強"
    
    return "盤整"

# ==========================================
# 5. Notification
# ==========================================
class NotificationManager:
    COOLDOWN_SECONDS = 600
    RATE_LIMIT_DELAY = 1.0
    EMOJI_MAP = {
        "🔥攻擊": "🚀", "💣伏擊": "💣", "👀量增": "👀",
        "💀出貨": "💀", "🚨撤退": "⚠️", "👑漲停": "👑",
        "⚠️價強": "💪", "❌誘多": "🎣", "🔥尾盤": "🔥",
        "⚠️過熱": "🚫", "⏳暖機": "⏳", "📉線下": "📉", "⚠️追高": "🚫"
    }

    def __init__(self):
        self._queue = queue.Queue()
        self._cooldowns = {}
        threading.Thread(target=self._worker_loop, daemon=True).start()

    def reset_daily_state(self):
        self._cooldowns.clear()

    def should_notify(self, event: SniperEvent) -> bool:
        if event.is_test: return True
        if not MarketSession.is_market_open(): return False
        
        # [核心過濾] 勝率 < 50% 且不是庫存撤退訊號 -> 直接靜音
        if event.scope == "watchlist" and event.event_label == "🔥攻擊":
            if event.win_rate < 50:
                return False

        # [新增] 追高訊號不推播
        if event.event_label == "⚠️追高":
            return False

        key = f"{event.code}_{event.scope}_{event.event_label}"
        
        if event.scope == "inventory":
            if event.event_label not in ["💀出貨", "🚨撤退", "🔥攻擊", "👑漲停"]:
                return False
            if "撤退" in event.event_label:
                 if time.time() - self._cooldowns.get(key, 0) < 300: return False
                 return True

        if event.scope == "watchlist":
            if event.event_label != "🔥攻擊":
                return False

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
        
        msg = (f"<b>{emoji} {event.event_label}｜{event.code} {event.name} ({event.win_rate:.0f}%)</b>\n"
               f"現價：{event.price:.2f} ({event.pct:.2f}% {up_dn})　均價：{event.vwap:.2f}\n"
               f"<b>🎯 止盈：{event.tp_price:.1f} (2%)｜🛡️ 止損：{event.sl_price:.1f} (均-1.5%)</b>\n"
               f"📊 量比：{event.ratio_yest:.1f} / <b>{event.ratio:.1f}</b>\n"
               f"💰 大戶：{event.net_10m} / <b>{event.net_1h}</b> / {event.net_day}")
               
        buttons = [[{"text": "📈 Yahoo", "url": f"https://tw.stock.yahoo.com/quote/{event.code}.TW"}]]
        try: requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "reply_markup": json.dumps({"inline_keyboard": buttons})}, timeout=5)
        except: pass

notification_manager = NotificationManager()

# ==========================================
# 6. Engine (Sniper Core - Final)
# ==========================================
class SniperEngine:
    def __init__(self):
        self.running = False
        self.clients = [RestClient(api_key=k) for k in API_KEYS] if API_KEYS else []
        self.client_cycle = cycle(self.clients) if self.clients else None
        self.targets = []
        self.inventory_codes = []
        self.base_vol_cache = {} 
        self.daily_net = {}
        self.vol_queues = {}
        self.prev_data = {}
        self.active_flags = {}
        self.daily_risk_flags = {}
        self.market_stats = {"Time": 0}
        self.twii_data = None 
        self.last_reset = datetime.now().date()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

    def update_targets(self):
        self.targets = db.get_all_codes()
        self.inventory_codes = db.get_inventory_codes()
        new_data = db.get_volume_map()
        if new_data:
            self.base_vol_cache.update(new_data)

    def start(self):
        if self.running: return
        self.update_targets()
        self.running = True
        threading.Thread(target=self._run_loop, daemon=True).start()

    def stop(self): self.running = False

    def _update_market_thermometer(self):
        if time.time() - self.market_stats.get("Time", 0) < 15: return
        try:
            tse = yf.Ticker("^TWII")
            fi = tse.fast_info
            price = fi.last_price
            prev = fi.previous_close
            pct = ((price - prev) / prev) * 100 if prev else 0
            
            hist = tse.history(period="10d", auto_adjust=True)
            if not hist.empty:
                price_5ma = hist['Close'].iloc[-6:-1].mean() if len(hist) >= 6 else hist['Close'].mean()
            else:
                price_5ma = price

            self.twii_data = {
                'code': '0000', 
                'name': '加權指數',
                'price': price,
                'pct': pct,
                'vwap': price, 
                'price_5ma': price_5ma,
                'ratio': 1.0, 'ratio_yest': 1.0,
                'net_10m': 0, 'net_1h': 0, 'net_day': 0,
                'situation': '市場指標',
                'event_label': '大盤',
                'is_pinned': 1,
                'win_rate': 0, 'avg_ret': 0 
            }
            
            self.market_stats["Time"] = time.time()
        except: pass

    def _dispatch_event(self, ev: SniperEvent):
        notification_manager.enqueue(ev)

    def _fetch_stock(self, code, now_time=None):
        try:
            if now_time is None: now_time = datetime.now(timezone.utc) + timedelta(hours=8)
            client = next(self.client_cycle) if self.client_cycle else None
            if not client: return None

            static_data = self.base_vol_cache.get(code, {})
            base_vol_5ma = static_data.get('vol_5ma', 0)
            base_vol_yest = static_data.get('vol_yest', 0)
            price_5ma = static_data.get('price_5ma', 0)
            win_rate = static_data.get('win_rate', 0)

            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', 0)
            if not price: return None

            order_book = q.get('order', {})
            best_asks = order_book.get('bestAsks', []) or []
            best_bids = order_book.get('bestBids', []) or []
            
            best_ask = best_asks[0].get('price', price) if best_asks else price
            best_bid = best_bids[0].get('price', price) if best_bids else price

            pct = q.get('changePercent', 0)
            vol_lots = q.get('total', {}).get('tradeVolume', 0)

            if vol_lots == 0 and code in self.prev_data:
                vol_lots = self.prev_data[code]['vol']

            vol = vol_lots * 1000
            est_lots = _calc_est_vol(vol_lots)
            
            ratio_5ma = est_lots / base_vol_5ma if base_vol_5ma > 0 else 0
            ratio_yest = est_lots / base_vol_yest if base_vol_yest > 0 else 0

            total_val = q.get('total', {}).get('tradeValue', 0)
            vwap = (total_val / vol) if vol > 0 else price

            delta_net = 0
            if code in self.prev_data:
                prev_v = self.prev_data[code]['vol']
                delta_v = vol_lots - prev_v
                
                if delta_v > 0:
                    if price >= best_ask: delta_net = int(delta_v)
                    elif price <= best_bid: delta_net = -int(delta_v)
                    else:
                        prev_p = self.prev_data[code]['price']
                        if price > prev_p: delta_net = int(delta_v)
                        elif price < prev_p: delta_net = -int(delta_v)

            self.prev_data[code] = {'vol': vol_lots, 'price': price}
            now_ts = time.time()
            if code not in self.vol_queues: self.vol_queues[code] = []
            if delta_net != 0: self.vol_queues[code].append((now_ts, delta_net))

            self.daily_net[code] = self.daily_net.get(code, 0) + delta_net
            self.vol_queues[code] = [x for x in self.vol_queues[code] if x[0] > now_ts - 3600]
            
            net_1h = sum(x[1] for x in self.vol_queues[code])
            net_10m = sum(x[1] for x in self.vol_queues[code] if x[0] > now_ts - 600)
            net_day = self.daily_net.get(code, 0)

            is_bullish = price >= (vwap * 1.005)
            is_breakdown = price < (vwap * 0.99)
            
            situation = "⚖️觀望"
            if net_1h > 0:
                if is_bullish: situation = "🔥主動吸籌"
                else: situation = "🛡️被動吃盤"
            elif net_1h < 0:
                if not is_bullish: situation = "💀主動倒貨"
                else: situation = "🎣拉高出貨"

            thresholds = get_dynamic_thresholds(price)
            
            raw_state = check_signal(pct, is_bullish, net_day, net_1h, ratio_5ma, thresholds, is_breakdown, price, vwap, code in self.active_flags, now_time, vol_lots)

            event_label = None
            scope = "inventory" if code in self.inventory_codes else "watchlist"
            
            trigger_price = vwap * 1.005
            
            tp_calc = adjust_to_tick(trigger_price * 1.02, method='round')
            sl_calc = adjust_to_tick(vwap * 0.985, method='floor')

            if "攻擊" in raw_state and code not in self.active_flags: event_label = "🔥攻擊"
            elif "漲停" in raw_state and scope == "inventory": event_label = "👑漲停"
            elif "撤退" in raw_state: 
                if scope == "inventory": event_label = "🚨撤退"
                else: event_label = None
            elif "出貨" in raw_state and code not in self.daily_risk_flags and scope == "inventory": event_label = "💀出貨"
            elif "尾盤" in raw_state: event_label = "🔥尾盤"

            if event_label:
                if "攻擊" in event_label: self.active_flags[code] = True
                if "出貨" in event_label or "撤退" in event_label: self.daily_risk_flags[code] = True
                ev = SniperEvent(
                    code=code, name=get_stock_name(code), scope=scope,
                    event_kind="STRATEGY", event_label=event_label,
                    price=price, pct=pct, vwap=vwap, ratio=ratio_5ma, ratio_yest=ratio_yest,
                    net_10m=net_10m, net_1h=net_1h, net_day=net_day,
                    tp_price=tp_calc, sl_price=sl_calc, win_rate=win_rate
                )
                self._dispatch_event(ev)

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol_lots, est_lots, ratio_5ma, net_1h, net_day, raw_state, now_ts, "DATA_OK", "B", "NORMAL", net_10m, situation, ratio_yest)
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
                self.base_vol_cache = {}
                notification_manager.reset_daily_state()
                self.last_reset = now.date()

            self._update_market_thermometer(now)

            targets = db.get_all_codes()
            self.inventory_codes = db.get_inventory_codes()
            pinned_codes = db.get_pinned_codes()

            if not targets: time.sleep(2); continue

            inv_set = set(self.inventory_codes)
            pin_set = set(pinned_codes)

            def priority_key(code):
                if code in inv_set: return 0
                if code in pin_set: return 1
                return 2

            targets.sort(key=priority_key)

            batch = []
            futures = [self.executor.submit(self._fetch_stock, c, now) for c in targets]
            for f in concurrent.futures.as_completed(futures):
                if f.result(): batch.append(f.result())
            db.upsert_realtime_batch(batch)
            time.sleep(1.5 if MarketSession.is_market_open(now) else 5)

if "sniper_engine_core" not in st.session_state:
    st.session_state.sniper_engine_core = SniperEngine()
engine = st.session_state.sniper_engine_core

# ==========================================
# 7. UI (Table Layout)
# ==========================================
with st.sidebar:
    st.title("🛡️ 戰情室 v6.15 Elite")
    st.caption(f"Update: {datetime.now().strftime('%H:%M:%S')}")
    st.markdown("---")

    mode = st.radio("身分模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
    use_filter = st.checkbox("只看局勢活躍 (>70元)")

    if mode == "👨‍✈️ 指揮官":
        with st.expander("📦 庫存管理", expanded=False):
            inv_input = st.text_area("庫存清單", DEFAULT_INVENTORY, height=100)
            if st.button("更新庫存"):
                db.update_inventory_list(inv_input)
                time.sleep(0.5); engine.update_targets(); st.rerun()

        with st.expander("🔭 監控設定", expanded=True):
            raw_input = st.text_area("新選清單", DEFAULT_WATCHLIST, height=150)
            
            if st.button("1. 初始化並更新清單", type="primary"):
                if not API_KEYS: st.error("缺 API Key")
                else:
                    db.update_watchlist(raw_input)
                    time.sleep(0.5)
                    engine.update_targets()
                    targets = engine.targets
                    
                    status = st.status("正在建立戰略數據 (含60天回測)...", expanded=True)
                    static_list = []
                    progress_bar = status.progress(0)

                    for i, code in enumerate(targets):
                        status.write(f"正在分析 {code} 歷史戰報...")
                        current_key = API_KEYS[i % len(API_KEYS)]
                        client = RestClient(api_key=current_key)
                        vol_5ma, vol_yest, price_5ma, wr, ar = fetch_static_stats(client, code)
                        static_list.append((code, vol_5ma, vol_yest, price_5ma, wr, ar))
                        progress_bar.progress((i + 1) / len(targets))
                        time.sleep(0.1)

                    db.upsert_static(static_list)
                    engine.update_targets()
                    status.update(label="戰略數據建立完成！", state="complete")
                    st.rerun()

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🟢 啟動監控", disabled=engine.running):
                    engine.start(); st.toast("核心已啟動"); st.rerun()
            with col_b:
                if st.button("🔴 停止監控", disabled=not engine.running):
                    engine.stop(); st.toast("核心已停止"); st.rerun()

    st.caption(f"Engine: {'🟢 RUNNING' if engine.running else '🔴 STOPPED'}")

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
    with st.expander("📦 庫存戰況 (Inventory)", expanded=False):
        df_inv = db.get_inventory_view()
        if not df_inv.empty:
             st.dataframe(df_inv[['code', 'name', 'situation', 'price', 'pct', 'profit_val', 'signal_level']], hide_index=True)
        else: st.info("尚無庫存")

    st.markdown("---")
    st.subheader("⚔️ 精銳監控 (Tactical Table)")

    df_watch = db.get_watchlist_view()
    
    if engine.twii_data:
        twii_row = pd.DataFrame([engine.twii_data])
        if not df_watch.empty:
            for col in df_watch.columns:
                if col not in twii_row.columns:
                    twii_row[col] = 0
            df_watch = pd.concat([twii_row, df_watch], ignore_index=True)
        else:
            df_watch = twii_row

    if df_watch.empty:
        st.info("尚未加入監控標的")
        return

    numeric_cols = ['price', 'pct', 'vwap', 'ratio', 'ratio_yest', 'net_10m', 'net_1h', 'net_day', 'price_5ma', 'win_rate', 'avg_ret']
    for col in numeric_cols:
        if col in df_watch.columns:
            df_watch[col] = pd.to_numeric(df_watch[col], errors='coerce').fillna(0.0)

    if use_filter:
        df_watch = df_watch[df_watch['price'] > 70]

    table_start = """
<style>
table.sniper-table { width: 100%; border-collapse: collapse; font-family: 'Courier New', monospace; }
table.sniper-table th { text-align: left; background-color: #262730; color: white; padding: 8px; font-size: 14px; white-space: nowrap; }
table.sniper-table td { padding: 6px; border-bottom: 1px solid #444; font-size: 15px; vertical-align: middle; white-space: nowrap; }
table.sniper-table tr.pinned-row { background-color: #fff9c4 !important; color: black !important; }
table.sniper-table tr:hover { background-color: #f0f2f6; color: black; }
</style>
<table class="sniper-table">
<thead>
<tr>
<th>📌</th><th>代碼</th><th>名稱 (Link)</th><th>訊號</th><th>5MA</th><th>現價</th><th>漲跌%</th>
<th>均價 (燈/TP/SL)</th><th>量比 (昨/5日)</th><th>局勢</th><th>大戶 (10m/1H/日)</th>
</tr>
</thead>
<tbody>
"""
    html_rows = []
    for _, row in df_watch.iterrows():
        situation = str(row.get('situation') or '盤整')
        event_label = str(row.get('event_label') or '')
        
        is_pinned = row.get('is_pinned', 0)
        row_class = "pinned-row" if is_pinned else ""
        pin_icon = "📌" if is_pinned else ""
        
        is_twii = str(row['code']) == "0000"

        # 顯示勝率
        name_display = f"{row['name']} ({row.get('win_rate', 0):.0f}%)"
        win_rate = row.get("win_rate", 0)
        
        if not is_twii and win_rate < 50:
            name_display += " <span style='color:#ff4d4f; font-size:0.8em;'>(高風險)</span>"

        # 1. Price
        main_color = "#ff4d4f" if row['pct'] > 0 else "#2ecc71" if row['pct'] < 0 else "#999999"
        price_html = f"<span style='color:{main_color}; font-weight:bold'>{row['price']:.2f}</span>"
        pct_html = f"<span style='color:{main_color}'>{row['pct']:.2f}%</span>"

        # 2. VWAP Light (Logic: Price > VWAP * 1.005)
        # 這裡的燈號同步更新：必須在黃金走廊內才亮綠燈
        is_bullish = row['price'] >= (row['vwap'] * 1.005)
        is_in_zone = row['price'] <= (row['vwap'] * 1.015)
        
        vwap_color = "#ff4d4f" if is_bullish else "#2ecc71"
        if is_bullish:
             vwap_light = "🟢" if is_in_zone else "⚠️" # 超過1.5%顯示警告
        else:
             vwap_light = "🔴"
        
        vwap_html = f"<span style='color:{vwap_color}'>{row['vwap']:.2f} {vwap_light}</span>"
        
        if is_bullish and not is_twii:
            trigger_price = row['vwap'] * 1.005
            tp_price = adjust_to_tick(trigger_price * 1.02, method='round')
            sl_price = adjust_to_tick(row['vwap'] * 0.985, method='floor')
            vwap_html += f"<br><span style='font-size:0.85em; color:#888'>(TP:{tp_price:.1f} / SL:{sl_price:.1f})</span>"

        # 3. 5MA
        p_5ma = row.get('price_5ma', 0)
        c_5ma = "#ff4d4f" if row['price'] > p_5ma else "#2ecc71"
        ma_html = f"<span style='color:{c_5ma}'>{p_5ma:.2f}</span>"

        # 4. Volume Dual Track
        thresholds = get_dynamic_thresholds(row['price'])
        tgt_ratio = thresholds['tgt_ratio']
        r_yest = row.get('ratio_yest', 0)
        r_5ma = row['ratio']
        is_vol_strong = r_5ma >= tgt_ratio
        vol_light = "🟢" if is_vol_strong else "🔴"
        c_5ma_r = "#ff4d4f" if is_vol_strong else "#999999"
        ratio_html = f"{r_yest:.1f} / <span style='color:{c_5ma_r}; font-weight:bold'>{r_5ma:.1f} {vol_light}</span>"

        # 5. Situation
        sit_color = "#ff4d4f" if "吸籌" in situation or "攻擊" in situation else "#2ecc71" if "倒貨" in situation else "#e67e22" if "吃盤" in situation else "#999999"
        clean_situation = situation.replace("🔥", "").replace("🛡️", "").replace("💀", "").replace("🎣", "").replace("⚖️", "")
        situation_html = f"<span style='color:{sit_color}; font-weight:bold'>{clean_situation}</span>"
        
        # 6. Link
        if is_twii:
             name_html = f'<span style="font-weight:bold;">{name_display}</span>'
        else:
             name_html = f'<a href="https://tw.stock.yahoo.com/quote/{row["code"]}.TW" target="_blank" style="text-decoration:none; color:#3498db; font-weight:bold;">{name_display}</a>'

        # 7. Big Player
        if is_twii:
            bp_html = "<span style='color:#777'>- / - / -</span>"
        else:
            n10 = int(row['net_10m'])
            n1h = int(row['net_1h'])
            nd = int(row['net_day'])
            bp_light = "🟢" if n1h > 0 else "🔴"
            c10 = "#ff4d4f" if n10 > 0 else "#2ecc71" if n10 < 0 else "#999999"
            c1h = "#ff4d4f" if n1h > 0 else "#2ecc71" if n1h < 0 else "#999999"
            cd  = "#ff4d4f" if nd > 0 else "#2ecc71" if nd < 0 else "#999999"
            bp_html = f"<span style='color:{c10}'>{n10}</span> / <span style='color:{c1h}'>{n1h} {bp_light}</span> / <span style='color:{cd}'>{nd}</span>"

        html_rows.append(f'<tr class="{row_class}"><td>{pin_icon}</td><td>{row["code"]}</td><td>{name_html}</td><td>{event_label}</td><td>{ma_html}</td><td>{price_html}</td><td>{pct_html}</td><td>{vwap_html}</td><td>{ratio_html}</td><td>{situation_html}</td><td>{bp_html}</td></tr>')

    st.markdown(table_start + "".join(html_rows) + "</tbody></table>", unsafe_allow_html=True)

render_live_dashboard()
