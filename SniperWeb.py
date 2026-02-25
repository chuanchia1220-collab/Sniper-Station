import streamlit as st
import pandas as pd
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta, timezone, time as dt_time
from dataclasses import dataclass, field
import time, os, twstock, json, threading, sqlite3, concurrent.futures, requests, queue, io, base64
from itertools import cycle
import warnings
import logging
import math
import matplotlib
import matplotlib.pyplot as plt
import mplfinance as mpf

# [CRITICAL] 設定 Matplotlib 為非互動模式
matplotlib.use('Agg')

# [LOG FIX]
warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 1. Config & Domain Models
# ==========================================
st.set_page_config(page_title="Sniper v7.3 Architect", page_icon="🦅", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v7_arch.db"

DEFAULT_WATCHLIST = "3006 3037 1513 3189 1795 3491 8046 6274"
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
    signal_type: str
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
# 3. Database Layer
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
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (
            code TEXT PRIMARY KEY, name TEXT, category TEXT, price REAL, pct REAL, 
            vwap REAL, vol REAL, est_vol REAL, ratio REAL, net_1h REAL, net_10m REAL, net_day REAL, 
            signal TEXT, signal_type TEXT, update_time REAL, data_status TEXT DEFAULT 'DATA_OK', 
            signal_level TEXT DEFAULT 'B', risk_status TEXT DEFAULT 'NORMAL', situation TEXT, 
            ratio_yest REAL, active_light INTEGER DEFAULT 0
        )''')
        try: c.execute("ALTER TABLE realtime ADD COLUMN signal_type TEXT DEFAULT 'NONE'")
        except: pass
        try: c.execute("ALTER TABLE realtime ADD COLUMN active_light INTEGER DEFAULT 0")
        except: pass

        c.execute('''CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY, cost REAL, qty REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pinned (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS static_info (code TEXT PRIMARY KEY, vol_5ma REAL, vol_yest REAL, price_5ma REAL, win_rate REAL DEFAULT 0, avg_ret REAL DEFAULT 0, avg_amp REAL DEFAULT 0)''')
        try: c.execute("ALTER TABLE static_info ADD COLUMN avg_amp REAL DEFAULT 0")
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
        sql = '''INSERT OR REPLACE INTO realtime (code, name, category, price, pct, vwap, vol, est_vol, ratio, net_1h, net_10m, net_day, signal, signal_type, update_time, data_status, signal_level, risk_status, situation, ratio_yest, active_light) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.write_queue.put(('executemany', sql, data_list))

    def upsert_static(self, data_list):
        sql = 'INSERT OR REPLACE INTO static_info (code, vol_5ma, vol_yest, price_5ma, win_rate, avg_ret, avg_amp) VALUES (?, ?, ?, ?, ?, ?, ?)'
        self.write_queue.put(('executemany', sql, data_list))

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
        query = '''SELECT w.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.signal_type, r.net_10m, r.net_1h, r.net_day, r.situation, r.active_light, s.price_5ma, s.win_rate, s.avg_ret, s.avg_amp, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM watchlist w LEFT JOIN realtime r ON w.code = r.code LEFT JOIN static_info s ON w.code = s.code LEFT JOIN pinned p ON w.code = p.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_inventory_view(self):
        conn = self._get_conn()
        query = '''SELECT i.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.signal_type, r.net_1h, r.net_day, r.situation, s.price_5ma, i.cost, i.qty, (r.price - i.cost) * i.qty * 1000 as profit_val, (r.price - i.cost) / i.cost * 100 as profit_pct, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM inventory i LEFT JOIN realtime r ON i.code = r.code LEFT JOIN static_info s ON i.code = s.code LEFT JOIN pinned p ON i.code = p.code'''
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
        c.execute('SELECT code, vol_5ma, vol_yest, price_5ma, win_rate, avg_amp FROM static_info')
        rows = c.fetchall(); conn.close()
        return {r[0]: {'vol_5ma': r[1], 'vol_yest': r[2], 'price_5ma': r[3], 'win_rate': r[4], 'avg_amp': r[5]} for r in rows}

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
    if method == 'round': return round(price / tick) * tick
    else: return math.floor(price / tick) * tick

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

def fetch_static_stats(client, code):
    try:
        suffix = ".TW"
        if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
        hist = yf.Ticker(f"{code}{suffix}").history(period="10d", auto_adjust=True)
        vol_5ma, vol_yest, price_5ma = 0, 0, 0
        if not hist.empty and len(hist) >= 5:
            vol_yest = int(hist['Volume'].iloc[-2]) // 1000
            vol_5ma = int(hist['Volume'].tail(5).mean()) // 1000
            price_5ma = float(hist['Close'].tail(5).mean())
        return vol_5ma, vol_yest, price_5ma, 50, 0, 0 
    except: return 0, 0, 0, 0, 0, 0

# ==========================================
# 5. Visual Core: Chart Painter (Full Fugle)
# ==========================================
class ChartPainter:
    def __init__(self, engine_ref):
        self.engine = engine_ref
        self.chart_cache = {}
        self.running = False
        self._lock = threading.Lock()
        # [Fix 5: Load Balancing] 讓 Painter 也能輪替使用 Key，避免鎖定
        self.client_cycle = cycle([RestClient(api_key=k) for k in API_KEYS]) if API_KEYS else None

    def start(self):
        if not self.running:
            self.running = True
            threading.Thread(target=self._paint_loop, daemon=True, name="PainterThread").start()

    def stop(self): self.running = False

    def _paint_loop(self):
        mc = mpf.make_marketcolors(up='#ff4d4f', down='#2ecc71', inherit=True)
        style = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc, gridstyle=':', facecolor='#0e1117')
        
        while self.running:
            try:
                targets = list(set(self.engine.inventory_codes + self.engine.targets))
                if not targets:
                    time.sleep(5); continue

                for code in targets:
                    if not self.running: break
                    time.sleep(0.5)
                    self._generate_chart(code, style)
                
                time.sleep(10)
            except Exception: time.sleep(5)

    def _generate_chart(self, code, style):
        if not self.client_cycle: return
        client = next(self.client_cycle) # 輪替 Key
        
        try:
            chart_res = client.stock.intraday.chart(symbol=code, type='5k')
            if not chart_res or 'data' not in chart_res: return
            
            data = chart_res['data']
            if not data: return

            df = pd.DataFrame(data)
            df['Date'] = pd.to_datetime(df['time'])
            df.set_index('Date', inplace=True)
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
            
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                df[col] = df[col].astype(float)
            
            df['Cum_Vol'] = df['Volume'].cumsum()
            df['Cum_Val'] = (df['Close'] * df['Volume']).cumsum()
            df['VWAP'] = df['Cum_Val'] / df['Cum_Vol']
            
            net_day = self.engine.daily_net.get(code, 0)
            net_color = '#ff4d4f' if net_day > 0 else '#2ecc71'
            
            fig, axlist = mpf.plot(
                df, type='candle', style=style, volume=True, 
                mav=(5), returnfig=True, figsize=(5, 3.5), 
                tight_layout=True, show_nontrading=False
            )
            ax_main = axlist[0]
            ax_main.plot(range(len(df)), df['VWAP'], color='yellow', linewidth=1.5, alpha=0.7)
            
            ax_main.text(0.05, 0.95, f"Net Day: {net_day}", transform=ax_main.transAxes, 
                         color=net_color, fontsize=12, fontweight='bold', va='top')
            
            now_str = datetime.now().strftime("%H:%M:%S")
            ax_main.text(0.95, 0.95, f"Update: {now_str}", transform=ax_main.transAxes, 
                         color='white', fontsize=8, va='top', ha='right')

            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches='tight', facecolor='#0e1117')
            buf.seek(0)
            b64 = base64.b64encode(buf.read()).decode('utf-8')
            
            with self._lock:
                self.chart_cache[code] = f"data:image/png;base64,{b64}"
            
            plt.close(fig)
            del df
        except Exception: pass

# ==========================================
# 6. Notification System
# ==========================================
class NotificationManager:
    COOLDOWN_SECONDS = 600
    EMOJI_MAP = {
        "🔥攻擊": "🚀", "💣伏擊": "💣", "👀量增": "👀",
        "💀出貨": "💀", "🚨撤退": "⚠️", "👑漲停": "👑",
        "🔥尾盤": "🔥", "🌙鎖碼": "🔒", "💀尾盤殺": "🩸"
    }

    def __init__(self):
        self._queue = queue.Queue()
        self._cooldowns = {}
        threading.Thread(target=self._worker_loop, daemon=True).start()

    def reset_daily_state(self): self._cooldowns.clear()

    def should_notify(self, event: SniperEvent) -> bool:
        if event.is_test: return True
        if not MarketSession.is_market_open(): return False
        
        if event.win_rate < 40 and event.event_kind == "STRATEGY": return False
        
        key = f"{event.code}_{event.scope}_{event.event_label}"
        
        if event.scope == "inventory" and event.event_label in ["💀出貨", "🚨撤退", "🔥攻擊"]:
            if "撤退" in event.event_label:
                 if time.time() - self._cooldowns.get(key, 0) < 300: return False
                 return True

        if time.time() - self._cooldowns.get(key, 0) < self.COOLDOWN_SECONDS: return False
        return True

    def enqueue(self, event: SniperEvent):
        if self.should_notify(event):
            if not event.is_test: self._cooldowns[f"{event.code}_{event.scope}_{event.event_label}"] = time.time()
            self._queue.put(event)

    def _worker_loop(self):
        while True:
            event = self._queue.get()
            try: self._send_telegram(event); time.sleep(1.0)
            except: pass
            finally: self._queue.task_done()

    def _send_telegram(self, event: SniperEvent):
        if not TG_BOT_TOKEN or not TG_CHAT_ID: return
        emoji = self.EMOJI_MAP.get(event.event_label, "📌")
        up_dn = "UP" if event.pct >= 0 else "DN"
        type_tag = "☀️當沖" if event.signal_type == "DAY" else "🌙隔日" if event.signal_type == "SWING" else ""
        
        msg = (f"<b>{emoji} {event.event_label} [{type_tag}]｜{event.code} {event.name}</b>\n"
               f"現價：{event.price:.2f} ({event.pct:.2f}% {up_dn})　均價：{event.vwap:.2f}\n"
               f"<b>🎯 止盈：{event.tp_price:.1f}｜🛡️ 止損：{event.sl_price:.1f}</b>\n"
               f"📊 量比：{event.ratio:.1f} (昨{event.ratio_yest:.1f})\n"
               f"💰 大戶：{event.net_10m} / <b>{event.net_1h}</b> / {event.net_day}")
               
        buttons = [[{"text": "📈 Yahoo", "url": f"https://tw.stock.yahoo.com/quote/{event.code}.TW"}]]
        try: requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "reply_markup": json.dumps({"inline_keyboard": buttons})}, timeout=5)
        except: pass

notification_manager = NotificationManager()

# ==========================================
# 7. Engine Core (Sniper Hybrid)
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
        self.prev_data = {}       
        self.vol_queues = {}      
        self.active_flags = {}    
        self.market_stats = {"Time": 0}
        self.twii_data = None
        self.last_reset = datetime.now().date()
        
        self.painter = ChartPainter(self)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def update_targets(self):
        self.targets = db.get_all_codes()
        self.inventory_codes = db.get_inventory_codes()
        new_data = db.get_volume_map()
        if new_data: self.base_vol_cache.update(new_data)

    def start(self):
        if self.running: return
        self.update_targets()
        self.running = True
        self.painter.start()
        threading.Thread(target=self._run_loop, daemon=True).start()

    def stop(self):
        self.running = False
        self.painter.stop()

    def _update_market_thermometer(self):
        if time.time() - self.market_stats.get("Time", 0) < 15: return
        try:
            tse = yf.Ticker("^TWII"); fi = tse.fast_info
            price = fi.last_price; prev = fi.previous_close
            pct = ((price - prev) / prev) * 100 if prev else 0
            self.twii_data = {
                'code': '0000', 'name': '加權指數', 'price': price, 'pct': pct, 
                'vwap': price, 'ratio': 1.0, 'ratio_yest': 1.0, 'net_10m': 0, 'net_1h': 0, 'net_day': 0,
                'situation': '市場指標', 'event_label': '大盤', 'signal_type': 'NONE',
                'active_light': 1, 'is_pinned': 1
            }
            self.market_stats["Time"] = time.time()
        except: pass

    def _deep_backfill(self, client, code):
        try:
            # 1. 總量回補 (修正單位: 股 -> 張)
            chart = client.stock.intraday.chart(symbol=code, type='1k')
            daily_sim = 0
            if chart and 'data' in chart:
                for k in chart['data']:
                    o = k.get('open', 0); c = k.get('close', 0)
                    v = k.get('volume', 0) / 1000 # [Fix 3] 股轉張
                    if c > o: daily_sim += v
                    elif c < o: daily_sim -= v

            # 2. 結構回補 (修正單位 + 時區)
            deals = client.stock.intraday.deal(symbol=code, limit=50)
            temp_queue = []
            last_price = 0
            
            if deals and 'data' in deals:
                trade_data = deals['data']
                trade_data.reverse() 
                
                last_price = trade_data[0]['price'] 
                
                for d in trade_data:
                    p = d['price']
                    v = d['volume'] / 1000 # [Fix 3] 股轉張
                    # [Fix 4] 時區修正
                    ts = pd.to_datetime(d['at'], utc=True).timestamp()
                    
                    delta = 0
                    if p > last_price: delta = v
                    elif p < last_price: delta = -v
                    
                    if delta != 0:
                        temp_queue.append((ts, delta))
                    
                    last_price = p

            return int(daily_sim), temp_queue, last_price
        except Exception:
            return 0, [], 0

    def _hybrid_strategy(self, code, price, pct, vwap, net_day, net_1h, ratio, now_time):
        signal = "盤整"
        signal_type = "NONE"
        
        is_bullish = price >= (vwap * 1.005)
        is_bearish = price < (vwap * 0.995)
        
        is_morning = now_time.hour < 12
        is_afternoon = now_time.hour >= 12

        if is_morning:
            if ratio >= 1.2 and is_bullish and net_1h > 0:
                signal = "🔥攻擊"; signal_type = "DAY"
            elif ratio >= 1.5 and is_bearish and net_1h < 0:
                signal = "💀出貨"; signal_type = "DAY"
            elif is_bullish and net_1h > 50 and ratio > 0.8: 
                signal = "💣伏擊"; signal_type = "DAY"

        elif is_afternoon:
            if pct >= 9.5:
                signal = "👑漲停"; signal_type = "SWING"
            elif 2.0 < pct < 8.5 and net_day > 150 and price > vwap * 1.01:
                signal = "🌙鎖碼"; signal_type = "SWING"
            elif net_1h < -50 and price < vwap:
                signal = "💀尾盤殺"; signal_type = "SWING"
            elif pct > 3.0 and net_1h > 0:
                signal = "🔥尾盤"; signal_type = "SWING"

        if signal == "盤整":
            if net_1h > 0 and is_bullish: signal = "🔥吸籌"
            elif net_1h < 0 and not is_bullish: signal = "💀倒貨"
            elif net_1h < 0 and is_bullish: signal = "🎣誘多"
            elif net_1h > 0 and not is_bullish: signal = "🛡️吃盤"

        return signal, signal_type

    def _fetch_stock(self, code, now_time=None):
        try:
            if now_time is None: now_time = datetime.now(timezone.utc) + timedelta(hours=8)
            client = next(self.client_cycle) if self.client_cycle else None
            if not client: return None

            if code not in self.prev_data:
                daily_val, queue_data, last_p = self._deep_backfill(client, code)
                self.daily_net[code] = daily_val
                self.vol_queues[code] = queue_data
                self.prev_data[code] = {'vol': 0, 'price': last_p if last_p > 0 else 0}
                
                if last_p == 0:
                     q_init = client.stock.intraday.quote(symbol=code)
                     if q_init and 'lastPrice' in q_init:
                        self.prev_data[code] = {'vol': q_init['total']['tradeVolume'], 'price': q_init['lastPrice']}

            static_data = self.base_vol_cache.get(code, {})
            base_vol_5ma = static_data.get('vol_5ma', 0)
            base_vol_yest = static_data.get('vol_yest', 0)
            win_rate = static_data.get('win_rate', 0)

            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', 0)
            if not price: return None

            pct = q.get('changePercent', 0)
            vol_lots = q.get('total', {}).get('tradeVolume', 0)
            
            # [Fix 2] 第一筆資料不 Return，初始化後繼續執行
            if self.prev_data[code]['vol'] == 0:
                 self.prev_data[code] = {'vol': vol_lots, 'price': price}
                 delta_net = 0 # 設為 0 並繼續
            else:
                 # 正常計算 Delta
                 delta_net = 0
                 prev_v = self.prev_data[code]['vol']
                 prev_p = self.prev_data[code]['price']
                 
                 delta_v = vol_lots - prev_v
                 
                 if delta_v > 0:
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

            raw_signal, signal_type = self._hybrid_strategy(code, price, pct, vwap, net_day, net_1h, ratio, now_time)
            
            active_light = 1 if (price >= vwap * 1.005) and (now_time.time() >= dt_time(9, 5)) else 0
            tp_price = adjust_to_tick(vwap * 1.025, 'round')
            sl_price = adjust_to_tick(vwap * 0.985, 'floor')

            event_label = None
            if raw_signal in ["🔥攻擊", "💣伏擊", "🌙鎖碼", "🔥尾盤", "👑漲停", "💀出貨", "💀尾盤殺"]:
                event_label = raw_signal

            scope = "inventory" if code in self.inventory_codes else "watchlist"
            
            if event_label and code not in self.active_flags:
                if "攻擊" in event_label or "鎖碼" in event_label: self.active_flags[code] = True
                ev = SniperEvent(
                    code=code, name=get_stock_name(code), scope=scope,
                    event_kind="STRATEGY", event_label=event_label, signal_type=signal_type,
                    price=price, pct=pct, vwap=vwap, ratio=ratio, ratio_yest=ratio_yest,
                    net_10m=net_10m, net_1h=net_1h, net_day=net_day,
                    tp_price=tp_price, sl_price=sl_price, win_rate=win_rate
                )
                self._dispatch_event(ev)

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol_lots, est_lots, ratio, net_1h, net_10m, net_day, raw_signal, signal_type, now_ts, "DATA_OK", "B", "NORMAL", raw_signal, ratio_yest, active_light)
        except: return None

    def _dispatch_event(self, ev):
        notification_manager.enqueue(ev)

    def _run_loop(self):
        while self.running:
            try:
                now = datetime.now(timezone.utc) + timedelta(hours=8)
                if now.date() > self.last_reset:
                    self.active_flags = {}; self.daily_net = {}; self.prev_data = {}; self.vol_queues = {}
                    notification_manager.reset_daily_state()
                    self.last_reset = now.date()

                self._update_market_thermometer()
                targets = db.get_all_codes()
                self.inventory_codes = db.get_inventory_codes()
                pinned_codes = db.get_pinned_codes()

                if not targets: time.sleep(2); continue
                
                inv_set = set(self.inventory_codes); pin_set = set(pinned_codes)
                targets.sort(key=lambda c: 0 if c in inv_set else 1 if c in pin_set else 2)

                batch = []
                futures = [self.executor.submit(self._fetch_stock, c, now) for c in targets]
                for f in concurrent.futures.as_completed(futures):
                    if f.result(): batch.append(f.result())
                
                db.upsert_realtime_batch(batch)
                time.sleep(1.5 if MarketSession.is_market_open(now) else 5)
            except Exception: time.sleep(5)

@st.cache_resource
def get_engine():
    return SniperEngine()

engine = get_engine()

# ==========================================
# 8. UI Rendering (Ultimate Stable Fix)
# ==========================================

# 1. 靜態資源注入 (只執行一次)
def render_static_assets():
    st.markdown("""
<style>
    #chart-tooltip {
        display: none; position: fixed; 
        z-index: 999999; /* 超高層級 */
        background-color: #0e1117; border: 1px solid #444;
        border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.8);
        padding: 5px; width: 420px; 
        pointer-events: none; /* 關鍵：防止滑鼠誤觸 */
    }
    #chart-tooltip img { width: 100%; height: auto; border-radius: 4px; }
    tr.hover-row:hover { background-color: #262730 !important; cursor: crosshair; }
    
    table.sniper-table { width: 100%; border-collapse: collapse; font-family: 'Courier New', monospace; }
    table.sniper-table th { text-align: left; background-color: #262730; color: white; padding: 8px; font-size: 14px; white-space: nowrap; }
    table.sniper-table td { padding: 6px; border-bottom: 1px solid #444; font-size: 15px; vertical-align: middle; white-space: nowrap; }
    table.sniper-table tr.pinned-row { background-color: #fff9c4 !important; color: black !important; }
    table.sniper-table tr.golden-row { background-color: #fff9c4 !important; color: black !important; font-weight: bold; }
</style>

<div id="chart-tooltip"><img id="tooltip-img" src="" /></div>

<script>
    window.showTooltip = function(event, b64_data) {
        if (!b64_data || b64_data === "" || b64_data === "None") return;
        const tooltip = document.getElementById('chart-tooltip');
        const img = document.getElementById('tooltip-img');
        img.src = b64_data;
        tooltip.style.display = 'block';
        moveTooltip(event);
    }
    window.hideTooltip = function() {
        document.getElementById('chart-tooltip').style.display = 'none';
    }
    window.moveTooltip = function(event) {
        const tooltip = document.getElementById('chart-tooltip');
        let left = event.clientX + 15;
        let top = event.clientY + 15;
        if (left + 420 > window.innerWidth) left = event.clientX - 435;
        if (top + 300 > window.innerHeight) top = event.clientY - 315;
        tooltip.style.left = left + 'px';
        tooltip.style.top = top + 'px';
    }
</script>
""", unsafe_allow_html=True)

# 2. 純表格生成 (不含 JS/CSS)
def render_table_only():
    df = db.get_watchlist_view()
    
    if engine.twii_data:
        twii_row = pd.DataFrame([engine.twii_data])
        df = pd.concat([twii_row, df], ignore_index=True) if not df.empty else twii_row

    if df.empty: st.info("無監控數據"); return

    for col in ['price', 'pct', 'vwap', 'ratio', 'ratio_yest', 'net_10m', 'net_1h', 'net_day', 'win_rate']:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    table_html = """<table class="sniper-table">
<thead>
<tr>
<th>📌</th><th>代碼</th><th>名稱 (Link)</th><th>訊號 (Type)</th><th>現價</th><th>漲跌%</th>
<th>均價 (燈/TP)</th><th>量比</th><th>大戶 (1H/Day)</th>
</tr>
</thead>
<tbody>"""
    
    rows = []
    for _, row in df.iterrows():
        code = str(row['code'])
        is_twii = (code == '0000')
        
        c_price = "#ff4d4f" if row['pct'] > 0 else "#2ecc71" if row['pct'] < 0 else "#999999"
        
        signal = row.get('event_label', '盤整')
        sig_type = row.get('signal_type', 'NONE')
        type_icon = "☀️" if sig_type == "DAY" else "🌙" if sig_type == "SWING" else ""
        sig_html = f"<span style='font-weight:bold; color:{c_price}'>{signal} {type_icon}</span>"
        
        active = row.get('active_light', 0)
        vwap_light = "🟢" if active == 1 else "🔴"
        tp_price = adjust_to_tick(row['vwap'] * 1.025)
        vwap_html = f"<span style='color:#e67e22'>{row['vwap']:.2f} {vwap_light}</span> <span style='font-size:0.8em; color:#888'>(TP:{tp_price})</span>"
        
        vol_light = "🟢" if row['ratio'] > 1.2 else "🔴"
        ratio_html = f"{row['ratio']:.1f} {vol_light}"
        
        n1h = int(row['net_1h']); nd = int(row['net_day'])
        c1h = "#ff4d4f" if n1h > 0 else "#2ecc71"
        cd = "#ff4d4f" if nd > 0 else "#2ecc71"
        net_html = f"<span style='color:{c1h}'>{n1h}</span> / <span style='color:{cd}'>{nd}</span>"
        
        chart_b64 = engine.painter.chart_cache.get(code, "")
        
        row_class = "hover-row"
        if active == 1 and row['ratio'] > 1.2 and n1h > 0: row_class += " golden-row"
        elif row.get('is_pinned'): row_class += " pinned-row"

        pin_icon = "📌" if row.get('is_pinned') else ""
        name_html = f"<a href='https://tw.stock.yahoo.com/quote/{code}.TW' target='_blank' style='text-decoration:none; color:#3498db;'>{row['name']}</a>"
        if is_twii: name_html = row['name']

        # 使用 data-img 傳遞 Base64
        tr = f"""<tr class="{row_class}" 
data-img="{chart_b64}"
onmouseover="window.showTooltip(event, this.dataset.img)" 
onmouseout="window.hideTooltip()" 
onmousemove="window.moveTooltip(event)">
<td>{pin_icon}</td>
<td>{code}</td>
<td>{name_html}</td>
<td>{sig_html}</td>
<td><span style='color:{c_price}; font-weight:bold'>{row['price']:.2f}</span></td>
<td><span style='color:{c_price}'>{row['pct']:.2f}%</span></td>
<td>{vwap_html}</td>
<td>{ratio_html}</td>
<td>{net_html}</td>
</tr>"""
        rows.append(tr)

    st.markdown(table_html + "".join(rows) + "</tbody></table>", unsafe_allow_html=True)

# ==========================================
# 9. Main Layout
# ==========================================
render_static_assets() # 1. 注入 CSS/JS

with st.sidebar:
    st.title("🦅 Sniper v7.3 Architect")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🚀 啟動"):
            engine.start(); st.toast("系統全開")
            st.rerun()
    with col2:
        if st.button("🛑 停止"):
            engine.stop(); st.toast("系統休眠")
            st.rerun()
            
    with st.expander("庫存 & 監控管理"):
        inv_txt = st.text_area("庫存", DEFAULT_INVENTORY)
        if st.button("更新庫存"): db.update_inventory_list(inv_txt); engine.update_targets()
        
        watch_txt = st.text_area("監控", DEFAULT_WATCHLIST)
        if st.button("更新監控"):
            if not API_KEYS: st.error("No API Key")
            else:
                db.update_watchlist(watch_txt)
                engine.update_targets()
                static_data = []
                for c in engine.targets:
                     static_data.append((c, *fetch_static_stats(None, c)))
                db.upsert_static(static_data)
                st.success("已更新")

    st.caption(f"Status: {'RUNNING' if engine.running else 'STOPPED'}")

# 3. [Fix 1] 使用 Placeholder Loop 替代 Fragment
placeholder = st.empty()

while True:
    with placeholder:
        # 這裡放入一個 container 來重新渲染表格
        with st.container():
            render_table_only()
    
    # 降低刷新頻率以節省資源，同時讓 JS 有機會執行
    time.sleep(2)
