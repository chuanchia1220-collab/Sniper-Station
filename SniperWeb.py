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

# [CRITICAL] 設定 Matplotlib 為非互動模式，防止 Streamlit 在背景繪圖時崩潰
matplotlib.use('Agg')

# [LOG FIX] 過濾雜訊
warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 1. Config & Domain Models
# ==========================================
st.set_page_config(page_title="Sniper v7.0 Hybrid", page_icon="🦅", layout="wide")

# 嘗試讀取 Secrets 或環境變數
try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v7.db"

# 預設清單
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
    event_kind: str     # STRATEGY
    event_label: str    # 攻擊/伏擊/鎖碼...
    signal_type: str    # DAY (當沖) / SWING (隔日)
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
        # Realtime table 增加 signal_type 欄位
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (
            code TEXT PRIMARY KEY, name TEXT, category TEXT, price REAL, pct REAL, 
            vwap REAL, vol REAL, est_vol REAL, ratio REAL, net_1h REAL, net_10m REAL, net_day REAL, 
            signal TEXT, signal_type TEXT, update_time REAL, data_status TEXT DEFAULT 'DATA_OK', 
            signal_level TEXT DEFAULT 'B', risk_status TEXT DEFAULT 'NORMAL', situation TEXT, 
            ratio_yest REAL, active_light INTEGER DEFAULT 0
        )''')
        
        # 嘗試補加欄位 (Migration)
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
    # 用於 "初始化" 時的靜態數據抓取
    try:
        suffix = ".TW"
        if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
        hist = yf.Ticker(f"{code}{suffix}").history(period="10d", auto_adjust=True)
        vol_5ma, vol_yest, price_5ma = 0, 0, 0
        if not hist.empty and len(hist) >= 5:
            vol_yest = int(hist['Volume'].iloc[-2]) // 1000
            vol_5ma = int(hist['Volume'].tail(5).mean()) // 1000
            price_5ma = float(hist['Close'].tail(5).mean())
        # 這裡簡化回測，實戰可保留原本的 _run_quick_backtest
        return vol_5ma, vol_yest, price_5ma, 50, 0, 0 
    except: return 0, 0, 0, 0, 0, 0

# ==========================================
# 5. Visual Core: Chart Painter 
# ==========================================
class ChartPainter:
    def __init__(self, engine_ref):
        self.engine = engine_ref
        self.chart_cache = {}  # {code: base64_string}
        self.running = False
        self._lock = threading.Lock()
        # 建立一個專用的 Client 給畫家用，避免跟 Engine 搶
        self.client = RestClient(api_key=API_KEYS[0]) if API_KEYS else None

    def start(self):
        if not self.running:
            self.running = True
            threading.Thread(target=self._paint_loop, daemon=True, name="PainterThread").start()

    def stop(self): self.running = False

    def _paint_loop(self):
        # 設定暗黑風格
        mc = mpf.make_marketcolors(up='#ff4d4f', down='#2ecc71', inherit=True)
        style = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc, gridstyle=':', facecolor='#0e1117')
        
        while self.running:
            try:
                targets = list(set(self.engine.inventory_codes + self.engine.targets))
                if not targets:
                    time.sleep(5); continue

                for code in targets:
                    if not self.running: break
                    time.sleep(0.5) # Fugle API 速度快，但仍需避免過熱
                    self._generate_chart(code, style)
                
                time.sleep(20) # 畫完一輪休息 20 秒
            except Exception as e:
                print(f"Painter Loop Error: {e}")
                time.sleep(5)

    def _generate_chart(self, code, style):
        if not self.client: return
        try:
            # 🔥 [修正] 改用 Fugle API 抓取 K 線 (取代 yfinance)
            chart_res = self.client.stock.intraday.chart(symbol=code, type='5k') # 用 5分K 畫圖
            if not chart_res or 'data' not in chart_res: return
            
            data = chart_res['data']
            if not data: return

            # 轉換為 DataFrame
            df = pd.DataFrame(data)
            # Fugle 回傳欄位: open, high, low, close, volume, unit, time
            df['Date'] = pd.to_datetime(df['time'])
            df.set_index('Date', inplace=True)
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
            
            # 確保數據格式正確 (float)
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                df[col] = df[col].astype(float)

            # 計算 VWAP
            df['Cum_Vol'] = df['Volume'].cumsum()
            df['Cum_Val'] = (df['Close'] * df['Volume']).cumsum()
            df['VWAP'] = df['Cum_Val'] / df['Cum_Vol']
            
            # 取得即時 Net Day (顯示在圖上)
            net_day = self.engine.daily_net.get(code, 0)
            net_color = '#ff4d4f' if net_day > 0 else '#2ecc71'
            
            # 繪圖
            fig, axlist = mpf.plot(
                df, type='candle', style=style, volume=True, 
                mav=(5), returnfig=True, figsize=(5, 3.5), 
                tight_layout=True, show_nontrading=False
            )
            ax_main = axlist[0]
            ax_main.plot(range(len(df)), df['VWAP'], color='yellow', linewidth=1.5, alpha=0.7)
            
            # 標註 Net Day
            ax_main.text(0.05, 0.95, f"Net Day: {net_day}", transform=ax_main.transAxes, 
                         color=net_color, fontsize=12, fontweight='bold', va='top')
            
            # 標註更新時間
            now_str = datetime.now().strftime("%H:%M")
            ax_main.text(0.95, 0.95, f"Update: {now_str}", transform=ax_main.transAxes, 
                         color='white', fontsize=8, va='top', ha='right')

            # 轉檔 Base64
            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches='tight', facecolor='#0e1117')
            buf.seek(0)
            b64 = base64.b64encode(buf.read()).decode('utf-8')
            
            with self._lock:
                self.chart_cache[code] = f"data:image/png;base64,{b64}"
            
            plt.close(fig)
            del df
        except Exception as e:
            # print(f"Chart Gen Error {code}: {e}") # Debug
            pass

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
        
        # 策略過濾：勝率過低不報
        if event.win_rate < 40 and event.event_kind == "STRATEGY": return False
        
        key = f"{event.code}_{event.scope}_{event.event_label}"
        
        # 庫存重要訊號不擋
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
        
        # Data Containers
        self.targets = []
        self.inventory_codes = []
        self.base_vol_cache = {} 
        self.daily_net = {}       # {code: net_volume}
        self.prev_data = {}       # {code: {vol, price}}
        self.vol_queues = {}      # {code: [(ts, delta), ...]}
        self.active_flags = {}    # {code: bool}
        self.market_stats = {"Time": 0}
        self.twii_data = None
        self.last_reset = datetime.now().date()
        
        # Components
        self.painter = ChartPainter(self)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

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

def _backfill_intraday_net(self, client, code):
        """ 🔥 強力回補：從 Fugle 1分K 推算早盤籌碼 """
        try:
            # 呼叫 1分K 數據
            chart = client.stock.intraday.chart(symbol=code, type='1k')
            if not chart or 'data' not in chart: 
                return 0
            
            sim_net = 0
            for k in chart['data']:
                o = k.get('open', 0)
                c = k.get('close', 0)
                v = k.get('volume', 0)
                
                # 簡單算法：紅K加項，黑K減項
                if c > o: sim_net += v
                elif c < o: sim_net -= v
            
            # Debug: 如果回補有數值，印出來看看 (或是用 st.toast)
            if sim_net != 0:
                print(f"[Backfill] {code}: {sim_net}")
                
            return int(sim_net)
        except Exception as e:
            print(f"[Backfill Error] {code}: {e}")
            return 0

def _fetch_stock(self, code, now_time=None):
        try:
            if now_time is None: now_time = datetime.now(timezone.utc) + timedelta(hours=8)
            client = next(self.client_cycle) if self.client_cycle else None
            if not client: return None

            # [🔥 Backfill Check] 
            # 邏輯修正：只要 daily_net 中沒有這個 code，或者值為 0 且現在已經開盤很久了，就嘗試回補一次
            if code not in self.daily_net:
                val = self._backfill_intraday_net(client, code)
                self.daily_net[code] = val
                # 初始化 prev_data 避免第一筆 tick 暴衝
                q_init = client.stock.intraday.quote(symbol=code)
                if q_init and 'lastPrice' in q_init:
                     self.prev_data[code] = {'vol': q_init['total']['tradeVolume'], 'price': q_init['lastPrice']}

            static_data = self.base_vol_cache.get(code, {})
            # ... (保持原本程式碼不變) ...

    def _hybrid_strategy(self, code, price, pct, vwap, net_day, net_1h, ratio, now_time):
        """ ⚖️ 混合策略中心：決定是當沖還是隔日沖 """
        
        signal = "盤整"
        signal_type = "NONE" # DAY (當沖) / SWING (隔日)
        
        is_bullish = price >= (vwap * 1.005)
        is_bearish = price < (vwap * 0.995)
        
        # --- 時間軸分割 ---
        is_morning = now_time.hour < 12
        is_afternoon = now_time.hour >= 12

        # 1. ☀️ 早盤當沖邏輯 (09:00 - 12:00)
        # 重視速率 (Velocity) 與 1H 動能
        if is_morning:
            if ratio >= 1.2 and is_bullish and net_1h > 0:
                signal = "🔥攻擊"
                signal_type = "DAY"
            elif ratio >= 1.5 and is_bearish and net_1h < 0:
                signal = "💀出貨"
                signal_type = "DAY"
            elif is_bullish and net_1h > 50 and ratio > 0.8: 
                signal = "💣伏擊"
                signal_type = "DAY"

        # 2. 🌙 尾盤隔日沖/鎖碼邏輯 (12:00 - 13:30)
        # 重視存量 (Volume) 與型態守護
        elif is_afternoon:
            if pct >= 9.5:
                signal = "👑漲停"
                signal_type = "SWING"
            # 鎖碼條件：漲幅 2%~8.5%，Net Day 大買，且價格在高檔
            elif 2.0 < pct < 8.5 and net_day > 150 and price > vwap * 1.01:
                signal = "🌙鎖碼" 
                signal_type = "SWING"
            elif net_1h < -50 and price < vwap:
                signal = "💀尾盤殺"
                signal_type = "SWING" # 隔日空
            elif pct > 3.0 and net_1h > 0:
                signal = "🔥尾盤"
                signal_type = "SWING"

        # 基本狀態描述修正
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

            # [Backfill Check] 新增標的自動回補
            if code not in self.daily_net:
                self.daily_net[code] = self._backfill_intraday_net(client, code)

            static_data = self.base_vol_cache.get(code, {})
            base_vol_5ma = static_data.get('vol_5ma', 0)
            base_vol_yest = static_data.get('vol_yest', 0)
            price_5ma = static_data.get('price_5ma', 0)
            win_rate = static_data.get('win_rate', 0)
            avg_amp = static_data.get('avg_amp', 0)

            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', 0)
            if not price: return None

            # Tick 計算
            order_book = q.get('order', {})
            best_asks = order_book.get('bestAsks', []) or []
            best_bids = order_book.get('bestBids', []) or []
            best_ask = best_asks[0].get('price') if best_asks else None
            best_bid = best_bids[0].get('price') if best_bids else None

            pct = q.get('changePercent', 0)
            vol_lots = q.get('total', {}).get('tradeVolume', 0)
            
            if vol_lots == 0 and code in self.prev_data: vol_lots = self.prev_data[code]['vol']
            
            vol = vol_lots * 1000
            est_lots = _calc_est_vol(vol_lots)
            ratio = est_lots / base_vol_5ma if base_vol_5ma > 0 else 0
            ratio_yest = est_lots / base_vol_yest if base_vol_yest > 0 else 0
            total_val = q.get('total', {}).get('tradeValue', 0)
            vwap = (total_val / vol) if vol > 0 else price

            # Net Volume Calculation
            delta_net = 0
            if code in self.prev_data:
                prev_v = self.prev_data[code]['vol']
                delta_v = vol_lots - prev_v
                if delta_v > 0:
                    if best_ask and price >= best_ask: delta_net = int(delta_v)
                    elif best_bid and price <= best_bid: delta_net = -int(delta_v)
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

            # [混合策略判讀]
            raw_signal, signal_type = self._hybrid_strategy(code, price, pct, vwap, net_day, net_1h, ratio, now_time)
            
            # Active Light (VWAP + Time Check)
            active_light = 1 if (price >= vwap * 1.005) and (now_time.time() >= dt_time(9, 5)) else 0

            # TP/SL
            tp_price = adjust_to_tick(vwap * 1.025, 'round')
            sl_price = adjust_to_tick(vwap * 0.985, 'floor')

            # Event Dispatch
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

# Singleton Engine using Streamlit Cache to persist across re-runs
@st.cache_resource
def get_engine():
    return SniperEngine()

engine = get_engine()

# ==========================================
# 8. UI Rendering (Inject JS + Table)
# ==========================================
def render_dashboard():
    # CSS/JS Injection for Hover Chart
    # 使用一條長的字串或 textwrap.dedent 來避免縮排問題
    st.markdown("""
<style>
    #chart-tooltip {
        display: none; position: fixed; z-index: 9999;
        background-color: #0e1117; border: 1px solid #444;
        border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.5);
        padding: 5px; width: 420px; pointer-events: none;
    }
    #chart-tooltip img { width: 100%; height: auto; border-radius: 4px; }
    tr.hover-row:hover { background-color: #262730 !important; cursor: crosshair; }
    
    /* Table Styles */
    table.sniper-table { width: 100%; border-collapse: collapse; font-family: 'Courier New', monospace; }
    table.sniper-table th { text-align: left; background-color: #262730; color: white; padding: 8px; font-size: 14px; white-space: nowrap; }
    table.sniper-table td { padding: 6px; border-bottom: 1px solid #444; font-size: 15px; vertical-align: middle; white-space: nowrap; }
    table.sniper-table tr.pinned-row { background-color: #fff9c4 !important; color: black !important; }
    table.sniper-table tr.golden-row { background-color: #fff9c4 !important; color: black !important; font-weight: bold; }
</style>

<div id="chart-tooltip"><img id="tooltip-img" src="" /></div>

<script>
    window.showTooltip = function(event, b64_data) {
        if (!b64_data) return;
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
        tooltip.style.left = left + 'px';
        tooltip.style.top = top + 'px';
    }
</script>
""", unsafe_allow_html=True)

    # Fetch Data
    df = db.get_watchlist_view()
    
    # Merge TWII data
    if engine.twii_data:
        twii_row = pd.DataFrame([engine.twii_data])
        df = pd.concat([twii_row, df], ignore_index=True) if not df.empty else twii_row

    if df.empty: st.info("無監控數據"); return

    # Numeric conversion
    for col in ['price', 'pct', 'vwap', 'ratio', 'ratio_yest', 'net_10m', 'net_1h', 'net_day', 'win_rate']:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # HTML Table Header (注意：這裡移除了縮排)
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
        
        # Colors & Style
        c_price = "#ff4d4f" if row['pct'] > 0 else "#2ecc71" if row['pct'] < 0 else "#999999"
        
        # Signal & Type
        signal = row.get('event_label', '盤整')
        sig_type = row.get('signal_type', 'NONE')
        type_icon = "☀️" if sig_type == "DAY" else "🌙" if sig_type == "SWING" else ""
        sig_html = f"<span style='font-weight:bold; color:{c_price}'>{signal} {type_icon}</span>"
        
        # VWAP Light
        active = row.get('active_light', 0)
        vwap_light = "🟢" if active == 1 else "🔴"
        tp_price = adjust_to_tick(row['vwap'] * 1.025)
        vwap_html = f"<span style='color:#e67e22'>{row['vwap']:.2f} {vwap_light}</span> <span style='font-size:0.8em; color:#888'>(TP:{tp_price})</span>"
        
        # Volume Light
        vol_light = "🟢" if row['ratio'] > 1.2 else "🔴"
        ratio_html = f"{row['ratio']:.1f} {vol_light}"
        
        # Net Light
        n1h = int(row['net_1h']); nd = int(row['net_day'])
        c1h = "#ff4d4f" if n1h > 0 else "#2ecc71"
        cd = "#ff4d4f" if nd > 0 else "#2ecc71"
        net_html = f"<span style='color:{c1h}'>{n1h}</span> / <span style='color:{cd}'>{nd}</span>"
        
        # Hover Chart Data
        chart_b64 = engine.painter.chart_cache.get(code, "")
        
        # Row Class (Three Lights = Golden)
        row_class = "hover-row"
        if active == 1 and row['ratio'] > 1.2 and n1h > 0: row_class += " golden-row"
        elif row.get('is_pinned'): row_class += " pinned-row"

        pin_icon = "📌" if row.get('is_pinned') else ""
        
        # Link
        name_html = f"<a href='https://tw.stock.yahoo.com/quote/{code}.TW' target='_blank' style='text-decoration:none; color:#3498db;'>{row['name']}</a>"
        if is_twii: name_html = row['name']

        # TR Construction (注意：這裡也移除了縮排)
        tr = f"""<tr class="{row_class}" 
onmouseover="window.showTooltip(event, '{chart_b64}')" 
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
with st.sidebar:
    st.title("🦅 Sniper Hybrid v7.0")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🚀 啟動"):
            engine.start(); st.toast("系統全開：引擎 + 繪圖師")
            st.rerun()
    with col2:
        if st.button("🛑 停止"):
            engine.stop(); st.toast("系統休眠")
            st.rerun()
            
    with st.expander("庫存 & 監控管理"):
        inv_txt = st.text_area("庫存", DEFAULT_INVENTORY)
        if st.button("更新庫存"): db.update_inventory_list(inv_txt); engine.update_targets()
        
        watch_txt = st.text_area("監控", DEFAULT_WATCHLIST)
        if st.button("更新監控 (含回測)"):
            if not API_KEYS: st.error("No API Key")
            else:
                db.update_watchlist(watch_txt)
                engine.update_targets()
                # 簡單初始化靜態數據
                static_data = []
                for c in engine.targets:
                     static_data.append((c, *fetch_static_stats(None, c)))
                db.upsert_static(static_data)
                st.success("已更新")

    st.caption(f"Status: {'RUNNING' if engine.running else 'STOPPED'}")

try:
    from streamlit import fragment
except ImportError:
    # Fallback if st.fragment not available
    def fragment(run_every=None):
        def decorator(f): return f
        return decorator

@fragment(run_every=2)
def auto_refresh_dashboard():
    render_dashboard()

auto_refresh_dashboard()
