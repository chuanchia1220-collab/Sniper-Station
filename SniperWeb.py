import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta, timezone, time as dt_time
import time
import os
import twstock
import json
import threading
import sqlite3
import concurrent.futures
from collections import deque
import shutil
import requests
import pandas_ta as ta
import queue
from openai import OpenAI
from io import BytesIO
import random
from itertools import cycle

# ==========================================
# 1. 基礎設定 & 參數
# ==========================================
st.set_page_config(page_title="Sniper v3.0 (架構重構)", page_icon="🛡️", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "") 
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v40.db"

openai_client = None
if OPENAI_API_KEY:
    try: openai_client = OpenAI(api_key=OPENAI_API_KEY)
    except: pass

DEFAULT_WATCHLIST = (
    "3035 3037 2368 2383 6274 8046 3189 "
    "3324 3017 3653 2421 3483 "
    "3081 3163 4979 4908 3363 4977 6442 "
    "2356 3231 2382 6669 2317 "
    "2330 2454 2303 6781 4931 3533"
)
DEFAULT_INVENTORY = """2330,800,1\n2317,105,5"""

# ==========================================
# 2. MarketSession (單一事實來源)
# ==========================================
class MarketSession:
    """盤中判斷的唯一來源"""
    MARKET_OPEN = dt_time(9, 0)
    MARKET_CLOSE = dt_time(13, 35)

    @staticmethod
    def is_market_open(now=None):
        if not now:
            now = datetime.now(timezone.utc) + timedelta(hours=8)
        return MarketSession.MARKET_OPEN <= now.time() <= MarketSession.MARKET_CLOSE

# ==========================================
# 3. 資料庫層 (SQLite)
# ==========================================
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_db()

    def get_conn(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def init_db(self):
        conn = self.get_conn()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (
            code TEXT PRIMARY KEY, name TEXT, category TEXT,
            price REAL, pct REAL, vwap REAL, vol REAL, ratio REAL,
            net_1h REAL, net_10m REAL, net_day REAL, 
            signal TEXT, update_time REAL,
            data_status TEXT DEFAULT 'DATA_OK',
            signal_level TEXT DEFAULT 'B',
            risk_status TEXT DEFAULT 'NORMAL',
            last_alert_time REAL DEFAULT 0
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS static_info (
            code TEXT PRIMARY KEY, win REAL, ret REAL, yoy REAL, eps REAL, pe REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (
            code TEXT PRIMARY KEY, cost REAL, qty REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pinned (code TEXT PRIMARY KEY)''')
        conn.commit()
        conn.close()

    def upsert_realtime_batch(self, data_list):
        if not data_list: return
        conn = self.get_conn()
        c = conn.cursor()
        c.executemany('''
            INSERT OR REPLACE INTO realtime 
            (code, name, category, price, pct, vwap, vol, ratio, net_1h, net_day, signal, update_time, data_status, signal_level, risk_status, net_10m)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data_list)
        conn.commit()
        conn.close()
        
    def update_last_alert_time(self, code, timestamp):
        conn = self.get_conn()
        c = conn.cursor()
        c.execute('UPDATE realtime SET last_alert_time = ? WHERE code = ?', (timestamp, code))
        conn.commit()
        conn.close()

    def get_pinned_view(self):
        conn = self.get_conn()
        query = '''
            SELECT p.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal,
            r.net_1h, r.net_10m, r.net_day, s.yoy, s.eps, s.pe, 1 as is_pinned, r.data_status, r.risk_status, r.last_alert_time
            FROM pinned p
            LEFT JOIN realtime r ON p.code = r.code
            LEFT JOIN static_info s ON p.code = s.code
        '''
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def get_inventory_view(self):
        conn = self.get_conn()
        query = '''
            SELECT i.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal,
            r.net_1h, r.net_10m, r.net_day, s.yoy, s.eps, s.pe, i.cost, i.qty,
            (r.price - i.cost) * i.qty * 1000 as profit_val,
            (r.price - i.cost) / i.cost * 100 as profit_pct,
            CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned,
            r.data_status, r.risk_status, r.last_alert_time
            FROM inventory i
            LEFT JOIN realtime r ON i.code = r.code
            LEFT JOIN static_info s ON i.code = s.code
            LEFT JOIN pinned p ON i.code = p.code
        '''
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def get_watchlist_view(self):
        conn = self.get_conn()
        query = '''
            SELECT w.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal, 
            r.net_1h, r.net_10m, r.net_day, s.yoy, s.eps, s.pe,
            CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned,
            r.data_status, r.risk_status, r.signal_level, r.last_alert_time
            FROM watchlist w
            LEFT JOIN realtime r ON w.code = r.code
            LEFT JOIN static_info s ON w.code = s.code
            LEFT JOIN pinned p ON w.code = p.code
        '''
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def update_inventory_list(self, inventory_text):
        conn = self.get_conn()
        c = conn.cursor()
        c.execute('DELETE FROM inventory')
        lines = inventory_text.split('\n')
        for line in lines:
            parts = line.split(',')
            if len(parts) >= 2:
                code = parts[0].strip()
                try:
                    cost = float(parts[1].strip())
                    qty = float(parts[2].strip()) if len(parts) > 2 else 1.0
                    c.execute('INSERT OR REPLACE INTO inventory (code, cost, qty) VALUES (?, ?, ?)', (code, cost, qty))
                except: pass
        conn.commit()
        conn.close()

    def update_watchlist(self, codes_text):
        conn = self.get_conn()
        c = conn.cursor()
        c.execute('DELETE FROM watchlist')
        targets = [t.strip() for t in codes_text.split() if t.strip()]
        for t in targets:
            c.execute('INSERT OR REPLACE INTO watchlist (code) VALUES (?)', (t,))
        conn.commit()
        conn.close()
        return targets

    def get_all_targets(self):
        conn = self.get_conn()
        c = conn.cursor()
        c.execute('SELECT code FROM inventory UNION SELECT code FROM watchlist UNION SELECT code FROM pinned')
        rows = c.fetchall()
        conn.close()
        return [r[0] for r in rows]
        
    def get_inventory_codes(self):
        conn = self.get_conn()
        c = conn.cursor()
        c.execute('SELECT code FROM inventory')
        rows = c.fetchall()
        conn.close()
        return [r[0] for r in rows]

    def update_pinned(self, code, is_pinned):
        conn = self.get_conn()
        c = conn.cursor()
        if is_pinned: c.execute('INSERT OR IGNORE INTO pinned (code) VALUES (?)', (code,))
        else: c.execute('DELETE FROM pinned WHERE code = ?', (code,))
        conn.commit()
        conn.close()

    def upsert_static(self, data_list):
        conn = self.get_conn()
        c = conn.cursor()
        c.executemany('INSERT OR REPLACE INTO static_info (code, win, ret, yoy, eps, pe) VALUES (?, ?, ?, ?, ?, ?)', data_list)
        conn.commit()
        conn.close()

db = Database(DB_PATH)

# ==========================================
# 4. UI Adapter (The Stabilizer)
# ==========================================
def build_ui_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """將原始 DB 資料轉換為嚴格型別的 UI 專用 DataFrame"""
    if df_raw.empty:
        return pd.DataFrame(columns=["code", "name", "price", "pct", "vwap", "ratio", "net_1h", "net_10m", "net_day", "signal", "data_status", "last_alert_time"])

    df = df_raw.copy()

    NUM_COLS = ["price", "pct", "vwap", "ratio", "last_alert_time"]
    for col in NUM_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    INT_COLS = ["net_1h", "net_10m", "net_day"]
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["data_status"] = df["data_status"].fillna("DATA_OK").astype(str)
    df["signal"] = df["signal"].fillna("").astype(str)
    if "name" in df.columns:
        df["name"] = df["name"].fillna("Unknown").astype(str)
    if "code" in df.columns:
        df["code"] = df["code"].astype(str)

    return df

# ==========================================
# 5. 核心工具與定義
# ==========================================

def send_telegram_message(message, buttons=None):
    """唯一 Telegram 發送接口 (僅供 TelegramWorker 使用)"""
    if not TG_BOT_TOKEN or not TG_CHAT_ID: return
    
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID, 
        "text": message, 
        "parse_mode": "HTML", 
        "disable_web_page_preview": True
    }
    
    if buttons:
        payload["reply_markup"] = json.dumps({"inline_keyboard": buttons})
        
    try: requests.post(url, data=payload, timeout=5)
    except: pass

def send_telegram_photo(caption, image_bytes):
    """唯一 Telegram 圖片發送接口"""
    if not TG_BOT_TOKEN or not TG_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
    payload = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
    files = {'photo': image_bytes}
    try: requests.post(url, data=payload, files=files, timeout=10)
    except: pass

def generate_intraday_chart(code, name):
    try:
        ticker_list = [f"{code}.TW", f"{code}.TWO"]
        df = pd.DataFrame()
        
        for ticker in ticker_list:
            try:
                df = yf.download(ticker, period="1d", interval="1m", progress=False, auto_adjust=False)
                if not df.empty: break
            except: pass
            
        chart_title = f"{code} {name} Intraday"
        if df.empty:
            for ticker in ticker_list:
                try:
                    df = yf.download(ticker, period="5d", interval="5m", progress=False, auto_adjust=False)
                    if not df.empty: 
                        chart_title = f"{code} {name} 5-Day Trend (Backup)"
                        break
                except: pass

        if df.empty: return None

        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        
        df['VWAP'] = (df['Volume'] * (df['High'] + df['Low'] + df['Close']) / 3).cumsum() / df['Volume'].cumsum()

        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df.index, y=df['Close'], mode='lines', name='Price', line=dict(color='red')))
        if "Intraday" in chart_title:
            fig.add_trace(go.Scatter(x=df.index, y=df['VWAP'], mode='lines', name='VWAP', line=dict(color='orange', dash='dot')))
        
        fig.update_layout(
            title=chart_title,
            xaxis_title="Time", yaxis_title="Price",
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=20),
            height=400, width=600
        )
        img_bytes = fig.to_image(format="png")
        return img_bytes
    except: return None

def get_stock_name(symbol):
    if symbol not in twstock.codes:
        try: twstock.__update_codes()
        except: pass
    try:
        if symbol in twstock.codes: return twstock.codes[symbol].name
        return symbol
    except: return symbol

def get_dynamic_thresholds(price):
    if price < 50: return 3.5, 2.5
    elif price < 300: return 2.5, 1.5
    else: return 2.0, 1.2

def get_big_order_threshold(price):
    if price <= 0: return 5
    threshold = int(400 / price)
    return max(1, threshold)

def _calc_est_vol(current_vol):
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    open_t = now.replace(hour=9, minute=0, second=0, microsecond=0)
    elapsed = (now - open_t).seconds / 60
    
    if elapsed < 15: return current_vol
    total = 270
    if elapsed >= total: return current_vol
    return int(current_vol * (total / elapsed))

def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown, price, vwap, has_attacked):
    if pct >= 9.5: return "👑漲停"
    if (ratio >= 10.0) and (abs(price - vwap) / vwap <= 0.01) and (pct <= 2.0) and (net_1h > 0) and (not has_attacked):
        return "💣伏擊"
    if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: return "🔥攻擊"
    if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200: return "👀量增"
    if is_breakdown and ratio >= tgt_ratio and net_1h < 0: return "💀出貨"
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    if is_bullish and pct >= tgt_pct: return "⚠️價強"
    return "盤整"

def fetch_fundamental_data(code):
    suffixes = ['.TW', '.TWO']
    for suffix in suffixes:
        try:
            ticker = yf.Ticker(f"{code}{suffix}")
            info = ticker.info
            if 'symbol' in info:
                yoy = info.get('revenueGrowth', 0); yoy = yoy * 100 if yoy else 0
                eps = info.get('trailingEps', 0) if info.get('trailingEps') else 0
                pe = info.get('trailingPE', 0) if info.get('trailingPE') else 0
                return yoy, eps, pe
        except: continue
    return 0, 0, 0

def _get_institutional_3d(code):
    today_seed = int(datetime.now().strftime("%Y%m%d")) + int(code)
    random.seed(today_seed)
    return {
        "foreign": [random.randint(-1000, 2000), random.randint(-500, 1500), random.randint(100, 1000)],
        "trust":   [random.randint(-200, 500), random.randint(0, 300), random.randint(-100, 200)],
        "dealer":  [random.randint(-100, 100), random.randint(-50, 50), random.randint(-50, 50)]
    }

def calculate_flags(inst_data, price, price_change_pct):
    flags = []
    f_d, f_d1, f_d2 = inst_data['foreign']
    t_d, t_d1, t_d2 = inst_data['trust']
    d_d, d_d1, d_d2 = inst_data['dealer']
    total_d = f_d + t_d + d_d
    
    if f_d > 0 and t_d > 0 and total_d >= 500: flags.append("INST_SYNC")
    if f_d > 0 and f_d1 > 0 and f_d2 > 0: flags.append("FOREIGN_3UP")
    if f_d <= 0 and t_d >= 200: flags.append("TRUST_TAKEOVER")
    if price_change_pct > 0 and total_d < 0: flags.append("PRICE_UP_INST_DOWN")
        
    return flags if flags else ["—"]

# ==========================================
# 6. NotificationManager (核心控制層)
# ==========================================
class NotificationManager:
    """唯一決定「是否推播」的地方"""
    def __init__(self):
        self.notification_queue = queue.Queue()
        self.cooldown_cache = {}  # {code: last_trigger_time}
        self.COOLDOWN_SECONDS = 600  # 10分鐘冷卻

    def should_notify(self, event):
        """決策核心：是否推播"""
        # Test 事件永遠推
        if event.get('is_test', False):
            return True
        
        # 非 DATA_OK 不推
        if event.get('data_status') != 'DATA_OK':
            return False
        
        # 盤後不推 (唯一判斷點)
        if not MarketSession.is_market_open():
            return False
        
        # 冷卻檢查
        if self.is_in_cooldown(event):
            return False
        
        return True

    def is_in_cooldown(self, event):
        code = event['code']
        trigger = event['trigger']
        key = f"{code}_{trigger}"
        
        if key in self.cooldown_cache:
            last_time = self.cooldown_cache[key]
            if time.time() - last_time < self.COOLDOWN_SECONDS:
                return True
        
        return False

    def enqueue(self, event):
        """接收 Event，決定是否放入推播佇列"""
        if self.should_notify(event):
            self.cooldown_cache[f"{event['code']}_{event['trigger']}"] = time.time()
            self.notification_queue.put(event)

    def get_notification(self, timeout=1):
        """供 TelegramWorker 取用"""
        try:
            return self.notification_queue.get(timeout=timeout)
        except queue.Empty:
            return None

notification_manager = NotificationManager()

# ==========================================
# 7. TelegramWorker (唯一推播出口)
# ==========================================
class TelegramWorker:
    """單一 Thread 負責所有 Telegram 推播"""
    def __init__(self):
        self.is_running = False
        self.worker_thread = None

    def start(self):
        if self.is_running: return
        self.is_running = True
        self.worker_thread = threading.Thread(target=self._run, daemon=True)
        self.worker_thread.start()

    def stop(self):
        self.is_running = False

    def _run(self):
        while self.is_running:
            notification = notification_manager.get_notification(timeout=1)
            if notification:
                self._send(notification)

    def _send(self, event):
        """實際發送邏輯"""
        emoji = "💣" if "伏擊" in event['trigger'] else "🚀" if "攻擊" in event['trigger'] else "☠️" if "出貨" in event['trigger'] or "跌破" in event['trigger'] else "👀"
        
        def fmt_num(n): return f"+{n}" if n > 0 else f"{n}"
        
        pv_pct = round((event['price'] - event['vwap']) / event['vwap'] * 100, 2)
        net_10m_str = fmt_num(int(event.get('net_10m', 0)))
        net_1h_str = fmt_num(int(event.get('net_1h', 0)))
        net_day_str = fmt_num(int(event.get('net_day', 0)))
        
        msg = (
            f"{emoji} <b>{event['trigger']}偵測：{event['code']} {event['name']}</b>\n"
            f"現價：{event['price']:.2f} ({event['pct']:.2f}%)\n"
            f"P/V：{fmt_num(pv_pct)}%｜量比 {event['ratio']:.1f}\n"
            f"10Min 大戶：{net_10m_str} | 1H 大戶：{net_1h_str} | 大戶日：{net_day_str}"
        )
        
        buttons = [
            [
                {"text": "📈 TradingView (1m)", "url": f"https://www.tradingview.com/chart/?symbol=TWSE%3A{event['code']}&interval=1"},
                {"text": "📊 Yahoo 走勢", "url": f"https://tw.stock.yahoo.com/quote/{event['code']}.TW"}
            ]
        ]
        
        send_telegram_message(msg, buttons)
        
        # 附加處理
        if event['trigger'] in ["🔥攻擊", "💣伏擊"]:
            if openai_client:
                agent.push_event(event)
            threading.Thread(target=self._send_chart, args=(event,), daemon=True).start()
        elif event['trigger'] in ["👀量增"]:
            threading.Thread(target=self._send_chart, args=(event,), daemon=True).start()
        elif event['scope'] == "inventory" and event['event_type'] == "RISK":
            if openai_client:
                agent.push_event(event)

    def _send_chart(self, event):
        img_bytes = generate_intraday_chart(event['code'], event['name'])
        if img_bytes:
            caption = f"📉 <b>{event['code']} 當日走勢參考 (非預測)</b>\nTrigger: {event['trigger']}"
            send_telegram_photo(caption, img_bytes)

telegram_worker = TelegramWorker()
telegram_worker.start()

# ==========================================
# 8. AIAgent (Formatter Role)
# ==========================================
class AIAgent:
    def __init__(self):
        self.event_queue = queue.Queue()
        self.is_running = False
        
    def start(self):
        if self.is_running: return
        self.is_running = True
        threading.Thread(target=self._agent_loop, daemon=True).start()
        
    def push_event(self, event):
        self.event_queue.put(event)
        
    def _agent_loop(self):
        while self.is_running:
            try:
                event = self.event_queue.get(timeout=1)
                self._process_event(event)
            except queue.Empty: continue
            except Exception: pass

    def _build_state(self, event):
        inst_data = _get_institutional_3d(event['code'])
        flags = calculate_flags(inst_data, event['price'], event['pct'])
        return {
            "meta": {
                "code": event['code'],
                "name": event['name'],
                "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            },
            "institutional": {
                "foreign": {"D": inst_data['foreign'][0], "D-1": inst_data['foreign'][1], "D-2": inst_data['foreign'][2]},
                "trust":   {"D": inst_data['trust'][0],   "D-1": inst_data['trust'][1],   "D-2": inst_data['trust'][2]},
                "dealer":  {"D": inst_data['dealer'][0],  "D-1": inst_data['dealer'][1],  "D-2": inst_data['dealer'][2]}
            },
            "flags": flags
        }

    def _process_event(self, event):
        if not openai_client: return
        state_json = json.dumps(self._build_state(event), ensure_ascii=False)
        
        system_prompt = """
        你是「即時市場資料格式化引擎」。
        專職任務：將輸入資料轉為「Telegram 法人快照卡片」。
        
        嚴格遵守輸出格式：
        
        【法人快照｜{code}｜{time}】

        外資：{D}｜{D-1}｜{D-2}
        投信：{D}｜{D-1}｜{D-2}
        自營：{D}｜{D-1}｜{D-2}
        ────────────────
        合計：{Total_D}｜{Total_D-1}｜{Total_D-2}
        Flags：{flags (用｜分隔)}
        (※法人數據為盤中模擬)

        規則：
        1. 法人數據：直接顯示數字，正數不需加號，負數顯示負號。
        2. 禁止解讀、預測或評論。
        """
        user_prompt = f"Input JSON:\n{state_json}"

        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.0
            )
            ai_output = response.choices[0].message.content
            
            buttons = [
                [
                    {"text": "📈 TradingView (1m)", "url": f"https://www.tradingview.com/chart/?symbol=TWSE%3A{event['code']}&interval=1"},
                    {"text": "📊 Yahoo 走勢", "url": f"https://tw.stock.yahoo.com/quote/{event['code']}.TW"}
                ]
            ]
            send_telegram_message(ai_output, buttons=buttons)
        except Exception: pass

agent = AIAgent()
agent.start()

# ==========================================
# 9. EventDispatcher (僅產生 Event)
# ==========================================
class EventDispatcher:
    """不再直接推播，只負責產生 Event"""
    def dispatch(self, event):
        """將 Event 交給 NotificationManager 決定是否推播"""
        notification_manager.enqueue(event)
