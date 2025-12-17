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
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import queue
from openai import OpenAI
from io import BytesIO
import random

# ==========================================
# 1. 基礎設定 & 參數
# ==========================================
st.set_page_config(page_title="Sniper v2.5 (Chart Debug)", page_icon="📈", layout="wide")

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
DB_PATH = "sniper_v35.db"

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
# 2. 資料庫層 (SQLite)
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
            net_1h REAL, net_day REAL, signal TEXT, update_time REAL,
            data_status TEXT DEFAULT 'DATA_OK',
            signal_level TEXT DEFAULT 'B',
            risk_status TEXT DEFAULT 'NORMAL'
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
            (code, name, category, price, pct, vwap, vol, ratio, net_1h, net_day, signal, update_time, data_status, signal_level, risk_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data_list)
        conn.commit()
        conn.close()

    def get_pinned_view(self):
        conn = self.get_conn()
        query = '''
            SELECT p.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal,
            r.net_1h, r.net_day, s.yoy, s.eps, s.pe, 1 as is_pinned, r.data_status, r.risk_status
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
            r.net_1h, r.net_day, s.yoy, s.eps, s.pe, i.cost, i.qty,
            (r.price - i.cost) * i.qty * 1000 as profit_val,
            (r.price - i.cost) / i.cost * 100 as profit_pct,
            CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned,
            r.data_status, r.risk_status
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
            r.net_1h, r.net_day, s.yoy, s.eps, s.pe,
            CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned,
            r.data_status, r.risk_status, r.signal_level
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
# 3. 核心工具與定義
# ==========================================

def send_telegram_message(message):
    if not TG_BOT_TOKEN or not TG_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try: requests.post(url, data=payload, timeout=5)
    except: pass

def send_telegram_photo(caption, image_bytes):
    print(f"[TG] Sending Photo... Size: {len(image_bytes)} bytes")
    if not TG_BOT_TOKEN or not TG_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
    payload = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
    # 這裡直接傳 BytesIO
    files = {'photo': ('chart.png', image_bytes, 'image/png')}
    try: 
        r = requests.post(url, data=payload, files=files, timeout=10)
        print(f"[TG] Photo Resp: {r.status_code} | {r.text[:50]}")
    except Exception as e:
        print(f"[TG] Photo Err: {e}")

# --- 產圖工具 (Debug Enhanced) ---
def generate_intraday_chart(code, name):
    print(f"[CHART] Start generating for {code}...")
    try:
        ticker_list = [f"{code}.TW", f"{code}.TWO"]
        df = pd.DataFrame()
        
        # 策略 A: 1日 1分K
        for ticker in ticker_list:
            try:
                print(f"[CHART] Fetching {ticker} (1d/1m)...")
                df = yf.download(ticker, period="1d", interval="1m", progress=False, auto_adjust=False)
                if not df.empty: 
                    print(f"[CHART] Got {len(df)} rows (1m)")
                    break
            except Exception as e:
                print(f"[CHART] YF Error 1: {e}")
            
        # 策略 B: 5日 5分K
        chart_title = f"{code} {name} Intraday"
        if df.empty:
            print("[CHART] 1m failed, trying 5d/5m...")
            for ticker in ticker_list:
                try:
                    df = yf.download(ticker, period="5d", interval="5m", progress=False, auto_adjust=False)
                    if not df.empty: 
                        print(f"[CHART] Got {len(df)} rows (5m)")
                        chart_title = f"{code} {name} 5-Day Trend (Backup)"
                        break
                except Exception as e:
                    print(f"[CHART] YF Error 2: {e}")

        if df.empty: 
            print("[CHART] All fetch failed!")
            return None

        # 處理 MultiIndex
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        
        # 繪圖
        df['VWAP'] = (df['Volume'] * (df['High'] + df['Low'] + df['Close']) / 3).cumsum() / df['Volume'].cumsum()

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
        print(f"[CHART] Generated PNG: {len(img_bytes)} bytes")
        return img_bytes
    except Exception as e: 
        print(f"[CHART] Generate Exception: {e}")
        return None

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
    if elapsed <= 0: return current_vol
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

# ==========================================
# 4. Agent Logic: 模擬數據與規則引擎 (Backend)
# ==========================================

def _get_institutional_3d(code):
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
# 5. AIAgent (Formatter Role)
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
                "mode": "standard"
            },
            "institutional": {
                "foreign": {"D": inst_data['foreign'][0], "D-1": inst_data['foreign'][1], "D-2": inst_data['foreign'][2]},
                "trust":   {"D": inst_data['trust'][0],   "D-1": inst_data['trust'][1],   "D-2": inst_data['trust'][2]},
                "dealer":  {"D": inst_data['dealer'][0],  "D-1": inst_data['dealer'][1],  "D-2": inst_data['dealer'][2]}
            },
            "price_ctx": {
                "price_vs_vwap_pct": round((event['price'] - event['vwap']) / event['vwap'] * 100, 2),
                "volume_ratio": event['ratio'],
                "net_main_1h": "BUY" if event['net_1h'] > 0 else "SELL"
            },
            "flags": flags
        }

    def _process_event(self, event):
        if not openai_client: return
        state_json = json.dumps(self._build_state(event), ensure_ascii=False)
        
        # V2.5 System Prompt: 格式化引擎
        system_prompt = """
        你不是分析師，也不是交易顧問。
        你是「即時市場資料格式化引擎（Market Snapshot Formatter）」。

        專職任務：
        將輸入資料轉為「極短、可掃描」的 Telegram 快照卡片。
        
        嚴格遵守以下輸出格式 (包含分隔線與符號)：
        
        【法人快照｜{code}｜{time}】

        外資：{D}｜{D-1}｜{D-2}
        投信：{D}｜{D-1}｜{D-2}
        自營：{D}｜{D-1}｜{D-2}
        ────────────────
        合計：{Total_D}｜{Total_D-1}｜{Total_D-2}
        P/V：{price_vs_vwap_pct}%｜量比 {volume_ratio}
        1H 大戶：{net_main_1h}
        Flags：{flags (用｜分隔)}

        禁止事項：
        ❌ 禁止使用任何形容詞 (偏多/偏空/強/弱)
        ❌ 禁止進行解讀、預測或評論
        ❌ 禁止推論不存在的資料
        ❌ 禁止修改 Flags 內容
        
        若資料缺失，請顯示 N/A。
        """
        user_prompt = f"Input JSON:\n{state_json}"

        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.0
            )
            ai_output = response.choices[0].message.content
            send_telegram_message(ai_output)
        except Exception: pass

agent = AIAgent()
agent.start()

# ==========================================
# 5. Event Dispatcher
# ==========================================
class EventDispatcher:
    def __init__(self):
        self.alert_history = {}

    def dispatch(self, event):
        event_type = event['event_type']
        trigger = event['trigger']
        code = event['code']
        scope = event['scope']

        if event_type == "SYSTEM":
            alert_key = f"{code}_SYSTEM"
            last_time = self.alert_history.get(alert_key, 0)
            if time.time() - last_time > 3600:
                send_telegram_message(f"⚠️ <b>系統異常報告</b>\n{code}: {trigger}")
                self.alert_history[alert_key] = time.time()
            return

        alert_key = f"{code}_{trigger}"
        last_time = self.alert_history.get(alert_key, 0)
        
        is_test = event.get('is_test', False)
        
        if (time.time() - last_time > 600) or is_test:
            st.toast(f"🚀 觸發事件: {trigger} | {code}")
            self._send_instant_notification(event)
            
            if trigger in ["🔥攻擊", "💣伏擊"]:
                agent.push_event(event) # AI Formatter
                threading.Thread(target=self._send_chart, args=(event,)).start()

            elif trigger in ["👀量增"]:
                threading.Thread(target=self._send_chart, args=(event,)).start()

            elif scope == "inventory" and event_type == "RISK":
                agent.push_event(event)

            self.alert_history[alert_key] = time.time()

    def _send_instant_notification(self, event):
        emoji = "💣" if "伏擊" in event['trigger'] else "🚀" if "攻擊" in event['trigger'] else "☠️" if "出貨" in event['trigger'] or "跌破" in event['trigger'] else "👀"
        msg = (
            f"{emoji} <b>{event['trigger']}偵測：{event['code']} {event['name']}</b>\n"
            f"現價：{event['price']:.2f} ({event['pct']:.2f}%)\n"
            f"量比：{event['ratio']:.1f}\n"
            f"🤖 正在快照法人數據..."
        )
        send_telegram_message(msg)

    def _send_chart(self, event):
        print(f"[CHART] Thread Start: {event['code']}")
        img_bytes = generate_intraday_chart(event['code'], event['name'])
        if img_bytes:
            print(f"[CHART] Success, sending TG photo...")
            caption = f"📉 <b>{event['code']} 當日走勢參考 (非預測)</b>\nTrigger: {event['trigger']}"
            send_telegram_photo(caption, img_bytes)
        else:
            print("[CHART] Failed to generate image.")

dispatcher = EventDispatcher()

# ==========================================
# 6. Sniper Engine
# ==========================================
@st.cache_resource
class SniperEngine:
    def __init__(self):
        self.is_running = False
        self.targets = []
        self.inventory_codes = []
        self.vol_queues = {} 
        self.daily_net = {} 
        self.prev_data = {} 
        self.history_vol = {}
        
        self.daily_active_flags = {} 
        self.daily_risk_flags = {}   
        
        self.last_reset_date = datetime.now().date()
        self.clients = []
        self._init_clients()
        self.update_targets()

    def _init_clients(self):
        if not API_KEYS: return
        for key in API_KEYS:
            try: self.clients.append(RestClient(api_key=key))
            except: pass

    def update_targets(self):
        self.targets = db.get_all_targets()
        self.inventory_codes = db.get_inventory_codes()

    def start(self):
        if self.is_running: return
        self.update_targets()
        self.is_running = True
        threading.Thread(target=self._run_loop, daemon=True).start()

    def stop(self):
        self.is_running = False

    def _fetch_single_stock(self, client, code):
        try:
            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', q.get('previousClose', 0))
            if price is None or price == 0:
                return (code, "Unknown", "Unknown", 0, 0, 0, 0, 0, 0, 0, "DATA_ERROR", time.time(), "DATA_ERROR", "B", "NORMAL")

            pct = q.get('changePercent', 0)
            vol = q.get('total', {}).get('tradeVolume', 0) * 1000
            total_val = q.get('total', {}).get('tradeValue', 0)
            vwap = total_val / vol if vol > 0 else price
            
            if code not in self.history_vol:
                try:
                    candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                    if candles and 'data' in candles and len(candles['data']) >= 2:
                        self.history_vol[code] = int(candles['data'][-2]['volume']) // 1000
                    else: self.history_vol[code] = 1000
                except: self.history_vol[code] = 1000
            
            base_vol = self.history_vol.get(code, 1000)
            est_vol = _calc_est_vol(q.get('total', {}).get('tradeVolume', 0))
            ratio = est_vol / base_vol if base_vol > 0 else 0
            
            delta_net = 0
            if code in self.prev_data:
                prev_v = self.prev_data[code]['vol']
                prev_p = self.prev_data[code]['price']
                delta_v = (vol - prev_v) / 1000
                threshold = get_big_order_threshold(price)
                if delta_v >= threshold:
                    if price >= prev_p: delta_net = int(delta_v)
                    elif price < prev_p: delta_net = -int(delta_v)
            
            self.prev_data[code] = {'vol': vol, 'price': price}
            now_ts = time.time()
            if code not in self.vol_queues: self.vol_queues[code] = []
            if delta_net != 0:
                self.vol_queues[code].append((now_ts, delta_net))
                self.daily_net[code] = self.daily_net.get(code, 0) + delta_net
            
            one_hour_ago = now_ts - 3600
            self.vol_queues[code] = [x for x in self.vol_queues[code] if x[0] > one_hour_ago]
            net_1h = sum(x[1] for x in self.vol_queues[code])
            net_day = self.daily_net.get(code, 0)
            
            tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
            is_bullish = price >= vwap
            is_breakdown = price < (vwap * 0.99)
            
            event_type = "STRATEGY"
            signal_level = "B"
            risk_status = "NORMAL"
            
            if code in self.daily_risk_flags:
                risk_status = "RISK_LOCKED"
            
            has_active_signal = False
            if code in self.daily_active_flags and (self.daily_active_flags[code] & {"ATTACK", "AMBUSH"}):
                has_active_signal = True

            raw_signal = check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown, price, vwap, has_active_signal)
            
            scope = "inventory" if code in self.inventory_codes else "watchlist"
            trigger = None

            if scope == "inventory":
                if pct <= -2.0:
                    trigger = "跌破-2%"
                    event_type = "RISK"
                    risk_status = "RISK_LOCKED"
                    if code not in self.daily_risk_flags:
                        self.daily_risk_flags.setdefault(code, set()).add("BREAKDOWN")
                    else: trigger = None 

                elif "出貨" in raw_signal:
                    trigger = "💀出貨"
                    event_type = "RISK"
                    risk_status = "RISK_LOCKED"
                    if code not in self.daily_risk_flags:
                        self.daily_risk_flags.setdefault(code, set()).add("DISTRIBUTION")
                    else: trigger = None

                elif "攻擊" in raw_signal and not has_active_signal:
                    trigger = "🔥攻擊"
                    signal_level = "A_PLUS"
                    self.daily_active_flags.setdefault(code, set()).add("ATTACK")

            if scope == "watchlist":
                if not has_active_signal:
                    if "攻擊" in raw_signal:
                        trigger = "🔥攻擊"
                        signal_level = "A_PLUS"
                        self.daily_active_flags.setdefault(code, set()).add("ATTACK")
                    elif "伏擊" in raw_signal:
                        trigger = "💣伏擊"
                        signal_level = "A_PLUS"
                        self.daily_active_flags.setdefault(code, set()).add("AMBUSH")
                
                if not trigger and "量增" in raw_signal:
                    trigger = "👀量增"
                    signal_level = "A_MINUS"

            if trigger:
                event = {
                    "code": code,
                    "name": get_stock_name(code),
                    "scope": scope,
                    "event_type": event_type,
                    "trigger": trigger,
                    "price": price,
                    "pct": pct,
                    "vwap": vwap,
                    "ratio": ratio,
                    "net_1h": net_1h,
                    "net_day": net_day,
                    "timestamp": now_ts
                }
                dispatcher.dispatch(event)

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol, ratio, net_1h, net_day, raw_signal, now_ts, "DATA_OK", signal_level, risk_status)
        except Exception as e:
            error_event = {
                "code": code, "scope": "system", "event_type": "SYSTEM",
                "trigger": f"DATA_ERROR: {str(e)}", "timestamp": time.time()
            }
            dispatcher.dispatch(error_event)
            return (code, "Unknown", "Unknown", 0, 0, 0, 0, 0, 0, 0, "DATA_ERROR", time.time(), "DATA_ERROR", "B", "NORMAL")

    def _run_loop(self):
        while self.is_running:
            if not self.targets: time.sleep(1); continue
            
            now = datetime.now(timezone.utc) + timedelta(hours=8)
            if now.date() > self.last_reset_date:
                self.daily_active_flags = {}
                self.daily_risk_flags = {}
                self.last_reset_date = now.date()

            market_open = dt_time(9, 0)
            market_close = dt_time(13, 35)
            is_market_open = market_open <= now.time() <= market_close
            sleep_time = 0.5 if is_market_open else 10
            
            batch_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for i, code in enumerate(self.targets):
                    client = self.clients[i % len(self.clients)] if self.clients else None
                    if client: futures.append(executor.submit(self._fetch_single_stock, client, code))
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res: batch_data.append(res)
            
            if batch_data: db.upsert_realtime_batch(batch_data)
            time.sleep(sleep_time)

engine = SniperEngine()

# ==========================================
# 7. UI 呈現
# ==========================================

inv_container = st.container()
st.divider()
watch_container = st.container()

with st.sidebar:
    st.title("⚙️ 戰情室設定")
    mode = st.radio("身分模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
    st.subheader("🔍 濾網設定")
    use_filter = st.checkbox("只看基本面良好")
    if use_filter: st.caption("條件：YoY > 0, EPS > 0, PE < 50")

    if mode == "👨‍✈️ 指揮官":
        with st.expander("📦 庫存管理 (Inventory)", expanded=False):
            inv_input = st.text_area("庫存清單 (代碼,成本,張數)", DEFAULT_INVENTORY, height=100)
            if st.button("更新庫存"):
                db.update_inventory_list(inv_input)
                engine.update_targets()
                st.toast("庫存已更新！")

        with st.expander("🔭 監控設定 (Watchlist)", expanded=True):
            raw_input = st.text_area("新選清單", DEFAULT_WATCHLIST, height=150)
            if st.button("1. 初始化並更新清單", type="primary"):
                if not API_KEYS:
                    st.error("缺 API Key")
                else:
                    db.update_watchlist(raw_input)
                    engine.update_targets()
                    targets = engine.targets
                    status = st.status("正在初始化數據 (含基本面)...", expanded=True)
                    client = RestClient(api_key=API_KEYS[0])
                    history_vol = {}
                    static_list = []
                    progress_bar = status.progress(0)
                    for i, code in enumerate(targets):
                        try:
                            candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                            if candles and 'data' in candles and len(candles['data']) >= 2:
                                history_vol[code] = int(candles['data'][-2]['volume']) // 1000
                            else: history_vol[code] = 1000
                        except: history_vol[code] = 1000
                        yoy, eps, pe = fetch_fundamental_data(code)
                        static_list.append((code, 0, 0, yoy, eps, pe))
                        progress_bar.progress((i + 1) / len(targets))
                        time.sleep(0.1)
                    engine.history_vol = history_vol
                    db.upsert_static(static_list)
                    status.update(label="初始化完成！", state="complete")
                    
            if st.checkbox("2. 啟動監控核心", value=engine.is_running):
                if not engine.is_running: engine.start(); st.toast("核心已啟動")
            else: engine.stop()

    st.markdown("---")
    
    tg_status = "✅" if TG_BOT_TOKEN and TG_CHAT_ID else "❌ Missing"
    gpt_status = "✅" if OPENAI_API_KEY else "❌ Missing"
    if tg_status == "✅": tg_display = f"{tg_status} ({TG_BOT_TOKEN[:4]}...)"
    else: tg_display = tg_status
        
    st.caption(f"Telegram: {tg_display}")
    st.caption(f"GPT: {gpt_status}")

    st.subheader("🧪 系統測試 (盤後專用)")
    if st.button("發送測試訊號 (Test Fire 🔥)"):
        # Mock Attack
        mock_event = {
            "code": "2330", "name": "台積電 (測試)", "scope": "watchlist", "event_type": "STRATEGY",
            "trigger": "🔥攻擊", "price": 888.0, "pct": 3.5, "vwap": 870.0, "ratio": 2.5, "net_1h": 500, "net_day": 1200, "timestamp": time.time(),
            "is_test": True
        }
        dispatcher.dispatch(mock_event)
        st.success("測試「攻擊」訊號已發送！")
        
    if st.button("發送測試訊號 (Test Ambush 💣)"):
        # Mock Ambush
        mock_ambush = {
            "code": "2603", "name": "長榮 (測試)", "scope": "watchlist", "event_type": "STRATEGY",
            "trigger": "💣伏擊", "price": 155.5, "pct": 0.8, "vwap": 155.0, "ratio": 12.5, "net_1h": 800, "net_day": 1500, "timestamp": time.time(),
            "is_test": True
        }
        dispatcher.dispatch(mock_ambush)
        st.success("測試「伏擊」訊號已發送！")

    if st.button("發送測試訊號 (Test Risk 💀)"):
        # Mock Risk
        mock_risk = {
            "code": "2317", "name": "鴻海 (測試)", "scope": "inventory", "event_type": "RISK",
            "trigger": "💀出貨", "price": 100.0, "pct": -2.5, "vwap": 103.0, "ratio": 1.8, "net_1h": -300, "net_day": -800, "timestamp": time.time(),
            "is_test": True
        }
        dispatcher.dispatch(mock_risk)
        st.success("測試「風險」訊號已發送！")

now_time = datetime.now(timezone.utc) + timedelta(hours=8)
st.caption(f"最後更新: {now_time.strftime('%H:%M:%S')} (每3秒)")

with inv_container:
    st.subheader("📦 庫存損益 (Portfolio)")
    df_inv = db.get_inventory_view()
    if not df_inv.empty:
        df_inv = df_inv.rename(columns={'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 'code': '代碼', 'name': '名稱', 'signal': '訊號', 'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE', 'cost': '成本', 'profit_val': '損益$', 'profit_pct': '報酬%', 'risk_status': '狀態'})
        cols = ['代碼', '名稱', '狀態', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE', '成本', '損益$', '報酬%']
        for c in cols: 
            if c not in df_inv.columns: df_inv[c] = 0
        df_inv_show = df_inv[cols].copy()
        df_inv_show['營收YoY'] = df_inv_show['營收YoY'].fillna(0); df_inv_show['EPS'] = df_inv_show['EPS'].fillna(0); df_inv_show['PE'] = df_inv_show['PE'].fillna(0)
        row_count = len(df_inv_show)
        calc_height = (row_count + 1) * 35 + 3
        if calc_height > 400: calc_height = 400
        
        st.data_editor(
            df_inv_show,
            column_config={"代碼": st.column_config.TextColumn("代碼", width="small", pinned=True), "名稱": st.column_config.TextColumn("名稱", pinned=True), "狀態": st.column_config.TextColumn("狀態", width="small"), "成本": st.column_config.NumberColumn("成本", format="%.2f"), "現價": st.column_config.NumberColumn("現價", format="%.2f"), "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"), "均價": st.column_config.NumberColumn("均價", format="%.2f"), "量比": st.column_config.NumberColumn("量比", format="%.1f"), "損益$": st.column_config.NumberColumn("損益$", format="%d"), "報酬%": st.column_config.NumberColumn("報酬%", format="%.2f%%"), "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"), "EPS": st.column_config.NumberColumn("EPS", format="%.2f"), "PE": st.column_config.NumberColumn("PE", format="%.1f")},
            width='stretch', hide_index=True, height=calc_height, disabled=True, key="inv_table"
        )
    else: st.info("尚無庫存資料，請在左側「指揮官」模式設定。")

with watch_container:
    st.subheader("🔭 監控雷達 (Watchlist)")
    df_watch = db.get_watchlist_view()
    if not df_watch.empty:
        df_watch['Pinned'] = df_watch['is_pinned'].astype(bool)
        if use_filter: df_watch = df_watch[(df_watch['yoy'] > 0) & (df_watch['eps'] > 0) & (df_watch['pe'] < 50)]
        df_watch = df_watch.rename(columns={'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 'code': '代碼', 'name': '名稱', 'signal': '訊號', 'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE', 'signal_level': '等級'})
        df_watch['level_score'] = df_watch['等級'].apply(lambda x: 10 if x == 'A_PLUS' else (5 if x == 'A_MINUS' else 0))
        df_watch = df_watch.sort_values(by=['Pinned', 'level_score', '漲跌%'], ascending=[False, False, False])
        cols_w = ['Pinned', '代碼', '名稱', '等級', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE']
        for c in cols_w: 
            if c not in df_watch.columns: df_watch[c] = 0
        df_watch_show = df_watch[cols_w].copy()
        
        rows = len(df_watch_show)
        row_height = 35
        max_rows = 45 
        calc_height = (rows + 1) * row_height + 5
        limit_height = (max_rows + 1) * row_height + 5
        final_height = max(min(calc_height, limit_height), 300)

        edited_watch = st.data_editor(
            df_watch_show,
            column_config={"Pinned": st.column_config.CheckboxColumn("📌", width="small", pinned=True), "代碼": st.column_config.TextColumn("代碼", width="small", pinned=True), "名稱": st.column_config.TextColumn("名稱", pinned=True), "等級": st.column_config.TextColumn("等級", width="small"), "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"), "EPS": st.column_config.NumberColumn("EPS", format="%.2f"), "PE": st.column_config.NumberColumn("PE", format="%.1f"), "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"), "現價": st.column_config.NumberColumn("現價", format="%.2f"), "均價": st.column_config.NumberColumn("均價", format="%.2f"), "量比": st.column_config.NumberColumn("量比", format="%.1f")},
            width='stretch', hide_index=True, height=final_height, key="watch_editor"
        )
        if not df_watch.empty:
            changes = edited_watch[['代碼', 'Pinned']].set_index('代碼')
            for index, row in changes.iterrows(): db.update_pinned(index, row['Pinned'])
    else: st.info("尚無監控資料。")

time.sleep(3)
st.rerun()
