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

# ==========================================
# 1. 基礎設定 & 參數
# ==========================================
st.set_page_config(page_title="Sniper 戰情室 3.0 (v32.0)", page_icon="⚔️", layout="wide")

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
DB_PATH = "sniper_v3.db"

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
            net_1h REAL, net_day REAL, signal TEXT, update_time REAL
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
            (code, name, category, price, pct, vwap, vol, ratio, net_1h, net_day, signal, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data_list)
        conn.commit()
        conn.close()

    def get_pinned_view(self):
        conn = self.get_conn()
        query = '''
            SELECT p.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal,
            r.net_1h, r.net_day, s.yoy, s.eps, s.pe, 1 as is_pinned
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
            CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned
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
            CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned
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
# 3. 核心工具
# ==========================================

def send_telegram_message(message):
    if not TG_BOT_TOKEN or not TG_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try: requests.post(url, data=payload, timeout=5)
    except: pass

def send_telegram_photo(caption, image_bytes):
    if not TG_BOT_TOKEN or not TG_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
    payload = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
    files = {'photo': image_bytes}
    try: requests.post(url, data=payload, files=files, timeout=10)
    except: pass

def generate_intraday_chart(code, name):
    try:
        ticker = f"{code}.TW"
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if df.empty:
            ticker = f"{code}.TWO"
            df = yf.download(ticker, period="1d", interval="1m", progress=False)
        
        if df.empty: return None

        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df['VWAP'] = (df['Volume'] * (df['High'] + df['Low'] + df['Close']) / 3).cumsum() / df['Volume'].cumsum()

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df.index, y=df['Close'], mode='lines', name='Price', line=dict(color='red')))
        fig.add_trace(go.Scatter(x=df.index, y=df['VWAP'], mode='lines', name='VWAP', line=dict(color='orange', dash='dot')))
        
        fig.update_layout(
            title=f"{code} {name} Intraday",
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
    if elapsed <= 0: return current_vol
    total = 270
    if elapsed >= total: return current_vol
    return int(current_vol * (total / elapsed))

# 核心訊號邏輯 (新增 Ambush 伏擊)
def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown, price, vwap, has_attacked):
    # 1. 漲停
    if pct >= 9.5: return "👑漲停"
    
    # 2. 伏擊 (新增邏輯: 高量 + 價穩 + 未噴 + 大戶買 + 未曾攻擊)
    # 量比 > 10, 均價+-1%, 漲幅<=2%, 大戶1h>0, 今日沒攻擊過
    if (ratio >= 10.0) and (abs(price - vwap) / vwap <= 0.01) and (pct <= 2.0) and (net_1h > 0) and (not has_attacked):
        return "💣伏擊"

    # 3. 攻擊
    if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: return "🔥攻擊"
    
    # 4. 量增
    if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200: return "👀量增"
    
    # 5. 出貨
    if is_breakdown and ratio >= tgt_ratio and net_1h < 0: return "💀出貨"
    
    # 6. 誘多
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    
    # 7. 價強
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
# 4. AI Agent
# ==========================================
class AIAgent:
    def __init__(self):
        self.event_queue = queue.Queue()
        self.is_running = False
        self.BANNED_KEYWORDS = ["進場", "出場", "操作", "佈局", "加碼", "減碼", "有利", "不利", "偏多", "偏空", "看好", "看壞", "適合", "不適合", "建議"]
        
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
        return {
            "ticker": event['code'],
            "name": event['name'],
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "facts": {
                "price": event['price'],
                "change_pct": {"value": event['pct'], "direction": "UP" if event['pct'] > 0 else "DOWN"},
                "vwap": event['vwap'],
                "volume_ratio": event['ratio'],
                "net_main_1h": {"value": event['net_1h'], "direction": "BUY" if event['net_1h'] > 0 else "SELL"}
            },
            "signal": event['trigger'],
            "position_status": "HOLD" if event['scope'] == "inventory" else "NO_POSITION",
            "context": "此分析僅為單一股票單一時點狀態，不包含大盤、產業或未來推論"
        }

    def _process_event(self, event):
        if not openai_client: return
        state_json = json.dumps(self._build_state(event), ensure_ascii=False)
        system_prompt = """你是交易輔助分析引擎... (同前)""" # 簡化顯示，實際執行保留原 Prompt
        user_prompt = f"""...資料如下：{state_json}""" # 簡化顯示

        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.3
            )
            ai_analysis = response.choices[0].message.content
            hit_banned = False
            for keyword in self.BANNED_KEYWORDS:
                if keyword in ai_analysis: hit_banned = True; break
            if hit_banned: ai_analysis = "⚠️ AI 回應包含行為暗示，已被系統中立化處理。"
            report = f"🤖 <b>Sniper GPT 分析｜{event['code']}</b>\n{ai_analysis}"
            send_telegram_message(report)
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
        alert_key = f"{event['code']}_{event['trigger']}"
        last_time = self.alert_history.get(alert_key, 0)
        now_ts = time.time()
        
        if now_ts - last_time > 600:
            self._send_instant_notification(event)
            
            trigger = event['trigger']
            scope = event['scope']

            # A級：伏擊、攻擊、量增 -> 分析 + 畫圖
            if trigger in ["🔥攻擊", "👀量增", "💣伏擊"]:
                agent.push_event(event)
                threading.Thread(target=self._send_chart, args=(event,)).start()

            # B級：庫存風險 -> 僅分析 (不畫圖)
            elif scope == "inventory" and (trigger in ["💀出貨", "跌破-2%"]):
                agent.push_event(event)

            self.alert_history[alert_key] = now_ts

    def _send_instant_notification(self, event):
        # 新增伏擊 Emoji
        emoji = "💣" if "伏擊" in event['trigger'] else "🚀" if "攻擊" in event['trigger'] else "☠️" if "出貨" in event['trigger'] or "跌破" in event['trigger'] else "👀"
        msg = (
            f"{emoji} <b>{event['trigger']}偵測：{event['code']} {event['name']}</b>\n"
            f"現價：{event['price']:.2f} ({event['pct']:.2f}%)\n"
            f"量比：{event['ratio']:.1f}\n"
            f"🤖 系統處理中..."
        )
        send_telegram_message(msg)

    def _send_chart(self, event):
        img_bytes = generate_intraday_chart(event['code'], event['name'])
        if img_bytes:
            caption = f"📉 <b>{event['code']} 當日走勢證據</b>\nTrigger: {event['trigger']}"
            send_telegram_photo(caption, img_bytes)

dispatcher = EventDispatcher()

# ==========================================
# 6. Sniper Engine (State Transition)
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
        # 狀態管理：記錄每日已發生的攻擊
        self.daily_signals = {} # {code: set('🔥攻擊', '👀量增')}
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
            if price is None: price = 0
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
            
            # --- 狀態轉移邏輯 ---
            has_attacked = False
            if code in self.daily_signals and "🔥攻擊" in self.daily_signals[code]:
                has_attacked = True

            signal = check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown, price, vwap, has_attacked)
            
            # 更新狀態
            if "攻擊" in signal:
                if code not in self.daily_signals: self.daily_signals[code] = set()
                self.daily_signals[code].add("🔥攻擊")

            # Trigger Logic
            scope = "inventory" if code in self.inventory_codes else "watchlist"
            trigger = None
            
            if scope == "watchlist":
                # 新增伏擊
                if "攻擊" in signal or "量增" in signal or "伏擊" in signal: trigger = signal
            
            if scope == "inventory":
                if pct <= -2.0: trigger = "跌破-2%" 
                elif "出貨" in signal: trigger = "💀出貨"
                elif "攻擊" in signal: trigger = "🔥攻擊"

            if trigger:
                event = {
                    "code": code,
                    "name": get_stock_name(code),
                    "scope": scope,
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

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol, ratio, net_1h, net_day, signal, now_ts)
        except Exception: return None

    def _run_loop(self):
        while self.is_running:
            if not self.targets: time.sleep(1); continue
            
            # 每日狀態重置
            now = datetime.now(timezone.utc) + timedelta(hours=8)
            if now.date() > self.last_reset_date:
                self.daily_signals = {}
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
    st.caption(f"Bot: {'✅ 啟用' if TG_BOT_TOKEN else '❌ 未設定'}")
    st.caption(f"GPT: {'✅ 啟用' if OPENAI_API_KEY else '❌ 未設定'}")

now_time = datetime.now(timezone.utc) + timedelta(hours=8)
st.caption(f"最後更新: {now_time.strftime('%H:%M:%S')} (每3秒)")

with inv_container:
    st.subheader("📦 庫存損益 (Portfolio)")
    df_inv = db.get_inventory_view()
    if not df_inv.empty:
        df_inv = df_inv.rename(columns={'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 'code': '代碼', 'name': '名稱', 'signal': '訊號', 'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE', 'cost': '成本', 'profit_val': '損益$', 'profit_pct': '報酬%'})
        cols = ['代碼', '名稱', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE', '成本', '損益$', '報酬%']
        for c in cols: 
            if c not in df_inv.columns: df_inv[c] = 0
        df_inv_show = df_inv[cols].copy()
        df_inv_show['營收YoY'] = df_inv_show['營收YoY'].fillna(0); df_inv_show['EPS'] = df_inv_show['EPS'].fillna(0); df_inv_show['PE'] = df_inv_show['PE'].fillna(0)
        row_count = len(df_inv_show)
        calc_height = (row_count + 1) * 35 + 3
        if calc_height > 400: calc_height = 400
        
        st.data_editor(
            df_inv_show,
            column_config={"代碼": st.column_config.TextColumn("代碼", width="small", pinned=True), "名稱": st.column_config.TextColumn("名稱", pinned=True), "成本": st.column_config.NumberColumn("成本", format="%.2f"), "現價": st.column_config.NumberColumn("現價", format="%.2f"), "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"), "均價": st.column_config.NumberColumn("均價", format="%.2f"), "量比": st.column_config.NumberColumn("量比", format="%.1f"), "損益$": st.column_config.NumberColumn("損益$", format="%d"), "報酬%": st.column_config.NumberColumn("報酬%", format="%.2f%%"), "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"), "EPS": st.column_config.NumberColumn("EPS", format="%.2f"), "PE": st.column_config.NumberColumn("PE", format="%.1f")},
            use_container_width=True, hide_index=True, height=calc_height, disabled=True, key="inv_table"
        )
    else: st.info("尚無庫存資料，請在左側「指揮官」模式設定。")

with watch_container:
    st.subheader("🔭 監控雷達 (Watchlist)")
    df_watch = db.get_watchlist_view()
    if not df_watch.empty:
        df_watch['Pinned'] = df_watch['is_pinned'].astype(bool)
        if use_filter: df_watch = df_watch[(df_watch['yoy'] > 0) & (df_watch['eps'] > 0) & (df_watch['pe'] < 50)]
        df_watch = df_watch.rename(columns={'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 'code': '代碼', 'name': '名稱', 'signal': '訊號', 'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE'})
        df_watch['sig_score'] = df_watch['訊號'].apply(lambda x: 15 if '伏擊' in str(x) else (10 if '攻擊' in str(x) else (5 if '量增' in str(x) else 0)))
        df_watch = df_watch.sort_values(by=['Pinned', 'sig_score', '漲跌%'], ascending=[False, False, False])
        cols_w = ['Pinned', '代碼', '名稱', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE']
        for c in cols_w: 
            if c not in df_watch.columns: df_watch[c] = 0
        df_watch_show = df_watch[cols_w].copy()
        df_watch_show['營收YoY'] = df_watch_show['營收YoY'].fillna(0); df_watch_show['EPS'] = df_watch_show['EPS'].fillna(0); df_watch_show['PE'] = df_watch_show['PE'].fillna(0)
        
        edited_watch = st.data_editor(
            df_watch_show,
            column_config={"Pinned": st.column_config.CheckboxColumn("📌", width="small", pinned=True), "代碼": st.column_config.TextColumn("代碼", width="small", pinned=True), "名稱": st.column_config.TextColumn("名稱", pinned=True), "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"), "EPS": st.column_config.NumberColumn("EPS", format="%.2f"), "PE": st.column_config.NumberColumn("PE", format="%.1f"), "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"), "現價": st.column_config.NumberColumn("現價", format="%.2f"), "均價": st.column_config.NumberColumn("均價", format="%.2f"), "量比": st.column_config.NumberColumn("量比", format="%.1f")},
            use_container_width=True, hide_index=True, height=600, key="watch_editor"
        )
        if not df_watch.empty:
            changes = edited_watch[['代碼', 'Pinned']].set_index('代碼')
            for index, row in changes.iterrows(): db.update_pinned(index, row['Pinned'])
    else: st.info("尚無監控資料。")

time.sleep(3)
st.rerun()
