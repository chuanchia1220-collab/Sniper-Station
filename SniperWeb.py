import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, time as dt_time, timedelta, timezone
import time
import os
import twstock
import json
import threading
import sqlite3
import concurrent.futures
from collections import deque

# ==========================================
# 1. 基礎設定 & 常數
# ==========================================
st.set_page_config(page_title="Sniper 戰情室 (Pro v23.1)", page_icon="🚀", layout="wide")

# 讀取多組 API Key
try:
    raw_keys = st.secrets["Fugle_API_Key"]
except:
    raw_keys = os.getenv("Fugle_API_Key", "")
API_KEYS = [k.strip() for k in raw_keys.split(',') if k.strip()]

DB_PATH = "sniper.db"

# 核心股池
DEFAULT_POOL = (
    "3706 2449 6442 3017 6139 4977 3163 3037 2359 1519 "
    "2330 2317 2382 3231 2356 2454 2303 3711 3081 4979 3363 3450 2345 "
    "3324 3653 2421 3032 2059 3323 6781 4931 "
    "3189 8046 2368 6274 2383 6191 5469 8021 "
    "2344 2408 8299 3260 2409 3481 "
    "1513 2609 2615 8033 2634 2201 4763 5284 3264"
)

STOCK_CATS = {
    '2330': '半導體', '2303': '半導體', '2454': '半導體', '3711': '半導體',
    '2317': 'AI組裝', '2382': 'AI組裝', '3231': 'AI組裝', '2356': 'AI組裝', '3706': 'AI組裝',
    '3081': 'CPO/網通', '3163': 'CPO/網通', '4979': 'CPO/網通', '4977': 'CPO/網通', '3363': 'CPO/網通', '3450': 'CPO/網通', '2345': 'CPO/網通', '6442': 'CPO/網通',
    '3017': '散熱/BBU', '3324': '散熱/BBU', '3653': '散熱/BBU', '2421': '散熱/BBU', '3032': '散熱/BBU', '2059': '散熱/BBU', '3323': '散熱/BBU', '6781': '散熱/BBU', '4931': '散熱/BBU',
    '3037': 'PCB/載板', '3189': 'PCB/載板', '8046': 'PCB/載板', '2368': 'PCB/載板', '6274': 'PCB/載板', '2383': 'PCB/載板', '6191': 'PCB/載板', '5469': 'PCB/載板', '8021': 'PCB/載板',
    '2344': '記憶體', '2408': '記憶體', '8299': '記憶體', '3260': '記憶體',
    '2409': '面板', '3481': '面板',
    '2359': '機器人', '8033': '軍工', '2634': '軍工', '5284': '軍工',
    '2609': '航運', '2615': '航運',
    '2449': '封測', '3264': '封測'
}

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
            code TEXT PRIMARY KEY, win REAL, ret REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS pinned (
            code TEXT PRIMARY KEY
        )''')
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

    def get_display_data(self):
        conn = self.get_conn()
        # 這裡會選出 vwap
        query = '''
            SELECT 
                r.code, r.name, r.category, 
                s.win, s.ret, 
                r.price, r.pct, r.vwap, r.ratio, r.net_1h, r.net_day, r.signal,
                CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned
            FROM realtime r
            LEFT JOIN static_info s ON r.code = s.code
            LEFT JOIN pinned p ON r.code = p.code
        '''
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def update_pinned(self, code, is_pinned):
        conn = self.get_conn()
        c = conn.cursor()
        if is_pinned:
            c.execute('INSERT OR IGNORE INTO pinned (code) VALUES (?)', (code,))
        else:
            c.execute('DELETE FROM pinned WHERE code = ?', (code,))
        conn.commit()
        conn.close()
    
    def upsert_static(self, data_list):
        conn = self.get_conn()
        c = conn.cursor()
        c.executemany('INSERT OR REPLACE INTO static_info (code, win, ret) VALUES (?, ?, ?)', data_list)
        conn.commit()
        conn.close()

db = Database(DB_PATH)

# ==========================================
# 3. 邏輯層
# ==========================================
def get_yahoo_ticker(raw_code):
    code = raw_code.strip().upper()
    if code in twstock.codes:
        if twstock.codes[code].market == '上櫃': return f"{code}.TWO"
    return f"{code}.TW"

def get_stock_name(symbol):
    try:
        if symbol in twstock.codes: return twstock.codes[symbol].name
        return symbol
    except: return symbol

def get_category(code):
    return STOCK_CATS.get(code, '其他')

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

def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown):
    if pct >= 9.5: return "👑漲停"
    if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: return "🔥攻擊"
    if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200: return "👀量增"
    if is_breakdown and ratio >= tgt_ratio and net_1h < 0: return "💀出貨"
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    if is_bullish and pct >= tgt_pct: return "⚠️價強"
    return "盤整"

# ==========================================
# 4. 核心引擎 (Singleton)
# ==========================================
@st.cache_resource
class SniperEngine:
    def __init__(self):
        self.is_running = False
        self.targets = []
        self.vol_queues = {} 
        self.daily_net = {} 
        self.prev_data = {} 
        self.history_vol = {}
        self.clients = []
        self._init_clients()

    def _init_clients(self):
        if not API_KEYS: return
        for key in API_KEYS:
            try:
                self.clients.append(RestClient(api_key=key))
            except: pass

    def start(self, targets):
        if self.is_running: return
        self.targets = targets
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
            
            # 歷史量快取
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
            
            # 大戶籌碼
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
            
            tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
            is_bullish = price >= vwap
            is_breakdown = price < (vwap * 0.99)
            signal = check_signal(pct, is_bullish, self.daily_net.get(code, 0), net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown)
            
            return (code, get_stock_name(code), get_category(code), price, pct, vwap, vol, ratio, net_1h, self.daily_net.get(code, 0), signal, now_ts)
            
        except Exception as e:
            return None

    def _run_loop(self):
        while self.is_running:
            if not self.targets or not self.clients:
                time.sleep(1)
                continue

            now = datetime.now(timezone.utc) + timedelta(hours=8)
            market_open = dt_time(9, 0)
            market_close = dt_time(13, 35)
            is_market_open = market_open <= now.time() <= market_close
            
            sleep_time = 0.5 if is_market_open else 30
            
            batch_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for i, code in enumerate(self.targets):
                    client = self.clients[i % len(self.clients)]
                    futures.append(executor.submit(self._fetch_single_stock, client, code))
                
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res:
                        batch_data.append(res)
            
            if batch_data:
                db.upsert_realtime_batch(batch_data)
            
            time.sleep(sleep_time)

engine = SniperEngine()

# ==========================================
# 5. UI 呈現
# ==========================================

# 側邊欄
with st.sidebar:
    st.title("⚙️ 指揮中心")
    mode = st.radio("模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
    
    if mode == "👨‍✈️ 指揮官":
        if engine.is_running:
            st.success("🟢 核心運算中")
        else:
            st.warning("⚪ 核心待機")

        raw_input = st.text_area("監控清單", DEFAULT_POOL, height=100)
        
        if st.button("初始化 & 啟動"):
            if not API_KEYS:
                st.error("請設定 API Key")
            else:
                targets = [t.strip() for t in raw_input.split() if t.strip()]
                
                status = st.status("執行回測中...", expanded=True)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=180)
                static_list = []
                
                for code in targets:
                    tk = get_yahoo_ticker(code)
                    try:
                        df = yf.download(tk, start=start_date, end=end_date, progress=False)
                        if df.empty:
                             alt = tk.replace('.TW', '.TWO') if '.TW' in tk else tk.replace('.TWO', '.TW')
                             df = yf.download(alt, start=start_date, end=end_date, progress=False)
                        
                        win, ret = 0, 0
                        if not df.empty and len(df) > 20:
                            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                            df['ret'] = df['Close'].pct_change()
                            df = df.dropna()
                            if len(df) > 0:
                                ret = (df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0] * 100
                                win = len(df[df['ret'] > 0]) / len(df) * 100
                        
                        static_list.append((code, win, ret))
                    except: 
                        static_list.append((code, 0, 0))
                
                db.upsert_static(static_list)
                status.update(label="初始化完成，啟動監控！", state="complete")
                
                engine.start(targets)
                time.sleep(1)
                st.rerun()
                
        if st.button("停止運算"):
            engine.stop()
            st.rerun()

# 主畫面
now_time = datetime.now(timezone.utc) + timedelta(hours=8)
st.title(f"⚡ Sniper 戰情室 (Pro v23.1)")
st.caption(f"最後更新: {now_time.strftime('%H:%M:%S')}")

df = db.get_display_data()

if not df.empty:
    df['Pinned'] = df['is_pinned'].astype(bool)
    
    # === 顯示設定：加入 vwap (均價) ===
    column_config = {
        "Pinned": st.column_config.CheckboxColumn("📌", width="small"),
        "code": "代碼",
        "name": "名稱",
        "category": "類別",
        "win": st.column_config.NumberColumn("勝率%", format="%.0f%%"),
        "ret": st.column_config.NumberColumn("報酬%", format="%.1f%%"),
        "price": st.column_config.NumberColumn("現價", format="%.2f"),
        "pct": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
        "vwap": st.column_config.NumberColumn("均價", format="%.2f"), # <--- 補回這一行
        "ratio": st.column_config.NumberColumn("量比", format="%.1f"),
        "net_1h": st.column_config.NumberColumn("大戶1H", format="%d"),
        "net_day": st.column_config.NumberColumn("大戶日", format="%d"),
        "signal": "訊號"
    }
    
    df['sig_score'] = df['signal'].apply(lambda x: 10 if '攻擊' in str(x) or '漲停' in str(x) else (5 if '量增' in str(x) else 0))
    df = df.sort_values(by=['Pinned', 'sig_score', 'pct'], ascending=[False, False, False])
    
    # 在 column_order 中加入 "vwap"
    edited_df = st.data_editor(
        df,
        column_config=column_config,
        column_order=["Pinned", "code", "name", "price", "pct", "vwap", "signal", "ratio", "net_1h", "net_day", "win", "ret", "category"],
        hide_index=True,
        use_container_width=True,
        height=1000,
        key="data_editor"
    )
    
    if not df.empty:
        changes = edited_df[['code', 'Pinned']].set_index('code')
        for index, row in changes.iterrows():
            db.update_pinned(index, row['Pinned'])

else:
    st.info("尚無數據。請切換至「指揮官」模式進行初始化。")

time.sleep(3)
st.rerun()
