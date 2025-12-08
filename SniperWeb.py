import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta, timezone
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

# ==========================================
# 1. 基礎設定 & 參數
# ==========================================
st.set_page_config(page_title="Sniper 戰情室 3.0", page_icon="⚔️", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "") 
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v3.db"

# 預設新選監控 (Watchlist)
DEFAULT_WATCHLIST = (
    "3035 3037 2368 2383 6274 8046 3189 "
    "3324 3017 3653 2421 3483 "
    "3081 3163 4979 4908 3363 4977 6442 "
    "2356 3231 2382 6669 2317 "
    "2330 2454 2303 6781 4931 3533"
)

DEFAULT_INVENTORY = """2330,800,1
2317,105,5"""

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
            code TEXT PRIMARY KEY, win REAL, ret REAL,
            yoy REAL, eps REAL, pe REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (
            code TEXT PRIMARY KEY, cost REAL, qty REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (
            code TEXT PRIMARY KEY
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

    # --- 新增: 取得釘選區資料 ---
    def get_pinned_view(self):
        conn = self.get_conn()
        query = '''
            SELECT 
                p.code, r.name, 
                r.pct, r.price, r.vwap, r.ratio, r.signal,
                r.net_1h, r.net_day,
                s.yoy, s.eps, s.pe,
                1 as is_pinned  -- 這裡出來的一定是 pinned
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
            SELECT 
                i.code, r.name, 
                r.pct, r.price, r.vwap, r.ratio, r.signal,
                r.net_1h, r.net_day,
                s.yoy, s.eps, s.pe,
                i.cost, i.qty,
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
            SELECT 
                w.code, r.name, 
                r.pct, r.price, r.vwap, r.ratio, r.signal, 
                r.net_1h, r.net_day,
                s.yoy, s.eps, s.pe,
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
        c.executemany('INSERT OR REPLACE INTO static_info (code, win, ret, yoy, eps, pe) VALUES (?, ?, ?, ?, ?, ?)', data_list)
        conn.commit()
        conn.close()

db = Database(DB_PATH)

# ==========================================
# 3. 核心邏輯層 (TA & 爬蟲)
# ==========================================

def send_telegram_message(msg):
    if not TG_BOT_TOKEN or not TG_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except: pass

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

def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown):
    if pct >= 9.5: return "👑漲停"
    if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: return "🔥攻擊"
    if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200: return "👀量增"
    if is_breakdown and ratio >= tgt_ratio and net_1h < 0: return "💀出貨"
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    if is_bullish and pct >= tgt_pct: return "⚠️價強"
    return "盤整"

@st.dialog("📈 戰情分析", width="large")
def show_technical_analysis(code, name):
    st.caption(f"正在分析 {code} {name} 的 K 線與技術指標...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=150)
    ticker_id = f"{code}.TW"
    try:
        df = yf.download(ticker_id, start=start_date, end=end_date, progress=False)
        if df.empty:
            ticker_id = f"{code}.TWO"
            df = yf.download(ticker_id, start=start_date, end=end_date, progress=False)
        if df.empty:
            st.error("無法取得 K 線資料")
            return
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # pandas_ta
        kdj = df.ta.kdj(length=9, signal=3)
        df = pd.concat([df, kdj], axis=1)
        macd = df.ta.macd(fast=12, slow=26, signal=9)
        df = pd.concat([df, macd], axis=1)
        bbands = df.ta.bbands(length=20, std=2)
        df = pd.concat([df, bbands], axis=1)
        df['RSI'] = df.ta.rsi(length=14)

        fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                            vertical_spacing=0.02, 
                            row_heights=[0.5, 0.15, 0.15, 0.2],
                            subplot_titles=(f'{name} 日K + 布林', '成交量', 'KD', 'MACD'))

        fig.add_trace(go.Candlestick(x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], name='K線'), row=1, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['BBU_20_2.0'], line=dict(color='gray', width=1), name='上軌'), row=1, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['BBL_20_2.0'], line=dict(color='gray', width=1), name='下軌'), row=1, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['BBM_20_2.0'], line=dict(color='orange', width=1), name='中軌'), row=1, col=1)
        colors = ['red' if c >= o else 'green' for o, c in zip(df['Open'], df['Close'])]
        fig.add_trace(go.Bar(x=df.index, y=df['Volume'], marker_color=colors, name='成交量'), row=2, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['K_9_3'], line=dict(color='orange', width=1), name='K'), row=3, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['D_9_3'], line=dict(color='blue', width=1), name='D'), row=3, col=1)
        fig.add_trace(go.Bar(x=df.index, y=df['MACDh_12_26_9'], marker_color='red', name='MACD柱'), row=4, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['MACD_12_26_9'], line=dict(color='orange', width=1), name='快線'), row=4, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['MACDs_12_26_9'], line=dict(color='blue', width=1), name='慢線'), row=4, col=1)

        fig.update_layout(xaxis_rangeslider_visible=False, height=800, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"分析失敗: {e}")

# ==========================================
# 4. 全局狀態與後端引擎 (Engine)
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
        self.alert_history = {}
        self.clients = []
        self._init_clients()
        self.targets = db.get_all_targets()

    def _init_clients(self):
        if not API_KEYS: return
        for key in API_KEYS:
            try:
                self.clients.append(RestClient(api_key=key))
            except: pass

    def update_targets(self):
        self.targets = db.get_all_targets()

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
            
            tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
            is_bullish = price >= vwap
            is_breakdown = price < (vwap * 0.99)
            signal = check_signal(pct, is_bullish, self.daily_net.get(code, 0), net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown)
            
            if signal in ["🔥攻擊", "💀出貨", "👑漲停", "👀量增"]:
                alert_key = f"{code}_{signal}"
                last_alert_time = self.alert_history.get(alert_key, 0)
                if now_ts - last_alert_time > 600:
                    emoji = "🚀" if signal == "🔥攻擊" else "☠️" if signal == "💀出貨" else "👀"
                    msg = (
                        f"{emoji} <b>【Sniper 戰報】</b>\n"
                        f"股票：<code>{code} {get_stock_name(code)}</code>\n"
                        f"訊號：<b>{signal}</b>\n"
                        f"現價：{price:.2f} ({pct:.2f}%)\n"
                        f"量比：{ratio:.1f}\n"
                        f"大戶1H：{net_1h}"
                    )
                    send_telegram_message(msg)
                    self.alert_history[alert_key] = now_ts

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol, ratio, net_1h, self.daily_net.get(code, 0), signal, now_ts)
        except Exception as e:
            return None

    def _run_loop(self):
        while self.is_running:
            if not self.targets:
                time.sleep(1)
                continue
            now = datetime.now(timezone.utc) + timedelta(hours=8)
            market_open = dt_time(9, 0)
            market_close = dt_time(13, 35)
            is_market_open = market_open <= now.time() <= market_close
            sleep_time = 0.5 if is_market_open else 10
            
            batch_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for i, code in enumerate(self.targets):
                    client = self.clients[i % len(self.clients)] if self.clients else None
                    if client:
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
# 5. UI 呈現 (上下分區架構)
# ==========================================

# A. 佈局容器定義
top_area = st.container() # 釘選區
st.divider() # 分隔線
main_area = st.container() # 主戰區

# --- 側邊欄 ---
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
                    for code in targets:
                        try:
                            candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                            if candles and 'data' in candles and len(candles['data']) >= 2:
                                history_vol[code] = int(candles['data'][-2]['volume']) // 1000
                            else: history_vol[code] = 1000
                        except: history_vol[code] = 1000
                        
                        tk = f"{code}.TW"
                        yoy, eps, pe = 0, 0, 0
                        try:
                            yf_tk = yf.Ticker(tk)
                            info = yf_tk.info
                            if 'trailingEps' not in info:
                                yf_tk = yf.Ticker(f"{code}.TWO")
                                info = yf_tk.info
                            yoy = info.get('revenueGrowth', 0) * 100
                            eps = info.get('trailingEps', 0)
                            pe = info.get('trailingPE', 0)
                        except: pass
                        static_list.append((code, 0, 0, yoy, eps, pe))
                        time.sleep(0.1)
                    engine.history_vol = history_vol
                    db.upsert_static(static_list)
                    status.update(label="初始化完成！", state="complete")
                    
            if st.checkbox("2. 啟動監控核心", value=engine.is_running):
                if not engine.is_running:
                    engine.start()
                    st.toast("核心已啟動")
            else:
                engine.stop()

    st.markdown("---")
    st.caption(f"Bot: {'✅ 啟用' if TG_BOT_TOKEN else '❌ 未設定'}")

# --- 共用樣式函式 ---
def style_df(row):
    yoy_color = 'color: #00FF00' if row['yoy'] < 0 else ('color: #FF4444' if row['yoy'] >= 20 else '')
    eps_color = 'color: gray' if row['eps'] < 0 else ''
    pe_color = 'color: orange' if row['pe'] > 80 else ''
    return [yoy_color if col == '營收YoY' else eps_color if col == 'EPS' else pe_color if col == 'PE' else '' for col in row.index]

now_time = datetime.now(timezone.utc) + timedelta(hours=8)
st.caption(f"最後更新: {now_time.strftime('%H:%M:%S')} (每3秒)")

# ==========================================
# B. Top Area: 釘選區 (Pinned)
# ==========================================
with top_area:
    st.subheader("📌 精選戰情 (Pinned)")
    df_pinned = db.get_pinned_view()
    
    if not df_pinned.empty:
        df_pinned['Pinned'] = True # 這裡一定是 True
        
        # 格式化與更名
        df_pinned = df_pinned.rename(columns={
            'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 
            'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 
            'code': '代碼', 'name': '名稱', 'signal': '訊號',
            'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE'
        })
        
        # 欄位選擇
        cols_p = ['Pinned', '代碼', '名稱', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE']
        df_p_show = df_pinned[cols_p].copy()
        
        # Data Editor (允許取消釘選)
        edited_pinned = st.data_editor(
            df_p_show,
            column_config={
                "Pinned": st.column_config.CheckboxColumn("📌", width="small"),
                "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"),
                "EPS": st.column_config.NumberColumn("EPS", format="%.2f"),
                "PE": st.column_config.NumberColumn("PE", format="%.1f"),
                "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                "現價": st.column_config.NumberColumn("現價", format="%.2f"),
                "均價": st.column_config.NumberColumn("均價", format="%.2f"),
                "量比": st.column_config.NumberColumn("量比", format="%.1f")
            },
            use_container_width=True,
            hide_index=True,
            key="pinned_editor"
        )
        
        # 處理取消釘選
        if not df_pinned.empty:
            changes = edited_pinned[['代碼', 'Pinned']].set_index('代碼')
            for index, row in changes.iterrows():
                if not row['Pinned']: # 如果被取消勾選
                    db.update_pinned(index, False)
                    st.rerun() # 立即刷新移除
    else:
        st.info("尚無釘選股票。請在下方列表勾選 📌 加入。")

# ==========================================
# C. Main Area: 庫存 + 新選 (Split View)
# ==========================================
with main_area:
    col_inv, col_watch = st.columns([45, 55])

    # === 左側：庫存視窗 ===
    with col_inv:
        st.subheader("📦 庫存損益 (Portfolio)")
        df_inv = db.get_inventory_view()
        
        if not df_inv.empty:
            df_inv['Pinned'] = df_inv['is_pinned'].astype(bool)
            
            df_inv = df_inv.rename(columns={
                'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 
                'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 
                'code': '代碼', 'name': '名稱', 'signal': '訊號',
                'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE',
                'cost': '成本', 'profit_val': '損益$', 'profit_pct': '報酬%'
            })
            
            cols = ['Pinned', '代碼', '名稱', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '成本', '損益$', '報酬%']
            df_inv_show = df_inv[cols].copy()
            
            edited_inv = st.data_editor(
                df_inv_show,
                column_config={
                    "Pinned": st.column_config.CheckboxColumn("📌", width="small"),
                    "成本": st.column_config.NumberColumn("成本", format="%.2f"),
                    "現價": st.column_config.NumberColumn("現價", format="%.2f"),
                    "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                    "損益$": st.column_config.NumberColumn("損益$", format="%d"),
                    "報酬%": st.column_config.NumberColumn("報酬%", format="%.2f%%"),
                    "均價": st.column_config.NumberColumn("均價", format="%.2f"),
                    "量比": st.column_config.NumberColumn("量比", format="%.1f")
                },
                use_container_width=True,
                hide_index=True,
                height=400,
                key="inv_editor"
            )
            
            # 更新釘選
            if not df_inv.empty:
                changes = edited_inv[['代碼', 'Pinned']].set_index('代碼')
                for index, row in changes.iterrows():
                    db.update_pinned(index, row['Pinned'])
        else:
            st.info("尚無庫存資料。")

    # === 右側：新選監控 ===
    with col_watch:
        st.subheader("🔭 監控雷達 (Watchlist)")
        df_watch = db.get_watchlist_view()
        
        if not df_watch.empty:
            df_watch['Pinned'] = df_watch['is_pinned'].astype(bool)
            
            if use_filter:
                df_watch = df_watch[
                    (df_watch['yoy'] > 0) & 
                    (df_watch['eps'] > 0) & 
                    (df_watch['pe'] < 50)
                ]

            df_watch = df_watch.rename(columns={
                'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 
                'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 
                'code': '代碼', 'name': '名稱', 'signal': '訊號',
                'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE'
            })
            
            # 排序
            df_watch['sig_score'] = df_watch['訊號'].apply(lambda x: 10 if '攻擊' in str(x) else (5 if '量增' in str(x) else 0))
            df_watch = df_watch.sort_values(by=['sig_score', '漲跌%'], ascending=[False, False])

            cols_w = ['Pinned', '代碼', '名稱', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE']
            df_watch_show = df_watch[cols_w].copy()

            edited_watch = st.data_editor(
                df_watch_show,
                column_config={
                    "Pinned": st.column_config.CheckboxColumn("📌", width="small"),
                    "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"),
                    "EPS": st.column_config.NumberColumn("EPS", format="%.2f"),
                    "PE": st.column_config.NumberColumn("PE", format="%.1f"),
                    "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                    "現價": st.column_config.NumberColumn("現價", format="%.2f"),
                    "均價": st.column_config.NumberColumn("均價", format="%.2f"),
                    "量比": st.column_config.NumberColumn("量比", format="%.1f")
                },
                use_container_width=True,
                hide_index=True,
                height=600,
                key="watch_editor"
            )
            
            # 更新釘選
            if not df_watch.empty:
                changes = edited_watch[['代碼', 'Pinned']].set_index('代碼')
                for index, row in changes.iterrows():
                    db.update_pinned(index, row['Pinned'])
        else:
            st.info("尚無監控資料。")

time.sleep(3)
st.rerun()
