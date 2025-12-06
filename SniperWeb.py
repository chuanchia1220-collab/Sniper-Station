import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta
import time
import os
import twstock
import json
import threading
from collections import deque
import shutil

# ==========================================
# 🔑 設定區
# ==========================================
try:
    FUGLE_API_KEY = st.secrets["Fugle_API_Key"]
except:
    FUGLE_API_KEY = os.getenv("Fugle_API_Key")

# 核心股池
DEFAULT_POOL = (
    "3706 2449 6442 3017 6139 4977 3163 3037 2359 1519 "
    "2330 2317 2382 3231 2356 2454 2303 3711 3081 4979 3363 3450 2345 "
    "3324 3653 2421 3032 2059 3323 6781 4931 "
    "3189 8046 2368 6274 2383 6191 5469 8021 "
    "2344 2408 8299 3260 2409 3481 "
    "1513 2609 2615 8033 2634 2201 4763 5284 3264"
)

# 類別表
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

# 頁面設定
st.set_page_config(page_title="Sniper 戰情室 (v17.1)", page_icon="🎯", layout="wide")

# 資料庫路徑
DB_FILE = "sniper_db.json"

# === 核心邏輯函式庫 ===

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
    now = datetime.utcnow() + timedelta(hours=8)
    open_t = now.replace(hour=9, minute=0, second=0)
    elapsed = (now - open_t).seconds / 60
    if elapsed <= 0: return current_vol
    total = 270
    if elapsed >= total: return current_vol
    return int(current_vol * (total / elapsed))

# === 資料庫存取 ===
def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return {}

def save_db(data):
    try:
        temp_file = f"{DB_FILE}.tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        shutil.move(temp_file, DB_FILE)
    except: pass

# === 全局狀態管理 ===
@st.cache_resource
class GlobalState:
    def __init__(self):
        self.data_store = {}
        self.prev_data = {}
        self.history_vol = {}
        self.backtest_results = {}
        self.sorted_targets = []
        self.snapshot = []
        self.last_update = 0
        self.is_running = False
        self.logs = []
        
state = GlobalState()

# === 背景工作執行緒 ===
def background_worker(targets):
    client = RestClient(api_key=FUGLE_API_KEY)
    
    while state.is_running:
        snapshot_list = []
        
        for code in targets:
            try:
                if not state.is_running: break
                
                q = client.stock.intraday.quote(symbol=code)
                price = q.get('lastPrice', q.get('previousClose', 0))
                if price == 0 or price is None: price = q.get('previousClose', 0)
                
                pct = q.get('changePercent', 0)
                vol = q.get('total', {}).get('tradeVolume', 0) * 1000
                name = get_stock_name(code)
                total_val = q.get('total', {}).get('tradeValue', 0)
                vwap = total_val / vol if vol > 0 else price
                
                current_vol_share = q.get('total', {}).get('tradeVolume', 0)
                est_vol = _calc_est_vol(current_vol_share)
                base_vol = state.history_vol.get(code, 1000)
                ratio = est_vol / base_vol if base_vol > 0 else 0
                
                # 大戶運算
                delta_net = 0
                if code in state.prev_data:
                    prev_v = state.prev_data[code]['vol']
                    prev_p = state.prev_data[code]['price']
                    delta_v = (vol - prev_v) / 1000
                    
                    threshold = get_big_order_threshold(price)
                    if delta_v >= threshold:
                        if price >= prev_p: delta_net = int(delta_v)
                        elif price < prev_p: delta_net = -int(delta_v)
                
                state.prev_data[code] = {'vol': vol, 'price': price}
                
                if code not in state.data_store:
                    state.data_store[code] = {'daily': 0, '1h_queue': deque()}
                
                if delta_net != 0:
                    state.data_store[code]['daily'] += delta_net
                    state.data_store[code]['1h_queue'].append((time.time(), delta_net))
                
                # 1H 清理
                now_ts = time.time()
                queue = state.data_store[code]['1h_queue']
                one_hour_ago = now_ts - 3600
                while len(queue) > 0 and queue[0][0] < one_hour_ago: queue.popleft()
                
                net_1h = sum(i[1] for i in queue)
                net_day = state.data_store[code]['daily']
                
                # 訊號邏輯
                tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
                is_bullish = price >= vwap
                is_breakdown = price < (vwap * 0.99)
                
                signal = "-"
                if pct >= 9.5: signal = "👑漲停"
                elif is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: signal = "🔥攻擊"
                elif is_bullish and net_day > 200 and pct >= tgt_pct: signal = "⚠️價強"
                elif is_breakdown and ratio >= tgt_ratio and net_1h < 0: signal = "💀出貨"
                elif is_bullish and ratio >= tgt_ratio and net_1h > 200: signal = "👀量增"
                elif pct > 2.0 and net_1h < 0: signal = "❌誘多"
                
                # 記錄訊號
                if signal not in ["-", "盤整"]:
                    log_entry = {
                        "time": (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M:%S'),
                        "code": code, "name": name, "signal": signal
                    }
                    if not state.logs or state.logs[0]['time'] != log_entry['time'] or state.logs[0]['code'] != code:
                        state.logs.insert(0, log_entry)
                        state.logs = state.logs[:50] 

                bt = state.backtest_results.get(code, {'win':0, 'ret':0})
                
                snapshot_list.append({
                    "代碼": code, "名稱": name, "類別": get_category(code),
                    "勝率%": f"{bt['win']:.0f}%", "報酬%": f"{bt['ret']:.1f}%", 
                    "現價": price, "漲跌%": f"{pct}%", "均價": f"{vwap:.1f}", 
                    "量比": f"{ratio:.1f}", "大戶(1H)": net_1h, "大戶(日)": net_day, 
                    "訊號": signal
                })
                
            except: pass
            time.sleep(0.2)
        
        state.snapshot = snapshot_list
        state.last_update = time.time()
        
        db_data = {
            "snapshot": snapshot_list,
            "last_update": state.last_update,
            "logs": state.logs
        }
        save_db(db_data)
        
        time.sleep(1) 

# === 主介面 ===

taiwan_time = datetime.utcnow() + timedelta(hours=8)
st.title("🎯 Sniper 戰情室 (v17.1)")
st.caption(f"系統時間: {taiwan_time.strftime('%H:%M:%S')} | 核心: {'🟢 運作中' if state.is_running else '⚪ 待機'}")

# 側邊欄
with st.sidebar:
    st.header("⚙️ 控制台")
    mode = st.radio("身分模式", ["👀 戰情官 (讀取數據)", "👨‍✈️ 指揮官 (執行運算)"])
    
    if mode == "👨‍✈️ 指揮官 (執行運算)":
        raw_input = st.text_area("監控代碼", DEFAULT_POOL, height=150)
        
        if st.button("1. 初始化與回測", type="primary"):
            targets = [t.strip() for t in raw_input.split() if t.strip()]
            state.is_running = False
            time.sleep(1)
            
            status = st.status("正在初始化...", expanded=True)
            status.write("執行 180 天回測...")
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            bt_results = {}
            
            for i, code in enumerate(targets):
                ticker = get_yahoo_ticker(code)
                try:
                    df = yf.download(ticker, start=start_date, end=end_date, progress=False)
                    if df.empty:
                         alt = ticker.replace('.TW', '.TWO') if '.TW' in ticker else ticker.replace('.TWO', '.TW')
                         df = yf.download(alt, start=start_date, end=end_date, progress=False)
                    
                    if not df.empty and len(df) > 20:
                        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                        df['Prev_Close'] = df['Close'].shift(1)
                        df['Change_Pct'] = (df['Close'] - df['Prev_Close']) / df['Prev_Close']
                        df['Vol_MA5'] = df['Volume'].rolling(5).mean().shift(1)
                        
                        trades = []
                        for date, row in df.iterrows():
                            close = row['Close']
                            if pd.isna(close): continue
                            tgt_pct, tgt_ratio = get_dynamic_thresholds(close)
                            d_pct = row['Change_Pct'] * 100
                            vol_r = row['Volume'] / row['Vol_MA5'] if (not pd.isna(row['Vol_MA5']) and row['Vol_MA5'] > 0) else 0
                            
                            if d_pct > tgt_pct and d_pct < 9.5 and vol_r > tgt_ratio:
                                nxt_idx = df.index.get_loc(date) + 1
                                if nxt_idx < len(df):
                                    trades.append((df.iloc[nxt_idx]['Open'] - close) / close - 0.006)
                        
                        win = len([x for x in trades if x > 0]) / len(trades) * 100 if trades else 0
                        ret = sum(trades) * 100
                        bt_results[code] = {'win': win, 'ret': ret}
                    else:
                        bt_results[code] = {'win': 0, 'ret': 0}
                except:
                    bt_results[code] = {'win': 0, 'ret': 0}
            
            state.backtest_results = bt_results
            
            # 排序
            ranked = sorted(targets, key=lambda x: bt_results.get(x, {'ret': -999})['ret'], reverse=True)
            state.sorted_targets = ranked
            
            # 抓歷史量
            status.write("抓取歷史成交量...")
            client = RestClient(api_key=FUGLE_API_KEY)
            h_vols = {}
            for code in ranked:
                try:
                    candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                    if candles and 'data' in candles and len(candles['data']) >= 2:
                        h_vols[code] = int(candles['data'][-2]['volume']) // 1000
                    else: h_vols[code] = 1000
                except: h_vols[code] = 1000
                time.sleep(0.1)
            state.history_vol = h_vols
            
            status.update(label="初始化完成！", state="complete", expanded=False)
        
        if st.checkbox("2. 啟動運算核心", value=state.is_running):
            if not state.is_running:
                state.is_running = True
                t = threading.Thread(target=background_worker, args=(state.sorted_targets,), daemon=True)
                t.start()
                st.toast("運算核心已啟動！")
        else:
            state.is_running = False

# === 資料顯示區 ===

# 載入數據
if mode == "👀 戰情官 (讀取數據)":
    db_data = load_db()
    if db_data:
        state.snapshot = db_data.get("snapshot", [])
        state.logs = db_data.get("logs", [])
        state.last_update = db_data.get("last_update", 0)

# 分頁顯示
tab1, tab2 = st.tabs(["📊 戰情監控", "📡 訊號中心"])

with tab1:
    if state.snapshot:
        df = pd.DataFrame(state.snapshot)
        
        def style_df(val):
            if '攻擊' in str(val) or '漲停' in str(val): return 'background-color: #FFDDDD; color: red; font-weight: bold'
            if '出貨' in str(val) or '誘多' in str(val): return 'background-color: #DDFFDD; color: green; font-weight: bold'
            if '量增' in str(val): return 'background-color: #FFFFDD; color: #888800; font-weight: bold'
            return ''
        
        def style_net(val):
            try:
                if val > 0: return 'color: red; font-weight: bold'
                if val < 0: return 'color: green; font-weight: bold'
            except: pass
            return ''
        
        st.dataframe(
            df.style
              .map(style_df, subset=['訊號'])
              .map(style_net, subset=['大戶(1H)', '大戶(日)']),
            use_container_width=True,
            height=800,
            hide_index=True
        )
    else:
        st.info("等待數據更新...")

with tab2:
    if state.logs:
        for log in state.logs:
            color = "red" if "攻擊" in log['signal'] else "green" if "出貨" in log['signal'] else "orange"
            st.markdown(f"**{log['time']}** | {log['code']} {log['name']} : :{color}[{log['signal']}]")
    else:
        st.write("尚無訊號紀錄...")

# 自動刷新
time.sleep(3)
st.rerun()
