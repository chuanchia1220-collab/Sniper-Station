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
from collections import deque

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
st.set_page_config(page_title="Sniper 戰情室 (v16.0)", page_icon="🎯", layout="wide")

# === 檔案資料庫路徑 ===
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
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    except: pass

# === 初始化 Session State ===
if 'data_store' not in st.session_state:
    st.session_state.data_store = {}
if 'prev_data' not in st.session_state:
    st.session_state.prev_data = {}
if 'history_vol' not in st.session_state:
    st.session_state.history_vol = {}
if 'backtest_results' not in st.session_state:
    st.session_state.backtest_results = {}
if 'sorted_targets' not in st.session_state:
    st.session_state.sorted_targets = []
if 'client' not in st.session_state:
    try:
        if FUGLE_API_KEY:
            st.session_state.client = RestClient(api_key=FUGLE_API_KEY)
    except: pass

# === 標題區 ===
taiwan_time = datetime.utcnow() + timedelta(hours=8)
st.title("🎯 股市狙擊手 (Cloud Team v16.0)")
st.caption(f"最後更新: {taiwan_time.strftime('%H:%M:%S')} | 模式: {st.session_state.get('mode', '未設定')}")

# === 側邊欄 ===
with st.sidebar:
    st.header("⚙️ 戰情室設定")
    
    # === 關鍵模式選擇 ===
    mode = st.radio("身分選擇", ["👀 戰情官 (只看盤)", "👨‍✈️ 指揮官 (負責運算)"], index=0)
    st.session_state.mode = mode
    
    if mode == "👨‍✈️ 指揮官 (負責運算)":
        st.warning("⚠️ 注意：同一時間只能有一台電腦當指揮官！")
        if not FUGLE_API_KEY:
            st.error("未偵測到 API Key！")

        raw_input = st.text_area("監控代碼", DEFAULT_POOL, height=150)
        
        if st.button("1. 執行回測與初始化 (每日一次)", type="primary"):
            targets = [t.strip() for t in raw_input.split() if t.strip()]
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # --- 執行回測 (修正版) ---
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            bt_results = {}
            
            status_text.text("正在執行回測 (修正版)...")
            
            for i, code in enumerate(targets):
                progress_bar.progress((i + 1) / len(targets))
                ticker = get_yahoo_ticker(code)
                try:
                    # 修正：強制 auto_adjust=True 且不使用 multi_level_index (如果版本支援)
                    # 這裡使用通用解法：檢查 columns
                    df = yf.download(ticker, start=start_date, end=end_date, progress=False)
                    
                    # 如果主要代碼沒資料，嘗試另一種後綴
                    if df.empty:
                        alt = ticker.replace('.TW', '.TWO') if '.TW' in ticker else ticker.replace('.TWO', '.TW')
                        df = yf.download(alt, start=start_date, end=end_date, progress=False)

                    if not df.empty:
                        # 處理 MultiIndex Column 問題
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = df.columns.get_level_values(0)
                        
                        # 確保有 Close 欄位
                        if 'Close' in df.columns and len(df) > 20:
                            df['Prev_Close'] = df['Close'].shift(1)
                            df['Change_Pct'] = (df['Close'] - df['Prev_Close']) / df['Prev_Close']
                            df['Vol_MA5'] = df['Volume'].rolling(5).mean().shift(1)
                            
                            trades = []
                            for date, row in df.iterrows():
                                close = row['Close']
                                if pd.isna(close): continue
                                tgt_pct, tgt_ratio = get_dynamic_thresholds(close)
                                d_pct = row['Change_Pct'] * 100
                                # 避免除以0
                                vol_r = row['Volume'] / row['Vol_MA5'] if (not pd.isna(row['Vol_MA5']) and row['Vol_MA5'] > 0) else 0
                                
                                if d_pct > tgt_pct and d_pct < 9.5 and vol_r > tgt_ratio:
                                    nxt_idx = df.index.get_loc(date) + 1
                                    if nxt_idx < len(df):
                                        ret = (df.iloc[nxt_idx]['Open'] - close) / close - 0.006
                                        trades.append(ret)
                            
                            win = len([x for x in trades if x > 0]) / len(trades) * 100 if trades else 0
                            ret = sum(trades) * 100
                            bt_results[code] = {'win': win, 'ret': ret}
                        else:
                            bt_results[code] = {'win': 0, 'ret': 0} # 資料不足
                    else:
                        bt_results[code] = {'win': 0, 'ret': 0} # 下載失敗

                except Exception as e:
                    # print(f"Error {code}: {e}")
                    bt_results[code] = {'win': 0, 'ret': 0}
            
            st.session_state.backtest_results = bt_results
            
            # 排序
            ranked = sorted(targets, key=lambda x: bt_results.get(x, {'ret': -999})['ret'], reverse=True)
            st.session_state.sorted_targets = ranked
            
            # 抓取歷史量
            status_text.text("初始化歷史成交量...")
            client = st.session_state.client
            h_vols = {}
            for code in ranked:
                try:
                    candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                    if candles and 'data' in candles and len(candles['data']) >= 2:
                        h_vols[code] = int(candles['data'][-2]['volume']) // 1000
                    else: h_vols[code] = 1000
                except: h_vols[code] = 1000
                time.sleep(0.1)
                
            st.session_state.history_vol = h_vols
            
            # 儲存到共用資料庫 (讓手機端可以讀到排序和回測結果)
            db_data = {
                "targets": ranked,
                "backtest": bt_results,
                "history_vol": h_vols,
                "timestamp": time.time()
            }
            save_db(db_data)
            
            st.success(f"初始化完成！共 {len(ranked)} 檔。資料已同步至雲端。")
            progress_bar.empty()
            status_text.empty()

        run_monitor = st.checkbox("2. 啟動運算核心 (Commander)", value=False)

    else: # 戰情官模式
        st.info("正在接收指揮官的數據...")
        # 嘗試載入設定
        db_data = load_db()
        if db_data:
            st.session_state.sorted_targets = db_data.get("targets", [])
            st.session_state.backtest_results = db_data.get("backtest", {})
            st.session_state.history_vol = db_data.get("history_vol", {})
            st.success(f"已連接數據源！監控 {len(st.session_state.sorted_targets)} 檔")
        else:
            st.warning("尚未收到指揮官數據，請先在電腦端執行初始化。")
        
        run_monitor = st.checkbox("開啟自動刷新", value=True)


# === 核心邏輯 (分流處理) ===

if run_monitor and st.session_state.sorted_targets:
    
    display_list = []
    targets = st.session_state.sorted_targets
    
    # === A. 指揮官模式：負責打 API、算數據、存檔 ===
    if mode == "👨‍✈️ 指揮官 (負責運算)":
        client = st.session_state.client
        
        # 載入舊的大戶累計數據 (從 DB 或 Session)
        # 這裡我們簡化：指揮官主要負責「產生」即時行情報表，並存入 DB 供戰情官讀取
        # 大戶累計邏輯在指揮官端運算
        
        # 確保 data_store 存在
        if 'data_store' not in st.session_state: st.session_state.data_store = {}
        
        snapshot_data = [] # 準備存入 DB 的快照

        for code in targets:
            try:
                q = client.stock.intraday.quote(symbol=code)
                price = q.get('lastPrice', q.get('previousClose', 0))
                if price == 0 or price is None: price = q.get('previousClose', 0)
                
                pct = q.get('changePercent', 0)
                vol = q.get('total', {}).get('tradeVolume', 0) * 1000
                name = get_stock_name(code)
                total_val = q.get('total', {}).get('tradeValue', 0)
                vwap = total_val / vol if vol > 0 else price
                
                est_vol = _calc_est_vol(q.get('total', {}).get('tradeVolume', 0))
                base_vol = st.session_state.history_vol.get(code, 1000)
                ratio = est_vol / base_vol if base_vol > 0 else 0
                
                # 大戶運算
                delta_net = 0
                if code in st.session_state.prev_data:
                    prev_v = st.session_state.prev_data[code]['vol']
                    prev_p = st.session_state.prev_data[code]['price']
                    delta_v = (vol - prev_v) / 1000
                    
                    threshold = get_big_order_threshold(price)
                    if delta_v >= threshold:
                        if price >= prev_p: delta_net = int(delta_v)
                        elif price < prev_p: delta_net = -int(delta_v)
                
                st.session_state.prev_data[code] = {'vol': vol, 'price': price}
                
                if code not in st.session_state.data_store:
                    st.session_state.data_store[code] = {'daily': 0, '1h_queue': deque()}
                
                if delta_net != 0:
                    st.session_state.data_store[code]['daily'] += delta_net
                    st.session_state.data_store[code]['1h_queue'].append((time.time(), delta_net))
                
                # 1H 清理
                now_ts = time.time()
                queue = st.session_state.data_store[code]['1h_queue']
                one_hour_ago = now_ts - 3600
                while len(queue) > 0 and queue[0][0] < one_hour_ago: queue.popleft()
                
                net_1h = sum(i[1] for i in queue)
                net_day = st.session_state.data_store[code]['daily']
                
                # 訊號
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
                
                bt = st.session_state.backtest_results.get(code, {'win':0, 'ret':0})
                
                row_data = {
                    "代碼": code, "名稱": name, "類別": get_category(code),
                    "勝率%": f"{bt['win']:.0f}%", "報酬%": f"{bt['ret']:.1f}%", 
                    "現價": price, "漲跌%": f"{pct}%", "均價": f"{vwap:.1f}", 
                    "量比": f"{ratio:.1f}", "大戶(1H)": net_1h, "大戶(日)": net_day, 
                    "訊號": signal
                }
                display_list.append(row_data)
                snapshot_data.append(row_data)
                
            except: pass
        
        # 指揮官將最新盤勢寫入 DB (供戰情官讀取)
        # 這裡我們更新 DB 中的 'snapshot' 欄位
        db_data = load_db()
        db_data["snapshot"] = snapshot_data
        db_data["last_update"] = time.time()
        save_db(db_data)
        
        # 顯示
        df = pd.DataFrame(display_list)

    # === B. 戰情官模式：只讀 DB，不運算 ===
    else:
        db_data = load_db()
        snapshot = db_data.get("snapshot", [])
        last_update = db_data.get("last_update", 0)
        
        # 顯示資料過期警告 (超過 30 秒沒更新)
        if time.time() - last_update > 30:
            st.warning(f"⚠️ 數據延遲：指揮官似乎離線了 (最後更新: {datetime.fromtimestamp(last_update + 28800).strftime('%H:%M:%S')})")
        
        if snapshot:
            df = pd.DataFrame(snapshot)
        else:
            df = pd.DataFrame()
            st.info("等待指揮官傳送數據...")

    # === 共同顯示區 (Styling) ===
    if not df.empty and '訊號' in df.columns:
        def style_df(val):
            if '攻擊' in str(val) or '漲停' in str(val): return 'background-color: #FFDDDD; color: red; font-weight: bold'
            if '出貨' in str(val) or '誘多' in str(val): return 'background-color: #DDFFDD; color: green; font-weight: bold'
            if '量增' in str(val): return 'background-color: #FFFFDD; color: #888800; font-weight: bold'
            return ''
        
        def style_net(val):
            try:
                v = int(val)
                if v > 0: return 'color: red; font-weight: bold'
                if v < 0: return 'color: green; font-weight: bold'
            except: pass
            return ''
        
        def style_bt(val):
            try:
                v = float(val.replace('%',''))
                if v >= 70: return 'color: #FF00FF; font-weight: bold' # 高勝率紫色
            except: pass
            return ''

        st.dataframe(
            df.style
              .map(style_df, subset=['訊號'])
              .map(style_net, subset=['大戶(1H)', '大戶(日)'])
              .map(style_bt, subset=['勝率%']),
            use_container_width=True,
            height=1000,
            hide_index=True
        )
    
    # 自動刷新 (每 3 秒)
    time.sleep(3)
    st.rerun()

else:
    if mode == "👨‍✈️ 指揮官 (負責運算)":
        st.info("👈 請先點擊「1. 執行回測」初始化，再勾選「2. 啟動」。")
    else:
        st.info("👈 請勾選「開啟自動刷新」來接收指揮官的戰情。")
