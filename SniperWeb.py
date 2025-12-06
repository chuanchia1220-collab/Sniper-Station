import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta
import time
import os
import twstock
from collections import deque

# ==========================================
# 🔑 設定區
# ==========================================
# 嘗試從 Streamlit Secrets 或 環境變數 讀取 Key
try:
    FUGLE_API_KEY = st.secrets["Fugle_API_Key"]
except:
    FUGLE_API_KEY = os.getenv("Fugle_API_Key")

# 核心股池 (53檔)
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
st.set_page_config(page_title="Sniper 戰情室 (v14.1)", page_icon="🎯", layout="wide")

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
    now = datetime.now()
    open_t = now.replace(hour=9, minute=0, second=0)
    elapsed = (now - open_t).seconds / 60
    if elapsed <= 0: return current_vol
    total = 270
    if elapsed >= total: return current_vol
    return int(current_vol * (total / elapsed))

# === 初始化 Session State (數據持久化) ===
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
        if not FUGLE_API_KEY: raise ValueError("API Key is missing.")
        st.session_state.client = RestClient(api_key=FUGLE_API_KEY)
    except:
        st.error("⚠️ API 連線初始化失敗！請檢查 Secrets 設定。")

# === 標題區 ===
st.title("🎯 股市狙擊手 (v14.1 雲端版)")
st.caption(f"最後更新: {datetime.now().strftime('%H:%M:%S')} | 狀態: {'監控中 🟢' if 'run_monitor' in st.session_state and st.session_state.run_monitor else '待機 ⚪'}")

# === 側邊欄：控制台 ===
with st.sidebar:
    st.header("⚙️ 戰情室設定")
    
    raw_input = st.text_area("監控代碼 (Top 10)", DEFAULT_POOL, height=150)
    
    # 按鈕 1: 初始化與回測
    if st.button("1. 執行回測與初始化", type="primary"):
        targets = [t.strip() for t in raw_input.split() if t.strip()]
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 1. 執行回測
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        bt_results = {}
        
        status_text.text("正在執行 180 天回測排序...")
        
        # 為了速度，這裡做一個簡化版的回測 (只測最近 3 個月) 或完整版
        # 這裡示範完整版
        for i, code in enumerate(targets):
            progress_bar.progress((i + 1) / len(targets))
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
                        vol_r = row['Volume'] / row['Vol_MA5'] if row['Vol_MA5'] > 0 else 0
                        
                        if d_pct > tgt_pct and d_pct < 9.5 and vol_r > tgt_ratio:
                            nxt_idx = df.index.get_loc(date) + 1
                            if nxt_idx < len(df):
                                ret = (df.iloc[nxt_idx]['Open'] - close) / close - 0.006
                                trades.append(ret)
                    
                    win = len([x for x in trades if x > 0]) / len(trades) * 100 if trades else 0
                    ret = sum(trades) * 100
                    bt_results[code] = {'win': win, 'ret': ret}
            except: pass
        
        st.session_state.backtest_results = bt_results
        
        # 2. 排序
        ranked = sorted(targets, key=lambda x: bt_results.get(x, {'ret': -999})['ret'], reverse=True)
        st.session_state.sorted_targets = ranked
        
        # 3. 抓取歷史量
        status_text.text("正在抓取歷史成交量...")
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
        st.success(f"初始化完成！共 {len(ranked)} 檔")
        progress_bar.empty()
        status_text.empty()

    # 開關 2: 啟動監控
    # 使用 session_state 來控制開關狀態
    if 'run_monitor' not in st.session_state:
        st.session_state.run_monitor = False

    def toggle_run():
        st.session_state.run_monitor = not st.session_state.run_monitor

    st.checkbox("2. 啟動即時監控 (自動刷新)", value=st.session_state.run_monitor, on_change=toggle_run)


# === 主畫面監控邏輯 ===

if st.session_state.run_monitor and st.session_state.sorted_targets:
    
    client = st.session_state.client
    targets = st.session_state.sorted_targets
    display_list = []
    
    # 批次掃描 (模擬 Real-time)
    for code in targets:
        try:
            # 取得報價
            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', q.get('previousClose', 0))
            
            # 處理休市或無數據
            if price == 0 or price is None:
                display_list.append({
                    "代碼": code, "名稱": get_stock_name(code), "類別": get_category(code),
                    "勝率%": "-", "報酬%": "-", "現價": "-", "漲跌%": "-", 
                    "均價": "-", "量比": "-", "大戶(1H)": 0, "大戶(日)": 0, "訊號": "休市"
                })
                continue

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
            
            # 數據儲存
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
            
            # 訊號判定
            tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
            is_bullish = price >= vwap
            
            signal = "-"
            if pct >= 9.5: signal = "👑漲停"
            elif is_bullish and net_day > 200:
                if pct >= tgt_pct and ratio >= tgt_ratio: signal = "🔥攻擊"
                elif pct >= tgt_pct: signal = "⚠️價強"
            elif not is_bullish and ratio >= tgt_ratio:
                signal = "💀出貨"
            
            # 回測數據
            bt = st.session_state.backtest_results.get(code, {'win':0, 'ret':0})
            
            display_list.append({
                "代碼": code,
                "名稱": name,
                "類別": get_category(code),
                "勝率%": f"{bt['win']:.0f}%",
                "報酬%": f"{bt['ret']:.1f}%",
                "現價": price,
                "漲跌%": f"{pct}%",
                "均價": f"{vwap:.1f}",
                "量比": f"{ratio:.1f}",
                "大戶(1H)": net_1h,
                "大戶(日)": net_day,
                "訊號": signal
            })
            
        except: pass

    # 顯示表格 (如果有資料)
    if display_list:
        df = pd.DataFrame(display_list)
        
        # 樣式設定 (防呆：先檢查欄位是否存在)
        if '訊號' in df.columns:
            def style_signal(val):
                if '攻擊' in str(val) or '漲停' in str(val): return 'background-color: #FFDDDD; color: red; font-weight: bold'
                if '出貨' in str(val): return 'background-color: #DDFFDD; color: green; font-weight: bold'
                return ''
            
            def style_net(val):
                try:
                    v = int(val)
                    if v > 0: return 'color: red; font-weight: bold'
                    if v < 0: return 'color: green; font-weight: bold'
                except: pass
                return ''

            st.dataframe(
                df.style
                  .map(style_signal, subset=['訊號'])
                  .map(style_net, subset=['大戶(1H)', '大戶(日)']),
                use_container_width=True,
                height=800,
                hide_index=True
            )
            
            # 自動刷新機制 (3秒後重跑一次 script)
            time.sleep(3)
            st.rerun()
            
    else:
        st.warning("尚無數據，請稍候或檢查市場是否開盤...")
        time.sleep(3)
        st.rerun()

else:
    st.info("👈 請在左側點擊「1. 執行回測」來初始化數據，完成後勾選「2. 啟動監控」。")
