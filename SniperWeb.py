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

# 核心股池
DEFAULT_POOL = (
    "3706 2449 6442 3017 6139 4977 3163 3037 2359 1519 "
    "2330 2317 2382 3231 2356 2454 2303 3711 3081 4979 3363 3450 2345 "
    "3324 3653 2421 3032 2059 3323 6781 4931 "
    "3189 8046 2368 6274 2383 6191 5469 8021 "
    "2344 2408 8299 3260 2409 3481 "
    "1513 2609 2615 8033 2634 2201 4763 5284 3264"
)

# 頁面設定
st.set_page_config(page_title="Sniper 戰情室 (v14.0)", page_icon="🎯", layout="wide")

# === 核心邏輯函式庫 (保持不變) ===

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

# === 初始化 Session State ===
if 'data_store' not in st.session_state:
    st.session_state.data_store = {}
if 'prev_data' not in st.session_state:
    st.session_state.prev_data = {}
if 'history_vol' not in st.session_state:
    st.session_state.history_vol = {}
if 'client' not in st.session_state:
    try:
        if not FUGLE_API_KEY: raise ValueError("API Key is missing.")
        st.session_state.client = RestClient(api_key=FUGLE_API_KEY)
    except:
        st.error("API 連線初始化失敗！請確認 Streamlit Secrets 或環境變數。")

# === 標題區 ===
st.title("🎯 股市狙擊手 (v14.0 穩健版)")
st.caption(f"最後更新: {datetime.now().strftime('%H:%M:%S')}")

# 輸入區
with st.sidebar:
    st.header("🚀 戰情室設定")
    if not FUGLE_API_KEY:
        st.error("未偵測到 API Key！")
    
    raw_input = st.text_area("監控代碼", DEFAULT_POOL, height=200)
    
    if st.button("1. 執行回測與初始化", type="primary"):
        st.session_state.sorted_targets = []
        st.session_state.data_store = {}
        st.session_state.prev_data = {}
        # 觸發 Rerun 重新跑初始化流程
        st.experimental_rerun() 

    run_monitor = st.checkbox("2. 啟動即時監控 (自動刷新)", value=False)
    
# === 核心監控執行 ===

if run_monitor and raw_input:
    
    # 確保 targets 列表正確
    if not st.session_state.sorted_targets:
        st.session_state.sorted_targets = [t.strip() for t in raw_input.split() if t.strip()]

    targets = st.session_state.sorted_targets
    
    # 1. 檢查歷史量是否已經初始化
    if not st.session_state.history_vol:
        with st.spinner("正在執行歷史量初始化..."):
            vols = {}
            client = st.session_state.client
            for i, code in enumerate(targets):
                try:
                    candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                    if candles and 'data' in candles and len(candles['data']) >= 2:
                        vols[code] = int(candles['data'][-2]['volume']) // 1000
                    else: vols[code] = 1000
                except: vols[code] = 1000
                time.sleep(0.1)
            st.session_state.history_vol = vols

    # 2. 執行單次數據掃描
    display_list = []
    client = st.session_state.client
    
    for code in targets:
        try:
            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', q.get('previousClose', 0))
            
            # 如果 price 是 0 或 None，通常代表市場休市或無數據
            if price == 0 or price is None:
                display_list.append({
                    "代碼": code, "名稱": get_stock_name(code), "類別": get_category(code),
                    "勝率%": "-", "報酬%": "-", "現價": "休市", "漲跌%": "-", "均價": "-",
                    "量比": "-", "大戶(1H)": 0, "大戶(日)": 0, "訊號": "休市/無數據"
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
            
            # 大戶累計
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
            
            now_ts = time.time()
            queue = st.session_state.data_store[code]['1h_queue']
            one_hour_ago = now_ts - 3600
            while len(queue) > 0 and queue[0][0] < one_hour_ago: queue.popleft()
            
            net_1h = sum(i[1] for i in queue)
            net_day = st.session_state.data_store[code]['daily']
            
            # 訊號
            tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
            is_bullish = price >= vwap
            
            signal = "盤整"
            if pct >= 9.5: signal = "👑漲停"
            elif is_bullish and net_day > 200:
                if pct >= tgt_pct and ratio >= tgt_ratio: signal = "🔥攻擊"
                elif pct >= tgt_pct: signal = "⚠️價強"
            elif not is_bullish and ratio >= tgt_ratio:
                signal = "💀出貨"
                
            # 回測數據 (簡化顯示，假設回測已跑完)
            bt = st.session_state.backtest_results.get(code, {'win':0, 'ret':0})
            
            display_list.append({
                "代碼": code, "名稱": name, "類別": get_category(code),
                "勝率%": f"{bt['win']:.0f}%", "報酬%": f"{bt['ret']:.1f}%", "現價": price, 
                "漲跌%": f"{pct}%", "均價": f"{vwap:.1f}", "量比": f"{ratio:.1f}", 
                "大戶(1H)": net_1h, "大戶(日)": net_day, "訊號": signal
            })
            
        except Exception as e:
            st.error(f"❌ 數據錯誤 ({code}): {e}")
            pass
    
    # 3. 顯示 DataFrame
    if display_list:
        df = pd.DataFrame(display_list)
        
        # 檢查 DataFrame 是否有數據且欄位存在，防止 Styling Crash
        if not df.empty and '訊號' in df.columns:
            
            # 樣式設定
            def style_df(val):
                if '攻擊' in str(val) or '漲停' in str(val): return 'background-color: #FFDDDD; color: red; font-weight: bold'
                if '出貨' in str(val): return 'background-color: #DDFFDD; color: green; font-weight: bold'
                return ''
            
            def style_net_flow(val):
                try:
                    num = int(str(val).replace('+', '').replace('-', ''))
                    if num > 0: return 'color: red; font-weight: bold'
                    if num < 0: return 'color: green; font-weight: bold'
                except: return ''
            
            def style_bt_win(val):
                try:
                    num = float(str(val).replace('%', ''))
                    if num >= 70: return 'background-color: #333300; color: #FFD700' # 金色背景
                except: return ''

            # 確保所有數字欄位都被轉換為字串以應用 style_net_flow
            st.dataframe(
                df.style
                    .map(style_df, subset=['訊號'])
                    .map(style_net_flow, subset=['大戶(1H)', '大戶(日)'])
                    .map(style_bt_win, subset=['勝率%']),
                use_container_width=True,
                height=800,
                hide_index=True
            )
        else:
            st.info("數據正在初始化或市場休市，無有效交易數據可顯示。")

    # 4. 自動刷新 (每 2 秒)
    time.sleep(2)
    st.experimental_rerun()
else:
    st.info("請先點擊左側「執行回測」初始化數據，並勾選「啟動即時監控」。")
