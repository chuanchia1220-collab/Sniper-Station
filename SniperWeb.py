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
st.set_page_config(page_title="Sniper 戰情室 (v15.0)", page_icon="🎯", layout="wide")

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

# === 初始化 Session State ===
if 'data_store' not in st.session_state:
    st.session_state.data_store = {}
if 'prev_data' not in st.session_state:
    st.session_state.prev_data = {}
if 'history_vol' not in st.session_state:
    st.session_state.history_vol = {}
if 'last_display_df' not in st.session_state:
    st.session_state.last_display_df = pd.DataFrame()
if 'client' not in st.session_state:
    try:
        if not FUGLE_API_KEY: raise ValueError("API Key is missing.")
        st.session_state.client = RestClient(api_key=FUGLE_API_KEY)
    except:
        st.error("API 連線初始化失敗！請確認 Streamlit Secrets 或環境變數。")

# === 標題區 ===
taiwan_time = datetime.utcnow() + timedelta(hours=8)
st.title("🎯 股市狙擊手 (Logic Pro v15.0)")
st.caption(f"最後更新: {taiwan_time.strftime('%H:%M:%S')} (台灣時間)")

# === 側邊欄 ===
with st.sidebar:
    st.header("⚙️ 戰情室設定")
    
    raw_input = st.text_area("監控代碼", DEFAULT_POOL, height=150)
    
    if st.button("1. 執行回測與初始化", type="primary"):
        st.session_state.sorted_targets = []
        st.session_state.data_store = {}
        st.session_state.prev_data = {}
        st.rerun()

    run_monitor = st.checkbox("2. 啟動即時監控 (自動刷新)", value=False)
    
    st.markdown("---")
    st.subheader("🚥 最新訊號規則")
    st.markdown("""
    **🔥 攻擊 (Attack)**
    - 漲幅達標 + 量能達標 + 大戶(日)紅
    
    **👀 量增 (Accumulation)** (新)
    - 漲幅未達標 (還沒噴)
    - 現價 > 均價 (多方)
    - 量比 > 門檻 (有量)
    - 大戶(1H) > +200 (主力在收貨)
    
    **⚠️ 價強 (Strong)**
    - 漲幅達標，量能/大戶未達標
    
    **💀 出貨 (Dump)** (優化)
    - 現價 < 均價*0.99 (破線)
    - 量比 > 門檻 (爆量)
    - 大戶(1H) < 0 (主力在賣)
    
    **❌ 誘多 (Bull Trap)** (新)
    - 漲幅 > 2% (誘多)
    - 大戶(1H) < 0 (主力其實在賣)
    """)

# === 核心監控執行 ===

if run_monitor and raw_input:
    
    if not st.session_state.sorted_targets:
        st.session_state.sorted_targets = [t.strip() for t in raw_input.split() if t.strip()]

    targets = st.session_state.sorted_targets
    
    # 1. 初始化歷史量
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

    # 2. 掃描數據
    display_list = []
    client = st.session_state.client
    
    for code in targets:
        try:
            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', q.get('previousClose', 0))
            
            if price == 0 or price is None:
                price = q.get('previousClose', 0)
            
            pct = q.get('changePercent', 0)
            vol = q.get('total', {}).get('tradeVolume', 0) * 1000
            name = get_stock_name(code)
            total_val = q.get('total', {}).get('tradeValue', 0)
            vwap = total_val / vol if vol > 0 else price
            
            current_vol_share = q.get('total', {}).get('tradeVolume', 0)
            est_vol = _calc_est_vol(current_vol_share)
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
            
            now_ts = time.time()
            queue = st.session_state.data_store[code]['1h_queue']
            one_hour_ago = now_ts - 3600
            while len(queue) > 0 and queue[0][0] < one_hour_ago: queue.popleft()
            
            net_1h = sum(i[1] for i in queue)
            net_day = st.session_state.data_store[code]['daily']
            
            # === 訊號邏輯 (用戶指定 + 增強版) ===
            tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
            is_bullish = price >= vwap
            is_breakdown = price < (vwap * 0.99) # 跌破均價 1%
            
            signal = "-"
            
            if pct >= 9.5:
                signal = "👑漲停"
            elif is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio:
                signal = "🔥攻擊" # 規則 1
            elif is_bullish and net_day > 200 and pct >= tgt_pct:
                signal = "⚠️價強" # 規則 2
            elif is_breakdown and ratio >= tgt_ratio and net_1h < 0:
                signal = "💀出貨" # 規則 3 (用戶優化: 破線+爆量+1H賣)
            elif is_bullish and ratio >= tgt_ratio and net_1h > 200:
                signal = "👀量增" # 規則 4 (用戶優化: 吸籌)
            elif pct > 2.0 and net_1h < 0:
                signal = "❌誘多" # 額外建議: 漲但大戶在賣
                
            bt = st.session_state.backtest_results.get(code, {'win':0, 'ret':0})
            
            display_list.append({
                "代碼": code, "名稱": name, "類別": get_category(code),
                "勝率%": f"{bt['win']:.0f}%", "報酬%": f"{bt['ret']:.1f}%", "現價": price, 
                "漲跌%": f"{pct}%", "均價": f"{vwap:.1f}", "量比": f"{ratio:.1f}", 
                "大戶(1H)": net_1h, "大戶(日)": net_day, "訊號": signal
            })
            
        except: pass

    if display_list:
        df = pd.DataFrame(display_list)
        st.session_state.last_display_df = df
        
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

            st.dataframe(
                df.style
                  .map(style_df, subset=['訊號'])
                  .map(style_net, subset=['大戶(1H)', '大戶(日)']),
                use_container_width=True,
                height=800,
                hide_index=True
            )
            
            time.sleep(2)
            st.rerun()
            
    elif not st.session_state.last_display_df.empty:
        st.warning(f"目前非盤中時間，顯示 {datetime.now().strftime('%m/%d')} 收盤數據：")
        st.dataframe(st.session_state.last_display_df, use_container_width=True, height=800, hide_index=True)
    else:
        st.info("數據初始化中...")
        time.sleep(2)
        st.rerun()

else:
    st.info("👈 請在左側點擊「1. 執行回測」來初始化數據，並勾選「啟動即時監控」。")
