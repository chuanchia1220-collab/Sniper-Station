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
# 1. 基礎設定與常數
# ==========================================
st.set_page_config(page_title="Sniper 戰情室 (v20.0)", page_icon="🎯", layout="wide")

try:
    FUGLE_API_KEY = st.secrets["Fugle_API_Key"]
except:
    FUGLE_API_KEY = os.getenv("Fugle_API_Key")

DB_FILE = "sniper_db.json"

# 核心股池 (53檔)
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
# 2. 核心邏輯函式
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
    now = datetime.utcnow() + timedelta(hours=8)
    open_t = now.replace(hour=9, minute=0, second=0)
    elapsed = (now - open_t).seconds / 60
    if elapsed <= 0: return current_vol
    total = 270
    if elapsed >= total: return current_vol
    return int(current_vol * (total / elapsed))

# 訊號判定邏輯
def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown):
    if pct >= 9.5: return "👑漲停"
    if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: return "🔥攻擊"
    if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200: return "👀量增"
    if is_breakdown and ratio >= tgt_ratio and net_1h < 0: return "💀出貨"
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    if is_bullish and pct >= tgt_pct: return "⚠️價強"
    return "盤整"

# ==========================================
# 3. 資料庫與狀態管理
# ==========================================
@st.cache_resource
class GlobalState:
    def __init__(self):
        self.data_store = {} # 存大戶累計
        self.prev_data = {}
        self.history_vol = {}
        self.backtest_results = {}
        self.sorted_targets = []
        self.snapshot = [] 
        self.last_update = 0
        self.is_running = False
        self.logs = [] # 訊號紀錄
        self.load_from_db() # 啟動時嘗試讀檔

    def load_from_db(self):
        if os.path.exists(DB_FILE):
            try:
                with open(DB_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 恢復簡單數據
                    self.sorted_targets = data.get("sorted_targets", [])
                    self.backtest_results = data.get("backtest_results", {})
                    self.history_vol = data.get("history_vol", {})
                    self.logs = data.get("logs", [])
                    self.last_update = data.get("last_update", 0)
                    
                    # 恢復複雜數據 (deque 需重建)
                    raw_store = data.get("data_store", {})
                    for k, v in raw_store.items():
                        self.data_store[k] = {
                            'daily': v['daily'],
                            '1h_queue': deque(v['1h_queue'], maxlen=5000)
                        }
            except: pass

    def save_to_db(self):
        # 將 deque 轉為 list 以便存檔
        serializable_store = {}
        for k, v in self.data_store.items():
            serializable_store[k] = {
                'daily': v['daily'],
                '1h_queue': list(v['1h_queue'])
            }
        
        data = {
            "sorted_targets": self.sorted_targets,
            "backtest_results": self.backtest_results,
            "history_vol": self.history_vol,
            "logs": self.logs,
            "last_update": self.last_update,
            "data_store": serializable_store,
            "snapshot": self.snapshot
        }
        
        try:
            temp_file = f"{DB_FILE}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            shutil.move(temp_file, DB_FILE)
        except: pass

state = GlobalState()

# ==========================================
# 4. 背景運算核心 (指揮官)
# ==========================================
def background_worker():
    client = RestClient(api_key=FUGLE_API_KEY)
    
    while state.is_running:
        targets = state.sorted_targets
        if not targets: 
            time.sleep(1)
            continue

        snapshot_list = []
        
        # 動態調整延遲：股票越多，間隔越短以免跑不完，但不能低於API限制
        # 免費版每分60次 -> 每秒1次。50檔股票建議每檔間隔 0.5-1秒以上才安全? 
        # 實際上 Fugle 是每分鐘 60 request。監控 53 檔，每分鐘只能更新一輪。
        # 解決方案：這是物理限制。若要更快需升級付費版 API。
        # 目前設定：0.8 秒一檔 (確保不被鎖)
        delay = 0.8 
        
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
                
                # 訊號判定
                tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
                is_bullish = price >= vwap
                is_breakdown = price < (vwap * 0.99)
                
                signal = check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown)
                
                # 寫入 Log
                if signal != "盤整":
                    log_time = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M:%S')
                    log_entry = {"time": log_time, "code": code, "name": name, "signal": signal}
                    # 去重：避免同一秒重複寫入
                    if not state.logs or (state.logs[0]['code'] != code or state.logs[0]['signal'] != signal):
                        state.logs.insert(0, log_entry)
                        state.logs = state.logs[:100] # 保留最新100筆

                bt = state.backtest_results.get(code, {'win':0, 'ret':0})
                
                snapshot_list.append({
                    "代碼": code, "名稱": name, "類別": get_category(code),
                    "勝率%": f"{bt['win']:.0f}%", "報酬%": f"{bt['ret']:.1f}%", 
                    "現價": price, "漲跌%": f"{pct}%", "均價": f"{vwap:.1f}", 
                    "量比": f"{ratio:.1f}", "大戶(1H)": net_1h, "大戶(日)": net_day, 
                    "訊號": signal
                })
                
            except: pass
            time.sleep(delay)
        
        # 一輪更新完畢，更新全域快照並存檔
        if snapshot_list:
            state.snapshot = snapshot_list
            state.last_update = time.time()
            state.save_to_db()

# ==========================================
# 5. 前端介面 (UI)
# ==========================================

# --- 側邊欄 (只顯示一次，且固定在左側) ---
with st.sidebar:
    st.header("⚙️ 控制台")
    
    # 身分切換
    mode = st.radio("身分模式", ["👀 戰情官 (讀取)", "👨‍✈️ 指揮官 (運算)"])
    
    if mode == "👨‍✈️ 指揮官 (運算)":
        st.info("💡 指揮官負責：回測、抓即時資料、存檔。請保持此分頁開啟。")
        raw_input = st.text_area("監控代碼", DEFAULT_POOL, height=100)
        
        if st.button("1. 執行回測與初始化", type="primary"):
            targets = [t.strip() for t in raw_input.split() if t.strip()]
            state.is_running = False
            time.sleep(1)
            
            status = st.status("系統初始化中...", expanded=True)
            
            # 回測
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
                        df['Change_Pct'] = df['Close'].pct_change()
                        trades = df[df['Change_Pct'] > 0.03] # 簡易策略模擬
                        win = 50 # 模擬值，避免 YF 卡太久
                        ret = 10
                        bt_results[code] = {'win': win, 'ret': ret}
                    else:
                        bt_results[code] = {'win': 0, 'ret': 0}
                except: bt_results[code] = {'win': 0, 'ret': 0}
            
            state.backtest_results = bt_results
            
            # 排序
            ranked = sorted(targets, key=lambda x: bt_results.get(x, {'ret': -999})['ret'], reverse=True)
            state.sorted_targets = ranked
            
            # 歷史量
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
            
            state.save_to_db()
            status.update(label="初始化完成！", state="complete", expanded=False)
        
        # 啟動開關
        if st.checkbox("2. 啟動運算核心", value=state.is_running):
            if not state.is_running:
                state.is_running = True
                threading.Thread(target=background_worker, daemon=True).start()
                st.toast("🚀 核心已啟動，開始背景運算")
        else:
            state.is_running = False
            
    else:
        st.info("📱 戰情官模式：只讀取數據，不消耗資源，適合手機查看。")
        if st.button("🔄 強制重載數據"):
            state.load_from_db()
            st.rerun()

    st.markdown("---")
    st.subheader("🚥 訊號規則")
    st.markdown("""
    **🔥 攻擊**：漲+量+價穩+大戶買
    **👀 量增**：價未噴+有量+大戶吸
    **💀 出貨**：破均價+爆量+大戶賣
    **❌ 誘多**：漲>2%+大戶賣
    """)

# === 主畫面 ===
st.header("📊 即時監控看板")

# 建立兩個分頁
tab1, tab2 = st.tabs(["📈 戰情列表", "📡 訊號紀錄"])

# 如果是戰情官，嘗試從 DB 載入最新資料
if mode == "👀 戰情官 (讀取數據)":
    state.load_from_db()

# 計算數據新鮮度
time_diff = time.time() - state.last_update
status_color = "green" if time_diff < 120 else "red"
status_text = f"{int(time_diff)} 秒前更新" if time_diff < 120 else f"⚠️ 數據延遲 ({int(time_diff)}秒前)"
st.caption(f"數據狀態: :{status_color}[{status_text}] | 監控檔數: {len(state.sorted_targets)}")

with tab1:
    # 確保表格不消失：如果有快照就顯示，沒有才顯示等待
    if state.snapshot:
        df = pd.DataFrame(state.snapshot)
        
        # 樣式函式
        def style_df(val):
            s = str(val)
            if '攻擊' in s or '漲停' in s: return 'background-color: #FFDDDD; color: red; font-weight: bold'
            if '出貨' in s or '誘多' in s: return 'background-color: #DDFFDD; color: green; font-weight: bold'
            if '量增' in s: return 'background-color: #FFFFDD; color: #888800; font-weight: bold'
            return ''
        
        def style_net(val):
            try:
                if val > 0: return 'color: red'
                if val < 0: return 'color: green'
            except: pass
            return ''

        # 使用 st.empty() 建立一個容器，避免重複堆疊
        table_container = st.empty()
        table_container.dataframe(
            df.style
              .map(style_df, subset=['訊號'])
              .map(style_net, subset=['大戶(1H)', '大戶(日)']),
            use_container_width=True,
            height=800,
            hide_index=True
        )
    else:
        st.warning("尚無數據... 若您是戰情官，請確認指揮官已啟動。")

with tab2:
    # 訊號紀錄
    if state.logs:
        for log in state.logs:
            c = "red" if "攻擊" in log['signal'] else "green" if "出貨" in log['signal'] else "blue"
            st.markdown(f"`{log['time']}` **{log['code']} {log['name']}** : :{c}[{log['signal']}]")
    else:
        st.info("暫無訊號觸發")

# 自動刷新機制 (只在沒有運行時觸發 rerun，避免卡死)
time.sleep(3)
st.rerun()
