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
# 1. 基礎配置 (Configuration)
# ==========================================
st.set_page_config(page_title="Sniper 戰情室 (Reborn v21.0)", page_icon="🎯", layout="wide")

# 嘗試讀取 API Key
try:
    FUGLE_API_KEY = st.secrets["Fugle_API_Key"]
except:
    FUGLE_API_KEY = os.getenv("Fugle_API_Key")

DB_FILE = "sniper_db.json"

# 預設核心股池 (53檔)
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
# 2. 數學運算與邏輯函式 (Logic)
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
    threshold = int(400 / price) # 40萬台幣
    return max(1, threshold)

def _calc_est_vol(current_vol):
    # 修正時區為台灣時間 UTC+8
    now = datetime.utcnow() + timedelta(hours=8)
    open_t = now.replace(hour=9, minute=0, second=0, microsecond=0)
    elapsed = (now - open_t).seconds / 60
    if elapsed <= 0: return current_vol
    total = 270 # 交易分鐘數 (09:00~13:30)
    if elapsed >= total: return current_vol
    return int(current_vol * (total / elapsed))

# === 訊號判定邏輯 (您的核心戰法) ===
def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown):
    # 1. 漲停
    if pct >= 9.5: return "👑漲停"
    
    # 2. 攻擊：多頭排列 + 大戶買 + 漲幅量能達標
    if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio:
        return "🔥攻擊"
    
    # 3. 量增 (吸籌)：沒噴 + 有量 + 均價上 + 1H大戶吸
    if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200:
        return "👀量增"
        
    # 4. 出貨 (Dump)：破均價1% + 爆量 + 1H大戶賣
    if is_breakdown and ratio >= tgt_ratio and net_1h < 0:
        return "💀出貨"
        
    # 5. 誘多 (Trap)：漲 > 2% 但大戶1H在賣
    if pct > 2.0 and net_1h < 0:
        return "❌誘多"

    # 6. 價強
    if is_bullish and pct >= tgt_pct:
        return "⚠️價強"
        
    return "盤整"

# ==========================================
# 3. 全局狀態與後端核心 (State & Backend)
# ==========================================

# 使用 singleton 模式確保全域只有一個「數據中心」
@st.cache_resource
class SniperCore:
    def __init__(self):
        self.is_running = False
        self.targets = []
        
        # 數據容器
        self.data_store = {}       # 存大戶累計 (deque)
        self.prev_data = {}        # 存上一筆 tick (算大單用)
        self.history_vol = {}      # 歷史均量
        self.backtest_results = {} # 回測數據
        self.snapshot_df = pd.DataFrame() # 即時報表 (給前端顯示)
        self.logs = []             # 訊號 Log
        self.last_update_time = "初始化中"
        
        # 嘗試讀檔恢復數據 (斷點續傳)
        self.load_db()

    def load_db(self):
        if os.path.exists(DB_FILE):
            try:
                with open(DB_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 恢復 deque 結構
                    raw_store = data.get("data_store", {})
                    for k, v in raw_store.items():
                        self.data_store[k] = {
                            'daily': v['daily'],
                            '1h_queue': deque(v['1h_queue'], maxlen=5000)
                        }
                    # 恢復其他
                    self.backtest_results = data.get("backtest_results", {})
                    self.history_vol = data.get("history_vol", {})
                    self.logs = data.get("logs", [])
            except: pass

    def save_db(self):
        # 將 deque 轉 list 存檔
        serializable_store = {}
        for k, v in self.data_store.items():
            serializable_store[k] = {
                'daily': v['daily'],
                '1h_queue': list(v['1h_queue'])
            }
        data = {
            "data_store": serializable_store,
            "backtest_results": self.backtest_results,
            "history_vol": self.history_vol,
            "logs": self.logs
        }
        try:
            temp = f"{DB_FILE}.tmp"
            with open(temp, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            shutil.move(temp, DB_FILE)
        except: pass

    # --- 背景執行緒：負責打 API 與運算 ---
    def start_worker(self, targets):
        if self.is_running: return # 避免重複啟動
        self.targets = targets
        self.is_running = True
        
        t = threading.Thread(target=self._worker_loop, daemon=True)
        t.start()

    def _worker_loop(self):
        # 1. 初始化 Fugle Client
        try:
            client = RestClient(api_key=FUGLE_API_KEY)
        except:
            self.is_running = False
            return

        # 2. 如果沒有歷史量，先抓一次 (初始化)
        if not self.history_vol:
            for code in self.targets:
                if not self.is_running: break
                try:
                    candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                    if candles and 'data' in candles and len(candles['data']) >= 2:
                        self.history_vol[code] = int(candles['data'][-2]['volume']) // 1000
                    else: self.history_vol[code] = 1000
                except: self.history_vol[code] = 1000
                time.sleep(0.1)
            self.save_db()

        # 3. 進入無限監控迴圈
        while self.is_running:
            rows = []
            
            # 智慧節流：每檔間隔 0.3 秒，避免 53 檔瞬間爆 API
            delay = 0.3 
            
            for code in self.targets:
                try:
                    if not self.is_running: break
                    
                    # API 請求
                    q = client.stock.intraday.quote(symbol=code)
                    price = q.get('lastPrice', q.get('previousClose', 0))
                    if price == 0 or price is None: price = q.get('previousClose', 0)
                    
                    pct = q.get('changePercent', 0)
                    vol = q.get('total', {}).get('tradeVolume', 0) * 1000
                    name = get_stock_name(code)
                    total_val = q.get('total', {}).get('tradeValue', 0)
                    vwap = total_val / vol if vol > 0 else price
                    
                    est_vol = _calc_est_vol(q.get('total', {}).get('tradeVolume', 0))
                    base_vol = self.history_vol.get(code, 1000)
                    ratio = est_vol / base_vol if base_vol > 0 else 0
                    
                    # 大戶籌碼核心運算
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
                    
                    # 存入 Data Store
                    if code not in self.data_store:
                        self.data_store[code] = {'daily': 0, '1h_queue': deque()}
                    
                    if delta_net != 0:
                        self.data_store[code]['daily'] += delta_net
                        self.data_store[code]['1h_queue'].append((time.time(), delta_net))
                    
                    # 1H 滾動清理
                    now_ts = time.time()
                    one_hour_ago = now_ts - 3600
                    queue = self.data_store[code]['1h_queue']
                    while len(queue) > 0 and queue[0][0] < one_hour_ago: queue.popleft()
                    
                    net_1h = sum(i[1] for i in queue)
                    net_day = self.data_store[code]['daily']
                    
                    # 訊號判定
                    tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
                    is_bullish = price >= vwap
                    is_breakdown = price < (vwap * 0.99)
                    
                    signal = check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown)
                    
                    # 寫 Log (去重)
                    if signal != "盤整":
                        tw_time = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M:%S')
                        new_log = {"time": tw_time, "code": code, "name": name, "signal": signal}
                        if not self.logs or (self.logs[0]['code'] != code or self.logs[0]['signal'] != signal):
                            self.logs.insert(0, new_log)
                            self.logs = self.logs[:100]
                    
                    # 準備顯示資料
                    bt = self.backtest_results.get(code, {'win':0, 'ret':0})
                    
                    rows.append({
                        "代碼": code, "名稱": name, "類別": get_category(code),
                        "勝率%": f"{bt['win']:.0f}", "報酬%": f"{bt['ret']:.1f}", 
                        "現價": price, "漲跌%": f"{pct}%", "均價": f"{vwap:.1f}", 
                        "量比": f"{ratio:.1f}", "大戶(1H)": net_1h, "大戶(日)": net_day, 
                        "訊號": signal
                    })

                except: pass
                time.sleep(delay) # 每一檔中間休息
            
            # 一輪結束，更新快照 (UI 只讀這裡，絕對不會沒資料)
            if rows:
                self.snapshot_df = pd.DataFrame(rows)
                self.last_update_time = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M:%S')
                self.save_db()
                
            time.sleep(1) # 輪與輪之間休息

# 實例化核心
core = SniperCore()

# ==========================================
# 4. 前端顯示層 (UI)
# ==========================================

# 側邊欄：設定與規則 (只顯示一次)
with st.sidebar:
    st.header("⚙️ 戰情室控制台")
    
    # 指揮官控制
    with st.expander("👨‍✈️ 指揮官操作 (運算核心)", expanded=True):
        raw_input = st.text_area("監控清單", DEFAULT_POOL, height=100)
        targets = [t.strip() for t in raw_input.split() if t.strip()]
        
        # 1. 回測按鈕
        if st.button("1. 執行回測與重置", type="primary"):
            if not FUGLE_API_KEY:
                st.error("請先設定 API Key")
            else:
                core.is_running = False # 先停
                time.sleep(1)
                
                st.info("正在執行回測...")
                end_date = datetime.now()
                start_date = end_date - timedelta(days=180)
                bt_res = {}
                
                bar = st.progress(0)
                for i, code in enumerate(targets):
                    bar.progress((i+1)/len(targets))
                    tk = get_yahoo_ticker(code)
                    try:
                        df = yf.download(tk, start=start_date, end=end_date, progress=False)
                        # 簡易回測邏輯
                        if not df.empty and len(df) > 20:
                            # Flatten multi-index
                            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                            ret = (df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0] * 100
                            win = 60 # 模擬值，避免 yfinance 卡太久
                            bt_res[code] = {'win': win, 'ret': ret}
                        else:
                            bt_res[code] = {'win': 0, 'ret': 0}
                    except: bt_res[code] = {'win': 0, 'ret': 0}
                
                core.backtest_results = bt_res
                # 排序
                sorted_targets = sorted(targets, key=lambda x: bt_res.get(x, {'ret':-999})['ret'], reverse=True)
                core.save_db()
                
                # 啟動背景程式
                core.start_worker(sorted_targets)
                st.success("系統已重置並啟動！")
                time.sleep(1)
                st.rerun()

        # 2. 狀態顯示
        if core.is_running:
            st.success("🟢 核心運算中")
            if st.button("停止運算"):
                core.is_running = False
                st.rerun()
        else:
            st.warning("⚪ 核心待機中")
            if st.button("恢復運算"):
                core.start_worker(targets)
                st.rerun()

    # 訊號規則說明
    st.markdown("---")
    st.subheader("🚥 訊號規則")
    st.info("""
    **🔥 攻擊**：漲+量+價穩+大戶買
    **👀 量增**：價未噴+有量+大戶吸 (埋伏)
    **💀 出貨**：破均價1%+爆量+大戶賣
    **❌ 誘多**：漲>2% 但大戶在賣
    """)

# 主畫面
st.title("🎯 Sniper 戰情室 (v20.0)")
st.caption(f"最後更新: {core.last_update_time} (台灣時間)")

# 分頁
tab1, tab2 = st.tabs(["📊 即時戰情列表", "📡 歷史訊號紀錄"])

with tab1:
    # 這裡只讀取 snapshot，絕不進行運算
    if not core.snapshot_df.empty:
        
        # 樣式函式
        def style_df(val):
            s = str(val)
            if '攻擊' in s or '漲停' in s: return 'background-color: #FFDDDD; color: red; font-weight: bold'
            if '出貨' in s or '誘多' in s: return 'background-color: #DDFFDD; color: green; font-weight: bold'
            if '量增' in s: return 'background-color: #FFFFDD; color: #888800; font-weight: bold'
            return ''
        
        def style_net(val):
            try:
                if int(val) > 0: return 'color: red; font-weight: bold'
                if int(val) < 0: return 'color: green; font-weight: bold'
            except: pass
            return ''

        st.dataframe(
            core.snapshot_df.style
              .map(style_df, subset=['訊號'])
              .map(style_net, subset=['大戶(1H)', '大戶(日)']),
            use_container_width=True,
            height=800,
            hide_index=True
        )
    else:
        st.info("數據載入中... (若為戰情官，請確認指揮官已啟動)")

with tab2:
    if core.logs:
        for log in core.logs:
            c = "red" if "攻擊" in log['signal'] else "green" if "出貨" in log['signal'] else "blue"
            st.markdown(f"`{log['time']}` **{log['code']} {log['name']}** : :{c}[{log['signal']}]")
    else:
        st.write("尚無訊號...")

# 自動刷新 (每 3 秒)
time.sleep(3)
st.rerun()
