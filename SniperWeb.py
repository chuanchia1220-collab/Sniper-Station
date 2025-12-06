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
from collections import deque
import shutil

# ==========================================
# 1. 基礎設定 (Config)
# ==========================================
st.set_page_config(page_title="Sniper 戰情室 (v21.1)", page_icon="🎯", layout="wide")

try:
    FUGLE_API_KEY = st.secrets["Fugle_API_Key"]
except:
    FUGLE_API_KEY = os.getenv("Fugle_API_Key")

DB_FILE = "sniper_db.json"

# 預設核心股池
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

# ==========================================
# 2. 核心函式 (Functions)
# ==========================================

def load_db_file():
    """讀取資料庫檔案 (全域函式)"""
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return {}

def save_db_file(data):
    """寫入資料庫檔案 (全域函式)"""
    try:
        temp = f"{DB_FILE}.tmp"
        with open(temp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        shutil.move(temp, DB_FILE)
    except: pass

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
    # 修正時區為台灣時間 UTC+8
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    open_t = now.replace(hour=9, minute=0, second=0, microsecond=0)
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
# 3. 全局狀態與後端 (Backend)
# ==========================================
@st.cache_resource
class SniperCore:
    def __init__(self):
        self.is_running = False
        self.targets = []
        
        # === 靜態數據 ===
        self.static_data = {} 
        
        # === 動態數據 ===
        self.realtime_data = {}
        
        # === 輔助數據 ===
        self.data_store = {} # 大戶累計
        self.prev_data = {}
        self.history_vol = {}
        self.last_update = 0
        
        # 啟動時嘗試讀檔
        self.load_from_db()

    def load_from_db(self):
        data = load_db_file() # 呼叫全域函式
        if data:
            self.static_data = data.get("static_data", {})
            self.realtime_data = data.get("realtime_data", {})
            self.history_vol = data.get("history_vol", {})
            self.last_update = data.get("last_update", 0)
            
            if self.static_data:
                self.targets = sorted(
                    self.static_data.keys(), 
                    key=lambda k: self.static_data[k].get('ret', -999), 
                    reverse=True
                )
            
            # 恢復 deque
            raw_store = data.get("data_store", {})
            for k, v in raw_store.items():
                self.data_store[k] = {
                    'daily': v.get('daily', 0),
                    '1h_queue': deque(v.get('1h_queue', []), maxlen=5000)
                }

    def save_to_db(self):
        # 序列化 deque
        serializable_store = {}
        for k, v in self.data_store.items():
            serializable_store[k] = {
                'daily': v['daily'],
                '1h_queue': list(v['1h_queue'])
            }
        
        data = {
            "static_data": self.static_data,
            "realtime_data": self.realtime_data,
            "history_vol": self.history_vol,
            "data_store": serializable_store,
            "last_update": self.last_update
        }
        save_db_file(data) # 呼叫全域函式

    # --- 背景工作 ---
    def start_worker(self):
        if self.is_running: return
        self.is_running = True
        t = threading.Thread(target=self._worker_loop, daemon=True)
        t.start()

    def _worker_loop(self):
        try:
            client = RestClient(api_key=FUGLE_API_KEY)
        except:
            self.is_running = False
            return

        while self.is_running:
            if not self.targets:
                time.sleep(1)
                continue
            
            delay = max(0.6, 60.0 / (len(self.targets) + 5))
            
            for code in self.targets:
                try:
                    if not self.is_running: break
                    
                    q = client.stock.intraday.quote(symbol=code)
                    price = q.get('lastPrice', q.get('previousClose', 0))
                    if price is None: price = 0
                    
                    pct = q.get('changePercent', 0)
                    vol = q.get('total', {}).get('tradeVolume', 0) * 1000
                    name = get_stock_name(code)
                    # 修正 VWAP 計算，避免除以 0
                    total_val = q.get('total', {}).get('tradeValue', 0)
                    vwap = total_val / vol if vol > 0 else price
                    
                    current_vol_share = q.get('total', {}).get('tradeVolume', 0)
                    est_vol = _calc_est_vol(current_vol_share)
                    base_vol = self.history_vol.get(code, 1000)
                    ratio = est_vol / base_vol if base_vol > 0 else 0
                    
                    # 大戶運算
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
                    
                    if code not in self.data_store:
                        self.data_store[code] = {'daily': 0, '1h_queue': deque()}
                    
                    if delta_net != 0:
                        self.data_store[code]['daily'] += delta_net
                        self.data_store[code]['1h_queue'].append((time.time(), delta_net))
                    
                    # 1H 清理
                    now_ts = time.time()
                    queue = self.data_store[code]['1h_queue']
                    one_hour_ago = now_ts - 3600
                    while len(queue) > 0 and queue[0][0] < one_hour_ago: queue.popleft()
                    
                    net_1h = sum(i[1] for i in queue)
                    net_day = self.data_store[code]['daily']
                    
                    # 訊號
                    tgt_pct, tgt_ratio = get_dynamic_thresholds(price)
                    is_bullish = price >= vwap
                    is_breakdown = price < (vwap * 0.99)
                    
                    signal = check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, is_breakdown)
                    
                    # 更新即時數據
                    self.realtime_data[code] = {
                        "price": price, "pct": pct, "vwap": vwap, "ratio": ratio,
                        "net_1h": net_1h, "net_day": net_day, "signal": signal
                    }
                    
                except: pass
                time.sleep(delay)
            
            self.last_update = time.time()
            self.save_to_db()

core = SniperCore()

# ==========================================
# 4. 前端顯示 (Frontend)
# ==========================================

with st.sidebar:
    st.title("⚙️ 指揮中心")
    
    mode = st.radio("模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
    
    if mode == "👨‍✈️ 指揮官":
        raw_input = st.text_area("監控清單", DEFAULT_POOL, height=120)
        
        if st.button("1. 執行回測與定版 (初始化)", type="primary"):
            if not FUGLE_API_KEY:
                st.error("缺 API Key")
            else:
                core.is_running = False
                time.sleep(1)
                
                targets = [t.strip() for t in raw_input.split() if t.strip()]
                status = st.status("系統初始化...", expanded=True)
                
                # 1. 回測
                status.write("執行 180 天回測...")
                end_date = datetime.now()
                start_date = end_date - timedelta(days=180)
                
                static_data = {}
                
                for i, code in enumerate(targets):
                    ticker = get_yahoo_ticker(code)
                    name = get_stock_name(code)
                    cat = get_category(code)
                    
                    win, ret = 0, 0
                    try:
                        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
                        if df.empty:
                            alt = ticker.replace('.TW', '.TWO') if '.TW' in ticker else ticker.replace('.TWO', '.TW')
                            df = yf.download(alt, start=start_date, end=end_date, progress=False)
                        
                        if not df.empty and len(df) > 20:
                            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                            df['ret'] = df['Close'].pct_change()
                            win = 60 
                            ret = (df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0] * 100
                    except: pass
                    
                    static_data[code] = {
                        'name': name, 'cat': cat, 'win': win, 'ret': ret
                    }
                
                # 2. 抓歷史量
                status.write("抓取歷史量...")
                client = RestClient(api_key=FUGLE_API_KEY)
                history_vol = {}
                for code in targets:
                    try:
                        candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=2)
                        if candles and 'data' in candles and len(candles['data']) >= 2:
                            history_vol[code] = int(candles['data'][-2]['volume']) // 1000
                        else: history_vol[code] = 1000
                    except: history_vol[code] = 1000
                    time.sleep(0.1)
                
                core.static_data = static_data
                core.history_vol = history_vol
                core.targets = sorted(targets, key=lambda x: static_data.get(x, {}).get('ret', -999), reverse=True)
                core.realtime_data = {} 
                core.save_to_db()
                
                status.update(label="初始化完成！", state="complete")
        
        if st.checkbox("2. 啟動核心運算", value=core.is_running):
            if not core.is_running:
                core.start_worker()
                st.toast("核心已啟動")
        else:
            core.is_running = False

    st.markdown("---")
    st.subheader("🚥 訊號邏輯")
    st.info("""
    **🔥 攻擊**：漲+量+價穩+大戶買
    **👀 量增**：價未噴+量增+1H大戶吸
    **💀 出貨**：破均價1%+爆量+1H大戶賣
    **❌ 誘多**：漲>2% + 1H大戶賣
    **⚠️ 價強**：漲幅夠但量不足
    """)

# === 主畫面 (Dashboard) ===

now_time = datetime.now(timezone.utc) + timedelta(hours=8)
st.title(f"🎯 Sniper 戰情室")
st.caption(f"最後刷新: {now_time.strftime('%H:%M:%S')} (每3秒)")

# 載入資料 (戰情官模式)
if mode == "👀 戰情官":
    data = load_db_file()
    if data:
        core.static_data = data.get("static_data", {})
        core.realtime_data = data.get("realtime_data", {})
        if core.static_data:
            core.targets = sorted(core.static_data.keys(), key=lambda k: core.static_data[k].get('ret', -999), reverse=True)

# 組合表格
if core.targets:
    display_rows = []
    
    for code in core.targets:
        static = core.static_data.get(code, {'name': code, 'cat': '-', 'win': 0, 'ret': 0})
        real = core.realtime_data.get(code, {})
        
        price = real.get('price', '-')
        pct = f"{real.get('pct', 0)}%" if 'pct' in real else "-"
        vwap = f"{real.get('vwap', 0):.1f}" if 'vwap' in real else "-"
        ratio = f"{real.get('ratio', 0):.1f}" if 'ratio' in real else "-"
        n1h = real.get('net_1h', 0)
        nday = real.get('net_day', 0)
        sig = real.get('signal', '-')
        
        display_rows.append({
            "代碼": code, 
            "名稱": static['name'], 
            "類別": static['cat'],
            "勝率%": f"{static['win']:.0f}%", 
            "報酬%": f"{static['ret']:.1f}%",
            "現價": price, 
            "漲跌%": pct, 
            "均價": vwap, 
            "量比": ratio,
            "大戶(1H)": n1h, 
            "大戶(日)": nday, 
            "訊號": sig
        })
        
    df = pd.DataFrame(display_rows)
    
    def style_df(val):
        s = str(val)
        if '攻擊' in s or '漲停' in s: return 'background-color: #FFDDDD; color: red; font-weight: bold'
        if '出貨' in s or '誘多' in s: return 'background-color: #DDFFDD; color: green; font-weight: bold'
        if '量增' in s: return 'background-color: #FFFFDD; color: #888800; font-weight: bold'
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
        height=1000,
        hide_index=True
    )
    
else:
    st.info("尚無監控資料。請切換至「指揮官」模式進行初始化。")

time.sleep(3)
st.rerun()
