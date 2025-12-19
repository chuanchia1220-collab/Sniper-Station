import streamlit as st
import pandas as pd
import time, os, json, queue, threading, sqlite3, requests
from datetime import datetime, timedelta, timezone, time as dt_time
from dataclasses import dataclass, field
from itertools import cycle
import concurrent.futures
import twstock
from fugle_marketdata import RestClient

# =====================================================
# 0. App Config
# =====================================================
st.set_page_config(
    page_title="Sniper v4.0 Pro",
    page_icon="🛡️",
    layout="wide"
)

# =====================================================
# 1. Secrets & Constants
# =====================================================
try:
    RAW_KEYS = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    RAW_KEYS = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in RAW_KEYS.split(",") if k.strip()]
DB_PATH = "sniper_v40.db"

# =====================================================
# 2. Domain Model
# =====================================================
@dataclass(frozen=True)
class SniperEvent:
    code: str
    name: str
    scope: str            # watchlist / inventory / test
    event_type: str       # STRATEGY / RISK / TEST
    trigger: str          # 🔥攻擊 / 💣伏擊 / 💀出貨
    price: float
    pct: float
    ratio: float
    net_1h: int
    net_10m: int
    net_day: int
    timestamp: float = field(default_factory=time.time)
    data_status: str = "DATA_OK"
    is_test: bool = False

# =====================================================
# 3. Market Session
# =====================================================
class MarketSession:
    OPEN, CLOSE = dt_time(9, 0), dt_time(13, 35)

    @staticmethod
    def is_open(now=None):
        if not now:
            now = datetime.now(timezone.utc) + timedelta(hours=8)
        return MarketSession.OPEN <= now.time() <= MarketSession.CLOSE

# =====================================================
# 4. Database (State Only)
# =====================================================
class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.path, check_same_thread=False)

    def _init_db(self):
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS realtime (
                code TEXT PRIMARY KEY,
                name TEXT,
                price REAL,
                pct REAL,
                ratio REAL,
                net_1h INTEGER,
                net_10m INTEGER,
                net_day INTEGER,
                signal TEXT,
                update_time REAL,
                data_status TEXT
            )
        """)
        c.execute("CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)")
        c.execute("CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY)")
        conn.commit()
        conn.close()

    def upsert_realtime(self, rows):
        if not rows:
            return
        conn = self._conn()
        c = conn.cursor()
        c.executemany("""
            INSERT OR REPLACE INTO realtime
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()

    def get_codes(self):
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT code FROM watchlist UNION SELECT code FROM inventory")
        rows = [r[0] for r in c.fetchall()]
        conn.close()
        return rows

    def get_view(self):
        conn = self._conn()
        df = pd.read_sql("""
            SELECT w.code, r.name, r.price, r.pct, r.signal, r.data_status
            FROM watchlist w
            LEFT JOIN realtime r ON w.code = r.code
        """, conn)
        conn.close()
        return df

db = Database(DB_PATH)

# =====================================================
# 5. Notification Manager (Single Gate)
# =====================================================
class NotificationManager:
    COOLDOWN = 600

    def __init__(self):
        self.queue = queue.Queue()
        self.cooldown = {}
        threading.Thread(target=self._worker, daemon=True).start()

    def allow(self, ev: SniperEvent) -> bool:
        if ev.is_test:
            return True
        if ev.data_status != "DATA_OK":
            return False
        if not MarketSession.is_open():
            return False

        key = f"{ev.code}_{ev.trigger}"
        last = self.cooldown.get(key, 0)
        if time.time() - last < self.COOLDOWN:
            return False
        return True

    def enqueue(self, ev: SniperEvent):
        if self.allow(ev):
            if not ev.is_test:
                self.cooldown[f"{ev.code}_{ev.trigger}"] = time.time()
            self.queue.put(ev)

    def _worker(self):
        while True:
            ev = self.queue.get()
            try:
                self._send(ev)
            finally:
                self.queue.task_done()

    def _send(self, ev: SniperEvent):
        if not TG_BOT_TOKEN or not TG_CHAT_ID:
            return
        msg = (
            f"<b>{ev.trigger} {ev.code} {ev.name}</b>\n"
            f"現價 {ev.price:.2f} ({ev.pct:.2f}%)\n"
            f"量比 {ev.ratio:.1f}"
        )
        requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=5
        )

notifier = NotificationManager()

# =====================================================
# 6. Dispatcher (Hard Boundary)
# =====================================================
class Dispatcher:
    def dispatch(self, ev: SniperEvent):
        notifier.enqueue(ev)

dispatcher = Dispatcher()

# =====================================================
# 7. Engine (ONLY Place With Loop)
# =====================================================
class SniperEngine:
    def __init__(self):
        self.running = False
        self.clients = [RestClient(api_key=k) for k in API_KEYS]
        self.cycle = cycle(self.clients) if self.clients else None
        self.thread = None

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            now = datetime.now(timezone.utc) + timedelta(hours=8)
            codes = db.get_codes()
            if not codes:
                time.sleep(2)
                continue

            rows = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                futures = [ex.submit(self._process, c) for c in codes]
                for f in concurrent.futures.as_completed(futures):
                    if f.result():
                        rows.append(f.result())

            db.upsert_realtime(rows)
            time.sleep(3 if MarketSession.is_open(now) else 10)

    def _process(self, code):
        try:
            client = next(self.cycle)
            q = client.stock.intraday.quote(symbol=code)
            price = q.get("lastPrice", 0)
            pct = q.get("changePercent", 0)

            trigger = None
            if pct > 3:
                trigger = "🔥攻擊"

            if trigger:
                ev = SniperEvent(
                    code=code,
                    name=twstock.codes.get(code, twstock.codes.get(code)).name,
                    scope="watchlist",
                    event_type="STRATEGY",
                    trigger=trigger,
                    price=price,
                    pct=pct,
                    ratio=1.0,
                    net_1h=0,
                    net_10m=0,
                    net_day=0
                )
                dispatcher.dispatch(ev)

            return (
                code,
                twstock.codes.get(code, twstock.codes.get(code)).name,
                price,
                pct,
                1.0,
                0,
                0,
                0,
                trigger or "盤整",
                time.time(),
                "DATA_OK"
            )
        except:
            return None

# =====================================================
# 8. UI (Pure Passive)
# =====================================================
def render_ui():
    st.title("🛡️ Sniper v4.0 Pro")

    if "engine" not in st.session_state:
        st.session_state.engine = SniperEngine()
        st.session_state.running = False

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🟢 啟動", disabled=st.session_state.running):
            st.session_state.engine.start()
            st.session_state.running = True
    with col2:
        if st.button("🔴 停止", disabled=not st.session_state.running):
            st.session_state.engine.stop()
            st.session_state.running = False

    st.markdown("---")
    st.dataframe(db.get_view(), use_container_width=True)

render_ui()
