def _update_market_thermometer(self):
        # 冷卻時間：3秒更新一次
        if time.time() - self.market_stats.get("Time", 0) < 3: return
        
        current_price = 0
        current_pct = 0
        source_status = "Init"
        
        # === 方案 A: 優先嘗試 Fugle API (秒級同步) ===
        try:
            client = self.clients[0] if self.clients else None
            if client:
                # 指揮官注意：富果的指數代碼是 "IX0001"
                q = client.stock.intraday.quote(symbol="IX0001")
                
                # [關鍵修正] 指數的 JSON 結構不同，價格通常在最外層
                # 優先抓 lastPrice (盤中)，沒有則抓 close (收盤)
                price = q.get('lastPrice') or q.get('close')
                
                if price:
                    current_price = price
                    current_pct = q.get('changePercent', 0)
                    source_status = f"{datetime.now().strftime('%H:%M:%S')}"
        except Exception as e:
            # 如果 Fugle 失敗 (例如權限不足或連線逾時)，默默進入備援
            # print(f"Fugle Index Error: {e}") # 除錯用
            pass

        # === 方案 B: 備援 Yahoo Finance (延遲報價) ===
        # 如果方案 A 沒抓到價格，才啟動 B 計畫
        if not current_price:
            try:
                tse = yf.Ticker("^TWII")
                fi = tse.fast_info
                current_price = fi.last_price
                prev = fi.previous_close
                if current_price and prev:
                    current_pct = ((current_price - prev) / prev) * 100
                    # 標註 (Delay) 讓您知道現在看的是舊資料
                    source_status = f"{datetime.now().strftime('%H:%M')} (Delay)"
            except:
                pass

        # === 更新戰情看板 ===
        if current_price:
             # 取得 5MA (若無則用現價代替)
             price_5ma = self.market_stats.get("Price5MA", current_price)
             
             self.twii_data = {
                'code': '0000', 
                'name': f'加權指數 {source_status}', 
                'price': current_price,
                'pct': current_pct,
                'vwap': current_price, 
                'price_5ma': price_5ma,
                'ratio': 1.0, 'ratio_yest': 1.0,
                'net_10m': 0, 'net_1h': 0, 'net_day': 0,
                'situation': '市場指標',
                'event_label': '大盤',
                'is_pinned': 1,
                'win_rate': 0, 'avg_ret': 0 
            }
             self.market_stats["Time"] = time.time()
