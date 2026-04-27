#!/usr/bin/env python3
"""
铁血哨兵 - THS概念成分股采集 (CDP)
通过 Chrome DevTools Protocol 直接控制 Chrome，
绕过 THS chameleon.js 反爬，获取 AJAX 分页数据。
"""
import json
import re
import time
import sqlite3
import os
import websocket

DB_PATH = os.path.join(os.path.dirname(__file__), "../stock_data.db")
CDP_URL = "ws://127.0.0.1:28800/devtools/browser"

class CDPSession:
    def __init__(self):
        # Get browser websocket URL
        import urllib.request
        resp = urllib.request.urlopen("http://127.0.0.1:28800/json/version")
        info = json.loads(resp.read())
        ws_url = info["webSocketDebuggerUrl"]
        self.ws = websocket.create_connection(ws_url, timeout=20)
        self.msg_id = 0
    
    def send(self, method, params=None):
        self.msg_id += 1
        msg = {"id": self.msg_id, "method": method}
        if params:
            msg["params"] = params
        self.ws.send(json.dumps(msg))
        # Read responses until we get the right id
        while True:
            resp = json.loads(self.ws.recv())
            if resp.get("id") == self.msg_id:
                return resp
            # Skip events
    
    def create_page(self):
        r = self.send("Target.createTarget", {"url": "about:blank"})
        target_id = r["result"]["targetId"]
        # Attach to target
        r2 = self.send("Target.attachToTarget", {"targetId": target_id, "flatten": True})
        session_id = r2["result"]["sessionId"]
        return CDPPage(self, target_id, session_id)
    
    def close(self):
        self.ws.close()


class CDPPage:
    def __init__(self, browser, target_id, session_id):
        self.browser = browser
        self.target_id = target_id
        self.session_id = session_id
    
    def send(self, method, params=None):
        self.browser.msg_id += 1
        msg = {"id": self.browser.msg_id, "method": method, "sessionId": self.session_id}
        if params:
            msg["params"] = params
        self.browser.ws.send(json.dumps(msg))
        while True:
            resp = json.loads(self.browser.ws.recv())
            if resp.get("id") == self.browser.msg_id:
                return resp
    
    def navigate(self, url, timeout=15000):
        self.send("Page.enable")
        self.send("Page.navigate", {"url": url})
        # Wait for load
        start = time.time()
        while time.time() - start < timeout / 1000:
            try:
                resp = json.loads(self.browser.ws.recv())
                if resp.get("method") == "Page.loadEventFired":
                    return True
            except:
                break
        return False
    
    def evaluate(self, expression):
        r = self.send("Runtime.evaluate", {"expression": expression, "returnByValue": True})
        if "result" in r and "result" in r["result"]:
            val = r["result"]["result"].get("value")
            return val
        return None
    
    def get_content(self):
        return self.evaluate("document.body.innerHTML")
    
    def close(self):
        self.browser.send("Target.closeTarget", {"targetId": self.target_id})


def get_ths_boards():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT board_code, board_name, source_codes 
        FROM concept_boards_unified
        WHERE stock_count = 0 AND source_codes LIKE '%ths%'
        ORDER BY board_code
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def get_ths_industry_boards():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT board_code, board_name, source_codes 
        FROM industry_boards_unified
        WHERE stock_count = 0 AND source_codes LIKE '%ths%'
        ORDER BY board_code
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def scrape_board(page, board_code, board_name, is_concept=True):
    raw_code = board_code.replace("THS_", "")
    
    if is_concept:
        url = f"https://q.10jqka.com.cn/gn/detail/code/{raw_code}/"
    else:
        url = f"https://q.10jqka.com.cn/hy/detail/code/{raw_code}/"
    
    # Navigate to the board page
    page.navigate(url)
    time.sleep(2)  # Wait for JS to execute
    
    # Get stock codes from rendered page
    content = page.get_content() or ""
    codes = set(re.findall(r'href="https?://stockpage\.10jqka\.com\.cn/(\d{6})/"', content))
    codes = set(c for c in codes if c[0] in '0368')
    
    # Try AJAX pagination with JS fetch (using the page's own cookies)
    if codes:
        # Get total pages
        total_pages = page.evaluate("""
            (function() {
                const pager = document.querySelector('.pager_container');
                if (pager) {
                    const match = pager.textContent.match(/(\\d+)/g);
                    if (match) return parseInt(match[match.length - 1]);
                }
                return 1;
            })()
        """) or 1
        
        # Fetch remaining pages via AJAX
        for p in range(2, min(total_pages + 1, 21)):
            ajax_path = f"/gn/detail/field/264648/order/desc/page/{p}/ajax/1/code/{raw_code}/"
            if not is_concept:
                ajax_path = f"/hy/detail/field/264648/order/desc/page/{p}/ajax/1/code/{raw_code}/"
            
            js_fetch = f"""
                (async function() {{
                    try {{
                        const r = await fetch("{ajax_path}");
                        const t = await r.text();
                        const matches = t.match(/stockpage\\.10jqka\\.com\\.cn\\/(\\d{{6}})\\//g);
                        return matches ? matches.map(m => m.match(/(\\d{{6}})/)[1]) : [];
                    }} catch(e) {{
                        return [];
                    }}
                }})()
            """
            new_codes = page.evaluate(js_fetch)
            if new_codes:
                before = len(codes)
                codes.update(c for c in new_codes if c[0] in '0368')
                if len(codes) == before:
                    break
            else:
                break
            time.sleep(0.2)
    
    return list(codes)


def main():
    boards = get_ths_boards()
    ind_boards = get_ths_industry_boards()
    print(f"THS概念待采集: {len(boards)}")
    print(f"THS行业待采集: {len(ind_boards)}")
    
    cdp = CDPSession()
    
    # First visit main page to set cookies
    init_page = cdp.create_page()
    init_page.navigate("https://q.10jqka.com.cn/")
    time.sleep(3)
    print("  初始化完成")
    
    total_added = 0
    errors = 0
    
    for i, (board_code, board_name, _) in enumerate(boards):
        page = cdp.create_page()
        try:
            codes = scrape_board(page, board_code, board_name, is_concept=True)
            
            if codes:
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                for sc in codes:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_concept_unified (stock_code, board_code, source) VALUES (?,?,?)",
                        (sc, board_code, "ths"),
                    )
                    total_added += cur.rowcount
                cur.execute("UPDATE concept_boards_unified SET stock_count=? WHERE board_code=?", (len(codes), board_code))
                conn.commit()
                conn.close()
                print(f"  [{i+1}/{len(boards)}] {board_name}: {len(codes)} 只")
            else:
                errors += 1
                if errors <= 10:
                    print(f"  [{i+1}/{len(boards)}] {board_name}: 无数据")
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1}/{len(boards)}] {board_name}: ERR {e}")
        finally:
            page.close()
        
        time.sleep(0.3)
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(boards)}] +{total_added} 条, {errors} 个失败")
    
    # Industry boards
    total_ind = 0
    for i, (board_code, board_name, _) in enumerate(ind_boards):
        page = cdp.create_page()
        try:
            codes = scrape_board(page, board_code, board_name, is_concept=False)
            if codes:
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                for sc in codes:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_industry_unified (stock_code, board_code, source) VALUES (?,?,?)",
                        (sc, board_code, "ths"),
                    )
                    total_ind += cur.rowcount
                cur.execute("UPDATE industry_boards_unified SET stock_count=? WHERE board_code=?", (len(codes), board_code))
                conn.commit()
                conn.close()
        except:
            pass
        finally:
            page.close()
        time.sleep(0.3)
        if (i + 1) % 10 == 0:
            print(f"  行业[{i+1}/{len(ind_boards)}] +{total_ind} 条")
    
    cdp.close()
    print(f"\n概念: +{total_added} 条, {errors} 个失败")
    print(f"行业: +{total_ind} 条")

if __name__ == "__main__":
    main()
