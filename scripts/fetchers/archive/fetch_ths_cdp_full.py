#!/usr/bin/env python3
"""
THS 成分股采集 (CDP)
通过 Chrome DevTools Protocol 获取同花顺板块成分股
"""
import json
import re
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from fetchers.db.db_schema import get_conn

CDP_PORT = 28800
SLEEP_SEC = 0.3


class CDPSession:
    def __init__(self):
        import urllib.request
        import websocket
        resp = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/version")
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
        while True:
            resp = json.loads(self.ws.recv())
            if resp.get("id") == self.msg_id:
                return resp

    def create_page(self):
        r = self.send("Target.createTarget", {"url": "about:blank"})
        target_id = r["result"]["targetId"]
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
            return r["result"]["result"].get("value")
        return None

    def close(self):
        self.browser.send("Target.closeTarget", {"targetId": self.target_id})


def scrape_board_stocks(page, board_code, board_name, is_concept=True):
    """采集单个板块的成分股"""
    raw_code = board_code.replace("THS_", "")

    if is_concept:
        url = f"https://q.10jqka.com.cn/gn/detail/code/{raw_code}/"
    else:
        url = f"https://q.10jqka.com.cn/hy/detail/code/{raw_code}/"

    page.navigate(url)
    time.sleep(2)

    # 从页面提取股票代码
    html = page.evaluate("document.body.innerHTML") or ""
    codes = set(re.findall(r'href="https?://stockpage\.10jqka\.com\.cn/(\d{6})/"', html))
    codes = set(c for c in codes if c[0] in '0368')

    if not codes:
        return []

    # 获取总页数并翻页
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

    # 翻页获取剩余股票
    for p in range(2, min(total_pages + 1, 51)):  # 最多50页
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
            codes.update(c for c in new_codes if c[0] in '0368')
        else:
            break
        time.sleep(0.2)

    return list(codes)


def fetch_concept_relations(cdp, limit=0):
    """采集概念板块成分股"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source='ths' ORDER BY board_code")
    all_boards = cur.fetchall()

    # 断点续传
    cur.execute("SELECT DISTINCT board_code FROM stock_concept WHERE source='ths'")
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(c, n) for c, n in all_boards if c not in done]
    if limit > 0:
        pending = pending[:limit]

    print(f"[THS-concept] 需采集 {len(pending)} 个板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        page = cdp.create_page()
        try:
            stocks = scrape_board_stocks(page, board_code, board_name, is_concept=True)
            if stocks:
                conn = get_conn()
                cur = conn.cursor()
                for sc in stocks:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_concept (stock_code, board_code, source, fetched_at) "
                        "VALUES (?, ?, 'ths', datetime('now'))",
                        (sc, board_code),
                    )
                    if cur.rowcount > 0:
                        total_added += 1
                conn.commit()
                conn.close()

            if (i + 1) % 10 == 0 or i == len(pending) - 1:
                print(f"  [{i+1}/{len(pending)}] {board_name}: {len(stocks)} 只, +{total_added} 条")

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1}/{len(pending)}] {board_name}: ERR {e}")
        finally:
            page.close()

        time.sleep(SLEEP_SEC)

    return total_added, errors


def fetch_industry_relations(cdp, limit=0):
    """采集行业板块成分股"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT board_code, board_name FROM industry_boards WHERE source='ths' ORDER BY board_code")
    all_boards = cur.fetchall()

    cur.execute("SELECT DISTINCT board_code FROM stock_industry_board WHERE source='ths'")
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(c, n) for c, n in all_boards if c not in done]
    if limit > 0:
        pending = pending[:limit]

    print(f"[THS-industry] 需采集 {len(pending)} 个板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        page = cdp.create_page()
        try:
            stocks = scrape_board_stocks(page, board_code, board_name, is_concept=False)
            if stocks:
                conn = get_conn()
                cur = conn.cursor()
                for sc in stocks:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_industry_board (stock_code, board_code, source, fetched_at) "
                        "VALUES (?, ?, 'ths', datetime('now'))",
                        (sc, board_code),
                    )
                    if cur.rowcount > 0:
                        total_added += 1
                conn.commit()
                conn.close()

            if (i + 1) % 10 == 0 or i == len(pending) - 1:
                print(f"  [{i+1}/{len(pending)}] {board_name}: {len(stocks)} 只, +{total_added} 条")

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1}/{len(pending)}] {board_name}: ERR {e}")
        finally:
            page.close()

        time.sleep(SLEEP_SEC)

    return total_added, errors


def main():
    import websocket  # 确保导入

    print("=== THS 成分股采集 (CDP) ===")
    print(f"Chrome CDP: http://localhost:{CDP_PORT}")

    cdp = CDPSession()

    # 先访问主页确保登录态
    init_page = cdp.create_page()
    init_page.navigate("https://q.10jqka.com.cn/")
    time.sleep(3)
    print("  Chrome 初始化完成\n")

    # 采集概念板块
    print("[1/2] 采集概念板块成分股...")
    concept_added, concept_err = fetch_concept_relations(cdp)

    # 采集行业板块
    print("\n[2/2] 采集行业板块成分股...")
    industry_added, industry_err = fetch_industry_relations(cdp)

    cdp.close()

    print(f"\n=== 完成 ===")
    print(f"概念板块: +{concept_added} 条, {concept_err} 个错误")
    print(f"行业板块: +{industry_added} 条, {industry_err} 个错误")


if __name__ == "__main__":
    main()
