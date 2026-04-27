#!/usr/bin/env python3
"""
THS 成分股采集 (CDP) - 简化版
"""
import json
import re
import time
import sys
import os
import urllib.request
import websocket

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from fetchers.db.db_schema import get_conn

CDP_PORT = 28800
SLEEP_SEC = 1.0


def get_cdp_ws_url():
    resp = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/version")
    info = json.loads(resp.read())
    return info["webSocketDebuggerUrl"]


def scrape_board_stocks(ws, session_id, board_code, is_concept=True):
    """采集单个板块的成分股"""
    msg_id = [0]
    
    def send_to_session(method, params=None):
        msg_id[0] += 1
        msg = {"id": msg_id[0], "method": method, "sessionId": session_id}
        if params:
            msg["params"] = params
        ws.send(json.dumps(msg))
        while True:
            resp = json.loads(ws.recv())
            if resp.get("id") == msg_id[0]:
                return resp
    
    # 导航到板块页面
    send_to_session("Page.enable")
    
    raw_code = board_code.replace("THS_", "")
    if is_concept:
        url = f"https://q.10jqka.com.cn/gn/detail/code/{raw_code}/"
    else:
        url = f"https://q.10jqka.com.cn/hy/detail/code/{raw_code}/"
    
    send_to_session("Page.navigate", {"url": url})
    time.sleep(3)
    
    # 获取股票代码
    js_code = """
        [...document.querySelectorAll('table.m-table tbody tr')].map(tr => {
            const link = tr.querySelector('a[href*="stockpage"]');
            return link?.href?.match(/\\/(\\d{6})\\//)?.[1] || '';
        }).filter(c => c && '0368'.includes(c[0]))
    """
    
    r = send_to_session("Runtime.evaluate", {"expression": js_code, "returnByValue": True})
    codes = r["result"]["result"].get("value", [])
    
    if not codes:
        return []
    
    # 获取总页数并翻页
    r = send_to_session("Runtime.evaluate", {"expression": """
        (function() {
            const pager = document.querySelector('.pager_container');
            if (pager) {
                const match = pager.textContent.match(/\\d+/g);
                if (match) return parseInt(match[match.length - 1]);
            }
            return 1;
        })()
    """, "returnByValue": True})
    
    total_pages = r["result"]["result"].get("value", 1)
    all_codes = set(codes)
    
    # 翻页获取剩余股票
    for p in range(2, min(total_pages + 1, 51)):
        ajax_js = f"""
            (async function() {{
                try {{
                    const path = "{(is_concept and 'gn' or 'hy')}/detail/field/264648/order/desc/page/{p}/ajax/1/code/{raw_code}/";
                    const r = await fetch(path, {{credentials: 'same-origin'}});
                    const t = await r.text();
                    const matches = [...t.matchAll(/stockpage\\.10jqka\\.com\\.cn\\/(\\d{{6}})\\//g)];
                    return matches.map(m => m[1]).filter(c => '0368'.includes(c[0]));
                }} catch(e) {{
                    return [];
                }}
            }})()
        """
        r = send_to_session("Runtime.evaluate", {"expression": ajax_js, "returnByValue": True, "awaitPromise": True})
        new_codes = r["result"]["result"].get("value", [])
        if new_codes:
            all_codes.update(new_codes)
        else:
            break
        time.sleep(0.3)
    
    return list(all_codes)


def fetch_concept_relations(ws, limit=0):
    """采集概念板块成分股"""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source='ths' ORDER BY board_code")
    all_boards = cur.fetchall()
    
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
        # 创建新 target
        msg_id = [0]
        def send(method, params=None):
            msg_id[0] += 1
            msg = {"id": msg_id[0], "method": method}
            if params:
                msg["params"] = params
            ws.send(json.dumps(msg))
            while True:
                resp = json.loads(ws.recv())
                if resp.get("id") == msg_id[0]:
                    return resp
        
        try:
            r = send("Target.createTarget", {"url": "about:blank"})
            target_id = r["result"]["targetId"]
            r2 = send("Target.attachToTarget", {"targetId": target_id, "flatten": True})
            session_id = r2["result"]["sessionId"]
            
            stocks = scrape_board_stocks(ws, session_id, board_code, is_concept=True)
            
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
                print(f"  [{i+1}/{len(pending)}] {board_name}: {len(stocks)} 只, 累计+{total_added} 条")
            
            send("Target.closeTarget", {"targetId": target_id})
            
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1}/{len(pending)}] {board_name}: ERR {e}")
        
        time.sleep(SLEEP_SEC)
    
    return total_added, errors


def fetch_industry_relations(ws, limit=0):
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
        msg_id = [0]
        def send(method, params=None):
            msg_id[0] += 1
            msg = {"id": msg_id[0], "method": method}
            if params:
                msg["params"] = params
            ws.send(json.dumps(msg))
            while True:
                resp = json.loads(ws.recv())
                if resp.get("id") == msg_id[0]:
                    return resp
        
        try:
            r = send("Target.createTarget", {"url": "about:blank"})
            target_id = r["result"]["targetId"]
            r2 = send("Target.attachToTarget", {"targetId": target_id, "flatten": True})
            session_id = r2["result"]["sessionId"]
            
            stocks = scrape_board_stocks(ws, session_id, board_code, is_concept=False)
            
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
                print(f"  [{i+1}/{len(pending)}] {board_name}: {len(stocks)} 只, 累计+{total_added} 条")
            
            send("Target.closeTarget", {"targetId": target_id})
            
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1}/{len(pending)}] {board_name}: ERR {e}")
        
        time.sleep(SLEEP_SEC)
    
    return total_added, errors


def main():
    print("=== THS 成分股采集 (CDP) ===")
    print(f"Chrome CDP: http://localhost:{CDP_PORT}")
    
    ws_url = get_cdp_ws_url()
    ws = websocket.create_connection(ws_url, timeout=20)
    
    # 采集概念板块
    print("\n[1/2] 采集概念板块成分股...")
    concept_added, concept_err = fetch_concept_relations(ws)
    
    # 采集行业板块
    print("\n[2/2] 采集行业板块成分股...")
    industry_added, industry_err = fetch_industry_relations(ws)
    
    ws.close()
    
    print(f"\n=== 完成 ===")
    print(f"概念板块: +{concept_added} 条, {concept_err} 个错误")
    print(f"行业板块: +{industry_added} 条, {industry_err} 个错误")


if __name__ == "__main__":
    main()
