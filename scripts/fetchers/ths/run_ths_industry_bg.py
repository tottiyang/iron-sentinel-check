#!/usr/bin/env python3
"""
THS 行业板块采集 - 后台执行版
"""
import sys
import os
import json
import time
import urllib.request
import urllib.error
import websocket

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from fetchers.db.db_schema import get_conn

CDP_PORT = 28800

def log(msg):
    print(msg, flush=True)

def get_cdp_ws_url():
    resp = urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/version", timeout=5)
    info = json.loads(resp.read())
    return info["webSocketDebuggerUrl"]

def main():
    log("=== THS 行业板块采集 ===")
    
    # 获取待采集列表
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT board_code, board_name 
        FROM industry_boards 
        WHERE source='ths' 
        AND board_code NOT IN (
            SELECT DISTINCT board_code 
            FROM stock_industry_board 
            WHERE source='ths'
        )
        ORDER BY board_code
    """)
    pending = cur.fetchall()
    conn.close()
    
    log(f"待采集: {len(pending)} 个板块")
    if not pending:
        log("已完成！")
        return
    
    try:
        ws_url = get_cdp_ws_url()
        ws = websocket.create_connection(ws_url, timeout=30)
        log("✅ WebSocket 连接成功")
    except Exception as e:
        log(f"❌ 连接失败: {e}")
        return
    
    total_added = 0
    errors = 0
    
    for i, (board_code, board_name) in enumerate(pending):
        try:
            raw_code = board_code.replace("THS_", "")
            url = f"https://q.10jqka.com.cn/hy/detail/code/{raw_code}/"
            
            # 创建 target
            msg_id = int(time.time() * 1000000)
            ws.send(json.dumps({"id": msg_id, "method": "Target.createTarget", "params": {"url": "about:blank"}}))
            resp = json.loads(ws.recv())
            while resp.get("id") != msg_id:
                resp = json.loads(ws.recv())
            target_id = resp["result"]["targetId"]
            
            # 附加到 target
            msg_id += 1
            ws.send(json.dumps({"id": msg_id, "method": "Target.attachToTarget", "params": {"targetId": target_id, "flatten": True}}))
            resp = json.loads(ws.recv())
            while resp.get("id") != msg_id:
                resp = json.loads(ws.recv())
            session_id = resp["result"]["sessionId"]
            
            # 导航
            msg_id += 1
            ws.send(json.dumps({"id": msg_id, "method": "Page.navigate", "params": {"url": url}, "sessionId": session_id}))
            time.sleep(2)
            
            # 获取股票代码
            js_code = """
                [...document.querySelectorAll('table.m-table tbody tr')].map(tr => {
                    const link = tr.querySelector('a[href*="stockpage"]');
                    return link?.href?.match(/\\/(\\d{6})\\//)?.[1] || '';
                }).filter(c => c && '0368'.includes(c[0]))
            """
            msg_id += 1
            ws.send(json.dumps({"id": msg_id, "method": "Runtime.evaluate", "params": {"expression": js_code, "returnByValue": True}, "sessionId": session_id}))
            resp = json.loads(ws.recv())
            while resp.get("id") != msg_id:
                resp = json.loads(ws.recv())
            
            codes = resp["result"]["result"].get("value", [])
            
            if codes:
                conn = get_conn()
                cur = conn.cursor()
                for sc in codes:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_industry_board (stock_code, board_code, source, fetched_at) VALUES (?, ?, 'ths', datetime('now'))",
                        (sc, board_code),
                    )
                    if cur.rowcount > 0:
                        total_added += 1
                conn.commit()
                conn.close()
            
            if (i + 1) % 5 == 0 or i == len(pending) - 1:
                log(f"  [{i+1}/{len(pending)}] {board_name}: {len(codes)} 只, 累计+{total_added}")
            
            # 关闭 target
            msg_id += 1
            ws.send(json.dumps({"id": msg_id, "method": "Target.closeTarget", "params": {"targetId": target_id}}))
            time.sleep(0.5)
            
        except Exception as e:
            errors += 1
            log(f"  [{i+1}] {board_name}: ERR {e}")
            # 尝试重建连接
            try:
                ws.close()
            except:
                pass
            time.sleep(2)
            try:
                ws_url = get_cdp_ws_url()
                ws = websocket.create_connection(ws_url, timeout=30)
                log(f"  [{i+1}] 已重建连接")
            except:
                pass
    
    try:
        ws.close()
    except:
        pass
    
    log(f"\n=== 完成 ===")
    log(f"总计: +{total_added} 条, {errors} 个错误")

if __name__ == "__main__":
    main()
