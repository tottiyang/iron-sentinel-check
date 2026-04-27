#!/usr/bin/env python3
"""
铁血哨兵 - THS概念成分股采集 (via xbrowser)
THS反爬机制(chameleon.js)导致直接HTTP请求401，
需要用真实浏览器执行JS获取hexin-v token后才能访问AJAX分页。
"""
import subprocess
import json
import re
import time
import sqlite3
import sys
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "../stock_data.db")
XB = "/Applications/QClaw.app/Contents/Resources/node/node"
XB_SCRIPT = os.path.expanduser("~/Library/Application Support/QClaw/openclaw/config/skills/xbrowser/scripts/xb.cjs")

def xb_run(cmd):
    """Execute xbrowser command"""
    full_cmd = f'{XB} "{XB_SCRIPT}" {cmd}'
    r = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, timeout=30)
    try:
        return json.loads(r.stdout.strip())
    except:
        return {"ok": False, "error": r.stdout + r.stderr}

def get_ths_boards():
    """Get THS-only boards without constituent stocks"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT u.board_code, u.board_name, u.source_codes 
        FROM concept_boards_unified u
        LEFT JOIN stock_concept_unified s ON u.board_code = s.board_code
        WHERE u.stock_count = 0 AND u.source_codes LIKE '%ths%'
        GROUP BY u.board_code
        ORDER BY u.board_code
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def scrape_ths_board(board_code, board_name):
    """Scrape a THS concept board page for stock codes via xbrowser"""
    raw_code = board_code.replace("THS_", "")
    url = f"https://q.10jqka.com.cn/gn/detail/code/{raw_code}/"
    
    # Open the page
    result = xb_run(f'run --browser chrome open "{url}"')
    if not result.get("ok"):
        return []
    
    # Wait for JS to load
    xb_run('run --browser chrome wait --load networkidle')
    time.sleep(1)
    
    # Get the page source via snapshot
    result = xb_run('run --browser chrome snapshot')
    if not result.get("ok"):
        # Fallback: try evaluate to get stock codes
        pass
    
    # Try to get the AJAX data by executing JS
    # The page should have loaded the first batch of stock data via JS
    js_code = """
    (function() {
        // Find all stock code links in the rendered page
        const links = document.querySelectorAll('a[href*="stockpage.10jqka.com.cn"]');
        const codes = [];
        links.forEach(a => {
            const match = a.href.match(/(\\d{6})/);
            if (match) codes.push(match[1]);
        });
        return [...new Set(codes)];
    })()
    """
    result = xb_run(f'run --browser chrome evaluate "{js_code.replace(chr(34), chr(92)+chr(34))}"')
    
    codes = []
    if result.get("ok") and result.get("data", {}).get("result", {}).get("data"):
        codes = result["data"]["result"]["data"]
    
    # Also try pagination - get total count then fetch all pages
    if codes and len(codes) >= 10:
        # There are more pages - try to get total pages
        js_total = """
        (function() {
            const pager = document.querySelector('.pager_container, .pagejump, #paged');
            if (pager) return pager.textContent;
            // Try to find total items
            const totalEl = document.querySelector('.bold, .totalnums');
            return totalEl ? totalEl.textContent : 'unknown';
        })()
        """
        result = xb_run(f'run --browser chrome evaluate "{js_total.replace(chr(34), chr(92)+chr(34))}"')
    
    return codes

def main():
    boards = get_ths_boards()
    print(f"THS独占板块待采集: {len(boards)}")
    
    # Initialize xbrowser
    result = xb_run("init")
    if not result.get("ok"):
        print(f"xbrowser初始化失败: {result}")
        return
    
    total_added = 0
    errors = 0
    
    for i, (board_code, board_name, codes_json) in enumerate(boards):
        try:
            codes = scrape_ths_board(board_code, board_name)
            
            if codes:
                # Filter valid stock codes
                stock_codes = [c for c in codes if len(c) == 6 and c[0] in '0368']
                
                if stock_codes:
                    conn = sqlite3.connect(DB_PATH)
                    cur = conn.cursor()
                    for sc in stock_codes:
                        cur.execute(
                            "INSERT OR IGNORE INTO stock_concept_unified (stock_code, board_code, source) VALUES (?,?,?)",
                            (sc, board_code, "ths"),
                        )
                        total_added += cur.rowcount
                    cur.execute(
                        "UPDATE concept_boards_unified SET stock_count=? WHERE board_code=?",
                        (len(stock_codes), board_code),
                    )
                    conn.commit()
                    conn.close()
                    print(f"  [{i+1}/{len(boards)}] {board_name}: {len(stock_codes)} 只")
                else:
                    errors += 1
            else:
                errors += 1
                
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1}/{len(boards)}] {board_name}: ERR {e}")
        
        time.sleep(0.5)
        
        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(boards)}] +{total_added} 条, {errors} 个失败")
    
    print(f"\n完成: +{total_added} 条关联, {errors} 个失败")
    
    # Stop browser
    xb_run("stop chrome")

if __name__ == "__main__":
    main()
