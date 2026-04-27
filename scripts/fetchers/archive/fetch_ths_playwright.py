#!/usr/bin/env python3
"""
铁血哨兵 - THS概念成分股采集 (Playwright)
用真实浏览器绕过chameleon.js反爬，获取AJAX分页数据。
"""
import re
import time
import json
import sqlite3
import os
from playwright.sync_api import sync_playwright

DB_PATH = os.path.join(os.path.dirname(__file__), "../stock_data.db")

def get_ths_boards():
    """Get THS boards without constituent stocks"""
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

def scrape_board(browser, board_code, board_name, is_concept=True):
    """Scrape a THS board page for stock codes using Playwright"""
    raw_code = board_code.replace("THS_", "")
    
    if is_concept:
        url = f"https://q.10jqka.com.cn/gn/detail/code/{raw_code}/"
    else:
        url = f"https://q.10jqka.com.cn/hy/detail/code/{raw_code}/"
    
    all_codes = set()
    
    try:
        page = browser.new_page()
        page.set_default_timeout(15000)
        
        # Navigate to the board page
        page.goto(url, wait_until="networkidle", timeout=15000)
        
        # Wait for the stock table to load
        try:
            page.wait_for_selector("table.m-table", timeout=5000)
        except:
            pass
        
        # Extract stock codes from the first page
        content = page.content()
        codes = re.findall(r'href="https?://stockpage\.10jqka\.com\.cn/(\d{6})/"', content)
        all_codes.update(c for c in codes if c[0] in '0368')
        
        # Try to get total page count and iterate
        try:
            # Look for pagination info
            page_text = page.inner_text("body")
            page_match = re.search(r'共\s*(\d+)\s*页', page_text)
            total_pages = int(page_match.group(1)) if page_match else 1
            
            # If there are multiple pages, try AJAX pagination
            if total_pages > 1:
                for p in range(2, min(total_pages + 1, 21)):  # Cap at 20 pages
                    try:
                        # Click next page or use AJAX URL
                        ajax_url = f"https://q.10jqka.com.cn/gn/detail/field/264648/order/desc/page/{p}/ajax/1/code/{raw_code}/"
                        if not is_concept:
                            ajax_url = f"https://q.10jqka.com.cn/hy/detail/field/264648/order/desc/page/{p}/ajax/1/code/{raw_code}/"
                        
                        # Use page.evaluate to fetch AJAX data with proper cookies
                        response = page.evaluate("""
                            async (url) => {
                                const r = await fetch(url);
                                return await r.text();
                            }
                        """, ajax_url)
                        
                        if response:
                            new_codes = re.findall(r'href="https?://stockpage\.10jqka\.com\.cn/(\d{6})/"', response)
                            before = len(all_codes)
                            all_codes.update(c for c in new_codes if c[0] in '0368')
                            if len(all_codes) == before:
                                break  # No new codes, stop
                    except:
                        break
                    time.sleep(0.2)
                    
        except Exception as e:
            pass  # Pagination failed, use what we have
        
        page.close()
        
    except Exception as e:
        try:
            page.close()
        except:
            pass
    
    return list(all_codes)

def main():
    boards = get_ths_boards()
    ind_boards = get_ths_industry_boards()
    print(f"THS概念待采集: {len(boards)}")
    print(f"THS行业待采集: {len(ind_boards)}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        # First visit main page to get cookies
        try:
            init_page = context.new_page()
            init_page.goto("https://q.10jqka.com.cn/", wait_until="networkidle", timeout=15000)
            init_page.close()
            print("  初始化cookies完成")
        except:
            print("  初始化cookies失败，继续尝试...")
        
        total_added = 0
        errors = 0
        
        for i, (board_code, board_name, _) in enumerate(boards):
            codes = scrape_board(context, board_code, board_name, is_concept=True)
            
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
                if (i + 1) % 10 == 0 or len(codes) > 0:
                    print(f"  [{i+1}/{len(boards)}] {board_name}: {len(codes)} 只")
            else:
                errors += 1
            
            time.sleep(0.3)
            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(boards)}] +{total_added} 条, {errors} 个失败")
        
        # Industry boards
        total_ind = 0
        for i, (board_code, board_name, _) in enumerate(ind_boards):
            codes = scrape_board(context, board_code, board_name, is_concept=False)
            
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
            
            time.sleep(0.3)
            if (i + 1) % 10 == 0:
                print(f"  行业[{i+1}/{len(ind_boards)}] +{total_ind} 条")
        
        browser.close()
    
    print(f"\n概念: +{total_added} 条, {errors} 个失败")
    print(f"行业: +{total_ind} 条")

if __name__ == "__main__":
    main()
