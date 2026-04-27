#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 概念成分股关联采集（浏览器方式）

当东方财富API不通时，通过web_fetch爬取东方财富板块页面获取成分股。
URL格式: https://data.eastmoney.com/bkzj/gn/BKxxxx.html

断点续传 + 来源标记
"""

import sys
import os
import time
import re
import json
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from db_schema import get_conn

SLEEP_SEC = 1.0


def fetch_concept_cons_webfetch(board_code, board_name):
    """通过 web_fetch 东方财富板块资金流向页面提取成分股
    
    页面URL: https://data.eastmoney.com/bkzj/gn/{board_code}.html
    但这个页面是JS渲染的，web_fetch可能拿不到动态数据
    """
    # 这个方法可能不可行，因为是JS渲染
    # 换用 quote.eastmoney.com 的接口
    return []


def fetch_concept_cons_api(board_code):
    """直接用requests调用东方财富API（如果网络通的话）"""
    import requests
    
    url = f"https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        'pn': 1, 'pz': 500, 'po': 1, 'np': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2, 'invt': 2, 'fid': 'f12',
        'fs': f'b:{board_code} f:!50',
        'fields': 'f12,f14',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/',
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()
        if data.get('data') and data['data'].get('diff'):
            stocks = []
            for item in data['data']['diff']:
                code = str(item.get('f12', '')).zfill(6)
                if code and len(code) == 6:
                    stocks.append(code)
            return stocks
    except:
        pass
    return []


def fetch_concept_cons_api_v2(board_code):
    """用多个CDN前缀尝试"""
    import requests
    
    prefixes = ['', '1.', '2.', '10.', '20.', '29.', '40.', '79.', '80.', '100.']
    
    params = {
        'pn': 1, 'pz': 500, 'po': 1, 'np': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2, 'invt': 2, 'fid': 'f12',
        'fs': f'b:{board_code} f:!50',
        'fields': 'f12,f14',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/',
    }
    
    for prefix in prefixes:
        try:
            host = f"{prefix}push2.eastmoney.com" if prefix else "push2.eastmoney.com"
            url = f"https://{host}/api/qt/clist/get"
            r = requests.get(url, params=params, headers=headers, timeout=8)
            data = r.json()
            if data.get('data') and data['data'].get('diff'):
                stocks = []
                for item in data['data']['diff']:
                    code = str(item.get('f12', '')).zfill(6)
                    if code and len(code) == 6:
                        stocks.append(code)
                if stocks:
                    return stocks, prefix
        except:
            continue
    return [], None


def fetch_industry_cons_api(board_code):
    """获取行业板块成分股"""
    import requests
    
    params = {
        'pn': 1, 'pz': 500, 'po': 1, 'np': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2, 'invt': 2, 'fid': 'f12',
        'fs': f'b:{board_code} f:!50',
        'fields': 'f12,f14',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/',
    }
    
    for prefix in ['', '1.', '29.', '79.']:
        try:
            host = f"{prefix}push2.eastmoney.com" if prefix else "push2.eastmoney.com"
            url = f"https://{host}/api/qt/clist/get"
            r = requests.get(url, params=params, headers=headers, timeout=8)
            data = r.json()
            if data.get('data') and data['data'].get('diff'):
                stocks = []
                for item in data['data']['diff']:
                    code = str(item.get('f12', '')).zfill(6)
                    if code and len(code) == 6:
                        stocks.append(code)
                return stocks
        except:
            continue
    return []


def test_api_connectivity():
    """测试东方财富API连通性"""
    import requests
    
    test_codes = ['BK0477', 'BK0428']  # 华为概念, 5G
    
    for prefix in ['', '1.', '2.', '10.', '29.', '79.', '80.']:
        host = f"{prefix}push2.eastmoney.com" if prefix else "push2.eastmoney.com"
        url = f"https://{host}/api/qt/clist/get"
        params = {
            'pn': 1, 'pz': 3, 'po': 1, 'np': 1,
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': 2, 'invt': 2, 'fid': 'f12',
            'fs': f'b:BK0477 f:!50',
            'fields': 'f12,f14',
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Referer': 'https://quote.eastmoney.com/',
        }
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            data = r.json()
            total = data.get('data', {}).get('total', 0) if data.get('data') else 0
            print(f"  {prefix or 'no-prefix'}push2: OK, total={total}")
            if total > 0:
                return prefix
        except Exception as e:
            print(f"  {prefix or 'no-prefix'}push2: FAIL ({type(e).__name__})")
    
    return None


def fetch_concept_relations_batch(source='em', limit=50):
    """采集概念-个股关联（断点续传）"""
    conn = get_conn()
    cur = conn.cursor()
    
    # 获取该来源的板块
    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source=? ORDER BY board_code", (source,))
    all_boards = cur.fetchall()
    
    # 已完成的板块
    cur.execute("SELECT DISTINCT board_code FROM stock_concept WHERE source=?", (source,))
    done = set(r[0] for r in cur.fetchall())
    conn.close()
    
    pending = [(code, name) for code, name in all_boards if code not in done]
    
    if not pending:
        print(f"  [{source}] 概念关联全部完成 ({len(all_boards)} 板块)")
        return 0
    
    batch = pending[:limit]
    total_added = 0
    errors = 0
    
    # 先测试API连通性
    if source == 'em':
        working_prefix = test_api_connectivity()
        if working_prefix is None:
            print(f"  [em] 东方财富API不通，跳过本轮")
            return len(pending)
    
    for i, (board_code, board_name) in enumerate(batch):
        if source == 'em':
            stocks, _ = fetch_concept_cons_api_v2(board_code)
        else:
            stocks = []  # ths 暂无成分股接口
        
        if stocks:
            conn = get_conn()
            cur = conn.cursor()
            for stock_code in stocks:
                cur.execute(
                    "INSERT OR IGNORE INTO stock_concept (stock_code, board_code, source) VALUES (?, ?, ?)",
                    (stock_code, board_code, source)
                )
                total_added += 1
            conn.commit()
            conn.close()
        else:
            errors += 1
        
        time.sleep(SLEEP_SEC)
        
        if (i + 1) % 10 == 0 or i == len(batch) - 1:
            print(f"  [{source}] {i+1}/{len(batch)}, +{total_added} 条, {errors} 个失败")
    
    return len(pending) - len(batch)


def fetch_industry_board_relations_batch(limit=50):
    """采集行业板块-个股关联"""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT board_code, board_name FROM industry_boards ORDER BY board_code")
    all_boards = cur.fetchall()
    
    # 检查已完成数量
    cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_industry_board")
    done_count = cur.fetchone()[0]
    conn.close()
    
    if done_count >= len(all_boards):
        print("  [industry] 行业板块关联全部完成")
        return 0
    
    # 获取已完成的板块
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT board_code FROM stock_industry_board WHERE board_code IS NOT NULL AND board_code != ''")
    done = set(r[0] for r in cur.fetchall())
    conn.close()
    
    pending = [(code, name) for code, name in all_boards if code not in done]
    batch = pending[:limit]
    
    total_added = 0
    errors = 0
    
    # 测试API
    working_prefix = test_api_connectivity()
    if working_prefix is None:
        print("  [industry] 东方财富API不通，跳过")
        return len(pending)
    
    for i, (board_code, board_name) in enumerate(batch):
        stocks = fetch_industry_cons_api(board_code)
        
        if stocks:
            conn = get_conn()
            cur = conn.cursor()
            for stock_code in stocks:
                cur.execute(
                    "INSERT OR REPLACE INTO stock_industry_board (stock_code, board_code) VALUES (?, ?)",
                    (stock_code, board_code)
                )
                total_added += 1
            conn.commit()
            conn.close()
        else:
            errors += 1
        
        time.sleep(SLEEP_SEC)
        
        if (i + 1) % 10 == 0 or i == len(batch) - 1:
            print(f"  [industry] {i+1}/{len(batch)}, +{total_added} 条, {errors} 个失败")
    
    return len(pending) - len(batch)


if __name__ == "__main__":
    import sys
    task = sys.argv[1] if len(sys.argv) > 1 else 'test'
    
    if task == 'test':
        print("测试东方财富API连通性...")
        prefix = test_api_connectivity()
        if prefix:
            print(f"可用前缀: {prefix}")
        else:
            print("所有CDN节点不通")
    elif task == 'concept':
        source = sys.argv[2] if len(sys.argv) > 2 else 'em'
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 50
        fetch_concept_relations_batch(source, limit)
    elif task == 'industry':
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        fetch_industry_board_relations_batch(limit)
    elif task == 'status':
        from fetch_concepts_multi import status_report
        status_report()
