#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 概念关联 + 行业板块关联 增量采集（分批）
"""

import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

import akshare as ak
from db_schema import get_conn

BATCH_SIZE = 30
SLEEP_SEC = 0.25


def fetch_concept_batch():
    """采集一批个股-概念关联"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT board_code, board_name FROM concept_boards ORDER BY board_code")
    all_boards = cur.fetchall()

    cur.execute("SELECT DISTINCT board_code FROM stock_concept")
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]
    if not pending:
        print("✅ 概念关联全部完成")
        return 0

    batch = pending[:BATCH_SIZE]
    success = 0

    for i, (board_code, board_name) in enumerate(batch):
        print(f"  [{i+1}/{len(batch)}, 剩余{len(pending)}] {board_name}")
        try:
            df = ak.stock_board_concept_cons_em(symbol=board_name)
            if df is None or len(df) == 0:
                continue

            conn = get_conn()
            cur = conn.cursor()
            for _, row in df.iterrows():
                stock_code = str(row.get('代码', '')).zfill(6)
                if not stock_code:
                    continue
                cur.execute("INSERT OR IGNORE INTO stock_concept (stock_code, board_code) VALUES (?, ?)",
                            (stock_code, board_code))
                success += 1
            conn.commit()
            conn.close()
            time.sleep(SLEEP_SEC)
        except Exception as e:
            print(f"    Error: {e}")
            time.sleep(1)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_concept")
    total = cur.fetchone()[0]
    conn.close()

    print(f"  本轮: +{success}, 总计: {total}, 剩余: ~{len(pending) - len(batch)}")
    return len(pending) - len(batch)


def fetch_industry_board_batch():
    """采集一批个股-行业板块关联"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT board_code, board_name FROM industry_boards ORDER BY board_code")
    all_boards = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM stock_industry_board")
    existing = cur.fetchone()[0]
    conn.close()

    if existing > 0:
        # 已经采集过，跳过
        print("✅ 行业板块关联已完成")
        return 0

    batch = all_boards[:BATCH_SIZE]
    success = 0

    for i, (board_code, board_name) in enumerate(batch):
        print(f"  [{i+1}/{len(batch)}, 剩余{len(all_boards)}] {board_name}")
        try:
            df = ak.stock_board_industry_cons_em(symbol=board_name)
            if df is None or len(df) == 0:
                continue

            conn = get_conn()
            cur = conn.cursor()
            for _, row in df.iterrows():
                stock_code = str(row.get('代码', '')).zfill(6)
                if not stock_code:
                    continue
                cur.execute("INSERT OR REPLACE INTO stock_industry_board (stock_code, board_code) VALUES (?, ?)",
                            (stock_code, board_code))
                success += 1
            conn.commit()
            conn.close()
            time.sleep(SLEEP_SEC)
        except Exception as e:
            print(f"    Error: {e}")
            time.sleep(1)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_industry_board")
    total = cur.fetchone()[0]
    conn.close()

    print(f"  本轮: +{success}, 总计: {total}, 剩余: ~{len(all_boards) - len(batch)}")
    return len(all_boards) - len(batch)


if __name__ == "__main__":
    import sys
    task = sys.argv[1] if len(sys.argv) > 1 else 'concept'
    if task == 'concept':
        remaining = fetch_concept_batch()
    elif task == 'industry':
        remaining = fetch_industry_board_batch()
    else:
        print("用法: python3 fetch_rel_batch.py [concept|industry]")
