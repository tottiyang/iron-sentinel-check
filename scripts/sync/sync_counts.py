#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量重算所有板块的 stock_count（快照值）

从 stock_concept / stock_industry_board 反推各板块的成分股数量，
修复 stock_count 全为 NULL 的问题。
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fetchers.db.db_schema import get_conn


def sync_concept_counts():
    """重算 concept_boards.stock_count"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT board_code, COUNT(DISTINCT stock_code) as cnt
        FROM stock_concept
        WHERE source = 'sina'
        GROUP BY board_code
    """)
    rows = cur.fetchall()
    updated = 0
    for board_code, cnt in rows:
        cur.execute(
            "UPDATE concept_boards SET stock_count=? WHERE board_code=? AND source='sina'",
            (cnt, board_code),
        )
        updated += cur.rowcount

    # THS
    cur.execute("""
        SELECT board_code, COUNT(DISTINCT stock_code) as cnt
        FROM stock_concept
        WHERE source = 'ths'
        GROUP BY board_code
    """)
    for board_code, cnt in cur.fetchall():
        cur.execute(
            "UPDATE concept_boards SET stock_count=? WHERE board_code=? AND source='ths'",
            (cnt, board_code),
        )
        updated += cur.rowcount

    conn.commit()
    conn.close()
    return updated


def sync_industry_counts():
    """重算 industry_boards.stock_count"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT board_code, COUNT(DISTINCT stock_code) as cnt
        FROM stock_industry_board
        GROUP BY board_code
    """)
    updated = 0
    for board_code, cnt in cur.fetchall():
        cur.execute(
            "UPDATE industry_boards SET stock_count=? WHERE board_code=?",
            (cnt, board_code),
        )
        updated += cur.rowcount

    conn.commit()
    conn.close()
    return updated


if __name__ == "__main__":
    print("=== 重算概念板块 stock_count ===")
    n = sync_concept_counts()
    print(f"  更新 {n} 个概念板块")

    print("\n=== 重算行业板块 stock_count ===")
    n = sync_industry_counts()
    print(f"  更新 {n} 个行业板块")

    # 验证
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE stock_count > 0")
    print(f"\n  概念板块有 stock_count: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM industry_boards WHERE stock_count > 0")
    print(f"  行业板块有 stock_count: {cur.fetchone()[0]}")
    conn.close()
