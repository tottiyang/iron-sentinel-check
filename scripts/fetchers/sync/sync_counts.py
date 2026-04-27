#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量重算 stock_count（解决历史积累的计数不准问题）

问题背景：
  - concept_boards.stock_count：申万采集时批量写入，没有按 source 更新
  - industry_boards.stock_count：同样有问题
  - 旧值可能是 0 或 NULL，或来自错误的数据源

解决方案：
  从 stock_concept / stock_industry_board 实时 COUNT，重算每个板块的 stock_count

Usage:
  python3 sync_counts.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from fetchers.db.db_schema import get_conn


def recalc_concept_boards():
    """重算 concept_boards.stock_count（按 source）"""
    conn = get_conn()
    cur = conn.cursor()

    # 重算 sina
    cur.execute("""
        UPDATE concept_boards
        SET stock_count = (
            SELECT COUNT(DISTINCT stock_code)
            FROM stock_concept
            WHERE stock_concept.board_code = concept_boards.board_code
              AND stock_concept.source = concept_boards.source
        )
        WHERE source = 'sina'
    """)
    n_sina = cur.rowcount

    # 重算 ths
    cur.execute("""
        UPDATE concept_boards
        SET stock_count = (
            SELECT COUNT(DISTINCT stock_code)
            FROM stock_concept
            WHERE stock_concept.board_code = concept_boards.board_code
              AND stock_concept.source = concept_boards.source
        )
        WHERE source = 'ths'
    """)
    n_ths = cur.rowcount

    # 重算 em
    cur.execute("""
        UPDATE concept_boards
        SET stock_count = (
            SELECT COUNT(DISTINCT stock_code)
            FROM stock_concept
            WHERE stock_concept.board_code = concept_boards.board_code
              AND stock_concept.source = concept_boards.source
        )
        WHERE source = 'em'
    """)
    n_em = cur.rowcount

    conn.commit()
    conn.close()
    return n_sina, n_ths, n_em


def recalc_industry_boards():
    """重算 industry_boards.stock_count（按 source）"""
    conn = get_conn()
    cur = conn.cursor()

    for src in ['sina', 'ths', 'em']:
        cur.execute(f"""
            UPDATE industry_boards
            SET stock_count = (
                SELECT COUNT(DISTINCT stock_code)
                FROM stock_industry_board
                WHERE stock_industry_board.board_code = industry_boards.board_code
                  AND stock_industry_board.source = industry_boards.source
            )
            WHERE source = ?
        """, (src,))
        print(f"  {src}: 更新 {cur.rowcount} 个行业板块")

    conn.commit()
    conn.close()


def main():
    print("=== 批量重算 stock_count ===\n")

    print("[1/2] 重算 concept_boards.stock_count...")
    n_sina, n_ths, n_em = recalc_concept_boards()
    print(f"  sina: {n_sina} 个")
    print(f"  ths:  {n_ths} 个")
    print(f"  em:   {n_em} 个")

    print("\n[2/2] 重算 industry_boards.stock_count...")
    recalc_industry_boards()

    print("\n验证结果:")
    conn = get_conn()
    cur = conn.cursor()

    print("\n  concept_boards（按 source）:")
    for src in ['em', 'sina', 'ths']:
        cur.execute(
            "SELECT COUNT(*), SUM(stock_count) FROM concept_boards WHERE source=?",
            (src,),
        )
        cnt, total = cur.fetchone()
        print(f"    {src}: {cnt} 板块, 合计 {total or 0} 股关联")

    print("\n  industry_boards（按 source）:")
    for src in ['em', 'sina', 'ths']:
        cur.execute(
            "SELECT COUNT(*), SUM(stock_count) FROM industry_boards WHERE source=?",
            (src,),
        )
        cnt, total = cur.fetchone()
        print(f"    {src}: {cnt} 板块, 合计 {total or 0} 股关联")

    conn.close()


if __name__ == "__main__":
    main()
