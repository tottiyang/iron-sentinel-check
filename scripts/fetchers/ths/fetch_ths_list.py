#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
THS（同花顺）板块列表采集

采集范围：
  1. 概念板块 → concept_boards, source='ths'
     接口: akshare stock_board_concept_name_ths()
     返回约 375 个概念板块（DB 现有 398 个，新采集会补充）
  2. 行业板块 → industry_boards, source='ths'
     接口: akshare stock_board_industry_name_ths()
     返回约 90 个行业板块

board_code 格式：THS_XXXXXX（6位数字，零填充）

⚠️ 成分股采集（fetch_ths_concepts.py）：
  akshare stock_board_concept_cons_ths() 只能返回前 100 条（截断）
  xbrowser 方式 q.10jqka.com.cn 需要登录 cookie（401）
  → 需要单独修复，暂不使用 INSERT OR REPLACE

Usage:
  python3 fetch_ths_list.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn


def fetch_concept_boards():
    """采集 THS 概念板块列表"""
    df = ak.stock_board_concept_name_ths()
    boards = []
    for _, row in df.iterrows():
        code_raw = str(row.get("板块代码", "")).strip()
        name = str(row.get("板块名称", "")).strip()
        if not code_raw or not name:
            continue
        # 统一 6 位数字格式
        code = f"THS_{int(code_raw):06d}"
        boards.append((code, name))
    return boards


def fetch_industry_boards():
    """采集 THS 行业板块列表"""
    df = ak.stock_board_industry_name_ths()
    boards = []
    for _, row in df.iterrows():
        code_raw = str(row.get("板块代码", "")).strip()
        name = str(row.get("板块名称", "")).strip()
        if not code_raw or not name:
            continue
        code = f"THS_{int(code_raw):06d}"
        boards.append((code, name))
    return boards


def save_concept_boards(boards):
    """写入 concept_boards（source='ths'）"""
    conn = get_conn()
    cur = conn.cursor()
    added = updated = 0
    for code, name in boards:
        cur.execute(
            "INSERT OR IGNORE INTO concept_boards (board_code, board_name, source) "
            "VALUES (?, ?, 'ths')",
            (code, name),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE concept_boards SET board_name=? "
                "WHERE board_code=? AND source='ths'",
                (name, code),
            )
            updated += 1
    conn.commit()
    conn.close()
    return added, updated


def save_industry_boards(boards):
    """写入 industry_boards（source='ths'）"""
    conn = get_conn()
    cur = conn.cursor()
    added = updated = 0
    for code, name in boards:
        cur.execute(
            "INSERT OR IGNORE INTO industry_boards (board_code, board_name, source) "
            "VALUES (?, ?, 'ths')",
            (code, name),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE industry_boards SET board_name=? "
                "WHERE board_code=? AND source='ths'",
                (name, code),
            )
            updated += 1
    conn.commit()
    conn.close()
    return added, updated


def main():
    print("=== THS 板块列表采集 ===")

    print("\n[1/2] 采集概念板块...")
    concepts = fetch_concept_boards()
    print(f"  获取 {len(concepts)} 个概念板块")
    added, updated = save_concept_boards(concepts)
    print(f"  新增 {added}, 更新 {updated}")

    print("\n[2/2] 采集行业板块...")
    industries = fetch_industry_boards()
    print(f"  获取 {len(industries)} 个行业板块")
    added, updated = save_industry_boards(industries)
    print(f"  新增 {added}, 更新 {updated}")

    # 统计
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='ths'")
    print(f"\n  concept_boards(ths):  {cur.fetchone()[0]} 个")
    cur.execute("SELECT COUNT(*) FROM industry_boards WHERE source='ths'")
    print(f"  industry_boards(ths): {cur.fetchone()[0]} 个")
    conn.close()


if __name__ == "__main__":
    main()
