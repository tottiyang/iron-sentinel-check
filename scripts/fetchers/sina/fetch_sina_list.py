#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sina 板块列表采集

采集范围：
  1. 概念板块（indicator='概念'）→ concept_boards, source='sina'
  2. 行业板块（indicator='行业'）→ industry_boards, source='sina'

使用 stock_sector_spot 接口，参数格式：
  - label 列： gn_xxx（概念）/ hangye_xxx（行业）
  - board_code 直接使用 label，source 列区分来源

Usage:
  python3 fetch_sina_list.py
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn


def fetch_concept_boards():
    """采集新浪概念板块列表"""
    df = ak.stock_sector_spot(indicator="概念")
    boards = []
    for _, row in df.iterrows():
        label = str(row.get("label", "")).strip()
        name = str(row.get("板块", "")).strip()
        count = int(row.get("公司家数", 0))
        if label and name:
            boards.append((label, name, count))
    return boards


def fetch_industry_boards():
    """采集新浪行业板块列表"""
    df = ak.stock_sector_spot(indicator="行业")
    boards = []
    for _, row in df.iterrows():
        label = str(row.get("label", "")).strip()
        name = str(row.get("板块", "")).strip()
        count = int(row.get("公司家数", 0))
        if label and name:
            # 新浪行业板块 label 已经是 hangye_XXX 格式
            # 统一加上 SINA_ 前缀作为 board_code
            board_code = f"SINA_{label}"
            boards.append((board_code, name, count))
    return boards


def save_concept_boards(boards):
    """写入 concept_boards（source='sina'）"""
    conn = get_conn()
    cur = conn.cursor()
    added = updated = 0
    for code, name, count in boards:
        cur.execute(
            "INSERT OR IGNORE INTO concept_boards (board_code, board_name, source, stock_count) "
            "VALUES (?, ?, 'sina', ?)",
            (code, name, count),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE concept_boards SET board_name=?, stock_count=? "
                "WHERE board_code=? AND source='sina'",
                (name, count, code),
            )
            updated += 1
    conn.commit()
    conn.close()
    return added, updated


def save_industry_boards(boards):
    """写入 industry_boards（source='sina'）"""
    conn = get_conn()
    cur = conn.cursor()
    added = updated = 0
    for board_code, name, count in boards:
        cur.execute(
            "INSERT OR IGNORE INTO industry_boards (board_code, board_name, source, stock_count) "
            "VALUES (?, ?, 'sina', ?)",
            (board_code, name, count),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE industry_boards SET board_name=?, stock_count=? "
                "WHERE board_code=? AND source='sina'",
                (name, count, board_code),
            )
            updated += 1
    conn.commit()
    conn.close()
    return added, updated


def main():
    print("=== Sina 板块列表采集 ===")

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
    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='sina'")
    print(f"\n  concept_boards(sina): {cur.fetchone()[0]} 个")
    cur.execute("SELECT COUNT(*) FROM industry_boards WHERE source='sina'")
    print(f"  industry_boards(sina): {cur.fetchone()[0]} 个")
    conn.close()


if __name__ == "__main__":
    main()