#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EM（东方财富）板块列表采集

采集范围：
  1. 概念板块 → concept_boards, source='em'
     接口: akshare stock_board_concept_name_em()
  2. 行业板块 → industry_boards, source='em'
     接口: akshare stock_board_industry_name_em()

⚠️ CDN 不通警告：
  push2.eastmoney.com 持续 RemoteDisconnected（历史问题，2026-04-22 起）
  akshare EM 接口目前大部分失败
  → 如果网络不通，脚本只报告状态，不写脏数据

已知可用替代：
  Sina（概念 175 + 行业 84）：稳定可用
  THS（概念 ~375 + 行业 90）：稳定可用，成分股每板块限 100 条

Usage:
  python3 fetch_em_list.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn


def fetch_concept_boards():
    """
    采集 EM 概念板块列表
    board_code 格式: BKxxxx（如 BK0493）
    """
    try:
        df = ak.stock_board_concept_name_em()
        if df is None or len(df) == 0:
            return [], "empty"
        boards = []
        for _, row in df.iterrows():
            code = str(row.get("板块代码", "")).strip()
            name = str(row.get("板块名称", "")).strip()
            if code and name:
                boards.append((code, name))
        return boards, None
    except Exception as e:
        return [], str(e)


def fetch_industry_boards():
    """采集 EM 行业板块列表"""
    try:
        df = ak.stock_board_industry_name_em()
        if df is None or len(df) == 0:
            return [], "empty"
        boards = []
        for _, row in df.iterrows():
            code = str(row.get("板块代码", "")).strip()
            name = str(row.get("板块名称", "")).strip()
            if code and name:
                boards.append((code, name))
        return boards, None
    except Exception as e:
        return [], str(e)


def save_concept_boards(boards):
    """写入 concept_boards（source='em'）"""
    conn = get_conn()
    cur = conn.cursor()
    added = updated = 0
    for code, name in boards:
        cur.execute(
            "INSERT OR IGNORE INTO concept_boards (board_code, board_name, source) "
            "VALUES (?, ?, 'em')",
            (code, name),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE concept_boards SET board_name=? "
                "WHERE board_code=? AND source='em'",
                (name, code),
            )
            updated += 1
    conn.commit()
    conn.close()
    return added, updated


def save_industry_boards(boards):
    """写入 industry_boards（source='em'）"""
    conn = get_conn()
    cur = conn.cursor()
    added = updated = 0
    for code, name in boards:
        cur.execute(
            "INSERT OR IGNORE INTO industry_boards (board_code, board_name, source) "
            "VALUES (?, ?, 'em')",
            (code, name),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE industry_boards SET board_name=? "
                "WHERE board_code=? AND source='em'",
                (name, code),
            )
            updated += 1
    conn.commit()
    conn.close()
    return added, updated


def check_cdn_status():
    """检查 EM CDN 是否可达"""
    import socket
    try:
        socket.create_connection(("push2.eastmoney.com", 80), timeout=3)
        return True
    except OSError:
        return False


def main():
    print("=== EM 板块列表采集 ===")
    print(f"  CDN 可达: {check_cdn_status()}")

    print("\n[1/2] 采集概念板块...")
    concepts, err = fetch_concept_boards()
    if err:
        print(f"  ❌ 失败: {err}")
        # CDN 不通时，检查 DB 已有数据
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='em'")
        existing = cur.fetchone()[0]
        conn.close()
        print(f"  DB 已有 EM 概念板块: {existing} 个（CDN 不通，暂时无法更新）")
    else:
        print(f"  获取 {len(concepts)} 个概念板块")
        added, updated = save_concept_boards(concepts)
        print(f"  新增 {added}, 更新 {updated}")

    print("\n[2/2] 采集行业板块...")
    industries, err = fetch_industry_boards()
    if err:
        print(f"  ❌ 失败: {err}")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM industry_boards WHERE source='em'")
        existing = cur.fetchone()[0]
        conn.close()
        print(f"  DB 已有 EM 行业板块: {existing} 个（CDN 不通，暂时无法更新）")
    else:
        print(f"  获取 {len(industries)} 个行业板块")
        added, updated = save_industry_boards(industries)
        print(f"  新增 {added}, 更新 {updated}")

    # 统计
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='em'")
    print(f"\n  concept_boards(em):  {cur.fetchone()[0]} 个")
    cur.execute("SELECT COUNT(*) FROM industry_boards WHERE source='em'")
    print(f"  industry_boards(em): {cur.fetchone()[0]} 个")
    conn.close()


if __name__ == "__main__":
    main()
