#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - Sina/THS 板块列表采集（Sina已有成分股，THS仅采集板块列表）

EM CDN 不通，仅保留历史导入的板块列表，不重复尝试 akshare EM 接口。

🚫 禁止 INSERT OR REPLACE（会覆盖同股多板块已有数据）
"""

import sys, os, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

import akshare as ak
from db_schema import get_conn

SLEEP_SEC = 0.35


def fetch_sina_concept_list():
    """新浪概念板块列表"""
    try:
        df = ak.stock_sector_spot(indicator="概念")
        results = []
        for _, row in df.iterrows():
            label = str(row.get("label", "")).strip()
            name = str(row.get("板块", "")).strip()
            count = int(row.get("公司家数", 0))
            if label and name:
                results.append((f"SINA_{label}", name, count, "sina"))
        print(f"  [Sina] 概念: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [Sina] 概念列表失败: {e}")
        return []


def fetch_sina_industry_list():
    """新浪行业板块列表"""
    try:
        df = ak.stock_sector_spot(indicator="行业")
        results = []
        for _, row in df.iterrows():
            label = str(row.get("label", "")).strip()
            name = str(row.get("板块", "")).strip()
            count = int(row.get("公司家数", 0))
            if label and name:
                results.append((f"SINA_{label}", name, count, "sina"))
        print(f"  [Sina] 行业: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [Sina] 行业列表失败: {e}")
        return []


def fetch_ths_concept_list():
    """同花顺概念板块列表"""
    try:
        df = ak.stock_board_concept_name_ths()
        results = []
        for _, row in df.iterrows():
            name = str(row.get("name", "")).strip()
            code = str(row.get("code", "")).strip()
            if name and code:
                results.append((f"THS_{code}", name, 0, "ths"))
        print(f"  [THS] 概念: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [THS] 概念列表失败: {e}")
        return []


def fetch_ths_industry_list():
    """同花顺行业板块列表"""
    try:
        df = ak.stock_board_industry_name_ths()
        results = []
        for _, row in df.iterrows():
            name = str(row.get("name", "")).strip()
            code = str(row.get("code", "")).strip()
            if name and code:
                results.append((f"THS_{code}", name, 0, "ths"))
        print(f"  [THS] 行业: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [THS] 行业列表失败: {e}")
        return []


def save_boards(table, boards):
    """
    保存板块列表到 DB。
    INSERT OR IGNORE：已存在则跳过（保留已有数据，source 不覆盖）
    """
    conn = get_conn()
    cur = conn.cursor()
    added = updated = 0
    for code, name, count, source in boards:
        cur.execute(
            f"INSERT OR IGNORE INTO {table} (board_code, board_name, stock_count, source) "
            "VALUES (?, ?, ?, ?)",
            (code, name, count, source),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                f"UPDATE {table} SET stock_count=? WHERE board_code=? AND source=?",
                (count, code, source),
            )
            updated += 1
    conn.commit()
    conn.close()
    return added, updated


def status_report():
    conn = get_conn()
    cur = conn.cursor()
    print("\n" + "=" * 60)
    print("  铁血哨兵 - 板块列表报告")
    print("=" * 60)
    for t in ["stocks", "concept_boards", "industry_boards",
              "stock_concept", "stock_industry_board"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:<28} {cur.fetchone()[0]:>8} 条")
        except:
            print(f"  {t:<28} (不存在)")

    print("\n  概念板块来源:")
    for row in cur.execute("SELECT source, COUNT(*) FROM concept_boards GROUP BY source"):
        print(f"    {row[0]:<10} {row[1]:>6} 个")
    print("  行业板块来源:")
    for row in cur.execute("SELECT source, COUNT(*) FROM industry_boards GROUP BY source"):
        print(f"    {row[0]:<10} {row[1]:>6} 个")
    conn.close()
    print("=" * 60)


if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "all"

    if task in ("all", "list"):
        print("=== 采集板块列表 ===")
        print("\n  [1] Sina 概念...")
        sina_c = fetch_sina_concept_list()
        added, updated = save_boards("concept_boards", sina_c)
        print(f"      新增 {added}, 更新 {updated}")

        print("\n  [2] Sina 行业...")
        sina_i = fetch_sina_industry_list()
        added, updated = save_boards("industry_boards", sina_i)
        print(f"      新增 {added}, 更新 {updated}")

        print("\n  [3] THS 概念...")
        ths_c = fetch_ths_concept_list()
        added, updated = save_boards("concept_boards", ths_c)
        print(f"      新增 {added}, 更新 {updated}")

        print("\n  [4] THS 行业...")
        ths_i = fetch_ths_industry_list()
        added, updated = save_boards("industry_boards", ths_i)
        print(f"      新增 {added}, 更新 {updated}")

        # 记录meta
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO meta (key, value, updated_at) VALUES (?, ?, ?)",
            ("boards_list_last_run", datetime.now().isoformat(), datetime.now().isoformat()),
        )
        conn.commit()
        conn.close()

    if task == "all":
        status_report()
    elif task == "status":
        status_report()
    else:
        print("用法: fetch_boards_complete.py [all|list|status]")
