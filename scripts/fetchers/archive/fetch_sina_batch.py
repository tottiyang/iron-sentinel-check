#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - Sina 源采集（概念+行业 板块列表 & 成分股）

数据源：akshare stock_sector_spot / stock_sector_detail
覆盖：
  - 概念板块列表 ~175 个
  - 行业板块列表 ~84 个
  - 概念-个股关联
  - 行业(证监会)-个股关联

🚫 禁止 INSERT OR REPLACE（会覆盖同股多板块已有数据）
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))

import akshare as ak
from db_schema import get_conn

SLEEP_SEC = 0.35
BATCH_LOG = 10


def fetch_concept_list_sina():
    """新浪概念板块列表"""
    try:
        df = ak.stock_sector_spot(indicator="概念")
        if df is None or len(df) == 0:
            return []
        results = []
        for _, row in df.iterrows():
            label = str(row.get("label", "")).strip()
            name = str(row.get("板块", "")).strip()
            count = int(row.get("公司家数", 0))
            if label and name:
                results.append((f"SINA_{label}", name, count))
        return results
    except Exception as e:
        print(f"  [SINA-concept-list] 失败: {e}")
        return []


def fetch_industry_list_sina():
    """新浪行业板块列表"""
    try:
        df = ak.stock_sector_spot(indicator="行业")
        if df is None or len(df) == 0:
            return []
        results = []
        for _, row in df.iterrows():
            label = str(row.get("label", "")).strip()
            name = str(row.get("板块", "")).strip()
            count = int(row.get("公司家数", 0))
            if label and name:
                results.append((f"SINA_{label}", name, count))
        return results
    except Exception as e:
        print(f"  [SINA-industry-list] 失败: {e}")
        return []


def fetch_sector_cons(sector_label):
    """新浪板块成分股"""
    try:
        df = ak.stock_sector_detail(sector=sector_label)
        if df is None or len(df) == 0:
            return []
        return [
            str(row.get("code", "")).zfill(6)
            for _, row in df.iterrows()
            if len(str(row.get("code", "")).zfill(6)) == 6
        ]
    except:
        return []


def save_concept_boards(boards):
    """保存概念板块列表"""
    conn = get_conn()
    cur = conn.cursor()
    added = 0
    for code, name, count in boards:
        cur.execute(
            "INSERT OR IGNORE INTO concept_boards (board_code, board_name, stock_count, source) "
            "VALUES (?, ?, ?, 'sina')",
            (code, name, count),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE concept_boards SET stock_count=? WHERE board_code=? AND source='sina'",
                (count, code),
            )
    conn.commit()
    conn.close()
    return added


def save_industry_boards(boards):
    """保存行业板块列表"""
    conn = get_conn()
    cur = conn.cursor()
    added = 0
    for code, name, count in boards:
        cur.execute(
            "INSERT OR IGNORE INTO industry_boards (board_code, board_name, stock_count, source) "
            "VALUES (?, ?, ?, 'sina')",
            (code, name, count),
        )
        if cur.rowcount > 0:
            added += 1
        else:
            cur.execute(
                "UPDATE industry_boards SET stock_count=? WHERE board_code=? AND source='sina'",
                (count, code),
            )
    conn.commit()
    conn.close()
    return added


def fetch_concept_relations(limit=0):
    """采集概念-个股关联（新浪源，断点续传）"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source='sina' ORDER BY board_code")
    all_boards = cur.fetchall()
    cur.execute("SELECT DISTINCT board_code FROM stock_concept WHERE source='sina'")
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]
    if not pending:
        print(f"  [SINA-concept] 全部完成 ({len(all_boards)} 个板块)")
        return 0
    if limit > 0:
        pending = pending[:limit]

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        raw_label = board_code.replace("SINA_", "", 1)
        try:
            stocks = fetch_sector_cons(raw_label)
            if stocks:
                conn = get_conn()
                cur = conn.cursor()
                for sc in stocks:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_concept (stock_code, board_code, source) "
                        "VALUES (?, ?, 'sina')",
                        (sc, board_code),
                    )
                    total_added += cur.rowcount
                cur.execute(
                    "UPDATE concept_boards SET stock_count=? WHERE board_code=? AND source='sina'",
                    (len(stocks), board_code),
                )
                conn.commit()
                conn.close()
            time.sleep(SLEEP_SEC)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"    Error {board_name}: {e}")
            time.sleep(1)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"  [SINA-concept] {i+1}/{len(pending)} 完成, +{total_added}, 错{errors}, 剩{remaining}")

    return len(pending)


def fetch_industry_relations(limit=0):
    """采集行业板块-个股关联（新浪源，断点续传）"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT board_code, board_name FROM industry_boards WHERE source='sina' ORDER BY board_code")
    all_boards = cur.fetchall()
    cur.execute("SELECT DISTINCT board_code FROM stock_industry_board WHERE source='sina'")
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]
    if not pending:
        print(f"  [SINA-industry] 全部完成 ({len(all_boards)} 个板块)")
        return 0
    if limit > 0:
        pending = pending[:limit]

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        raw_label = board_code.replace("SINA_", "", 1)
        try:
            stocks = fetch_sector_cons(raw_label)
            if stocks:
                conn = get_conn()
                cur = conn.cursor()
                for sc in stocks:
                    # INSERT OR IGNORE：允许多行业（一股属多个证监会行业）
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_industry_board (stock_code, board_code, source) "
                        "VALUES (?, ?, 'sina')",
                        (sc, board_code),
                    )
                    total_added += cur.rowcount
                cur.execute(
                    "UPDATE industry_boards SET stock_count=? WHERE board_code=? AND source='sina'",
                    (len(stocks), board_code),
                )
                conn.commit()
                conn.close()
            time.sleep(SLEEP_SEC)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"    Error {board_name}: {e}")
            time.sleep(1)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"  [SINA-industry] {i+1}/{len(pending)} 完成, +{total_added}, 错{errors}, 剩{remaining}")

    return len(pending)


def status_report():
    conn = get_conn()
    cur = conn.cursor()
    print("\n" + "=" * 60)
    print("  铁血哨兵 - Sina 数据报告")
    print("=" * 60)
    for t in ["stocks", "concept_boards", "industry_boards",
              "stock_concept", "stock_industry_board"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t:<25} {cur.fetchone()[0]:>8} 条")

    print("\n  概念板块来源:")
    for row in cur.execute("SELECT source, COUNT(*) FROM concept_boards GROUP BY source"):
        print(f"    {row[0]:<10} {row[1]:>6} 个")
    print("  概念关联来源:")
    for row in cur.execute("SELECT source, COUNT(*) FROM stock_concept GROUP BY source"):
        print(f"    {row[0]:<10} {row[1]:>6} 条")
    print("  行业关联来源:")
    for row in cur.execute("SELECT source, COUNT(*) FROM stock_industry_board GROUP BY source"):
        print(f"    {row[0]:<10} {row[1]:>6} 条")

    total = cur.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    covered = cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_concept WHERE source='sina'").fetchone()[0]
    print(f"\n  Sina概念覆盖: {covered}/{total} = {covered/total*100:.1f}%")
    ind_cov = cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_industry_board WHERE source='sina'").fetchone()[0]
    print(f"  Sina行业覆盖: {ind_cov}/{total} = {ind_cov/total*100:.1f}%")
    conn.close()
    print("=" * 60)


if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "all"

    if task == "all":
        print("=== Sina 板块列表 ===")
        concepts = fetch_concept_list_sina()
        added_c = save_concept_boards(concepts)
        print(f"  概念: {len(concepts)} 个, 新增 {added_c}")
        industries = fetch_industry_list_sina()
        added_i = save_industry_boards(industries)
        print(f"  行业: {len(industries)} 个, 新增 {added_i}")

        print("\n=== Sina 概念关联 ===")
        while True:
            r = fetch_concept_relations(limit=50)
            if r == 0: break
            time.sleep(2)

        print("\n=== Sina 行业关联 ===")
        while True:
            r = fetch_industry_relations(limit=50)
            if r == 0: break
            time.sleep(2)

        status_report()

    elif task == "list":
        concepts = fetch_concept_list_sina()
        added_c = save_concept_boards(concepts)
        print(f"  概念: {len(concepts)} 个, 新增 {added_c}")
        industries = fetch_industry_list_sina()
        added_i = save_industry_boards(industries)
        print(f"  行业: {len(industries)} 个, 新增 {added_i}")

    elif task == "concept":
        while True:
            r = fetch_concept_relations(limit=50)
            if r == 0: break
            time.sleep(2)
        status_report()

    elif task == "industry":
        while True:
            r = fetch_industry_relations(limit=50)
            if r == 0: break
            time.sleep(2)
        status_report()

    elif task == "status":
        status_report()
    else:
        print("用法: fetch_sina_batch.py [all|list|concept|industry|status]")
