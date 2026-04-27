#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sina 成分股关联采集

采集范围：
  1. 概念关联：stock_concept（source='sina'）
     - 每个板块用 stock_sector_detail(sector='gn_xxx') 采集
     - 参数是 label（gn_xxx），不是板块名称
  2. 行业关联：stock_industry_board（source='sina'）
     - 每个行业板块用 stock_sector_detail(sector='hangye_xxx') 采集
     - 注意：一只股票可属于多个行业，不能用 INSERT OR REPLACE！

断点续传：记录已完成的板块，完成后跳过。

Usage:
  python3 fetch_sina_concepts.py [concept|industry|all]
  python3 fetch_sina_concepts.py --dry-run  # 只看计划，不写DB
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn

SLEEP_SEC = 0.35
BATCH_LOG = 20


def fetch_sector_stocks(label):
    """
    采集单个板块的成分股代码列表
    label: 板块 label，如 'gn_hwqc'（概念）或 'hangye_ZA01'（行业）
    返回: ['000001', '000002', ...]
    """
    try:
        df = ak.stock_sector_detail(sector=label)
        if df is None or len(df) == 0:
            return []
        stocks = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).zfill(6)
            if code and len(code) == 6:
                stocks.append(code)
        return stocks
    except Exception as e:
        print(f"    ERROR fetch_sector_detail({label}): {e}")
        return []


def fetch_concept_relations(limit=0, dry_run=False):
    """采集概念-个股关联（新浪源，断点续传）"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT board_code, board_name FROM concept_boards WHERE source='sina' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    # 断点续传：已完成的板块
    cur.execute(
        "SELECT DISTINCT board_code FROM stock_concept WHERE source='sina'"
    )
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]

    if not pending:
        print(f"[SINA-concept] 全部 {len(all_boards)} 个板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"[SINA-concept] 需采集 {len(pending)} 个板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        label = board_code.replace("SINA_", "", 1)
        try:
            stocks = fetch_sector_stocks(label)
            if stocks:
                if not dry_run:
                    conn = get_conn()
                    cur = conn.cursor()
                    for sc in stocks:
                        cur.execute(
                            "INSERT OR IGNORE INTO stock_concept "
                            "(stock_code, board_code, source, fetched_at) "
                            "VALUES (?, ?, 'sina', datetime('now'))",
                            (sc, board_code),
                        )
                        if cur.rowcount > 0:
                            total_added += 1
                    conn.commit()
                    conn.close()
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"    ERROR {board_name}: {e}")

        time.sleep(SLEEP_SEC)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(
                f"  [{i+1}/{len(pending)}] +{total_added} 条, 剩余 {remaining} 板块"
            )

    return len(pending)


def fetch_industry_relations(limit=0, dry_run=False):
    """
    采集行业板块-个股关联（新浪源，断点续传）

    stock_industry_board PK = (stock_code, board_code, source)
    允许多行业：一股多板，各自 INSERT OR IGNORE，不会覆盖
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT board_code FROM industry_boards WHERE source='sina' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    # 断点续传
    cur.execute(
        "SELECT DISTINCT board_code FROM stock_industry_board WHERE source='sina'"
    )
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [code for code, in all_boards if code not in done]

    if not pending:
        print(f"[SINA-industry] 全部 {len(all_boards)} 个行业板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"[SINA-industry] 需采集 {len(pending)} 个行业板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, board_code in enumerate(pending):
        label = board_code.replace("SINA_", "", 1)
        try:
            stocks = fetch_sector_stocks(label)
            if stocks:
                if not dry_run:
                    conn = get_conn()
                    cur = conn.cursor()
                    for sc in stocks:
                        # INSERT OR IGNORE：允许多行业，不覆盖
                        cur.execute(
                            "INSERT OR IGNORE INTO stock_industry_board "
                            "(stock_code, board_code, source, fetched_at) "
                            "VALUES (?, ?, 'sina', datetime('now'))",
                            (sc, board_code),
                        )
                        if cur.rowcount > 0:
                            total_added += 1
                    conn.commit()
                    conn.close()
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"    ERROR {board_code}: {e}")

        time.sleep(SLEEP_SEC)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(
                f"  [{i+1}/{len(pending)}] +{total_added} 条, 剩余 {remaining} 板块"
            )

    return len(pending)


def status():
    conn = get_conn()
    cur = conn.cursor()

    print("\n" + "=" * 50)
    print("Sina 数据状态")
    print("=" * 50)

    # 板块列表
    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='sina'")
    cb = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM industry_boards WHERE source='sina'")
    ib = cur.fetchone()[0]
    print(f"  concept_boards(sina):  {cb} 个")
    print(f"  industry_boards(sina): {ib} 个")

    # 关联数
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='sina'")
    sc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_concept WHERE source='sina'")
    sc_boards = cur.fetchone()[0]
    print(f"\n  stock_concept(sina):    {sc} 条, 覆盖 {sc_boards}/{cb} 个板块")

    cur.execute("SELECT COUNT(*) FROM stock_industry_board WHERE source='sina'")
    sib = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_industry_board WHERE source='sina'")
    sib_boards = cur.fetchone()[0]
    print(f"  stock_industry_board(sina): {sib} 条, 覆盖 {sib_boards}/{ib} 个板块")

    # 多行业检查（一股多板 = 正常）
    cur.execute(
        "SELECT COUNT(DISTINCT stock_code) FROM stock_industry_board WHERE source='sina'"
    )
    sib_stocks = cur.fetchone()[0]
    if sib_stocks > 0:
        ratio = sib / sib_stocks
        print(f"  平均每只股票行业板块数: {ratio:.2f}（>1 = 多行业 ✓）")

    conn.close()
    print("=" * 50)


if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "all"
    dry = "--dry-run" in sys.argv

    if task == "concept":
        fetch_concept_relations(dry_run=dry)
    elif task == "industry":
        fetch_industry_relations(dry_run=dry)
    elif task == "all":
        fetch_concept_relations(dry_run=dry)
        fetch_industry_relations(dry_run=dry)
    elif task == "status":
        status()
    else:
        print("用法: fetch_sina_concepts.py [concept|industry|all|status] [--dry-run]")
