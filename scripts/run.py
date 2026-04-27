#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 v3.0 — 采集主脚本

执行顺序（Step 1~9）：
  1. 板块列表（Sina + THS + EM）
  2. 个股 + 申万行业
  3. 申万关联（L1/L2/L3）
  4. Sina 成分股关联
  5. THS 成分股关联
  6. EM 成分股关联（CDN 不通则跳过）
  7. 重算 stock_count
  8. 状态报告
  9. DB Schema 更新

Usage:
  python3 run.py                    # 执行全部步骤
  python3 run.py --dry-run         # 只看计划，不写 DB
  python3 run.py --step 4           # 从第 4 步开始
  python3 run.py --boards-only      # 只采集板块列表
  python3 run.py --relations-only   # 只采集关联数据
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))

from fetchers.db.db_schema import get_conn, init_db


def step1_boards():
    """Step 1: 采集板块列表（Sina + THS + EM）"""
    print("\n" + "=" * 50)
    print("Step 1/9: 板块列表采集")
    print("=" * 50)

    from fetchers.sina.fetch_sina_list import main as sina_list
    from fetchers.ths.fetch_ths_list import main as ths_list
    from fetchers.em.fetch_em_list import main as em_list

    sina_list()
    print()
    ths_list()
    print()
    em_list()


def step2_stocks():
    """Step 2: 采集个股 + 申万行业"""
    print("\n" + "=" * 50)
    print("Step 2/9: 个股 + 申万行业")
    print("=" * 50)

    from fetchers.akshare.fetch_stocks import fetch_all_stocks, fetch_sw_industry

    fetch_all_stocks()
    fetch_sw_industry()


def step3_sw_relations():
    """Step 3: 申万关联（L1/L2/L3）"""
    print("\n" + "=" * 50)
    print("Step 3/9: 申万关联")
    print("=" * 50)

    from fetchers.akshare.fetch_stocks import fetch_sw_relations, fetch_sw_industry

    _, _, l3_map, l3_to_l2 = fetch_sw_industry()

    # 需要 l2→l1 映射
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT code, l1_code FROM industry_l2")
    l2_to_l1 = dict(cur.fetchall())
    conn.close()

    # l1_map 需要重新获取
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name, code FROM industry_l1")
    l1_map = dict(cur.fetchall())
    # 反查 l2_map
    cur.execute("SELECT name, code FROM industry_l2")
    l2_map = dict(cur.fetchall())
    conn.close()

    fetch_sw_relations(l3_map, l3_to_l2, l2_map, l1_map)


def step4_sina_relations():
    """Step 4: Sina 成分股关联"""
    print("\n" + "=" * 50)
    print("Step 4/9: Sina 成分股关联")
    print("=" * 50)

    from fetchers.sina.fetch_sina_concepts import fetch_concept_relations, fetch_industry_relations

    n = fetch_concept_relations()
    if n > 0:
        print("  继续采集行业关联...")
        fetch_industry_relations()


def step5_ths_relations():
    """Step 5: THS 成分股关联"""
    print("\n" + "=" * 50)
    print("Step 5/9: THS 成分股关联")
    print("=" * 50)

    from fetchers.ths.fetch_ths_concepts import fetch_concept_relations, fetch_industry_relations

    n = fetch_concept_relations()
    if n > 0:
        print("  继续采集行业关联...")
        fetch_industry_relations()


def step6_em_relations():
    """Step 6: EM 成分股关联（CDN 不通则跳过）"""
    print("\n" + "=" * 50)
    print("Step 6/9: EM 成分股关联")
    print("=" * 50)

    from fetchers.em.fetch_em_concepts import fetch_concept_relations

    # CDN 不通会在脚本内部检测并跳过
    n = fetch_concept_relations()
    print(f"  EM 概念关联采集完成（{n} 板块处理）")


def step7_sync_counts():
    """Step 7: 重算 stock_count"""
    print("\n" + "=" * 50)
    print("Step 7/9: 重算 stock_count")
    print("=" * 50)

    from fetchers.sync.sync_counts import main as sync_counts
    sync_counts()


def step8_report():
    """Step 8: 状态报告"""
    print("\n" + "=" * 50)
    print("Step 8/9: 状态报告")
    print("=" * 50)

    from fetchers.status.report import report
    report()


def step9_schema():
    """Step 9: DB Schema 更新（确保新表结构正确）"""
    print("\n" + "=" * 50)
    print("Step 9/9: DB Schema 检查")
    print("=" * 50)

    init_db()
    conn = get_conn()
    cur = conn.cursor()

    # 验证关键表的 PK
    tables_pk = {
        "stock_concept": "PRIMARY KEY (stock_code, board_code, source)",
        "stock_industry_board": "PRIMARY KEY (stock_code, board_code, source)",
        "concept_boards": "PRIMARY KEY (board_code)",
        "industry_boards": "PRIMARY KEY (board_code)",
    }

    for t, expected_pk in tables_pk.items():
        cur.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,))
        row = cur.fetchone()
        if row and row[0]:
            has_pk = "PRIMARY KEY" in (row[0] or "")
            print(f"  {'✓' if has_pk else '✗'} {t}: {'有 PK' if has_pk else '无 PK！'}")
        else:
            print(f"  ? {t}: 表不存在")

    conn.close()


def main():
    import socket
    dry_run = "--dry-run" in sys.argv
    boards_only = "--boards-only" in sys.argv
    relations_only = "--relations-only" in sys.argv

    step_start = 1
    for arg in sys.argv:
        if arg.startswith("--step="):
            step_start = int(arg.split("=")[1])

    print("=" * 50)
    print("  铁血哨兵 v3.0 采集开始")
    print("=" * 50)
    print(f"  dry_run: {dry_run}")
    print(f"  从 Step {step_start} 开始")

    if dry_run:
        print("\n[DRY RUN] 不执行任何实际操作")
        return

    start = time.time()

    steps = [
        (1, "板块列表", step1_boards),
        (2, "个股+申万", step2_stocks),
        (3, "申万关联", step3_sw_relations),
        (4, "Sina关联", step4_sina_relations),
        (5, "THS关联", step5_ths_relations),
        (6, "EM关联", step6_em_relations),
        (7, "重算stock_count", step7_sync_counts),
        (8, "状态报告", step8_report),
        (9, "Schema检查", step9_schema),
    ]

    for num, label, fn in steps:
        if num < step_start:
            continue
        if boards_only and num > 1:
            break
        if relations_only and num < 3:
            continue
        try:
            fn()
        except Exception as e:
            print(f"\n❌ Step {num} 失败: {e}")
            import traceback
            traceback.print_exc()
            break

    elapsed = time.time() - start
    print(f"\n总耗时: {elapsed:.0f} 秒")


if __name__ == "__main__":
    main()
