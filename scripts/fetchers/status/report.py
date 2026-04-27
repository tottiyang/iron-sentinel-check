#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 全局状态报告

输出所有表的记录数、stock_count 一致性检查、多行业覆盖分析

Usage:
  python3 report.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from fetchers.db.db_schema import get_conn


def report():
    conn = get_conn()
    cur = conn.cursor()

    print("=" * 60)
    print("  铁血哨兵 v3.0 — 数据状态报告")
    print("=" * 60)

    # ── 基础表 ────────────────────────────────────────────
    print("\n【基础表】")
    for t, label in [
        ("stocks",        "个股"),
        ("industry_l1",   "申万 L1"),
        ("industry_l2",   "申万 L2"),
        ("industry_l3",   "申万 L3"),
        ("concept_boards","概念板块列表"),
        ("industry_boards","证监会行业板块列表"),
        ("meta",          "元数据"),
    ]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        n = cur.fetchone()[0]
        print(f"  {t:<22} {n:>6}  {label}")

    # ── 概念板块列表（按 source）─────────────────────────
    print("\n【concept_boards 按 source】")
    for src in ["em", "sina", "ths"]:
        cur.execute(
            "SELECT COUNT(*), SUM(stock_count) FROM concept_boards WHERE source=?",
            (src,),
        )
        cnt, total = cur.fetchone()
        print(f"  {src:<6} {cnt:>4} 板块, stock_count 合计 {total or 0:>6}")

    # ── 行业板块列表（按 source）─────────────────────────
    print("\n【industry_boards 按 source】")
    for src in ["em", "sina", "ths"]:
        cur.execute(
            "SELECT COUNT(*), SUM(stock_count) FROM industry_boards WHERE source=?",
            (src,),
        )
        cnt, total = cur.fetchone()
        print(f"  {src:<6} {cnt:>4} 板块, stock_count 合计 {total or 0:>6}")

    # ── 关联表（按 source）───────────────────────────────
    print("\n【stock_concept 按 source】")
    total_sc = 0
    for src in ["em", "sina", "ths"]:
        cur.execute(
            "SELECT COUNT(*) FROM stock_concept WHERE source=?", (src,)
        )
        n = cur.fetchone()[0]
        total_sc += n
        print(f"  {src:<6} {n:>6} 条")

    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_concept")
    sc_stocks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stocks")
    total_stocks = cur.fetchone()[0]
    print(f"  合计:  {total_sc:>6} 条, 覆盖个股 {sc_stocks}/{total_stocks} ({sc_stocks/total_stocks*100:.1f}%)")

    # ── stock_industry_board（多行业检查）─────────────────
    print("\n【stock_industry_board 按 source】")
    total_sib = 0
    for src in ["em", "sina", "ths"]:
        cur.execute(
            "SELECT COUNT(*) FROM stock_industry_board WHERE source=?", (src,)
        )
        n = cur.fetchone()[0]
        total_sib += n
        print(f"  {src:<6} {n:>6} 条")

    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_industry_board WHERE source='sina'")
    sib_stocks_sina = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stock_industry_board WHERE source='sina'")
    sib_sina = cur.fetchone()[0]
    if sib_stocks_sina > 0:
        ratio = sib_sina / sib_stocks_sina
        status = "✓ 多行业" if ratio > 1.1 else "✗ 一股一行业（INSERT OR REPLACE 遗留）"
        print(f"  Sina 平均每只股票: {ratio:.2f} 个行业 {status}")

    # ── stock_industry（申万）────────────────────────────
    print("\n【stock_industry（申万）】")
    cur.execute("SELECT COUNT(*) FROM stock_industry")
    si_total = cur.fetchone()[0]
    cur.execute("SELECT level, COUNT(*) FROM stock_industry GROUP BY level")
    for level, cnt in cur.fetchall():
        print(f"  {level}: {cnt:>6} 条")
    print(f"  合计:  {si_total:>6} 条")

    # ── 多源共存检查 ────────────────────────────────────
    print("\n【多源共存验证】")
    # 检查 stock_concept 里同一股同一板块是否多源（应各自独立）
    cur.execute("""
        SELECT sc1.stock_code, sc1.board_code, sc1.source
        FROM stock_concept sc1
        JOIN stock_concept sc2
          ON sc1.stock_code = sc2.stock_code
         AND sc1.board_code = sc2.board_code
         AND sc1.source < sc2.source
        LIMIT 5
    """)
    multi = cur.fetchall()
    if multi:
        print(f"  ✓ 存在多源共存（同一股同一板块，不同 source）:")
        for r in multi:
            print(f"      {r[0]} + {r[1]} : {r[2]}")
    else:
        print(f"  ⚠ 无多源共存记录（正常，因为 EM 成分股为空）")

    # ── 数据质量检查 ────────────────────────────────────
    print("\n【数据质量】")
    # stock_count 为 NULL 或 0 的板块
    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE stock_count IS NULL OR stock_count = 0")
    bad_cb = cur.fetchone()[0]
    print(f"  concept_boards(stock_count NULL/0): {bad_cb}")

    # board_code 前缀检查（验证 source 标注是否正确）
    cur.execute("SELECT source, COUNT(*) FROM concept_boards GROUP BY source")
    for src, cnt in cur.fetchall():
        print(f"  concept_boards({src}): {cnt} 个板块")

    # ── 下一步建议 ───────────────────────────────────────
    print("\n【下一步建议】")
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='em'")
    em_sc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stock_industry_board WHERE source='sina'")
    sib_sina = cur.fetchone()[0]

    suggestions = []
    if em_sc == 0:
        suggestions.append("⚠ EM 成分股为空（CDN 不通），待网络恢复后运行 fetch_em_concepts.py")
    if sib_sina < 1000:
        suggestions.append("⚠ stock_industry_board(sina) 数据量偏低，先跑 fetch_sina_concepts.py industry")
    if total_sc < 10000:
        suggestions.append("⚠ stock_concept 总量偏低，先完成 Sina + THS 采集")

    if not suggestions:
        suggestions = ["✓ 数据量充足，可运行 sync_counts.py 重算 stock_count"]
    for s in suggestions:
        print(f"  {s}")

    conn.close()
    print("=" * 60)


if __name__ == "__main__":
    report()
