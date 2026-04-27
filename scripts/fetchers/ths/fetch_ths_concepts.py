#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
THS 成分股关联采集

采集范围：stock_concept（source='ths'）

⚠️ 已知限制：
  akshare stock_board_concept_cons_ths(symbol=board_name) 只能返回前 100 条
  （同花顺官网接口截断，非 akshare bug）
  → stock_concept(ths) 会有缺失，大盘股完整但小盘股截断

  q.10jqka.com.cn 翻页 API 需要登录 cookie，目前 401
  → 需要 xbrowser 维持登录态，暂不可用（见 _exploration/）

数据写入策略：
  INSERT OR IGNORE INTO stock_concept (stock_code, board_code, source)
  原因：ths 和 sina/em 共存，board_code 格式不同（THS_XXXXX vs SINA_xxx vs BKxxxx）
  互不覆盖，不会冲突

断点续传：记录已完成的板块

Usage:
  python3 fetch_ths_concepts.py [concept|industry|all]
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn

SLEEP_SEC = 0.4
BATCH_LOG = 20


def fetch_ths_concept_stocks(board_name):
    """
    采集 THS 概念板块的成分股
    注意：akshare 接口最多返回 100 条（截断），这是已知的
    board_name: 板块名称，如 'AI PC'
    返回: ['000001', '000002', ...]
    """
    try:
        df = ak.stock_board_concept_cons_ths(symbol=board_name)
        if df is None or len(df) == 0:
            return []
        stocks = []
        for _, row in df.iterrows():
            code = str(row.get("股票代码", "")).strip().zfill(6)
            if code and len(code) == 6:
                stocks.append(code)
        return stocks
    except Exception as e:
        print(f"    ERROR stock_board_concept_cons_ths({board_name}): {e}")
        return []


def fetch_concept_relations(limit=0, dry_run=False):
    """采集概念-个股关联（THS 源，断点续传）"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT board_code, board_name FROM concept_boards WHERE source='ths' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    # 断点续传
    cur.execute(
        "SELECT DISTINCT board_code FROM stock_concept WHERE source='ths'"
    )
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]

    if not pending:
        print(f"[THS-concept] 全部 {len(all_boards)} 个板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"[THS-concept] 需采集 {len(pending)} 个板块（已有 {len(done)} 个完成）")
    print(f"  ⚠️ 注意：THS 接口最多返回 100 条/板块（截断已知）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        try:
            stocks = fetch_ths_concept_stocks(board_name)
            if stocks:
                if not dry_run:
                    conn = get_conn()
                    cur = conn.cursor()
                    for sc in stocks:
                        cur.execute(
                            "INSERT OR IGNORE INTO stock_concept "
                            "(stock_code, board_code, source, fetched_at) "
                            "VALUES (?, ?, 'ths', datetime('now'))",
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
            print(f"  [{i+1}/{len(pending)}] +{total_added} 条, 剩余 {remaining}")

    return len(pending)


def fetch_industry_relations(limit=0, dry_run=False):
    """
    采集行业板块-个股关联（THS 源）
    THS 行业板块数量较少（约 90 个），用途有限但可补充证监会行业覆盖
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT board_code, board_name FROM industry_boards WHERE source='ths' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    cur.execute(
        "SELECT DISTINCT board_code FROM stock_industry_board WHERE source='ths'"
    )
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]

    if not pending:
        print(f"[THS-industry] 全部 {len(all_boards)} 个行业板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"[THS-industry] 需采集 {len(pending)} 个行业板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        try:
            stocks = fetch_ths_concept_stocks(board_name)
            if stocks:
                if not dry_run:
                    conn = get_conn()
                    cur = conn.cursor()
                    for sc in stocks:
                        cur.execute(
                            "INSERT OR IGNORE INTO stock_industry_board "
                            "(stock_code, board_code, source, fetched_at) "
                            "VALUES (?, ?, 'ths', datetime('now'))",
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
            print(f"  [{i+1}/{len(pending)}] +{total_added} 条, 剩余 {remaining}")

    return len(pending)


def status():
    conn = get_conn()
    cur = conn.cursor()

    print("\n" + "=" * 50)
    print("THS 数据状态")
    print("=" * 50)

    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='ths'")
    cb = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM industry_boards WHERE source='ths'")
    ib = cur.fetchone()[0]
    print(f"  concept_boards(ths):  {cb} 个")
    print(f"  industry_boards(ths): {ib} 个")

    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='ths'")
    sc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_concept WHERE source='ths'")
    sc_boards = cur.fetchone()[0]
    print(f"\n  stock_concept(ths):   {sc} 条, 覆盖 {sc_boards}/{cb} 个板块")
    if sc_boards > 0:
        avg = sc / sc_boards
        print(f"  平均每板块: {avg:.1f} 只（<100 = 无截断, ≈100 = 可能截断）")

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
        print("用法: fetch_ths_concepts.py [concept|industry|all|status] [--dry-run]")
