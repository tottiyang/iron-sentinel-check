#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EM 成分股关联采集

采集范围：stock_concept（source='em'）

⚠️ CDN 不通：
  push2.eastmoney.com RemoteDisconnected
  akshare stock_board_concept_cons_em() 目前失败
  → 成分股关联当前为 0 条（491 个 EM 概念板块均无关联数据）

EM 数据目标：~50,000 条 stock_concept（待 CDN 恢复）
当前状态：0 条

替代方案（CDN 不通时）：
  Sina: ~10,000 条（稳定）
  THS:  ~24,000 条（有截断，但可用）
  两者合计 ~34,000 条，可作为临时替代

CDN 恢复后：
  运行本脚本重新采集 stock_concept(em)

Usage:
  python3 fetch_em_concepts.py
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn

SLEEP_SEC = 0.5
BATCH_LOG = 20


def fetch_em_concept_stocks(board_code):
    """
    采集 EM 概念板块成分股
    board_code: BKxxxx 格式
    board_name: 板块名称（EM 接口用名称查）
    """
    try:
        # akshare 用 board_name（板块名称）查询
        df = ak.stock_board_concept_cons_em(symbol=board_code)
        if df is None or len(df) == 0:
            return []
        stocks = []
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip().zfill(6)
            if code and len(code) == 6:
                stocks.append(code)
        return stocks
    except Exception as e:
        return None  # None = 失败，不是空


def fetch_concept_relations(limit=0, dry_run=False):
    """采集概念-个股关联（EM 源，断点续传）"""
    # 先检查 CDN 是否通
    import socket
    try:
        sock = socket.create_connection(("push2.eastmoney.com", 80), timeout=3)
        sock.close()
    except OSError:
        print("[EM-concept] ⚠️ CDN 不通（push2.eastmoney.com），跳过采集")
        print("             stock_concept(em) 将保持 0 条")
        print("             当前替代: sina + ths 合计约 34,000 条")
        return 0

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT board_code, board_name FROM concept_boards WHERE source='em' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    cur.execute(
        "SELECT DISTINCT board_code FROM stock_concept WHERE source='em'"
    )
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]

    if not pending:
        print(f"[EM-concept] 全部 {len(all_boards)} 个板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"[EM-concept] CDN 可达，开始采集 {len(pending)} 个板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        try:
            stocks = fetch_em_concept_stocks(board_name)
            if stocks is None:
                raise RuntimeError("fetch returned None")
            if stocks:
                if not dry_run:
                    conn = get_conn()
                    cur = conn.cursor()
                    for sc in stocks:
                        cur.execute(
                            "INSERT OR IGNORE INTO stock_concept "
                            "(stock_code, board_code, source, fetched_at) "
                            "VALUES (?, ?, 'em', datetime('now'))",
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
    print("EM 数据状态")
    print("=" * 50)

    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='em'")
    cb = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='em'")
    sc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_concept WHERE source='em'")
    sc_boards = cur.fetchone()[0]
    print(f"  concept_boards(em):     {cb} 个板块")
    print(f"  stock_concept(em):      {sc} 条关联")
    print(f"  板块覆盖率:              {sc_boards}/{cb} ({sc_boards/cb*100:.1f}%)" if cb > 0 else "  板块覆盖率: N/A")

    # CDN 状态
    import socket
    try:
        sock = socket.create_connection(("push2.eastmoney.com", 80), timeout=3)
        sock.close()
        print(f"\n  CDN 状态: ✅ 可达")
    except OSError:
        print(f"\n  CDN 状态: ❌ 不通（push2.eastmoney.com）")

    print(f"\n  当前替代数据量:")
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='sina'")
    print(f"    sina: {cur.fetchone()[0]} 条")
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='ths'")
    print(f"    ths:  {cur.fetchone()[0]} 条")

    conn.close()
    print("=" * 50)


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    if "--status" in sys.argv:
        status()
    else:
        fetch_concept_relations(dry_run=dry)
        status()
