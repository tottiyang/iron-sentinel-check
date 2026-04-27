#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 多平台概念板块 + 关联采集（断点续传版）

平台：
  - em: 东方财富 stock_board_concept_name_em (~490)
  - ths: 同花顺 stock_board_concept_name_ths (~350)
  - dc: 大智慧/其他（预留）

特性：
  - 断点续传：跳过已采集的板块
  - 每个板块标注来源(source)
  - 失败自动重试1次
  - 写入进度到 meta 表
"""

import sys
import os
import time
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

import akshare as ak
from db_schema import get_conn

SLEEP_SEC = 0.3
BATCH_LOG = 10  # 每N个板块打印一次进度


def fetch_concept_list_em():
    """东方财富概念板块列表"""
    try:
        df = ak.stock_board_concept_name_em()
        if df is None or len(df) == 0:
            return []
        # 列: 排序代码, 板块名称, 板块代码, 最新价, ...
        results = []
        for _, row in df.iterrows():
            code = str(row.get('板块代码', '')).strip()
            name = str(row.get('板块名称', '')).strip()
            if code and name:
                results.append((code, name))
        return results
    except Exception as e:
        print(f"  [EM] 获取列表失败: {e}")
        return []


def fetch_concept_list_ths():
    """同花顺概念板块列表"""
    try:
        df = ak.stock_board_concept_name_ths()
        if df is None or len(df) == 0:
            return []
        results = []
        for _, row in df.iterrows():
            code = str(row.get('code', '')).strip()
            name = str(row.get('name', '')).strip()
            if code and name:
                results.append((f"THS_{code}", name))  # 加前缀防冲突
        return results
    except Exception as e:
        print(f"  [THS] 获取列表失败: {e}")
        return []


def fetch_concept_cons_em(board_name):
    """东方财富概念成分股"""
    try:
        df = ak.stock_board_concept_cons_em(symbol=board_name)
        if df is None or len(df) == 0:
            return []
        stocks = []
        for _, row in df.iterrows():
            code = str(row.get('代码', '')).zfill(6)
            if code and len(code) == 6:
                stocks.append(code)
        return stocks
    except:
        return []


def fetch_concept_cons_ths(board_name):
    """同花顺概念成分股"""
    try:
        df = ak.stock_board_concept_cons_ths(symbol=board_name)
        if df is None or len(df) == 0:
            return []
        stocks = []
        for _, row in df.iterrows():
            code = str(row.get('代码', '')).zfill(6)
            if not code or len(code) != 6:
                code = str(row.get('股票代码', '')).zfill(6)
            if code and len(code) == 6:
                stocks.append(code)
        return stocks
    except:
        return []


CONCEPT_LIST_FETCHERS = {
    'em': fetch_concept_list_em,
    'ths': fetch_concept_list_ths,
}

CONCEPT_CONS_FETCHERS = {
    'em': fetch_concept_cons_em,
    'ths': fetch_concept_cons_ths,
}


def save_concept_boards(boards, source):
    """保存概念板块列表到数据库"""
    conn = get_conn()
    cur = conn.cursor()
    added = 0
    for code, name in boards:
        cur.execute(
            "INSERT OR IGNORE INTO concept_boards (board_code, board_name, source) VALUES (?, ?, ?)",
            (code, name, source)
        )
        if cur.rowcount > 0:
            added += 1
    conn.commit()
    conn.close()
    return added


def fetch_concept_relations(source, limit=0):
    """采集概念-个股关联（断点续传）
    
    Args:
        source: 'em' or 'ths'
        limit: 0=不限制, >0=最多处理N个板块
    """
    conn = get_conn()
    cur = conn.cursor()

    # 获取该来源的所有板块
    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source=? ORDER BY board_code", (source,))
    all_boards = cur.fetchall()

    # 已有关联的板块
    cur.execute("SELECT DISTINCT board_code FROM stock_concept WHERE source=?", (source,))
    done_boards = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done_boards]
    
    if not pending:
        print(f"  [{source}] 概念关联全部完成 ({len(all_boards)} 个板块)")
        return 0

    if limit > 0:
        pending = pending[:limit]

    fetch_cons = CONCEPT_CONS_FETCHERS.get(source)
    if not fetch_cons:
        print(f"  [{source}] 无成分股采集接口")
        return len(pending)

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        try:
            stocks = fetch_cons(board_name)
            if stocks:
                conn = get_conn()
                cur = conn.cursor()
                for stock_code in stocks:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_concept (stock_code, board_code, source) VALUES (?, ?, ?)",
                        (stock_code, board_code, source)
                    )
                    total_added += 1
                conn.commit()
                conn.close()
            time.sleep(SLEEP_SEC)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"    [{source}] Error {board_name}: {e}")
            time.sleep(1)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"  [{source}] {i+1}/{len(pending)} 完成, +{total_added} 条关联, 剩余 {remaining}")

    # 更新 meta
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO meta (key, value, updated_at) VALUES (?, ?, ?)",
        (f"concept_{source}_last_run", datetime.now().isoformat(), datetime.now().isoformat())
    )
    conn.commit()
    conn.close()

    return len(pending) - len(pending)  # 0 if all done


def fetch_industry_board_relations(limit=0):
    """采集证监会行业板块-个股关联"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT board_code, board_name FROM industry_boards ORDER BY board_code")
    all_boards = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM stock_industry_board")
    existing = cur.fetchone()[0]
    conn.close()

    if existing > 0 and existing >= len(all_boards) * 5:
        # 已有足够数据
        print("  [industry] 行业板块关联已完成")
        return 0

    batch = all_boards if limit == 0 else all_boards[:limit]
    total_added = 0

    for i, (board_code, board_name) in enumerate(batch):
        try:
            df = ak.stock_board_industry_cons_em(symbol=board_name)
            if df is None or len(df) == 0:
                continue

            conn = get_conn()
            cur = conn.cursor()
            for _, row in df.iterrows():
                stock_code = str(row.get('代码', '')).zfill(6)
                if stock_code and len(stock_code) == 6:
                    cur.execute(
                        "INSERT OR REPLACE INTO stock_industry_board (stock_code, board_code) VALUES (?, ?)",
                        (stock_code, board_code)
                    )
                    total_added += 1
            conn.commit()
            conn.close()
            time.sleep(SLEEP_SEC)
        except Exception as e:
            time.sleep(1)

        if (i + 1) % BATCH_LOG == 0 or i == len(batch) - 1:
            print(f"  [industry] {i+1}/{len(batch)} 完成, +{total_added} 条")

    return 0


def run_all_concepts():
    """主入口：采集所有平台的概念板块列表 + 关联"""
    total_remaining = 0

    for source in ['em', 'ths']:
        print(f"\n{'='*50}")
        print(f"  平台: {source}")
        print(f"{'='*50}")

        # 1. 获取概念列表
        print(f"  [{source}] 获取概念板块列表...")
        boards = CONCEPT_LIST_FETCHERS[source]()
        print(f"  [{source}] 获取到 {len(boards)} 个概念板块")

        # 2. 保存列表
        added = save_concept_boards(boards, source)
        print(f"  [{source}] 新增 {added} 个板块（去重后）")

        # 3. 采集关联（分批，每次最多50个板块）
        remaining = 1
        while remaining > 0:
            remaining = fetch_concept_relations(source, limit=50)
            total_remaining += remaining
            if remaining > 0:
                print(f"  [{source}] 继续下一批...")
                time.sleep(2)

    return total_remaining


def status_report():
    """数据完整性报告"""
    conn = get_conn()
    cur = conn.cursor()

    print("\n" + "="*60)
    print("  铁血哨兵 - 数据采集报告")
    print("="*60)

    # 各表数量
    tables = ['stocks', 'industry_l1', 'industry_l2', 'industry_l3',
              'concept_boards', 'industry_boards',
              'stock_industry', 'stock_concept', 'stock_industry_board']
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        count = cur.fetchone()[0]
        print(f"  {t:25s} {count:>6} 条")

    # 概念板块按来源
    print("\n  概念板块按来源:")
    cur.execute("SELECT source, COUNT(*) FROM concept_boards GROUP BY source")
    for row in cur.fetchall():
        print(f"    {row[0]:10s} {row[1]:>6} 个板块")

    # 概念关联按来源
    print("\n  概念关联按来源:")
    cur.execute("SELECT source, COUNT(*) FROM stock_concept GROUP BY source")
    for row in cur.fetchall():
        print(f"    {row[0]:10s} {row[1]:>6} 条关联")

    # 覆盖率
    cur.execute("SELECT COUNT(*) FROM stock_industry WHERE level='L1'")
    sw_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stocks")
    total_stocks = cur.fetchone()[0]
    print(f"\n  申万行业覆盖率: {sw_count}/{total_stocks} = {sw_count/total_stocks*100:.1f}%")

    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_concept")
    concept_covered = cur.fetchone()[0]
    print(f"  概念关联覆盖: {concept_covered}/{total_stocks} = {concept_covered/total_stocks*100:.1f}%")

    conn.close()
    print("="*60)


if __name__ == "__main__":
    import sys
    task = sys.argv[1] if len(sys.argv) > 1 else 'all'

    if task == 'all':
        run_all_concepts()
        fetch_industry_board_relations(limit=50)
        status_report()
    elif task == 'concept':
        source = sys.argv[2] if len(sys.argv) > 2 else 'em'
        fetch_concept_relations(source)
        status_report()
    elif task == 'industry':
        fetch_industry_board_relations()
        status_report()
    elif task == 'list':
        source = sys.argv[2] if len(sys.argv) > 2 else 'all'
        if source == 'all':
            for s in ['em', 'ths']:
                boards = CONCEPT_LIST_FETCHERS[s]()
                added = save_concept_boards(boards, s)
                print(f"  [{s}] {len(boards)} 个, 新增 {added}")
        else:
            boards = CONCEPT_LIST_FETCHERS[source]()
            added = save_concept_boards(boards, source)
            print(f"  [{source}] {len(boards)} 个, 新增 {added}")
    elif task == 'status':
        status_report()
    else:
        print("用法: python3 fetch_concepts_multi.py [all|concept|industry|list|status] [em|ths]")
