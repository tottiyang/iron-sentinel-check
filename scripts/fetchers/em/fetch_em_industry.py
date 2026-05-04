#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EM（东方财富）行业板块成分股采集

采集范围：stock_industry_board（source='em_industry'）

功能：
  1. CDN 检测
  2. 自动重试与指数退避
  3. 断点续传
  4. 详细日志

Usage:
  python3 fetch_em_industry.py              # 全量采集
  python3 fetch_em_industry.py --limit=10   # 只采 10 个板块（测试用）
  python3 fetch_em_industry.py --check-only # 只检测 CDN 状态
"""

import sys, os, time, random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn

SLEEP_MIN = 1.5
SLEEP_MAX = 2.5
BATCH_LOG = 10
MAX_RETRIES = 3
RETRY_BACKOFF = 2


def fetch_em_industry_stocks(board_name, max_retries=MAX_RETRIES):
    """
    采集 EM 行业板块成分股（带重试）
    board_name: 板块名称
    返回: list(股票代码) / None(失败)
    """
    for attempt in range(max_retries):
        try:
            df = ak.stock_board_industry_cons_em(symbol=board_name)
            if df is None or len(df) == 0:
                return []
            stocks = []
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip().zfill(6)
                if code and len(code) == 6:
                    stocks.append(code)
            return stocks
        except Exception as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF ** attempt
                print(f"      重试 {board_name} ({attempt+1}/{max_retries})，等待 {wait}s...")
                time.sleep(wait)
            else:
                return None
    return None


def fetch_industry_relations(limit=0, dry_run=False, check_only=False):
    """
    采集行业-个股关联（EM 源，断点续传）
    
    参数:
      limit: 限制处理的板块数（0=不限）
      dry_run: 只打印，不写入数据库
      check_only: 只检测 CDN，不采集
    
    返回: 剩余待处理板块数（0=全部完成）
    """
    # CDN 检测
    print("[EM-industry] CDN 检测中...")
    try:
        df = ak.stock_board_industry_name_em()
        if df is None or len(df) == 0:
            print("  ❌ CDN 不可用（返回空数据）")
            return 1
        print(f"  ✅ CDN 可用，可获取 {len(df)} 个行业板块")
        if check_only:
            return 0
    except Exception as e:
        print(f"  ❌ CDN 不可用: {e}")
        return 1

    # 连接数据库
    conn = get_conn()
    cur = conn.cursor()

    # 获取所有 EM 行业板块
    cur.execute(
        "SELECT board_code, board_name FROM industry_boards WHERE source='em' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    # 获取已完成的板块
    cur.execute(
        "SELECT DISTINCT board_code FROM stock_industry_board WHERE source='em_industry'"
    )
    done = set(r[0] for r in cur.fetchall())

    pending = [(code, name) for code, name in all_boards if code not in done]

    if not pending:
        print(f"[EM-industry] 全部 {len(all_boards)} 个板块已完成")
        conn.close()
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"[EM-industry] 开始采集 {len(pending)} 个板块（已有 {len(done)} 个完成，总计 {len(all_boards)} 个）")

    total_added = 0
    errors = 0
    empty_count = 0

    for i, (board_code, board_name) in enumerate(pending):
        stocks = fetch_em_industry_stocks(board_name)

        if stocks is None:
            errors += 1
            if errors <= 3:
                print(f"    ❌ {board_name}: 采集失败")
            continue

        if len(stocks) == 0:
            empty_count += 1

        if not dry_run:
            # 写入数据库
            for stock_code in stocks:
                cur.execute(
                    "INSERT OR IGNORE INTO stock_industry_board (stock_code, board_code, source, fetched_at) "
                    "VALUES (?, ?, 'em_industry', datetime('now'))",
                    (stock_code, board_code)
                )
            conn.commit()

        total_added += len(stocks)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            print(f"    [{i+1}/{len(pending)}] {board_name}: {len(stocks)} 只，累计 {total_added} 条")

        # 随机间隔防反爬
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    conn.close()

    print(f"\n[EM-industry] 完成 {len(pending)} 个板块")
    print(f"  新增关联: {total_added} 条")
    print(f"  失败板块: {errors} 个")
    print(f"  空数据板块: {empty_count} 个")

    # 返回剩余待处理数
    return max(0, len(all_boards) - len(done) - len(pending) + errors)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="EM 行业板块成分股采集")
    parser.add_argument("--limit", type=int, default=0, help="限制处理的板块数")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写入")
    parser.add_argument("--check-only", action="store_true", help="只检测 CDN")
    args = parser.parse_args()

    remaining = fetch_industry_relations(
        limit=args.limit,
        dry_run=args.dry_run,
        check_only=args.check_only
    )

    if args.check_only:
        return 0

    return 0 if remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
