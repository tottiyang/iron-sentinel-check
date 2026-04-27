#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 证监会行业板块成分股同步

证监会行业特点：
  - 一只股票可属于多个行业（如"医药"+"中药"+"医疗器械"）
  - 使用 INSERT OR IGNORE，不覆盖已有数据
  - PK=(stock_code, board_code)，允许多行业

数据源优先级：
  1. Sina（84个证监会行业，稳定可用）
  2. THS（90个行业板块，补充覆盖）
  3. EM（496个行业板块，CDN恢复后启用）

Usage:
  python3 sync_industry_boards.py           # 全量同步
  python3 sync_industry_boards.py --source=sina  # 只同步指定源
  python3 sync_industry_boards.py --status  # 查看状态
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn

SLEEP_SEC = 0.35
BATCH_LOG = 10


def fetch_sector_stocks(label):
    """采集单个板块的成分股（Sina label 格式）"""
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
        return []


def sync_sina_industry(limit=0):
    """同步 Sina 证监会行业成分股"""
    print("\n=== Sina 证监会行业同步 ===")

    conn = get_conn()
    cur = conn.cursor()

    # 获取 Sina 行业板块
    cur.execute(
        "SELECT board_code, board_name FROM industry_boards WHERE source='sina' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    # 断点续传
    cur.execute(
        "SELECT DISTINCT board_code FROM stock_industry_board WHERE source='sina'"
    )
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]

    if not pending:
        print(f"  全部 {len(all_boards)} 个板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"  需采集: {len(pending)} 个板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        label = board_code.replace("SINA_", "", 1)
        try:
            stocks = fetch_sector_stocks(label)
            if stocks:
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
                print(f"    ERROR {board_name}: {e}")

        time.sleep(SLEEP_SEC)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"  [{i+1}/{len(pending)}] +{total_added} 条, 剩余 {remaining}")

    return len(pending)


def sync_ths_industry(limit=0):
    """同步 THS 行业板块成分股"""
    print("\n=== THS 行业板块同步 ===")

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
        print(f"  全部 {len(all_boards)} 个板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"  需采集: {len(pending)} 个板块（已有 {len(done)} 个完成）")

    total_added = 0
    errors = 0

    for i, (board_code, board_name) in enumerate(pending):
        try:
            # THS 行业板块用概念接口采集
            df = ak.stock_board_concept_cons_ths(symbol=board_name)
            if df is None or len(df) == 0:
                continue

            conn = get_conn()
            cur = conn.cursor()
            for _, row in df.iterrows():
                code = str(row.get("股票代码", "")).strip().zfill(6)
                if code and len(code) == 6:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_industry_board "
                        "(stock_code, board_code, source, fetched_at) "
                        "VALUES (?, ?, 'ths', datetime('now'))",
                        (code, board_code),
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
    """显示同步状态"""
    conn = get_conn()
    cur = conn.cursor()

    print("\n" + "=" * 60)
    print("证监会行业板块同步状态")
    print("=" * 60)

    # 板块统计
    for src in ['sina', 'ths', 'em']:
        cur.execute("SELECT COUNT(*) FROM industry_boards WHERE source=?", (src,))
        boards = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_industry_board WHERE source=?", (src,))
        done = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM stock_industry_board WHERE source=?", (src,))
        records = cur.fetchone()[0]

        progress = f"{done}/{boards}" if boards > 0 else "N/A"
        pct = f"{done/boards*100:.1f}%" if boards > 0 else "N/A"

        print(f"\n  [{src}]")
        print(f"    板块数: {boards}")
        print(f"    已完成: {progress} ({pct})")
        print(f"    关联数: {records} 条")

    # 多行业检查
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_industry_board")
    stocks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stock_industry_board")
    total = cur.fetchone()[0]
    if stocks > 0:
        avg = total / stocks
        print(f"\n  平均每只股票行业数: {avg:.2f}")
        if avg > 1.5:
            print("  ✓ 多行业数据正常（>1.5）")
        else:
            print("  ⚠ 多行业数据偏少（<=1.5），可能遗漏")

    # 覆盖率
    cur.execute("SELECT COUNT(*) FROM stocks")
    total_stocks = cur.fetchone()[0]
    if total_stocks > 0:
        pct = stocks / total_stocks * 100
        print(f"\n  行业覆盖: {stocks}/{total_stocks} = {pct:.1f}%")

    conn.close()
    print("=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='证监会行业板块成分股同步')
    parser.add_argument('--source', choices=['sina', 'ths', 'em', 'all'],
                        default='all', help='指定数据源')
    parser.add_argument('--limit', type=int, default=0, help='每源最多处理板块数')
    parser.add_argument('--status', action='store_true', help='只显示状态')
    args = parser.parse_args()

    if args.status:
        status()
        return

    print("=" * 60)
    print("证监会行业板块成分股同步")
    print("=" * 60)

    sources = []
    if args.source in ('sina', 'all'):
        sources.append(('sina', sync_sina_industry))
    if args.source in ('ths', 'all'):
        sources.append(('ths', sync_ths_industry))

    for name, fn in sources:
        remaining = 1
        while remaining > 0:
            remaining = fn(limit=args.limit)
            if remaining > 0:
                print(f"  [{name}] 继续下一批...")
                time.sleep(2)

    print("\n同步完成！")
    status()


if __name__ == "__main__":
    main()
