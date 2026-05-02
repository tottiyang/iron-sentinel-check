#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 概念板块成分股统一调度（断点续传版）

调度策略：
  1. 优先 Sina（最稳定，无截断）
  2. 补充 THS（有截断，但覆盖更多板块）
  3. 最后 EM（CDN 恢复后启用）

断点续传：
  - 记录每个板块的采集状态到 meta 表
  - 支持中断后从断点继续

Usage:
  python3 sync_concepts.py              # 全量采集
  python3 sync_concepts.py --resume     # 从断点继续
  python3 sync_concepts.py --source=sina # 只采指定源
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from fetchers.db.db_schema import get_conn
from fetchers.sina.fetch_sina_concepts import fetch_concept_relations as sina_fetch
from fetchers.ths.fetch_ths_concepts import fetch_concept_relations as ths_fetch
from fetchers.em.fetch_em_concepts import main as em_main


def get_meta(key, default=None):
    """获取元数据"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else default


def set_meta(key, value):
    """设置元数据"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO meta (key, value, updated_at) VALUES (?, ?, datetime('now'))",
        (key, value)
    )
    conn.commit()
    conn.close()


def sync_sina(limit=0):
    """同步 Sina 概念成分股"""
    print("\n=== Sina 概念成分股同步 ===")
    remaining = sina_fetch(limit=limit)
    set_meta("sync_concepts_sina_last_run", time.strftime("%Y-%m-%d %H:%M:%S"))
    return remaining


def sync_ths(limit=0):
    """同步 THS 概念成分股"""
    print("\n=== THS 概念成分股同步 ===")
    remaining = ths_fetch(limit=limit)
    set_meta("sync_concepts_ths_last_run", time.strftime("%Y-%m-%d %H:%M:%S"))
    return remaining


def sync_em(limit=0):
    """同步 EM 概念成分股（CDN 不通则跳过）"""
    print("\n=== EM 概念成分股同步 ===")
    # 使用 main() 函数，它会返回剩余待处理数量
    import sys
    old_argv = sys.argv
    try:
        sys.argv = ['sync_concepts.py', '--limit', str(limit)]
        remaining = em_main()
    finally:
        sys.argv = old_argv
    set_meta("sync_concepts_em_last_run", time.strftime("%Y-%m-%d %H:%M:%S"))
    return remaining


def status():
    """显示同步状态"""
    conn = get_conn()
    cur = conn.cursor()

    print("\n" + "=" * 60)
    print("概念成分股同步状态")
    print("=" * 60)

    # 板块统计
    for src in ['sina', 'ths', 'em']:
        cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source=?", (src,))
        boards = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_concept WHERE source=?", (src,))
        done = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source=?", (src,))
        records = cur.fetchone()[0]

        last_run = get_meta(f"sync_concepts_{src}_last_run", "从未")
        progress = f"{done}/{boards}" if boards > 0 else "N/A"
        pct = f"{done/boards*100:.1f}%" if boards > 0 else "N/A"

        print(f"\n  [{src}]")
        print(f"    板块数: {boards}")
        print(f"    已完成: {progress} ({pct})")
        print(f"    关联数: {records} 条")
        print(f"    上次运行: {last_run}")

    # 覆盖率
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_concept")
    covered = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stocks")
    total = cur.fetchone()[0]
    if total > 0:
        print(f"\n  概念覆盖: {covered}/{total} = {covered/total*100:.1f}%")

    conn.close()
    print("=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='概念板块成分股统一调度')
    parser.add_argument('--source', choices=['sina', 'ths', 'em', 'all'],
                        default='all', help='指定数据源')
    parser.add_argument('--limit', type=int, default=0, help='每源最多处理板块数')
    parser.add_argument('--status', action='store_true', help='只显示状态')
    args = parser.parse_args()

    if args.status:
        status()
        return

    print("=" * 60)
    print("概念板块成分股统一调度")
    print("=" * 60)

    sources = []
    if args.source in ('sina', 'all'):
        sources.append(('sina', sync_sina))
    if args.source in ('ths', 'all'):
        sources.append(('ths', sync_ths))
    if args.source in ('em', 'all'):
        sources.append(('em', sync_em))

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
