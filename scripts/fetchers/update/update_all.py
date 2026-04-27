#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 每日更新主入口

执行顺序：
  1. 板块列表更新（Sina + THS + EM）
  2. 个股基础信息更新（增量）
  3. 申万行业关联（如有新增个股）
  4. 概念成分股同步（Sina → THS → EM）
  5. 证监会行业同步（Sina → THS）
  6. 重算 stock_count
  7. 生成状态报告

Usage:
  python3 update_all.py              # 完整更新
  python3 update_all.py --quick      # 快速模式（只更新关联数据）
  python3 update_all.py --boards     # 只更新板块列表
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from datetime import datetime


def log_step(step_num, msg):
    """打印步骤日志"""
    print(f"\n{'='*60}")
    print(f"Step {step_num}: {msg}")
    print('='*60)


def step1_boards():
    """更新板块列表"""
    log_step(1, "板块列表更新")

    from fetchers.sina.fetch_sina_list import main as sina_list
    from fetchers.ths.fetch_ths_list import main as ths_list
    from fetchers.em.fetch_em_list import main as em_list

    print("[1.1] Sina 板块列表...")
    sina_list()

    print("\n[1.2] THS 板块列表...")
    ths_list()

    print("\n[1.3] EM 板块列表...")
    em_list()


def step2_stocks():
    """更新个股基础信息"""
    log_step(2, "个股基础信息更新")

    from fetchers.akshare.fetch_stocks import fetch_all_stocks
    fetch_all_stocks()


def step3_sw_industry():
    """更新申万行业层级"""
    log_step(3, "申万行业层级更新")

    from fetchers.akshare.fetch_stocks import fetch_sw_industry
    fetch_sw_industry()


def step4_sw_relations():
    """更新申万行业关联"""
    log_step(4, "申万行业关联更新")

    from fetchers.akshare.fetch_stocks import fetch_sw_relations, fetch_sw_industry

    _, _, l3_map, l3_to_l2 = fetch_sw_industry()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name, code FROM industry_l1")
    l1_map = dict(cur.fetchall())
    cur.execute("SELECT name, code FROM industry_l2")
    l2_map = dict(cur.fetchall())
    conn.close()

    fetch_sw_relations(l3_map, l3_to_l2, l2_map, l1_map)


def step5_concept_relations():
    """同步概念成分股"""
    log_step(5, "概念成分股同步")

    from fetchers.sync.sync_concepts import sync_sina, sync_ths, sync_em

    print("[5.1] Sina 概念成分股...")
    sync_sina()

    print("\n[5.2] THS 概念成分股...")
    sync_ths()

    print("\n[5.3] EM 概念成分股...")
    sync_em()


def step6_industry_relations():
    """同步证监会行业成分股"""
    log_step(6, "证监会行业成分股同步")

    from fetchers.sync.sync_industry_boards import sync_sina_industry, sync_ths_industry

    print("[6.1] Sina 证监会行业...")
    sync_sina_industry()

    print("\n[6.2] THS 行业板块...")
    sync_ths_industry()


def step7_sync_counts():
    """重算 stock_count"""
    log_step(7, "重算 stock_count")

    from fetchers.sync.sync_counts import main as sync_counts
    sync_counts()


def step8_report():
    """生成状态报告"""
    log_step(8, "状态报告")

    from fetchers.status.report import report
    report()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='铁血哨兵每日更新')
    parser.add_argument('--quick', action='store_true', help='快速模式（跳过板块列表和个股）')
    parser.add_argument('--boards', action='store_true', help='只更新板块列表')
    parser.add_argument('--relations', action='store_true', help='只更新关联数据')
    args = parser.parse_args()

    start_time = time.time()

    print("=" * 60)
    print("铁血哨兵 - 每日更新")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        if args.boards:
            step1_boards()
        elif args.relations:
            step5_concept_relations()
            step6_industry_relations()
            step7_sync_counts()
            step8_report()
        elif args.quick:
            step5_concept_relations()
            step6_industry_relations()
            step7_sync_counts()
            step8_report()
        else:
            # 完整流程
            step1_boards()
            step2_stocks()
            step3_sw_industry()
            step4_sw_relations()
            step5_concept_relations()
            step6_industry_relations()
            step7_sync_counts()
            step8_report()

        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"更新完成！总耗时: {elapsed:.0f} 秒")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print('='*60)

    except Exception as e:
        print(f"\n❌ 更新失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    from fetchers.db.db_schema import get_conn
    main()
