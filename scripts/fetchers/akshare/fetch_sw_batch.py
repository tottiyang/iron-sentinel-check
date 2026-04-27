#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 申万行业关联增量采集（分批执行版）
每次运行处理一批未覆盖的三级行业，可反复调用直到完成。
"""

import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

import akshare as ak
from db_schema import get_conn

BATCH_SIZE = 30  # 每次处理30个三级行业
SLEEP_SEC = 0.3


def fetch_batch():
    """采集一批个股-申万行业关联"""
    conn = get_conn()
    cur = conn.cursor()

    # 获取所有三级行业
    cur.execute("SELECT code, name FROM industry_l3 ORDER BY code")
    all_l3 = cur.fetchall()

    # 获取已有关联的 L3 行业代码
    cur.execute("SELECT DISTINCT industry_code FROM stock_industry WHERE level='L3'")
    done_l3 = set(r[0] for r in cur.fetchall())

    # 筛选未处理的
    pending = [(code, name) for code, name in all_l3 if code not in done_l3]

    conn.close()

    if not pending:
        print(f"✅ 全部完成！共处理 {len(all_l3)} 个三级行业")
        return 0, []

    batch = pending[:BATCH_SIZE]
    remaining = len(pending)

    # 建立层级映射
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT code, name FROM industry_l3")
    l3_name_to_code = {name: code for code, name in cur.fetchall()}

    cur.execute("SELECT code, l2_code FROM industry_l3")
    l3_to_l2 = {code: l2_code for code, l2_code in cur.fetchall() if l2_code}

    cur.execute("SELECT code, l1_code FROM industry_l2")
    l2_to_l1 = {code: l1_code for code, l1_code in cur.fetchall() if l1_code}

    conn.close()

    success = 0
    errors = []
    processed = 0

    for l3_code, l3_name in batch:
        processed += 1
        print(f"  [{processed}/{len(batch)}, 剩余{remaining}] {l3_name} ({l3_code})")

        try:
            df = ak.sw_index_third_cons(symbol=l3_code + '.SI')
            if df is None or len(df) == 0:
                continue

            l2_code = l3_to_l2.get(l3_code)
            l1_code = l2_to_l1.get(l2_code) if l2_code else None

            conn = get_conn()
            cur = conn.cursor()

            for _, row in df.iterrows():
                stock_code = str(row['股票代码']).replace('.SH', '').replace('.SZ', '').replace('.BJ', '')
                l3_name_val = row.get('申万3级', '')

                if l3_name_val and l3_name_val in l3_name_to_code:
                    cur.execute("INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L3', ?)",
                                (stock_code, l3_name_to_code[l3_name_val]))
                    success += 1

                if l2_code:
                    cur.execute("INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L2', ?)",
                                (stock_code, l2_code))
                    success += 1

                if l1_code:
                    cur.execute("INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L1', ?)",
                                (stock_code, l1_code))
                    success += 1

            conn.commit()
            conn.close()
            time.sleep(SLEEP_SEC)

        except Exception as e:
            errors.append(f"{l3_name}: {e}")
            time.sleep(1)

    total_l1 = 0
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_industry WHERE level='L1'")
    total_l1 = cur.fetchone()[0]
    conn.close()

    print(f"\n本轮: +{success} 条关联, 已覆盖 {total_l1} 只股票, 剩余 ~{remaining - len(batch)} 个行业")
    return remaining - len(batch), errors


if __name__ == "__main__":
    remaining, errs = fetch_batch()
