#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量 A 股个股列表 + 申万行业（L1/L2/L3）采集

Usage:
  python3 fetch_stocks.py [--init-only]  # --init-only: 只采 stocks + 申万列表，不采关联
"""

import sys, os, time
import random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn


def fetch_sw_with_retry(symbol, max_retries=3, base_delay=2.0):
    """
    带重试的申万行业成分股采集
    遇到 No tables found（通常是429限流）时自动重试
    """
    for attempt in range(max_retries):
        try:
            df = ak.sw_index_third_cons(symbol)
            return df
        except ValueError as e:
            if "No tables found" in str(e) and attempt < max_retries - 1:
                # 可能是429限流，等待后重试
                sleep_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_time)
                continue
            raise
        except Exception:
            if attempt < max_retries - 1:
                sleep_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_time)
                continue
            raise
    return None


def fetch_all_stocks():
    """采集全部 A 股个股列表"""
    print("[1/5] 采集 A 股个股列表...")
    df = ak.stock_info_a_code_name()
    print(f"    获取 {len(df)} 只")

    conn = get_conn()
    cur = conn.cursor()
    added = 0
    for _, row in df.iterrows():
        code = str(row["code"]).zfill(6)
        name = row["name"]
        if code.startswith(("60", "68")):
            exchange = "SH"
        elif code.startswith(("00", "30")):
            exchange = "SZ"
        elif code.startswith(("4", "8")):
            exchange = "BJ"
        else:
            exchange = "UNKNOWN"

        cur.execute(
            """INSERT OR IGNORE INTO stocks (stock_code, stock_name, exchange, listing_status)
               VALUES (?, ?, ?, 'Normal')""",
            (code, name, exchange),
        )
        if cur.rowcount > 0:
            added += 1

    conn.commit()
    conn.close()
    print(f"    新增 {added} 只")
    return len(df)


def fetch_sw_industry():
    """采集申万 L1/L2/L3"""
    print("\n[2/5] 采集申万一级...")
    df1 = ak.sw_index_first_info()
    l1_map = {}
    conn = get_conn()
    cur = conn.cursor()
    for _, row in df1.iterrows():
        code = str(row["行业代码"]).replace(".SI", "").strip()
        name = row["行业名称"].strip()
        l1_map[name] = code
        cur.execute(
            "INSERT OR IGNORE INTO industry_l1 (code, name) VALUES (?,?)",
            (code, name),
        )
    conn.commit()
    print(f"    {len(df1)} 个一级")

    print("\n[3/5] 采集申万二级...")
    df2 = ak.sw_index_second_info()
    l2_map = {}
    for _, row in df2.iterrows():
        code = str(row["行业代码"]).replace(".SI", "").strip()
        name = row["行业名称"].strip()
        l1_name = row.get("上级行业", "")
        l1_code = l1_map.get(l1_name)
        l2_map[name] = code
        cur.execute(
            "INSERT OR IGNORE INTO industry_l2 (code, name, l1_code) VALUES (?,?,?)",
            (code, name, l1_code),
        )
    conn.commit()
    print(f"    {len(df2)} 个二级")

    print("\n[4/5] 采集申万三级...")
    df3 = ak.sw_index_third_info()
    l3_map = {}
    l3_to_l2 = {}
    for _, row in df3.iterrows():
        code = str(row["行业代码"]).replace(".SI", "").strip()
        name = row["行业名称"].strip()
        l2_name = row.get("上级行业", "")
        l2_code = l2_map.get(l2_name)
        l3_map[name] = code
        l3_to_l2[code] = l2_code
        cur.execute(
            "INSERT OR IGNORE INTO industry_l3 (code, name, l2_code) VALUES (?,?,?)",
            (code, name, l2_code),
        )
    conn.commit()
    conn.close()
    print(f"    {len(df3)} 个三级")

    return l1_map, l2_map, l3_map, l3_to_l2


def fetch_sw_relations(l3_map, l3_to_l2, l2_map, l1_map, limit=0, dry_run=False):
    """
    采集个股-申万行业关联
    通过 L3 成分股接口，向上反推 L1/L2
    使用重试机制和更长间隔避免429限流
    """
    print("\n[5/5] 采集个股-申万行业关联...")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT code, name FROM industry_l3")
    l3_list = list(cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in l3_list]
    if limit > 0:
        pending = pending[:limit]

    print(f"    共 {len(pending)} 个 L3 行业待处理")
    print(f"    使用重试机制（最多3次）和随机间隔（1.5-2.5秒）")

    total_l3 = 0
    errors = 0
    error_details = []  # 收集所有错误详情

    for i, (l3_code, l3_name) in enumerate(pending):
        try:
            # 使用带重试的采集函数
            df = fetch_sw_with_retry(l3_code + ".SI", max_retries=3, base_delay=2.0)
            
            if df is None or len(df) == 0:
                continue

            l2_code = l3_to_l2.get(l3_code)
            l1_code = None
            if l2_code:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("SELECT l1_code FROM industry_l2 WHERE code=?", (l2_code,))
                row = cur.fetchone()
                l1_code = row[0] if row else None
                conn.close()

            conn = get_conn()
            cur = conn.cursor()

            for _, r in df.iterrows():
                raw = str(r.get("股票代码", "")).replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
                sc = raw.zfill(6)
                if not sc:
                    continue

                # L3
                cur.execute(
                    "INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L3', ?)",
                    (sc, l3_code),
                )
                if cur.rowcount > 0:
                    total_l3 += 1

                # L2
                if l2_code:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L2', ?)",
                        (sc, l2_code),
                    )

                # L1
                if l1_code:
                    cur.execute(
                        "INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L1', ?)",
                        (sc, l1_code),
                    )

            conn.commit()
            conn.close()

        except Exception as e:
            errors += 1
            error_msg = f"{l3_name}({l3_code}): {e}"
            error_details.append(error_msg)
            # 每10个错误打印一次，避免刷屏
            if errors <= 5 or errors % 10 == 0:
                print(f"    ERROR {error_msg}")

        # 随机间隔 1.5-2.5 秒，避免规律性触发限流
        sleep_time = 1.5 + random.uniform(0, 1)
        time.sleep(sleep_time)

        if (i + 1) % 50 == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"    [{i+1}/{len(pending)}] +{total_l3} L3关联, 错误{errors}, 剩余 {remaining}")

    print(f"    完成！新增 {total_l3} 条 L3 关联, 错误 {errors} 个")
    if errors > 0:
        print(f"    错误详情已收集，共 {len(error_details)} 条")
    return total_l3


def status():
    conn = get_conn()
    cur = conn.cursor()
    for t, label in [
        ("stocks", "个股"),
        ("industry_l1", "L1行业"),
        ("industry_l2", "L2行业"),
        ("industry_l3", "L3行业"),
        ("stock_industry", "申万关联"),
    ]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t:<25} {cur.fetchone()[0]:>6} {label}")
    conn.close()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    init_only = "--init-only" in sys.argv

    fetch_all_stocks()
    l1_map, l2_map, l3_map, l3_to_l2 = fetch_sw_industry()

    if not init_only and not dry_run:
        fetch_sw_relations(l3_map, l3_to_l2, l2_map, l1_map)
    elif init_only:
        print("\n[SKIP] 申万关联已跳过（--init-only）")

    print("\n状态报告:")
    status()
