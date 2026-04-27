#!/usr/bin/env python3
import sys, os, time
sys.path.insert(0, '/Users/totti/.qclaw/skills/iron-sentinel/scripts')

import akshare as ak
from fetchers.db.db_schema import get_conn

def fetch_all_stocks():
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
            "INSERT OR IGNORE INTO stocks (stock_code, stock_name, exchange, listing_status) VALUES (?, ?, ?, 'Normal')",
            (code, name, exchange),
        )
        if cur.rowcount > 0:
            added += 1
    conn.commit()
    conn.close()
    print(f"    新增 {added} 只")
    return len(df)

def fetch_sw_industry():
    print("\n[2/5] 采集申万一级...")
    df1 = ak.sw_index_first_info()
    l1_map = {}
    conn = get_conn()
    cur = conn.cursor()
    for _, row in df1.iterrows():
        code = str(row["行业代码"]).replace(".SI", "").strip()
        name = row["行业名称"].strip()
        l1_map[name] = code
        cur.execute("INSERT OR IGNORE INTO industry_l1 (code, name) VALUES (?,?)", (code, name))
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
        cur.execute("INSERT OR IGNORE INTO industry_l2 (code, name, l1_code) VALUES (?,?,?)", (code, name, l1_code))
    conn.commit()
    print(f"    {len(df2)} 个二级")
    print("\n[4/5] 采集申万三级...")
    df3 = ak.sw_index_third_info()
    l3_to_l2 = {}
    for _, row in df3.iterrows():
        code = str(row["行业代码"]).replace(".SI", "").strip()
        name = row["行业名称"].strip()
        l2_name = row.get("上级行业", "")
        l2_code = l2_map.get(l2_name)
        l3_to_l2[code] = l2_code
        cur.execute("INSERT OR IGNORE INTO industry_l3 (code, name, l2_code) VALUES (?,?,?)", (code, name, l2_code))
    conn.commit()
    conn.close()
    print(f"    {len(df3)} 个三级")
    return l1_map, l2_map, l3_to_l2

def fetch_sw_relations(l3_map, l3_to_l2, l2_map, l1_map):
    print("\n[5/5] 采集个股-申万行业关联...")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT code, name FROM industry_l3")
    l3_list = list(cur.fetchall())
    conn.close()
    pending = [(code, name) for code, name in l3_list]
    print(f"    共 {len(pending)} 个 L3 行业待处理")
    total_l3 = 0
    errors = 0
    for i, (l3_code, l3_name) in enumerate(pending):
        try:
            df = ak.sw_index_third_cons(symbol=l3_code + ".SI")
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
                cur.execute("INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L3', ?)", (sc, l3_code))
                if cur.rowcount > 0:
                    total_l3 += 1
                if l2_code:
                    cur.execute("INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L2', ?)", (sc, l2_code))
                if l1_code:
                    cur.execute("INSERT OR IGNORE INTO stock_industry (stock_code, level, industry_code) VALUES (?, 'L1', ?)", (sc, l1_code))
            conn.commit()
            conn.close()
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"    ERROR {l3_name}: {e}")
        time.sleep(0.3)
        if (i + 1) % 50 == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"    [{i+1}/{len(pending)}] +{total_l3} L3关联, 错误{errors}, 剩余 {remaining}")
    print(f"    完成！新增 {total_l3} 条 L3 关联")
    return total_l3

def status():
    conn = get_conn()
    cur = conn.cursor()
    for t, label in [("stocks","个股"),("industry_l1","L1行业"),("industry_l2","L2行业"),("industry_l3","L3行业"),("stock_industry","申万关联")]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t:<25} {cur.fetchone()[0]:>6} {label}")
    conn.close()

if __name__ == "__main__":
    init_only = "--init-only" in sys.argv
    fetch_all_stocks()
    l1_map, l2_map, l3_to_l2 = fetch_sw_industry()
    if not init_only:
        fetch_sw_relations(None, l3_to_l2, l2_map, l1_map)
    print("\n状态报告:")
    status()
