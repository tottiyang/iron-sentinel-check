#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 统一板块数据采集
策略：
  1. 以板块名去重，合并 EM/THS/Sina 三源列表
  2. 成分股优先从 Sina 获取（稳定可用）
  3. THS-only 的板块，从 THS 页面爬取成分股
  4. EM-only 暂无法获取成分股（CDN挂），标记为待补
"""

import sys
import os
import re
import time
import sqlite3
from collections import defaultdict
from datetime import datetime

import akshare as ak
import requests

sys.path.insert(0, os.path.dirname(__file__))
from db_schema import get_conn

DB_PATH = os.path.join(os.path.dirname(__file__), "../stock_data.db")

# ============================================================
# 1. 板块列表采集
# ============================================================

def fetch_ths_concept_list():
    """同花顺概念板块列表"""
    try:
        df = ak.stock_board_concept_name_ths()
        results = []
        for _, row in df.iterrows():
            name = str(row["name"]).strip()
            code = str(row["code"]).strip()
            if name and code:
                results.append((f"THS_{code}", name, "ths"))
        print(f"  [THS] 概念: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [THS] 概念列表失败: {e}")
        return []


def fetch_ths_industry_list():
    """同花顺行业板块列表"""
    try:
        df = ak.stock_board_industry_name_ths()
        results = []
        for _, row in df.iterrows():
            name = str(row["name"]).strip()
            code = str(row["code"]).strip()
            if name and code:
                results.append((f"THS_{code}", name, "ths"))
        print(f"  [THS] 行业: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [THS] 行业列表失败: {e}")
        return []


def fetch_sina_concept_list():
    """新浪概念板块列表"""
    try:
        df = ak.stock_sector_spot(indicator="概念")
        results = []
        for _, row in df.iterrows():
            label = str(row.get("label", "")).strip()
            name = str(row.get("板块", "")).strip()
            if label and name:
                results.append((f"SINA_{label}", name, "sina"))
        print(f"  [Sina] 概念: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [Sina] 概念列表失败: {e}")
        return []


def fetch_sina_industry_list():
    """新浪行业板块列表"""
    try:
        df = ak.stock_sector_spot(indicator="行业")
        results = []
        for _, row in df.iterrows():
            label = str(row.get("label", "")).strip()
            name = str(row.get("板块", "")).strip()
            if label and name:
                results.append((f"SINA_{label}", name, "sina"))
        print(f"  [Sina] 行业: {len(results)} 个")
        return results
    except Exception as e:
        print(f"  [Sina] 行业列表失败: {e}")
        return []


# ============================================================
# 2. 成分股采集
# ============================================================

def fetch_sina_cons(sector_label):
    """新浪板块成分股"""
    try:
        df = ak.stock_sector_detail(sector=sector_label)
        if df is None or len(df) == 0:
            return []
        return [str(row["code"]).zfill(6) for _, row in df.iterrows()
                if len(str(row["code"]).zfill(6)) == 6]
    except:
        return []


def fetch_ths_cons_by_page(board_code, board_name, max_pages=5):
    """从同花顺页面爬取成分股代码"""
    # board_code format: THS_309121
    raw_code = board_code.replace("THS_", "")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "https://q.10jqka.com.cn/",
    }
    all_codes = set()
    
    for page in range(1, max_pages + 1):
        url = f"https://q.10jqka.com.cn/gn/detail/field/264648/order/desc/page/{page}/ajax/1/code/{raw_code}"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                break
            # Extract stock codes from HTML
            codes = re.findall(r'href="http://stockpage\.10jqka\.com\.cn/(\d{6})/"', r.text)
            if not codes:
                # Try alternative pattern
                codes = re.findall(r'/(\d{6})/"', r.text)
                codes = [c for c in codes if c.startswith(('0', '3', '6', '8'))]
            if not codes:
                break
            before = len(all_codes)
            all_codes.update(codes)
            if len(all_codes) == before:
                break  # No new codes, stop
            time.sleep(0.3)
        except Exception as e:
            break
    
    # If AJAX failed, try main page (only has partial data)
    if not all_codes:
        url = f"https://q.10jqka.com.cn/gn/detail/code/{raw_code}/"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                codes = re.findall(r'href="http://stockpage\.10jqka\.com\.cn/(\d{6})/"', r.text)
                all_codes.update(codes)
        except:
            pass
    
    return list(all_codes)


# ============================================================
# 3. 名称标准化 & 去重合并
# ============================================================

def normalize_name(name):
    """标准化板块名称用于匹配"""
    n = name.strip()
    # 去掉常见后缀
    for suffix in ["概念", "概念股"]:
        if n.endswith(suffix) and len(n) > len(suffix) + 1:
            n = n[:-len(suffix)]
    # 统一括号
    n = n.replace("（", "(").replace("）", ")")
    return n


def merge_boards(*board_lists):
    """
    合并多个来源的板块列表，按名称去重
    返回: [(unified_code, board_name, primary_source, [source_list])]
    """
    name_map = defaultdict(list)  # norm_name -> [(code, name, source)]
    
    for boards in board_lists:
        for code, name, source in boards:
            norm = normalize_name(name)
            name_map[norm].append((code, name, source))
    
    merged = []
    for norm_name, entries in name_map.items():
        # 优先级: ths > em > sina (金融公司优先)
        source_priority = {"ths": 0, "em": 1, "sina": 2}
        entries.sort(key=lambda x: source_priority.get(x[2], 99))
        
        primary = entries[0]
        all_sources = [e[2] for e in entries]
        all_codes = {e[2]: e[0] for e in entries}
        
        # 统一编码: 用优先级最高的源的编码
        unified_code = primary[0]
        board_name = primary[1]  # 用优先级最高源的原名
        
        merged.append((unified_code, board_name, primary[2], all_sources, all_codes))
    
    return merged


# ============================================================
# 4. 数据库操作
# ============================================================

def rebuild_concept_tables(merged_boards):
    """重建统一概念板块表"""
    conn = get_conn()
    cur = conn.cursor()
    
    # 创建统一概念板块表
    cur.execute("DROP TABLE IF EXISTS concept_boards_unified")
    cur.execute("""
    CREATE TABLE concept_boards_unified (
        board_code TEXT PRIMARY KEY,
        board_name TEXT NOT NULL,
        primary_source TEXT NOT NULL,
        all_sources TEXT,
        source_codes TEXT,
        stock_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 创建统一概念-个股关联表
    cur.execute("DROP TABLE IF EXISTS stock_concept_unified")
    cur.execute("""
    CREATE TABLE stock_concept_unified (
        stock_code TEXT NOT NULL,
        board_code TEXT NOT NULL,
        source TEXT NOT NULL,
        PRIMARY KEY (stock_code, board_code),
        FOREIGN KEY (board_code) REFERENCES concept_boards_unified(board_code)
    )
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scu_stock ON stock_concept_unified(stock_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scu_board ON stock_concept_unified(board_code)")
    
    # 插入合并后的板块
    for unified_code, name, primary_src, all_src, src_codes in merged_boards:
        import json
        cur.execute(
            "INSERT INTO concept_boards_unified (board_code, board_name, primary_source, all_sources, source_codes) VALUES (?, ?, ?, ?, ?)",
            (unified_code, name, primary_src, ",".join(all_src), json.dumps(src_codes, ensure_ascii=False))
        )
    
    conn.commit()
    conn.close()
    print(f"  写入 {len(merged_boards)} 个统一概念板块")


def rebuild_industry_tables(merged_boards):
    """重建统一行业板块表"""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS industry_boards_unified")
    cur.execute("""
    CREATE TABLE industry_boards_unified (
        board_code TEXT PRIMARY KEY,
        board_name TEXT NOT NULL,
        primary_source TEXT NOT NULL,
        all_sources TEXT,
        source_codes TEXT,
        stock_count INTEGER DEFAULT 0
    )
    """)
    
    cur.execute("DROP TABLE IF EXISTS stock_industry_unified")
    cur.execute("""
    CREATE TABLE stock_industry_unified (
        stock_code TEXT NOT NULL,
        board_code TEXT NOT NULL,
        source TEXT NOT NULL,
        PRIMARY KEY (stock_code, board_code)
    )
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_siu_stock ON stock_industry_unified(stock_code)")
    
    for unified_code, name, primary_src, all_src, src_codes in merged_boards:
        import json
        cur.execute(
            "INSERT INTO industry_boards_unified (board_code, board_name, primary_source, all_sources, source_codes) VALUES (?, ?, ?, ?, ?)",
            (unified_code, name, primary_src, ",".join(all_src), json.dumps(src_codes, ensure_ascii=False))
        )
    
    conn.commit()
    conn.close()
    print(f"  写入 {len(merged_boards)} 个统一行业板块")


# ============================================================
# 5. 成分股采集主流程
# ============================================================

def fetch_all_concept_cons():
    """采集所有概念板块的成分股"""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT board_code, board_name, primary_source, source_codes FROM concept_boards_unified ORDER BY board_code")
    boards = cur.fetchall()
    
    # 已完成的
    cur.execute("SELECT DISTINCT board_code FROM stock_concept_unified")
    done = set(r[0] for r in cur.fetchall())
    conn.close()
    
    pending = [(code, name, src, codes_json) for code, name, src, codes_json in boards if code not in done]
    
    if not pending:
        print(f"  所有概念成分股已采集完毕 ({len(boards)} 个板块)")
        return
    
    print(f"  待采集: {len(pending)} 个板块, 已完成: {len(done)}")
    
    total_added = 0
    errors = 0
    
    for i, (board_code, board_name, primary_src, codes_json) in enumerate(pending):
        import json
        src_codes = json.loads(codes_json) if codes_json else {}
        
        stocks = []
        source_used = ""
        
        # 优先用 Sina (最稳定)
        if "sina" in src_codes:
            sina_code = src_codes["sina"]
            raw_label = sina_code.replace("SINA_", "", 1)
            stocks = fetch_sina_cons(raw_label)
            source_used = "sina"
        
        # Sina 没有则尝试 THS 爬虫
        if not stocks and "ths" in src_codes:
            stocks = fetch_ths_cons_by_page(board_code, board_name)
            source_used = "ths"
        
        # EM 暂不可用
        # if not stocks and "em" in src_codes:
        #     ...
        
        if stocks:
            conn = get_conn()
            cur = conn.cursor()
            for stock_code in stocks:
                cur.execute(
                    "INSERT OR IGNORE INTO stock_concept_unified (stock_code, board_code, source) VALUES (?, ?, ?)",
                    (stock_code, board_code, source_used),
                )
                total_added += cur.rowcount
            # Update stock_count
            cur.execute("UPDATE concept_boards_unified SET stock_count=? WHERE board_code=?", (len(stocks), board_code))
            conn.commit()
            conn.close()
        else:
            errors += 1
            if errors <= 5:
                print(f"    [WARN] {board_name}({board_code}): 无法获取成分股")
        
        time.sleep(0.35)
        
        if (i + 1) % 20 == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"  [{i+1}/{len(pending)}] +{total_added} 条关联, {errors} 个失败, 剩余 {remaining}")
    
    return total_added


def fetch_all_industry_cons():
    """采集所有行业板块的成分股"""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT board_code, board_name, primary_source, source_codes FROM industry_boards_unified ORDER BY board_code")
    boards = cur.fetchall()
    
    cur.execute("SELECT DISTINCT board_code FROM stock_industry_unified")
    done = set(r[0] for r in cur.fetchall())
    conn.close()
    
    pending = [(code, name, src, codes_json) for code, name, src, codes_json in boards if code not in done]
    
    if not pending:
        print(f"  所有行业成分股已采集完毕 ({len(boards)} 个板块)")
        return
    
    print(f"  待采集: {len(pending)} 个行业板块")
    
    total_added = 0
    errors = 0
    
    for i, (board_code, board_name, primary_src, codes_json) in enumerate(pending):
        import json
        src_codes = json.loads(codes_json) if codes_json else {}
        
        stocks = []
        source_used = ""
        
        if "sina" in src_codes:
            sina_code = src_codes["sina"]
            raw_label = sina_code.replace("SINA_", "", 1)
            stocks = fetch_sina_cons(raw_label)
            source_used = "sina"
        
        if not stocks and "ths" in src_codes:
            stocks = fetch_ths_cons_by_page(board_code, board_name)
            source_used = "ths"
        
        if stocks:
            conn = get_conn()
            cur = conn.cursor()
            for stock_code in stocks:
                cur.execute(
                    "INSERT OR IGNORE INTO stock_industry_unified (stock_code, board_code, source) VALUES (?, ?, ?)",
                    (stock_code, board_code, source_used),
                )
                total_added += cur.rowcount
            cur.execute("UPDATE industry_boards_unified SET stock_count=? WHERE board_code=?", (len(stocks), board_code))
            conn.commit()
            conn.close()
        else:
            errors += 1
            if errors <= 5:
                print(f"    [WARN] {board_name}({board_code}): 无法获取成分股")
        
        time.sleep(0.35)
        
        if (i + 1) % 10 == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"  [{i+1}/{len(pending)}] +{total_added} 条关联, {errors} 个失败, 剩余 {remaining}")


# ============================================================
# 6. 报告
# ============================================================

def status_report():
    conn = get_conn()
    cur = conn.cursor()
    print("\n" + "=" * 60)
    print("  铁血哨兵 - 统一数据报告")
    print("=" * 60)
    
    for table in ["stocks", "industry_l1", "industry_l2", "industry_l3",
                  "concept_boards_unified", "industry_boards_unified",
                  "stock_concept_unified", "stock_industry_unified",
                  "stock_industry"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:35s} {cur.fetchone()[0]:>6} 条")
    
    # 按来源统计
    print("\n  概念板块 - 主数据源分布:")
    cur.execute("SELECT primary_source, COUNT(*) FROM concept_boards_unified GROUP BY primary_source")
    for row in cur.fetchall():
        print(f"    {row[0]:10s} {row[1]:>6} 个")
    
    print("\n  行业板块 - 主数据源分布:")
    cur.execute("SELECT primary_source, COUNT(*) FROM industry_boards_unified GROUP BY primary_source")
    for row in cur.fetchall():
        print(f"    {row[0]:10s} {row[1]:>6} 个")
    
    # 覆盖率
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_concept_unified")
    concept_covered = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stocks")
    total_stocks = cur.fetchone()[0]
    print(f"\n  概念关联覆盖: {concept_covered}/{total_stocks} = {concept_covered/total_stocks*100:.1f}%")
    
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_industry_unified")
    ind_covered = cur.fetchone()[0]
    print(f"  行业关联覆盖: {ind_covered}/{total_stocks} = {ind_covered/total_stocks*100:.1f}%")
    
    # 有成分股 vs 无成分股
    cur.execute("SELECT COUNT(*) FROM concept_boards_unified WHERE stock_count > 0")
    has_cons = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM concept_boards_unified")
    total = cur.fetchone()[0]
    print(f"\n  有成分股的概念板块: {has_cons}/{total} = {has_cons/total*100:.1f}%")
    
    cur.execute("SELECT COUNT(*) FROM industry_boards_unified WHERE stock_count > 0")
    has_cons_i = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM industry_boards_unified")
    total_i = cur.fetchone()[0]
    print(f"  有成分股的行业板块: {has_cons_i}/{total_i} = {has_cons_i/total_i*100:.1f}%")
    
    conn.close()
    print("=" * 60)


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    if task in ("all", "merge"):
        print("=== 1. 采集板块列表 ===")
        ths_concepts = fetch_ths_concept_list()
        ths_industries = fetch_ths_industry_list()
        sina_concepts = fetch_sina_concept_list()
        sina_industries = fetch_sina_industry_list()
        
        # EM 数据从已有 DB 读取
        conn = get_conn()
        cur = conn.cursor()
        em_concepts = []
        cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source='em'")
        for code, name in cur.fetchall():
            em_concepts.append((code, name, "em"))
        em_industries = []
        cur.execute("SELECT board_code, board_name FROM industry_boards WHERE board_code LIKE 'EM_%' OR board_code NOT LIKE 'SINA_%'")
        for code, name in cur.fetchall():
            if not code.startswith("SINA_"):
                em_industries.append((code, name, "em"))
        conn.close()
        print(f"  [EM] 概念(DB缓存): {len(em_concepts)} 个")
        print(f"  [EM] 行业(DB缓存): {len(em_industries)} 个")
        
        print(f"\n=== 2. 合并去重 ===")
        merged_concepts = merge_boards(ths_concepts, em_concepts, sina_concepts)
        print(f"  合并后概念: {merged_concepts.__len__()} 个 (去重前: {len(ths_concepts)+len(em_concepts)+len(sina_concepts)})")
        
        merged_industries = merge_boards(ths_industries, em_industries, sina_industries)
        print(f"  合并后行业: {len(merged_industries)} 个 (去重前: {len(ths_industries)+len(em_industries)+len(sina_industries)})")
        
        print(f"\n=== 3. 写入数据库 ===")
        rebuild_concept_tables(merged_concepts)
        rebuild_industry_tables(merged_industries)
        
    if task in ("all", "cons"):
        print("\n=== 4. 采集概念成分股 ===")
        fetch_all_concept_cons()
        
        print("\n=== 5. 采集行业成分股 ===")
        fetch_all_industry_cons()
    
    if task == "status":
        status_report()
    
    if task == "all":
        status_report()
