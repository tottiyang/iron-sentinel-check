#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
THS数据月度更新脚本
每月第一个周六02:00执行，更新所有个股的概念关联

功能：
1. 更新所有个股的概念数据（从THS抓取）
2. 更新概念板块的成分股数量
3. 记录更新日志到meta表
"""

import sys
import time
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

# 导入核心模块
sys.path.insert(0, str(Path(__file__).parent))
from fetch_ths_core import (
    get_all_stocks,
    process_single_stock,
    get_db_conn,
    fetch_ths_concepts_with_xbrowser,
    ensure_concept_board,
    DEFAULT_SLEEP
)


LOG_FILE = Path("/tmp/ths_monthly_update.log")


def log(msg: str):
    """打印并记录日志"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def update_board_stock_counts():
    """更新所有概念板块的成分股数量"""
    conn = get_db_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE concept_boards 
        SET stock_count = (
            SELECT COUNT(*) FROM stock_concept sc 
            WHERE sc.board_code = concept_boards.board_code
        )
    """)
    
    updated = cursor.rowcount
    conn.commit()
    conn.close()
    log(f"更新了 {updated} 个概念板块的成分股数量")
    return updated


def get_last_update_time() -> str:
    """获取上次更新时间"""
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM meta WHERE key = 'last_ths_update'")
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else "从未更新"


def set_last_update_time():
    """记录本次更新时间"""
    conn = get_db_conn()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        "INSERT OR REPLACE INTO meta (key, value, updated_at) VALUES ('last_ths_update', ?, ?)",
        (now, now)
    )
    conn.commit()
    conn.close()


def is_first_saturday() -> bool:
    """判断今天是否是该月的第一个周六"""
    today = datetime.now()
    if today.weekday() != 5:  # 0=周一, 5=周六
        return False
    # 该月第一个周六的日期一定在1-7日内
    return today.day <= 7


def run_full_update(batch_size: int = 50, sleep_between: float = DEFAULT_SLEEP):
    """执行全量更新
    
    Args:
        batch_size: 每多少只股票输出一次进度
        sleep_between: 每次抓取间隔（秒）
    """
    # 检查是否是该月的第一个周六
    if not is_first_saturday():
        log("今天不是该月的第一个周六，跳过执行")
        return None
    
    start_time = time.time()
    log("=" * 50)
    log("THS月度更新开始")
    log(f"上次更新: {get_last_update_time()}")
    
    # 获取所有股票
    stocks = get_all_stocks(exclude_bj=True)
    total = len(stocks)
    log(f"待处理股票数: {total}")
    
    success_count = 0
    fail_count = 0
    no_concept_count = 0
    
    for i, (code, name) in enumerate(stocks):
        success, concepts, msg = process_single_stock(code, name)
        
        if success:
            if msg == "ok":
                success_count += 1
            elif msg == "no_concepts":
                no_concept_count += 1
        else:
            fail_count += 1
        
        # 定期输出进度
        if (i + 1) % batch_size == 0 or (i + 1) == total:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total - i - 1) / rate if rate > 0 else 0
            log(f"进度: {i+1}/{total} | 成功: {success_count} | 无概念: {no_concept_count} | 失败: {fail_count} | 速度: {rate:.1f}/s | 预计剩余: {eta/60:.1f}分钟")
        
        time.sleep(sleep_between)
    
    # 更新板块成分股数量
    log("更新概念板块成分股数量...")
    update_board_stock_counts()
    
    # 记录更新时间
    set_last_update_time()
    
    # 汇总
    elapsed = time.time() - start_time
    log("=" * 50)
    log(f"THS月度更新完成!")
    log(f"总计: {total} | 成功: {success_count} | 无概念: {no_concept_count} | 失败: {fail_count}")
    log(f"耗时: {elapsed/60:.1f}分钟")
    
    return {
        "total": total,
        "success": success_count,
        "no_concept": no_concept_count,
        "fail": fail_count,
        "elapsed_minutes": elapsed / 60
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="THS月度更新脚本")
    parser.add_argument("--batch-size", type=int, default=50, help="进度输出频率")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="抓取间隔(秒)")
    parser.add_argument("--test", action="store_true", help="测试模式，只处理10只股票")
    args = parser.parse_args()
    
    if args.test:
        # 测试模式：只处理前10只股票
        stocks = get_all_stocks(exclude_bj=True)[:10]
        for code, name in stocks:
            print(f"\n处理 {code} {name}...")
            success, concepts, msg = process_single_stock(code, name)
            print(f"  结果: {msg}, 概念数: {len(concepts)}")
            if concepts:
                print(f"  概念: {concepts[:5]}...")
            time.sleep(args.sleep)
    else:
        run_full_update(batch_size=args.batch_size, sleep_between=args.sleep)
