#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
THS概念爬取核心模块
封装xbrowser抓取同花顺个股概念页面的核心函数
"""

import sqlite3
import subprocess
import json
import time
import shlex
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple

# 配置路径
SKILL_DIR = Path(__file__).parent.parent
DB_PATH = SKILL_DIR / "stock_data.db"
XBROWSER_PATH = Path.home() / "Library/Application Support/QClaw/openclaw/config/skills/xbrowser/scripts/xb.cjs"
NODE_BINARY = "node"

# 默认参数
DEFAULT_TIMEOUT = 30
DEFAULT_RETRY = 2
DEFAULT_SLEEP = 1.5


def get_db_conn():
    """获取数据库连接"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_all_stocks(exclude_bj: bool = True) -> List[Tuple[str, str]]:
    """获取所有股票代码和名称
    
    Args:
        exclude_bj: 是否排除北交所/新三板（代码以83/87/88/43开头）
    
    Returns:
        [(stock_code, stock_name), ...]
    """
    conn = get_db_conn()
    cur = conn.cursor()
    
    query = "SELECT stock_code, stock_name FROM stocks WHERE listing_status = 'Normal'"
    if exclude_bj:
        query += " AND exchange NOT IN ('BJ', 'UNKNOWN')"
    
    cur.execute(query)
    result = [(row[0], row[1]) for row in cur.fetchall()]
    conn.close()
    return result


def fetch_ths_concepts_with_xbrowser(stock_code: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[List[str]]:
    """使用xbrowser抓取单只股票的THS概念
    
    Args:
        stock_code: 股票代码，如 '000001'
        timeout: 超时秒数
    
    Returns:
        概念名称列表，失败返回None，无概念返回空列表[]
    """
    url = f"http://basic.10jqka.com.cn/{stock_code}/concept.html"
    
    # JS代码：提取第一个表格的第二列（概念名称）
    js_code = r"""(() => {
        const concepts = [];
        const tables = document.querySelectorAll('table');
        if (tables.length > 0) {
            const firstTable = tables[0];
            const rows = firstTable.querySelectorAll('tr');
            for (const row of rows) {
                const cells = row.querySelectorAll('td');
                if (cells.length >= 2) {
                    const conceptName = cells[1]?.textContent?.trim();
                    if (conceptName && conceptName.length > 0 && conceptName.length < 30) {
                        const cleanName = conceptName.replace(/\s+/g, ' ').trim();
                        const hasChinese = cleanName.split('').some(c => {
                            const code = c.charCodeAt(0);
                            return code >= 0x4e00 && code <= 0x9fff;
                        });
                        if (cleanName && !/^\d+$/.test(cleanName) && 
                            !cleanName.includes('龙头') &&
                            !cleanName.includes('展开') &&
                            !cleanName.includes('最近') &&
                            !cleanName.includes('详情') &&
                            !cleanName.startsWith('公司') &&
                            !cleanName.startsWith('根据') &&
                            hasChinese) {
                            concepts.push(cleanName);
                        }
                    }
                }
            }
        }
        return [...new Set(concepts)];
    })()"""
    
    cmd = [
        NODE_BINARY, str(XBROWSER_PATH),
        "run", "--browser", "chrome", "--headed",
        "batch", "--bail",
        f"open '{url}'",
        "wait --load networkidle",
        f"eval {shlex.quote(js_code)}"
    ]
    
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        if result.returncode != 0:
            return None
        
        data = json.loads(result.stdout)
        if not data.get("ok"):
            return None
        
        results = data.get("data", {}).get("result", [])
        if len(results) < 3:
            return None
        
        eval_result = results[2]
        if not eval_result.get("success"):
            return None
        
        concepts = eval_result.get("result", {}).get("result", [])
        return concepts if isinstance(concepts, list) else []
        
    except Exception:
        return None


def ensure_concept_board(cursor, board_name: str) -> str:
    """确保概念板块存在，返回board_code"""
    # 先查询
    cursor.execute("SELECT board_code FROM concept_boards WHERE board_name = ?", (board_name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    
    # 创建新板块（使用名称的hash作为code）
    board_code = f"THS_{abs(hash(board_name)) % 1000000:06d}"
    cursor.execute(
        "INSERT OR IGNORE INTO concept_boards (board_code, board_name) VALUES (?, ?)",
        (board_code, board_name)
    )
    return board_code


def update_stock_concepts(stock_code: str, concepts: List[str]) -> int:
    """更新单只股票的概念关联
    
    Args:
        stock_code: 股票代码
        concepts: 概念名称列表
    
    Returns:
        新增的概念数量
    """
    if not concepts:
        return 0
    
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # 先删除旧关联
    cursor.execute("DELETE FROM stock_concept WHERE stock_code = ?", (stock_code,))
    
    # 插入新关联
    added = 0
    for concept_name in concepts:
        board_code = ensure_concept_board(cursor, concept_name)
        cursor.execute(
            "INSERT OR IGNORE INTO stock_concept (stock_code, board_code) VALUES (?, ?)",
            (stock_code, board_code)
        )
        if cursor.rowcount > 0:
            added += 1
    
    conn.commit()
    conn.close()
    return added


def process_single_stock(stock_code: str, stock_name: str = "", retry: int = DEFAULT_RETRY) -> Tuple[bool, List[str], str]:
    """处理单只股票的概念抓取和入库
    
    Args:
        stock_code: 股票代码
        stock_name: 股票名称（可选）
        retry: 重试次数
    
    Returns:
        (success, concepts, error_msg)
    """
    for attempt in range(retry + 1):
        concepts = fetch_ths_concepts_with_xbrowser(stock_code)
        
        if concepts is not None:
            break
        time.sleep(0.5)
    
    if concepts is None:
        return False, [], "fetch_failed"
    
    if not concepts:
        return True, [], "no_concepts"
    
    # 更新数据库
    added = update_stock_concepts(stock_code, concepts)
    
    return True, concepts, "ok"


if __name__ == "__main__":
    # 测试：抓取单只股票
    import sys
    if len(sys.argv) > 1:
        code = sys.argv[1]
        print(f"测试抓取 {code}...")
        success, concepts, msg = process_single_stock(code)
        print(f"结果: {msg}")
        print(f"概念: {concepts}")
