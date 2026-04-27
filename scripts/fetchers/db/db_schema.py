# -*- coding: utf-8 -*-
"""
铁血哨兵 - SQLite 数据库 Schema v3.0（最终版）
与设计文档 v3.0 完全一致

关键变更记录：
  v3.0:
    - concept_boards PK=(board_code, source)  # 三源共存
    - industry_boards PK=(board_code, source)
    - stock_concept PK=(stock_code, board_code, source)
    - stock_industry_board PK=(stock_code, board_code)  # 去掉 source，允许多行业
    - 统一 fetched_at 字段
"""

import sqlite3
import os

# 数据目录规范 v1.0 - 统一路径，禁止随意变更
# 文档: ~/.qclaw/skills/iron-sentinel/DATA_DIR_SPEC.md
SKILL_DIR = os.path.expanduser("~/.qclaw/skills/iron-sentinel")
DATA_DIR = os.path.join(SKILL_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "stock_data.db")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化数据库（幂等）"""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stocks (
        stock_code TEXT PRIMARY KEY,
        stock_name TEXT NOT NULL,
        listing_status TEXT DEFAULT 'Normal',
        list_date TEXT,
        exchange TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS industry_l1 (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS industry_l2 (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        l1_code TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS industry_l3 (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        l2_code TEXT
    )
    """)

    # PK=(board_code, source) — 三源独立板块，可共存
    cur.execute("""
    CREATE TABLE IF NOT EXISTS concept_boards (
        board_code TEXT NOT NULL,
        source TEXT NOT NULL,
        board_name TEXT NOT NULL,
        stock_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (board_code, source)
    )
    """)

    # PK=(board_code, source)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS industry_boards (
        board_code TEXT NOT NULL,
        source TEXT NOT NULL,
        board_name TEXT NOT NULL,
        stock_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (board_code, source)
    )
    """)

    # 申万行业关联：L1/L2/L3 每级各一条
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_industry (
        stock_code TEXT NOT NULL,
        level TEXT NOT NULL,
        industry_code TEXT NOT NULL,
        fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (stock_code, level, industry_code)
    )
    """)

    # 概念关联：PK=(stock_code, board_code, source) — 三源独立存储
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_concept (
        stock_code TEXT NOT NULL,
        board_code TEXT NOT NULL,
        source TEXT NOT NULL,
        fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (stock_code, board_code, source)
    )
    """)

    # 证监会行业关联：PK=(stock_code, board_code) — 一股可属多行业（去掉了 source）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_industry_board (
        stock_code TEXT NOT NULL,
        board_code TEXT NOT NULL,
        source TEXT NOT NULL,
        fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (stock_code, board_code)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stocks_name ON stocks(stock_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stocks_exchange ON stocks(exchange)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cb_source ON concept_boards(source)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ib_source ON industry_boards(source)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_l3_l2 ON industry_l3(l2_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_l2_l1 ON industry_l2(l1_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_si_stock ON stock_industry(stock_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sc_stock ON stock_concept(stock_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sc_source ON stock_concept(source)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sib_stock ON stock_industry_board(stock_code)")

    conn.commit()
    conn.close()
    print(f"[DB] 数据库初始化完成: {DB_PATH}")


if __name__ == "__main__":
    init_db()
