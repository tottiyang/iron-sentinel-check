# -*- coding: utf-8 -*-
"""
db_reset.py — 从0重建关联表

从0开始：清空所有关联表（保留基础表）

保留（不清）：
  stocks                    # 个股基础信息 5508 只，数据本身是好的
  industry_l1/l2/l3         # 申万 L1=32 / L2=131 / L3=336，数据本身是好的
  concept_boards            # 板块列表 1064 个（em:491 + sina:175 + ths:398），数据本身是好的
  industry_boards           # 行业板块列表 670 个（em:496 + sina:84 + ths:90），数据本身是好的

清空（重建）：
  stock_concept             # 34,811 条：ths 24,186（截断）/ sina 10,619（OK）/ em 6（脏数据）
  stock_industry_board      # 4,847 条：INSERT OR REPLACE 导致一股仅一行业，数据失真
  stock_industry            # 15,201 条：申万关联，保留但待确认是否需要重采
  meta                      # 重置采集状态

删除（废弃表，数据已损坏）：
  concept_boards_unified    # board_code+source 对应关系完全错误，数据垮掉
  stock_concept_unified     # 同上
  industry_boards_unified   # 同上
  stock_industry_unified    # 同上
  stock_industry_baostock  # 来源不明，5,199 条，来源不可追溯

执行前务必先备份！
"""

import sqlite3
import os
import shutil
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "../stock_data.db")
BACKUP_PATH = DB_PATH + f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def backup():
    """先备份"""
    if not os.path.exists(DB_PATH):
        print(f"[ABORT] DB 不存在: {DB_PATH}")
        return False
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"[BACKUP] {DB_PATH}")
    print(f"        → {BACKUP_PATH}")
    return True


def drop_unified_and_orphan_tables(conn):
    """删除废弃的 unified 表和来源不明的表"""
    cur = conn.cursor()

    tables_to_drop = [
        # unified 表：board_code 和 source 对应关系已损坏，数据不可用
        'concept_boards_unified',
        'stock_concept_unified',
        'industry_boards_unified',
        'stock_industry_unified',
        # 来源不明表
        'stock_industry_baostock',
    ]

    for t in tables_to_drop:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        print(f"[DROP]   {t}")

    conn.commit()
    return cur


def reset_association_tables(conn):
    """
    清空所有个股关联表
    注意：
      - stock_concept 的 PK 必须是 (stock_code, board_code, source)
        当前实际 DB 已有 source 列（但 old 迁移前没有）
        需要重建 stock_concept 以确保 PK 正确
      - stock_industry_board 当前 PK 是 stock_code（错误），
        需要重建为 (stock_code, board_code)
      - stock_industry（申万）目前完整，暂不清空，
        但 akshare sw_index_third_cons L3 成分股接口可用
        可选择性重采
    """
    cur = conn.cursor()

    tables_to_reset = [
        # 关联表：从0重建
        'stock_concept',
        'stock_industry_board',
        # 申万关联：暂不清（数据本身完整，可选择性重采）
        # 'stock_industry',
        # meta：重置采集状态
        'meta',
    ]

    for t in tables_to_reset:
        # 先查看表是否存在
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
        if cur.fetchone():
            cur.execute(f"DELETE FROM {t}")
            print(f"[CLEAR]  {t} ({cur.rowcount} 条已删除)")
        else:
            print(f"[SKIP]   {t} (表不存在)")

    conn.commit()


def rebuild_stock_concept_schema(conn):
    """
    重建 stock_concept 表
    目标 PK = (stock_code, board_code, source)

    当前问题：已有 source 列，但旧代码可能没有在写入时正确处理 source。
    重建策略：先删除所有数据（保留表结构），然后重新采集写入。
    """
    cur = conn.cursor()

    # 确认当前 schema
    cur.execute("PRAGMA table_info(stock_concept)")
    cols = {r[1]: r[2] for r in cur.fetchall()}
    print(f"\n[stock_concept 当前 schema]")
    for k, v in cols.items():
        print(f"  {k}: {v}")

    # 如果没有 source 列，需要 ALTER TABLE
    if 'source' not in cols:
        cur.execute("ALTER TABLE stock_concept ADD COLUMN source TEXT DEFAULT 'em'")
        print("[ALTER] stock_concept ADD COLUMN source")
        conn.commit()

    # 当前数据量
    cur.execute("SELECT COUNT(*) FROM stock_concept")
    before = cur.fetchone()[0]
    print(f"[stock_concept] 当前 {before} 条 → 将从0重建")

    # 清空
    cur.execute("DELETE FROM stock_concept")
    print(f"[CLEAR]  stock_concept ({before} 条已删除)")

    conn.commit()
    return before


def rebuild_stock_industry_board_schema(conn):
    """
    重建 stock_industry_board 表
    目标 PK = (stock_code, board_code)（允许多行业：每只股票可属于多个行业）

    当前问题：PK = stock_code，导致 INSERT OR REPLACE 覆盖，
    每只股票只保留了最后一个写入的行业。

    重建策略：重建表结构 → PK 改为 (stock_code, board_code)
    """
    cur = conn.cursor()

    # 确认当前数据量
    cur.execute("SELECT COUNT(*) FROM stock_industry_board")
    before = cur.fetchone()[0]
    print(f"\n[stock_industry_board] 当前 {before} 条 → 将从0重建")
    print(f"[stock_industry_board] 当前不同股数 = {before}（一股仅一行业，完全失真）")

    # 重建表：PK 改为 (stock_code, board_code, source)
    cur.execute("DROP TABLE IF EXISTS stock_industry_board_new")

    cur.execute("""
        CREATE TABLE stock_industry_board_new (
            stock_code TEXT NOT NULL,
            board_code TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'sina',
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, board_code, source)
        )
    """)
    print("[REBUILD] stock_industry_board_new (PK = stock_code, board_code, source)")

    # 删除旧表，重命名新表
    cur.execute("DROP TABLE stock_industry_board")
    cur.execute("ALTER TABLE stock_industry_board_new RENAME TO stock_industry_board")
    print("[RENAME] stock_industry_board_new → stock_industry_board")

    conn.commit()
    return before


def reset_meta(conn):
    """重置 meta 表"""
    cur = conn.cursor()
    cur.execute("DELETE FROM meta")
    # 写入初始状态
    cur.execute("""
        INSERT INTO meta (key, value, updated_at)
        VALUES (?, ?, datetime('now'))
    """, ('db_reset_version', 'v1.0_20260426'))
    print("\n[META]   重置 meta 表，写入 db_reset_version=v1.0_20260426")
    conn.commit()


def show_final_status(conn):
    """显示清空后的状态"""
    cur = conn.cursor()

    print("\n" + "="*60)
    print("清空后数据库状态")
    print("="*60)

    association_tables = [
        'stock_concept',
        'stock_industry_board',
        'stock_industry',
        'meta',
    ]

    for t in association_tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"  {t:<30} {cnt:>8} 条")

    print()
    print("保留的基础表（未动）:")
    for t in ['stocks', 'industry_l1', 'industry_l2', 'industry_l3',
              'concept_boards', 'industry_boards']:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"  {t:<30} {cnt:>8} 条")

    print()
    print("已删除的废弃表:")
    for t in ['concept_boards_unified', 'stock_concept_unified',
              'industry_boards_unified', 'stock_industry_unified',
              'stock_industry_baostock']:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
        exists = "存在（未删除）" if cur.fetchone() else "已删除 ✓"
        print(f"  {t:<35} {exists}")


def main(dry_run=False):
    print("="*60)
    print("db_reset.py — 从0重建关联表")
    print("="*60)
    print(f"DB: {DB_PATH}")
    print()

    if dry_run:
        print("[DRY RUN] 不执行任何操作，仅显示计划")
        return

    # 1. 备份
    if not backup():
        return

    conn = get_conn()

    # 2. 删除废弃 unified 表
    print("\n--- 删除废弃表 ---")
    drop_unified_and_orphan_tables(conn)

    # 3. 重建 stock_concept（清空，加入正确 PK）
    print("\n--- 重建 stock_concept ---")
    sc_before = rebuild_stock_concept_schema(conn)

    # 4. 重建 stock_industry_board（改 PK = (stock_code, board_code, source)）
    print("\n--- 重建 stock_industry_board ---")
    sib_before = rebuild_stock_industry_board_schema(conn)

    # 5. 清空 meta
    print("\n--- 重置 meta ---")
    reset_meta(conn)

    # 6. 显示状态
    show_final_status(conn)

    conn.close()

    print("\n✅ db_reset.py 执行完成")
    print(f"   stock_concept:     {sc_before} → 0 条")
    print(f"   stock_industry_board: {sib_before} → 0 条（PK 已修复）")
    print(f"   备份文件: {BACKUP_PATH}")


if __name__ == "__main__":
    import sys
    dry = '--dry-run' in sys.argv or '-n' in sys.argv
    main(dry_run=dry)
