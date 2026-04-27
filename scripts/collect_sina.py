#!/usr/bin/env python3
"""
Sina 概念+行业 成分股采集（支持断点续传）
DB: /Users/totti/.qclaw/skills/iron-sentinel/stock_data.db
"""
import sys, os, time, sqlite3

DB = '/Users/totti/.qclaw/skills/iron-sentinel/stock_data.db'
SLEEP = 0.35
BATCH = 20

def get_conn():
    return sqlite3.connect(DB)

def fetch_sector_cons(sector_label):
    import akshare as ak
    try:
        df = ak.stock_sector_detail(sector=sector_label)
        if df is None or len(df) == 0:
            return []
        return [str(row.get("code","")).zfill(6) for _, row in df.iterrows()
                if len(str(row.get("code","")).zfill(6)) == 6]
    except:
        return []

def fetch_concept_relations():
    """采集新浪概念成分股"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source='sina' ORDER BY board_code")
    all_boards = cur.fetchall()
    cur.execute("SELECT DISTINCT board_code FROM stock_concept WHERE source='sina'")
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(c, n) for c, n in all_boards if c not in done]
    if not pending:
        print(f"[Sina-concept] 全部完成 ({len(all_boards)} 个板块)")
        return 0

    total_added = 0
    errors = 0
    for i, (bc, bn) in enumerate(pending):
        label = bc.replace("SINA_", "", 1)
        try:
            stocks = fetch_sector_cons(label)
            conn2 = get_conn()
            cur2 = conn2.cursor()
            for sc in stocks:
                cur2.execute("INSERT OR IGNORE INTO stock_concept (stock_code,board_code,source) VALUES (?,?,'sina')",
                             (sc, bc))
                total_added += cur2.rowcount
            cur2.execute("UPDATE concept_boards SET stock_count=? WHERE board_code=? AND source='sina'",
                         (len(stocks), bc))
            conn2.commit()
            conn2.close()
            time.sleep(SLEEP)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Error {bn}: {e}")
            time.sleep(1)

        if (i+1) % BATCH == 0 or i == len(pending)-1:
            print(f"[Sina-concept] {i+1}/{len(pending)} 板块, +{total_added} 条, 错{errors}, 剩{len(pending)-i-1}")

    return len(pending)

def fetch_industry_relations():
    """采集新浪行业(证监会)成分股"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT board_code, board_name FROM industry_boards WHERE source='sina' ORDER BY board_code")
    all_boards = cur.fetchall()
    cur.execute("SELECT DISTINCT board_code FROM stock_industry_board WHERE source='sina'")
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(c, n) for c, n in all_boards if c not in done]
    if not pending:
        print(f"[Sina-industry] 全部完成 ({len(all_boards)} 个板块)")
        return 0

    total_added = 0
    errors = 0
    for i, (bc, bn) in enumerate(pending):
        label = bc.replace("SINA_hangye_", "", 1)
        try:
            stocks = fetch_sector_cons(label)
            conn2 = get_conn()
            cur2 = conn2.cursor()
            for sc in stocks:
                cur2.execute("INSERT OR IGNORE INTO stock_industry_board (stock_code,board_code,source) VALUES (?,?,'sina')",
                             (sc, bc))
                total_added += cur2.rowcount
            cur2.execute("UPDATE industry_boards SET stock_count=? WHERE board_code=? AND source='sina'",
                         (len(stocks), bc))
            conn2.commit()
            conn2.close()
            time.sleep(SLEEP)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Error {bn}: {e}")
            time.sleep(1)

        if (i+1) % BATCH == 0 or i == len(pending)-1:
            print(f"[Sina-industry] {i+1}/{len(pending)} 板块, +{total_added} 条, 错{errors}, 剩{len(pending)-i-1}")

    return len(pending)

def status():
    conn = get_conn()
    cur = conn.cursor()
    print("\n" + "="*50)
    for t in ['stocks','concept_boards','industry_boards','stock_concept','stock_industry_board']:
        n = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {n:,}")
    print("\n  stock_concept by source:")
    for r in cur.execute("SELECT source,COUNT(*) FROM stock_concept GROUP BY source").fetchall():
        print(f"    {r[0]}: {r[1]:,}")
    print("  stock_industry_board by source:")
    for r in cur.execute("SELECT source,COUNT(*) FROM stock_industry_board GROUP BY source").fetchall():
        print(f"    {r[0]}: {r[1]:,}")
    conn.close()
    print("="*50)

if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "all"
    if task == "all":
        print("=== Sina 概念成分股 ===")
        while fetch_concept_relations(): time.sleep(2)
        print("=== Sina 行业成分股 ===")
        while fetch_industry_relations(): time.sleep(2)
        status()
    elif task == "concept":
        while fetch_concept_relations(): time.sleep(2)
        status()
    elif task == "industry":
        while fetch_industry_relations(): time.sleep(2)
        status()
    elif task == "status":
        status()
