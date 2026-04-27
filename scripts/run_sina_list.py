#!/usr/bin/env python3
import sys, os
sys.path.insert(0, '/Users/totti/.qclaw/skills/iron-sentinel/scripts/fetchers/db')
sys.path.insert(0, '/Users/totti/.qclaw/skills/iron-sentinel/scripts/fetchers/sina')

import db_schema, akshare as ak

# Sina 板块列表同步
print("=== 新浪板块列表同步 ===")
# 概念
df_con = ak.stock_sector_spot(indicator="概念")
print(f"概念板块: {len(df_con)}")
conn = db_schema.get_conn()
cur = conn.cursor()
for _, row in df_con.iterrows():
    label = str(row.get("label","")).strip()
    name = str(row.get("板块","")).strip()
    cnt = int(row.get("公司家数", 0))
    if label and name:
        cur.execute("""INSERT OR IGNORE INTO concept_boards (board_code,board_name,stock_count,source)
                        VALUES (?,?,'sina',?)""", (f"SINA_{label}", name, cnt))
conn.commit()

# 行业
df_ind = ak.stock_sector_spot(indicator="行业")
print(f"行业板块: {len(df_ind)}")
for _, row in df_ind.iterrows():
    label = str(row.get("label","")).strip()
    name = str(row.get("板块","")).strip()
    cnt = int(row.get("公司家数", 0))
    if label and name:
        cur.execute("""INSERT OR IGNORE INTO industry_boards (board_code,board_name,stock_count,source)
                        VALUES (?,?,'sina',?)""", (f"SINA_hangye_{label}", name, cnt))
conn.commit()
conn.close()
print("板块列表同步完成")

# 清空旧 stock_industry_board 准备重新采集
conn2 = db_schema.get_conn()
cur2 = conn2.cursor()
cur2.execute("DELETE FROM stock_industry_board WHERE source='sina'")
cur2.execute("DELETE FROM stock_concept WHERE source='sina'")
conn2.commit()
print(f"清空 Sina 旧数据: stock_industry_board={cur2.execute('SELECT COUNT(*) FROM stock_industry_board').fetchone()[0]}, stock_concept={cur2.execute('SELECT COUNT(*) FROM stock_concept WHERE source=sina').fetchone()[0]}")
conn2.close()
