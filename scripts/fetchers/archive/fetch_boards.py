#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁血哨兵 - 板块成分股采集（EM固定数据源）
策略：
  1. push2 API被封锁，通过xbrowser的Chrome执行JS fetch绕过
  2. 行业板块：AKShare直接API（已验证可用）
  3. 概念板块：xbrowser async JS fetch（每批20个，断点续传）
  4. cron监督自动续跑
"""
import subprocess, json, time, sqlite3, os, sys

XB = '/Users/totti/Library/Application Support/QClaw/openclaw/config/skills/xbrowser/scripts/xb.cjs'
NODE = 'node'
DB = '/Users/totti/.qclaw/skills/iron-sentinel/stock_data.db'

def xb_run(cmd_list, timeout=25):
    try:
        r = subprocess.run([NODE, XB, 'run', '--browser', 'chrome'] + cmd_list,
                          capture_output=True, text=True, timeout=timeout)
        return json.loads(r.stdout)
    except:
        return {'ok': False}

def xb_eval(js_code):
    """解析xb eval结果：d['data']['result']['data'] = JS返回值"""
    resp = xb_run(['eval', js_code], timeout=30)
    if not resp.get('ok'): return None
    outer = resp.get('data', {})
    inner = outer.get('result', {})
    js_val = inner.get('data')
    if isinstance(js_val, str):
        try: return json.loads(js_val)
        except: return js_val
    return js_val

def xb_open(url, timeout=20):
    return xb_run(['open', url, '--timeout', str(timeout)])

def fetch_via_js(board_code):
    """用xbrowser的Chrome执行async JS fetch获取成分股"""
    # 打开东方财富任意页面（提供正确的cookie/CDN上下文）
    xb_run(['open', 'https://data.eastmoney.com/', '--timeout', '10000'])
    time.sleep(3)
    
    # 在页面内用fetch调用push2 API（Chrome能访问，Python不能）
    js = """async () => {
        try {
            const url = 'https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=500&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f12&fs=b%3A""" + board_code + """%2Cf%3A%2150&fields=f12%2Cf14';
            const resp = await fetch(url);
            const text = await resp.text();
            return text.substring(0, 5000);
        } catch(e) { return 'error: ' + e.message; }
    }"""
    
    result = xb_eval(js)
    if not result:
        return []
    
    if isinstance(result, dict) and 'data' in result:
        # 嵌套在data.data里
        result = result.get('data', '')
    
    if isinstance(result, str) and result.startswith('{'):
        try:
            j = json.loads(result)
            if j.get('data') and j['data'].get('diff'):
                codes = [str(item.get('f12','')).zfill(6) for item in j['data']['diff'] if item.get('f12')]
                return codes
        except:
            pass
    return []

def fetch_industry_boards_ak():
    """用AKShare采集行业板块成分股（直接API，已验证可用）"""
    import warnings, akshare as ak
    warnings.filterwarnings('ignore')
    
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    
    # 获取所有行业板块
    cur.execute("SELECT board_code, board_name FROM industry_boards ORDER BY board_code")
    boards = cur.fetchall()
    
    added = 0
    for code, name in boards:
        try:
            df = ak.stock_board_industry_cons_em(symbol=name)
            if df is not None and len(df) > 0:
                codes = df.iloc[:, 0].astype(str).str.zfill(6).tolist()
                for sc in codes:
                    cur.execute(
                        "INSERT OR REPLACE INTO stock_industry_board (stock_code, board_code) VALUES (?, ?)",
                        (sc, code))
                    added += 1
            time.sleep(0.3)
        except Exception as e:
            print(f'  {name}: {str(e)[:40]}')
    
    conn.commit()
    conn.close()
    print(f'  行业板块成分股: +{added} 条')
    return added

def batch_fetch_concepts(source='em', batch_size=20):
    """采集一批概念板块成分股（断点续传）"""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    
    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source=? ORDER BY board_code", (source,))
    all_boards = [(r[0], r[1]) for r in cur.fetchall()]
    
    cur.execute("SELECT DISTINCT board_code FROM stock_concept WHERE source=?", (source,))
    done = set(r[0] for r in cur.fetchall())
    conn.close()
    
    pending = [(c, n) for c, n in all_boards if c not in done]
    if not pending:
        print(f'  [{source}] 全部完成 ({len(all_boards)} 板块)')
        return 0
    
    batch = pending[:batch_size]
    total_added = 0
    
    for i, (code, name) in enumerate(batch):
        print(f'  [{i+1}/{len(batch)}] {name} ({code})', end=' ', flush=True)
        codes = fetch_via_js(code)
        if codes:
            conn = sqlite3.connect(DB)
            cur = conn.cursor()
            for sc in codes:
                cur.execute(
                    "INSERT OR IGNORE INTO stock_concept (stock_code, board_code, source) VALUES (?, ?, ?)",
                    (sc, code, source))
                total_added += 1
            conn.commit()
            conn.close()
            print(f'+{len(codes)}')
        else:
            print('无数据')
        time.sleep(2)
    
    remaining = len(pending) - len(batch)
    print(f'  +{total_added} 条关联，剩余 {remaining} 个板块')
    return remaining

def status():
    """数据状态报告"""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    print('\n=== 铁血哨兵数据状态 ===')
    for t in ['stocks', 'industry_l1', 'industry_l2', 'industry_l3',
              'concept_boards', 'industry_boards',
              'stock_industry', 'stock_concept', 'stock_industry_board']:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'  {t}: {cur.fetchone()[0]}')
    # 覆盖率
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_concept")
    covered = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stocks")
    total = cur.fetchone()[0]
    print(f'  概念关联覆盖: {covered}/{total} = {covered/total*100:.1f}%')
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_industry_board")
    ib = cur.fetchone()[0]
    print(f'  行业关联覆盖: {ib}/{total} = {ib/total*100:.1f}%')
    conn.close()

if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'status'
    if cmd == 'status':
        status()
    elif cmd == 'industry':
        fetch_industry_boards_ak()
    elif cmd == 'concept':
        source = sys.argv[2] if len(sys.argv) > 2 else 'em'
        batch = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        remaining = batch_fetch_concepts(source, batch)
        status()
    elif cmd == 'all':
        print('=== 采集行业板块成分股 ===')
        fetch_industry_boards_ak()
        print('\n=== 采集概念板块成分股 ===')
        remaining = batch_fetch_concepts('em', 20)
        while remaining > 0:
            remaining = batch_fetch_concepts('em', 20)
        status()
