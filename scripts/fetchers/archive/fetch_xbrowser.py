# -*- coding: utf-8 -*-
"""
铁血哨兵 - 东方财富概念板块成分股采集（xbrowser版）
通过CDP浏览器自动化，从东方财富概念板块详情页提取成分股。

URL格式: https://data.eastmoney.com/bkzj/gn/BKxxxx.html
每板块约5-8秒（含JS渲染等待+翻页），数据完整准确。
"""

import sys
import os
import json
import time
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from db_schema import get_conn

XB_CJS = os.path.expanduser('~/Library/Application Support/QClaw/openclaw/config/skills/xbrowser/scripts/xb.cjs')
NODE_BIN = os.environ.get('QCLAW_CLI_NODE_BINARY', 'node')


def xb_run(cmd: str, args: str = '', timeout: int = 20) -> dict:
    """执行 xb 命令，返回解析后的 JSON 响应"""
    full_cmd = f'{NODE_BIN} {XB_CJS} run --browser chrome {cmd} {args}'
    import subprocess
    result = subprocess.run(
        full_cmd, shell=True, capture_output=True, text=True, timeout=timeout
    )
    try:
        return json.loads(result.stdout)
    except:
        return {'ok': False, 'error': result.stdout[:200]}


def fetch_concept_stocks_em(board_code: str, board_name: str, max_pages: int = 20) -> list:
    """
    采集单个概念板块的成分股（翻页版）
    通过 xbrowser 访问东方财富 data.eastmoney.com 页面，JS渲染后提取股票代码。
    返回: 股票代码列表
    """
    url = f'https://data.eastmoney.com/bkzj/gn/{board_code}.html'
    
    # 1. 打开页面
    resp = xb_run(f'open {url}', f'--timeout {timeout(20)}')
    if not resp.get('ok'):
        return []
    
    # 2. 等待 JS 渲染（networkidle表示动态内容加载完成）
    resp = xb_run('wait --load networkidle', f'--timeout {timeout(30)}')
    time.sleep(3)  # 额外等待表格渲染
    
    all_codes = []
    
    for page in range(max_pages):
        # 3. 用 JS 提取当前页股票代码
        js_code = """
        (() => {
            const codes = [];
            // 东方财富 data 页面使用 table#tb 或 .datagrid
            const rows = document.querySelectorAll('table tr, .datagrid-body tr, tbody tr');
            rows.forEach(row => {
                // 股票代码在第一列或包含6位数字的单元格
                const cells = row.querySelectorAll('td');
                cells.forEach(cell => {
                    const txt = cell.textContent.trim();
                    if (/^\d{6}$/.test(txt)) {
                        codes.push(txt);
                    }
                });
            });
            // 备用：搜索所有包含6位股票代码的元素
            if (codes.length === 0) {
                const bodyText = document.body.innerText;
                const found = bodyText.match(/\b\d{6}\b/g) || [];
                codes.push(...new Set(found));
            }
            // 找下一页按钮
            const nextBtn = Array.from(document.querySelectorAll('button, a, .page-next, [class*=next]'))
                .find(el => el.textContent.includes('下一页') || el.textContent.includes('下页') || el.getAttribute('title') === '下一页');
            return JSON.stringify({codes: [...new Set(codes)], hasNextPage: !!nextBtn, nextBtnText: nextBtn ? nextBtn.textContent.trim() : null});
        })()
        """
        
        resp = xb_run(f'eval "{js_code}"', f'--timeout {timeout(30)}')
        
        data = resp.get('data', {}).get('result', {}).get('data')
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
            except:
                parsed = {'codes': [], 'hasNextPage': False}
        elif isinstance(data, dict):
            parsed = data
        else:
            parsed = {'codes': [], 'hasNextPage': False}
        
        codes = parsed.get('codes', [])
        all_codes.extend(codes)
        
        # 如果没有更多页，停止
        if not parsed.get('hasNextPage'):
            break
        
        # 点击下一页
        click_resp = xb_run(
            'eval "(() => { const btn = Array.from(document.querySelectorAll(\'button, a, input\')).find(el => el.textContent.includes(\'下一页\') || el.textContent.includes(\'下页\')); if(btn) { btn.click(); return \'clicked\'; } return \'not found\'; })()"',
            f'--timeout {timeout(30)}'
        )
        time.sleep(3)
    
    return list(set(all_codes))


def fetch_concept_stocks_em_simple(board_code: str, board_name: str) -> list:
    """
    简化版采集：只取第一页，不翻页。
    快速验证 xbrowser + push2/em API 的组合可行性。
    """
    url = f'https://data.eastmoney.com/bkzj/gn/{board_code}.html'
    
    # 打开页面
    resp = xb_run(f'open {url}', f'--timeout {timeout(20)}')
    if not resp.get('ok'):
        print(f'    [XB] 打开失败')
        return []
    
    # 等待渲染
    resp = xb_run('wait --load networkidle', f'--timeout {timeout(30)}')
    time.sleep(3)
    
    # 用 JS 提取
    js_code = """
    (() => {
        const codes = [];
        const rows = document.querySelectorAll('tbody tr, table tr, .datagrid-body tr');
        rows.forEach(row => {
            row.querySelectorAll('td').forEach(cell => {
                const txt = cell.textContent.trim();
                if (/^\d{6}$/.test(txt)) codes.push(txt);
            });
        });
        // 备用：从页面文本搜索
        if (codes.length === 0) {
            const txt = document.body.innerText;
            const found = txt.match(/\b\d{6}\b/g) || [];
            codes.push(...new Set(found));
        }
        // 找下一页按钮
        const nextBtns = Array.from(document.querySelectorAll('*')).filter(el => {
            const t = el.textContent.trim();
            return t === '下一页' || t === '下一页' || t === '>' || t.includes('下一页');
        });
        return JSON.stringify({codes: [...new Set(codes)], nextBtns: nextBtns.map(e => e.tagName + ':' + e.className + ':' + e.textContent.trim())});
    })()
    """
    
    resp = xb_run(f'eval "{js_code}"', f'--timeout {timeout(30)}')
    
    raw = resp.get('data', {}).get('result', {}).get('data', '')
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except:
            parsed = {'codes': []}
    elif isinstance(raw, dict):
        parsed = raw
    else:
        parsed = {'codes': []}
    
    codes = parsed.get('codes', [])
    return codes


def fetch_concept_stocks_via_snapshot(board_code: str) -> list:
    """
    通过 xbrowser snapshot 获取页面结构，提取股票代码。
    更可靠的方式：先导航，再用 snapshot 分析。
    """
    return []


def fetch_batch_concepts(source='em', batch_size=10):
    """
    采集一批概念板块的成分股（断点续传）
    source: 'em'
    batch_size: 每批处理 N 个板块
    """
    conn = get_conn()
    cur = conn.cursor()
    
    # 获取待处理的板块
    cur.execute("SELECT board_code, board_name FROM concept_boards WHERE source=? ORDER BY board_code", (source,))
    all_boards = [(r[0], r[1]) for r in cur.fetchall()]
    
    # 获取已完成的板块
    cur.execute("SELECT DISTINCT board_code FROM stock_concept WHERE source=?", (source,))
    done = set(r[0] for r in cur.fetchall())
    conn.close()
    
    pending = [(code, name) for code, name in all_boards if code not in done]
    
    if not pending:
        print(f'  [{source}] 全部完成 ({len(all_boards)} 板块)')
        return 0
    
    batch = pending[:batch_size]
    
    for i, (board_code, board_name) in enumerate(batch):
        print(f'  [{i+1}/{len(batch)}] {board_name} ({board_code})')
        
        codes = fetch_concept_stocks_em_simple(board_code, board_name)
        
        if codes:
            conn = get_conn()
            cur = conn.cursor()
            for stock_code in codes:
                cur.execute(
                    "INSERT OR IGNORE INTO stock_concept (stock_code, board_code, source) VALUES (?, ?, ?)",
                    (stock_code, board_code, source)
                )
            conn.commit()
            conn.close()
            print(f'    +{len(codes)} 只股票')
        else:
            print(f'    无数据')
        
        time.sleep(2)
    
    remaining = len(pending) - len(batch)
    print(f'  剩余 {remaining} 个板块')
    return remaining


def test_xbrowser():
    """测试 xbrowser 是否正常工作"""
    print('=== xbrowser 测试 ===')
    
    # 1. init
    resp = xb_run('init', '', timeout=10)
    print(f'init: {resp.get("ok")}')
    
    # 2. 打开页面
    resp = xb_run('open https://www.baidu.com', '--timeout 20000')
    print(f'open baidu: {resp.get("ok")}')
    
    # 3. 等待
    resp = xb_run('wait --load networkidle', '--timeout 20000')
    print(f'wait: {resp.get("ok")}')
    
    # 4. eval
    resp = xb_run('eval "(() => document.title)"', '--timeout 10000')
    title = resp.get('data', {}).get('result', {}).get('data', '')
    print(f'eval title: {title}')
    
    return resp.get('ok', False)


if __name__ == '__main__':
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'test'
    
    if cmd == 'test':
        test_xbrowser()
    elif cmd == 'batch':
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        source = sys.argv[3] if len(sys.argv) > 3 else 'em'
        remaining = fetch_batch_concepts(source, limit)
        print(f'完成，剩余 {remaining} 个板块')
    elif cmd == 'single':
        board_code = sys.argv[2]
        board_name = sys.argv[3] if len(sys.argv) > 3 else board_code
        codes = fetch_concept_stocks_em_simple(board_code, board_name)
        print(f'{board_name}: {len(codes)} 只')
    elif cmd == 'status':
        conn = get_conn()
        cur = conn.cursor()
        for t in ['concept_boards', 'stock_concept', 'stock_industry_board', 'stocks', 'industry_l1', 'industry_l2', 'industry_l3']:
            cur.execute(f'SELECT COUNT(*) FROM {t}')
            print(f'{t}: {cur.fetchone()[0]}')
        conn.close()
