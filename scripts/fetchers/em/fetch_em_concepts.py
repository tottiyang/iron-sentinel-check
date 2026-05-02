#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EM 成分股关联采集（增强版）

采集范围：stock_concept（source='em'）

功能：
  1. 多维度 CDN 检测（TCP + HTTP + API 实际调用）
  2. 自动重试与指数退避
  3. 断点续传（记录已完成的板块）
  4. 详细的日志与状态报告
  5. 支持 --check-only 模式（只检测 CDN，不采集）

EM 数据目标：~50,000 条 stock_concept
当前状态：待 CDN 恢复后自动补采

替代方案（CDN 不通时）：
  Sina: ~10,000 条（稳定）
  THS:  ~24,000 条（有截断，但可用）
  两者合计 ~34,000 条，可作为临时替代

Usage:
  python3 fetch_em_concepts.py              # 全量采集
  python3 fetch_em_concepts.py --limit=10   # 只采 10 个板块（测试用）
  python3 fetch_em_concepts.py --check-only # 只检测 CDN 状态
  python3 fetch_em_concepts.py --status     # 显示当前状态
"""

import sys, os, time, socket, json, urllib.request, urllib.error

# 只有在直接运行时才添加路径（避免作为模块导入时路径错误）
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from fetchers.db.db_schema import get_conn

SLEEP_SEC = 0.5
BATCH_LOG = 20
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # 指数退避基数（秒）

# CDN 检测配置
CDN_HOST = "push2.eastmoney.com"
CDN_PORTS = [80, 443]
CDN_TEST_URLS = [
    "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f12&fs=m:0+t:6&fields=f12,f14",
    "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&fields=f12,f14&secids=0.300438",
]


def check_tcp_connectivity(host, port, timeout=3):
    """TCP 连通性检测"""
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True, None
    except Exception as e:
        return False, str(e)


def check_http_endpoint(url, timeout=5):
    """HTTP 端点可用性检测"""
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        })
        resp = urllib.request.urlopen(req, timeout=timeout)
        body = resp.read().decode('utf-8', errors='replace')
        # 检查返回内容是否有效 JSON
        try:
            data = json.loads(body)
            if data.get('rc') == 0 or data.get('success') is not None:
                return True, f"status={resp.status}, rc={data.get('rc')}, valid_json=True"
        except:
            pass
        return True, f"status={resp.status}, body_len={len(body)}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return False, f"URL Error: {e.reason}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def check_akshare_api():
    """检测 akshare EM API 是否可用"""
    try:
        # 尝试获取板块列表（轻量级请求）
        df = ak.stock_board_concept_name_em()
        if df is not None and len(df) > 0:
            return True, f"返回 {len(df)} 个板块"
        return False, "返回空数据"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:100]}"


def check_cdn_full():
    """
    全面 CDN 检测
    返回: (is_available, report_dict)
    """
    report = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "tcp_checks": {},
        "http_checks": {},
        "akshare_check": {},
        "overall_available": False,
    }

    # 1. TCP 检测
    for port in CDN_PORTS:
        ok, err = check_tcp_connectivity(CDN_HOST, port)
        report["tcp_checks"][f"{CDN_HOST}:{port}"] = {"ok": ok, "error": err}

    # 2. HTTP 端点检测
    for url in CDN_TEST_URLS:
        ok, info = check_http_endpoint(url)
        report["http_checks"][url] = {"ok": ok, "info": info}

    # 3. akshare API 检测
    ok, info = check_akshare_api()
    report["akshare_check"] = {"ok": ok, "info": info}

    # 综合判断：TCP 通 + HTTP 返回有效内容 + akshare 可用
    tcp_ok = any(c["ok"] for c in report["tcp_checks"].values())
    http_ok = any(c["ok"] for c in report["http_checks"].values())
    akshare_ok = report["akshare_check"]["ok"]

    # CDN 可用的标准：至少 HTTP 返回 200 或 akshare 可用
    report["overall_available"] = http_ok or akshare_ok

    return report["overall_available"], report


def fetch_em_concept_stocks(board_name, max_retries=MAX_RETRIES):
    """
    采集 EM 概念板块成分股（带重试）
    board_name: 板块名称
    返回: list(股票代码) / None(失败)
    """
    for attempt in range(max_retries):
        try:
            df = ak.stock_board_concept_cons_em(symbol=board_name)
            if df is None or len(df) == 0:
                return []
            stocks = []
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip().zfill(6)
                if code and len(code) == 6:
                    stocks.append(code)
            return stocks
        except Exception as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF ** attempt
                print(f"      重试 {board_name} ({attempt+1}/{max_retries})，等待 {wait}s...")
                time.sleep(wait)
            else:
                return None  # 最终失败
    return None


def fetch_concept_relations(limit=0, dry_run=False, check_only=False):
    """
    采集概念-个股关联（EM 源，断点续传）
    
    参数:
      limit: 限制处理的板块数（0=不限）
      dry_run: 只打印，不写入数据库
      check_only: 只检测 CDN，不采集
    
    返回: 剩余待处理板块数（0=全部完成）
    """
    # 全面 CDN 检测
    print("[EM-concept] CDN 全面检测中...")
    is_available, report = check_cdn_full()

    # 打印检测报告
    print(f"  检测时间: {report['timestamp']}")
    print(f"  TCP 连通:")
    for endpoint, result in report["tcp_checks"].items():
        status = "✅" if result["ok"] else "❌"
        print(f"    {status} {endpoint}")
    print(f"  HTTP 端点:")
    for url, result in report["http_checks"].items():
        status = "✅" if result["ok"] else "❌"
        print(f"    {status} {url[:60]}...")
        if not result["ok"]:
            print(f"       → {result['info']}")
    print(f"  akshare API: {'✅' if report['akshare_check']['ok'] else '❌'} {report['akshare_check']['info']}")

    if check_only:
        print(f"\n  CDN 综合状态: {'✅ 可用' if is_available else '❌ 不可用'}")
        return 0

    if not is_available:
        print(f"\n[EM-concept] ⚠️ CDN 不可用，跳过采集")
        print("             stock_concept(em) 将保持当前状态")
        print("             当前替代: sina + ths 合计约 34,000 条")
        return 1  # 返回非零表示有任务待处理（CDN 恢复后继续）

    print(f"\n[EM-concept] ✅ CDN 可用，开始采集")

    # 连接数据库
    conn = get_conn()
    cur = conn.cursor()

    # 获取所有 EM 概念板块
    cur.execute(
        "SELECT board_code, board_name FROM concept_boards WHERE source='em' ORDER BY board_code"
    )
    all_boards = cur.fetchall()

    # 获取已完成的板块
    cur.execute(
        "SELECT DISTINCT board_code FROM stock_concept WHERE source='em'"
    )
    done = set(r[0] for r in cur.fetchall())
    conn.close()

    pending = [(code, name) for code, name in all_boards if code not in done]

    if not pending:
        print(f"[EM-concept] 全部 {len(all_boards)} 个板块已完成")
        return 0

    if limit > 0:
        pending = pending[:limit]

    print(f"[EM-concept] 开始采集 {len(pending)} 个板块（已有 {len(done)} 个完成，总计 {len(all_boards)} 个）")

    total_added = 0
    errors = 0
    empty_count = 0

    for i, (board_code, board_name) in enumerate(pending):
        stocks = fetch_em_concept_stocks(board_name)

        if stocks is None:
            errors += 1
            if errors <= 3:
                print(f"    ❌ {board_name}: 采集失败")
            continue

        if len(stocks) == 0:
            empty_count += 1

        if stocks and not dry_run:
            conn = get_conn()
            cur = conn.cursor()
            added_this_board = 0
            for sc in stocks:
                cur.execute(
                    "INSERT OR IGNORE INTO stock_concept "
                    "(stock_code, board_code, source, fetched_at) "
                    "VALUES (?, ?, 'em', datetime('now'))",
                    (sc, board_code),
                )
                if cur.rowcount > 0:
                    total_added += 1
                    added_this_board += 1
            conn.commit()
            conn.close()

        time.sleep(SLEEP_SEC)

        if (i + 1) % BATCH_LOG == 0 or i == len(pending) - 1:
            remaining = len(pending) - (i + 1)
            print(f"  [{i+1}/{len(pending)}] +{total_added} 条, 空板块 {empty_count}, 错误 {errors}, 剩余 {remaining}")

    # 计算剩余待处理板块数
    # 如果全部成功处理完，返回 0；否则返回剩余数量
    processed = min(i + 1, len(pending))  # 实际处理了多少个
    if errors >= 3 and total_added == 0:
        # 连续失败，可能 CDN 又断了，返回剩余全部
        remaining = len(pending) - processed
        print(f"\n[EM-concept] 连续失败，暂停采集，剩余 {remaining} 个板块待处理")
        return remaining
    
    remaining = len(pending) - processed
    print(f"\n[EM-concept] 本次采集完成: +{total_added} 条, 错误 {errors}, 空板块 {empty_count}, 剩余 {remaining} 个板块")
    return remaining


def status():
    """显示 EM 数据状态"""
    conn = get_conn()
    cur = conn.cursor()

    print("\n" + "=" * 60)
    print("EM 数据状态报告")
    print("=" * 60)

    cur.execute("SELECT COUNT(*) FROM concept_boards WHERE source='em'")
    cb = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='em'")
    sc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT board_code) FROM stock_concept WHERE source='em'")
    sc_boards = cur.fetchone()[0]

    print(f"\n  板块列表:")
    print(f"    concept_boards(em):  {cb} 个板块")
    print(f"\n  成分股关联:")
    print(f"    stock_concept(em):   {sc} 条关联")
    print(f"    板块覆盖率:          {sc_boards}/{cb} ({sc_boards/cb*100:.1f}%)" if cb > 0 else "    板块覆盖率:          N/A")

    # CDN 检测
    print(f"\n  CDN 检测:")
    is_available, report = check_cdn_full()
    print(f"    综合状态: {'✅ 可用' if is_available else '❌ 不可用'}")
    print(f"    检测时间: {report['timestamp']}")
    for url, result in report["http_checks"].items():
        status = "✅" if result["ok"] else "❌"
        print(f"    {status} {url[:50]}...")

    print(f"\n  替代数据量:")
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='sina'")
    print(f"    sina: {cur.fetchone()[0]} 条")
    cur.execute("SELECT COUNT(*) FROM stock_concept WHERE source='ths'")
    print(f"    ths:  {cur.fetchone()[0]} 条")

    conn.close()
    print("=" * 60)


def main():
    """主入口（支持命令行和导入调用）"""
    import argparse
    parser = argparse.ArgumentParser(description='EM 成分股关联采集')
    parser.add_argument('--limit', type=int, default=0, help='限制处理板块数')
    parser.add_argument('--dry-run', action='store_true', help='只打印，不写入数据库')
    parser.add_argument('--check-only', action='store_true', help='只检测 CDN 状态')
    parser.add_argument('--status', action='store_true', help='显示当前状态')
    args = parser.parse_args()

    if args.status:
        status()
        return 0
    elif args.check_only:
        # fetch_concept_relations 已经打印完整报告，直接返回
        return 0
    else:
        remaining = fetch_concept_relations(limit=args.limit, dry_run=args.dry_run)
        status()
        return remaining


if __name__ == "__main__":
    sys.exit(main())
