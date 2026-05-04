#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CDN 检测脚本（独立运行版）"""

import sys, os, time, socket, json, urllib.request, urllib.error

CDN_HOST = "push2.eastmoney.com"
CDN_PORTS = [80, 443]
CDN_TEST_URLS = [
    "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f12&fs=m:0+t:6&fields=f12,f14",
    "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&fields=f12,f14&secids=0.300438",
]

def check_tcp_connectivity(host, port, timeout=3):
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True, None
    except Exception as e:
        return False, str(e)

def check_http_endpoint(url, timeout=5):
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        })
        resp = urllib.request.urlopen(req, timeout=timeout)
        body = resp.read().decode('utf-8', errors='replace')
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

def check_cdn_full():
    report = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "tcp_checks": {},
        "http_checks": {},
        "overall_available": False,
    }
    
    for port in CDN_PORTS:
        ok, err = check_tcp_connectivity(CDN_HOST, port)
        report["tcp_checks"][f"{CDN_HOST}:{port}"] = {"ok": ok, "error": err}
    
    for url in CDN_TEST_URLS:
        ok, info = check_http_endpoint(url)
        report["http_checks"][url] = {"ok": ok, "info": info}
    
    tcp_ok = any(c["ok"] for c in report["tcp_checks"].values())
    http_ok = any(c["ok"] for c in report["http_checks"].values())
    report["overall_available"] = http_ok
    
    return report["overall_available"], report

if __name__ == "__main__":
    is_available, report = check_cdn_full()
    print(f"检测时间: {report['timestamp']}")
    print(f"TCP 连通:")
    for endpoint, result in report["tcp_checks"].items():
        status = "✅" if result["ok"] else "❌"
        print(f"  {status} {endpoint}")
    print(f"HTTP 端点:")
    for url, result in report["http_checks"].items():
        status = "✅" if result["ok"] else "❌"
        print(f"  {status} {url[:60]}...")
        if not result["ok"]:
            print(f"     → {result['info']}")
    print(f"\nCDN 综合状态: {'✅ 可用' if is_available else '❌ 不可用'}")
    sys.exit(0 if is_available else 1)
