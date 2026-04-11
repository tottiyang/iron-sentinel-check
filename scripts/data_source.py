# -*- coding: utf-8 -*-
"""
铁血哨兵 v2 - 数据源层
====================
三层降级策略（优先级从高到低）:
  1. NeoData    → 官方高质量数据（行情/资金/K线/行业/股东）
  2. 腾讯/新浪  → 备用实时数据（行情/K线/指数）
  3. 同花顺/akshare → 降级兜底（免费但可能不稳定）

返回值规范：所有函数统一返回 (data, source, error_msg)
  - data: 实际数据（dict/list/DataFrame），失败时为 None
  - source: 数据来源字符串，失败时为 "none"
  - error_msg: 错误原因，空字符串=成功
"""

import os
import re
import time
import json
import uuid
import random
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional, Union

# ==================== 全局会话 ====================
_SESSION = requests.Session()
_SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
})


# ==================== 重试装饰器 ====================
def _retry(max_attempts: int = 3, base_delay: float = 1.0) -> callable:
    """带指数退避的请求重试装饰器"""
    def decorator(func: callable) -> callable:
        def wrapper(*args, **kwargs):
            last_e = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        print(f"    [重试] 第{attempt}次成功")
                    return result
                except Exception as e:
                    last_e = e
                    if attempt < max_attempts:
                        delay = min(base_delay * (2 ** (attempt - 1)), 8.0)
                        delay *= (0.5 + random.random() * 0.5)
                        time.sleep(delay)
            raise last_e
        return wrapper
    return decorator


# ==================== NeoData 主数据源 ====================
_ND_PROXY_PORT = os.environ.get('AUTH_GATEWAY_PORT', '19000')
_ND_PROXY_URL = f"http://localhost:{_ND_PROXY_PORT}/proxy/api"
_ND_REMOTE_URL = "https://jprx.m.qq.com/aizone/skillserver/v1/proxy/teamrouter_neodata/query"

# NeoData 查询缓存（避免同次审核重复请求）
_ND_CACHE: Dict[str, Tuple[dict, float]] = {}
_ND_CACHE_TTL = 180  # 缓存秒数


@_retry(max_attempts=2, base_delay=1.0)
def query_neodata(query: str) -> Tuple[Optional[Dict], str, str]:
    """
    调用 NeoData 查询接口。

    Returns:
        (data_dict, source, error_msg)
    """
    cache_key = query[:80]
    now = time.time()

    if cache_key in _ND_CACHE:
        cached_data, cached_ts = _ND_CACHE[cache_key]
        if now - cached_ts < _ND_CACHE_TTL:
            return cached_data, "neodata", ""

    try:
        r = _SESSION.post(
            _ND_PROXY_URL,
            headers={
                "Content-Type": "application/json",
                "Remote-URL": _ND_REMOTE_URL,
            },
            json={
                "channel": "neodata",
                "sub_channel": "qclaw",
                "query": query,
                "request_id": str(uuid.uuid4()),
                "data_type": "api",
            },
            timeout=15,
        )
        d = r.json()
        if d.get('code') == '200' and d.get('suc'):
            api_data = d.get('data', {})
            recall = api_data.get('apiRecall', [])
            result = {'recall': recall, 'raw': api_data}
            _ND_CACHE[cache_key] = (result, now)
            return result, "neodata", ""
        return None, "neodata", f"NeoData返回异常: {d.get('msg', '')}"
    except Exception as e:
        return None, "neodata", str(e)


def nd_extract_text(result: Optional[Dict], type_hint: str) -> str:
    """从 NeoData 结果中提取指定类型的文本
    
    数据路径（随API版本变化，两个都尝试）：
      - v1: result['recall']
      - v2: result['raw']['apiData']['apiRecall']
    """
    if not result:
        return ""
    # v1 路径（旧的）
    for item in result.get('recall', []):
        if type_hint in item.get('type', ''):
            return item.get('content', '')
    # v2 路径（当前有效）
    api_data = result.get('raw', {}).get('apiData', {})
    for item in api_data.get('apiRecall', []):
        if type_hint in item.get('type', ''):
            return item.get('content', '')
    return ""


def nd_extract_number(result: Optional[Dict], type_hint: str, pattern: str) -> Optional[float]:
    """从 NeoData 结果中用正则提取数值"""
    txt = nd_extract_text(result, type_hint)
    if not txt:
        return None
    m = re.search(pattern, txt)
    if m:
        try:
            return float(m.group(1))
        except (ValueError, IndexError):
            pass
    return None


# ==================== 腾讯备用数据源 ====================

@_retry(max_attempts=2, base_delay=0.5)
def get_realtime_tencent(code: str) -> Tuple[Optional[Dict], str, str]:
    """
    获取腾讯实时行情（备用数据源）

    Args:
        code: 标准格式，如 'sz300438' 或 'sh600519'
    """
    try:
        market, num = _market_code(code)
        # 腾讯实时API
        url = f"https://qt.gtimg.cn/q={market}{num}"
        r = _SESSION.get(url, timeout=8)
        text = r.text.strip()
        if not text or text.startswith('null') or text == 'pvsearch':
            return None, "tencent", "腾讯实时无数据"

        fields = text.split('~')
        if len(fields) < 50:
            return None, "tencent", f"腾讯实时数据格式异常，长度={len(fields)}"

        def f(idx: int, default="") -> str:
            return fields[idx] if idx < len(fields) else default

        price = float(f(3, 0))  # 当前价
        chg = float(f(31, 0))  # 涨跌额
        chg_pct = float(f(32, 0))  # 涨跌幅(%)
        vol = float(f(36, 0)) * 100  # 成交量(手→股)
        amount = float(f(37, 0)) * 10000  # 成交额(万→元)

        return {
            'price': price,
            'chg': chg,
            'chg_pct': chg_pct,
            'vol': vol,
            'amount': amount,
            'name': f(1, ''),
            'open': float(f(5, 0)),
            'high': float(f(33, 0)),
            'low': float(f(34, 0)),
            'prev_close': float(f(4, 0)),
        }, "tencent", ""

    except Exception as e:
        return None, "tencent", str(e)


@_retry(max_attempts=2, base_delay=0.5)
def get_daily_bars_tencent(code: str, count: int = 60) -> Tuple[Optional[List[Dict]], str, str]:
    """
    获取腾讯财经日K线数据（备用数据源）

    Returns:
        bars: [{date, open, high, low, close, vol, amount}, ...] 或 None
    """
    try:
        market, num = _market_code(code)
        url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayfqfund&param={market}{num},day,,,{count},qfq"
        r = _SESSION.get(url, timeout=10)
        text = r.text.strip()
        # 去掉变量赋值前缀
        if text.startswith('var '):
            text = text.split('=', 1)[1]

        d = json.loads(text)
        # 取日K线数据
        data = d.get('data', {})
        key = f"{market}{num}"
        day_data = data.get(key, {}).get('day', [])
        if not day_data:
            # 尝试 qfqday
            day_data = data.get(key, {}).get('qfqday', [])

        if not day_data:
            return None, "tencent", "腾讯日K线为空"

        bars = []
        for item in day_data:
            if len(item) >= 6:
                bars.append({
                    'date': item[0],
                    'open': float(item[1]),
                    'high': float(item[2]),
                    'low': float(item[3]),
                    'close': float(item[4]),
                    'vol': float(item[5]),
                    'amount': float(item[6]) if len(item) > 6 else 0,
                })

        return bars, "tencent", ""

    except Exception as e:
        return None, "tencent", str(e)


@_retry(max_attempts=2, base_delay=0.5)
def get_index_tencent(idx_code: str) -> Tuple[Optional[Dict], str, str]:
    """
    获取大盘指数实时数据（腾讯备用）

    idx_code: 'sh000001'(上证) 或 'sz399001'(深证)
    """
    try:
        data, src, err = get_realtime_tencent(idx_code)
        if err:
            return None, src, err
        return {
            'name': data.get('name', ''),
            'price': data.get('price', 0),
            'chg': data.get('chg', 0),
            'chg_pct': data.get('chg_pct', 0),
            'high': data.get('high', 0),
            'low': data.get('low', 0),
            'open': data.get('open', 0),
            'prev_close': data.get('prev_close', 0),
        }, src, ""
    except Exception as e:
        return None, "tencent", str(e)


@_retry(max_attempts=2, base_delay=0.5)
def get_index_bars_tencent(idx_code: str, count: int = 10) -> Tuple[Optional[List[Dict]], str, str]:
    """获取大盘指数日K线（腾讯备用）"""
    try:
        bars, src, err = get_daily_bars_tencent(idx_code, count)
        if err:
            return None, src, err
        return bars, src, ""
    except Exception as e:
        return None, "tencent", str(e)


# ==================== AkShare 降级数据源 ====================

def _ak_wrapper(func: callable, *args, **kwargs) -> Tuple[Any, str, str]:
    """统一包装 akshare 函数调用"""
    try:
        data = func(*args, **kwargs)
        if data is None or (hasattr(data, '__len__') and len(data) == 0):
            return None, "akshare", f"{func.__name__} 返回空数据"
        return data, "akshare", ""
    except Exception as e:
        return None, "akshare", str(e)


def get_realtime_akshare(code: str) -> Tuple[Optional[Dict], str, str]:
    """Akshare 实时行情（降级兜底）"""
    try:
        import akshare as ak
        _, num = _market_code(code)
        df = ak.stock_zh_a_spot_em()
        row = df[df['代码'] == num]
        if row.empty:
            return None, "akshare", "股票代码未找到"
        r = row.iloc[0]
        chg_pct = float(r.get('涨跌幅', 0))
        return {
            'price': float(r.get('最新价', 0)),
            'chg': float(r.get('涨跌额', 0)),
            'chg_pct': chg_pct,
            'vol': float(r.get('成交量', 0)) * 100,
            'amount': float(r.get('成交额', 0)) * 10000,
            'name': str(r.get('名称', '')),
            'open': float(r.get('今开', 0)),
            'high': float(r.get('最高', 0)),
            'low': float(r.get('最低', 0)),
            'prev_close': float(r.get('昨收', 0)),
        }, "akshare", ""
    except Exception as e:
        return None, "akshare", str(e)


@_retry(max_attempts=2, base_delay=0.5)
def get_daily_bars_sina(code: str, count: int = 80) -> Tuple[Optional[List[Dict]], str, str]:
    """新浪财经日K线（240分钟=日线，备用数据源）
    注意：新浪K线 volume 单位为"股"，需要 /100 转为"手"（1手=100股）
    """
    try:
        import requests as _req
        market, num = _market_code(code)
        # 新浪 symbol 格式: sz300438
        symbol = f"{market}{num}"
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {'symbol': symbol, 'scale': 240, 'ma': 'no', 'datalen': count}
        r = _req.get(url, params=params, timeout=10)
        data = r.json()
        if not data or not isinstance(data, list):
            return None, "sina", "新浪日K线为空"
        bars = []
        for item in data:
            # 新浪 volume 单位是"股"，除以100转为"手"以保持与其他数据源一致
            vol_raw = _to_float(item.get('volume'))
            vol_hand = vol_raw / 100.0 if vol_raw else 0.0
            bars.append({
                'date': str(item.get('day', '')),
                'open': _to_float(item.get('open')),
                'high': _to_float(item.get('high')),
                'low': _to_float(item.get('low')),
                'close': _to_float(item.get('close')),
                'vol': vol_hand,   # 统一为"手"
            })
        return bars, "sina", ""
    except Exception as e:
        return None, "sina", str(e)


def get_daily_bars_akshare(code: str, count: int = 60) -> Tuple[Optional[List[Dict]], str, str]:
    """Akshare 日K线（降级兜底）"""
    try:
        import akshare as ak
        from datetime import datetime, timedelta
        _, num = _market_code(code)
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=count * 2)).strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(
            symbol=num, period="daily",
            start_date=start_date, end_date=end_date,
            adjust="qfq"
        )
        if df is None or df.empty:
            return None, "akshare", "akshare日K线为空"
        bars = []
        for _, row in df.iterrows():
            bars.append({
                'date': str(row.get('日期', '')),
                'open': float(row.get('开盘', 0)),
                'high': float(row.get('最高', 0)),
                'low': float(row.get('最低', 0)),
                'close': float(row.get('收盘', 0)),
                'vol': float(row.get('成交量', 0)),
                'amount': float(row.get('成交额', 0)),
            })
        return bars, "akshare", ""
    except Exception as e:
        return None, "akshare", str(e)


def get_money_flow_akshare(code: str) -> Tuple[Optional[Dict], str, str]:
    """Akshare 资金流向（降级兜底）"""
    try:
        import akshare as ak
        _, num = _market_code(code)
        df = ak.stock_individual_fund_flow(stock=num, market="sh" if code.startswith('sh') else "sz")
        if df is None or df.empty:
            return None, "akshare", "资金流向为空"
        latest = df.iloc[-1]
        return {
            'main_net': float(latest.get('主力净流入', 0)),
            'super_net': float(latest.get('超大单净流入', 0)),
            'big_net': float(latest.get('大单净流入', 0)),
            'mid_net': float(latest.get('中单净流入', 0)),
            'small_net': float(latest.get('小单净流入', 0)),
        }, "akshare", ""
    except Exception as e:
        return None, "akshare", str(e)


def get_financial_akshare(code: str) -> Tuple[Optional[Dict], str, str]:
    """Akshare 财务数据（降级兜底）"""
    try:
        import akshare as ak
        _, num = _market_code(code)
        df = ak.stock_financial_analysis_indicator(symbol=num)
        if df is None or df.empty:
            return None, "akshare", "财务数据为空"
        latest = df.iloc[-1]
        pe = float(latest.get('市盈率（动态）', 0) or 0)
        pb = float(latest.get('市净率', 0) or 0)
        return {
            'pe': pe,
            'pb': pb,
            'roe': float(latest.get('净资产收益率(%)', 0) or 0),
            'revenue_growth': float(latest.get('营业总收入同比增长率(%)', 0) or 0),
            'profit_growth': float(latest.get('净利润同比增长率(%)', 0) or 0),
        }, "akshare", ""
    except Exception as e:
        return None, "akshare", str(e)


# ==================== 行业板块数据 ====================

def get_industry_info_neodata(code: str) -> Tuple[Optional[Dict], str, str]:
    """NeoData 行业归属查询"""
    try:
        result, src, err = query_neodata(f"查询{code}所属申万行业分类和板块名称")
        if err:
            return None, src, err
        txt = nd_extract_text(result, '行业分类与板块归属')
        if not txt:
            return None, "neodata", "NeoData未返回行业信息"
        # 提取行业名
        m = re.search(r'(申万一级|SW1|行业)[:：]?\s*([^\n,，。]+)', txt)
        industry = m.group(2).strip() if m else ""
        return {'industry_name': industry, 'raw_text': txt}, src, ""
    except Exception as e:
        return None, "neodata", str(e)


@_retry(max_attempts=2, base_delay=0.5)
def get_industry_info_akshare(code: str) -> Tuple[Optional[Dict], str, str]:
    """Akshare 行业归属（备用）"""
    try:
        import akshare as ak
        _, num = _market_code(code)
        df = ak.stock_board_industry_name_em()
        # 遍历申万行业
        sw_df = ak.stock_board_industry_cons_sw(sector="银行")
        # 简化：用板块成分股逆查
        board_df = ak.stock_board_industry_cons_em(symbol="电力设备")
        if board_df is None:
            return None, "akshare", "板块数据为空"
        # 判断股票属于哪个板块
        for _, row in board_df.iterrows():
            if str(row.get('代码', '')) == num:
                return {'industry_name': '电力设备'}, "akshare", ""
        return None, "akshare", "未找到股票所属板块"
    except Exception as e:
        return None, "akshare", str(e)


@_retry(max_attempts=2, base_delay=0.5)
def get_sector_index_akshare(sector_name: str) -> Tuple[Optional[Dict], str, str]:
    """获取板块指数涨跌（akshare）"""
    try:
        import akshare as ak
        df = ak.stock_board_industry_name_em()
        target = df[df['板块名称'].str.contains(sector_name, na=False)]
        if target.empty:
            return None, "akshare", f"未找到板块: {sector_name}"
        code = target.iloc[0]['板块代码']
        cons_df = ak.stock_board_industry_cons_em(symbol=code)
        if cons_df is None or cons_df.empty:
            return None, "akshare", "板块成分股为空"
        avg_chg = float(cons_df['涨跌幅'].mean())
        return {
            'sector_name': sector_name,
            'chg_pct': avg_chg,
            'up_count': int((cons_df['涨跌幅'] > 0).sum()),
            'total_count': len(cons_df),
        }, "akshare", ""
    except Exception as e:
        return None, "akshare", str(e)


def get_board_leaders_fixed(sector_name: str) -> List[Dict]:
    """
    固定成分股映射（腾讯API不可用时的兜底）
    按行业返回市值排名前5的代表性股票
    """
    LEADERS = {
        '电力设备': [
            ('宁德时代', 'sz300750'), ('阳光电源', 'sz300274'),
            ('亿纬锂能', 'sz300014'), ('赣锋锂业', 'sz002460'), ('三花智控', 'sz002050'),
        ],
        '医药生物': [
            ('恒瑞医药', 'sh600276'), ('药明康德', 'sh603259'),
            ('迈瑞医疗', 'sz300760'), ('爱尔眼科', 'sz300015'), ('智飞生物', 'sz300122'),
        ],
        '电子': [
            ('立讯精密', 'sz002475'), ('海康威视', 'sz002415'),
            ('中芯国际', 'sh688981'), ('韦尔股份', 'sh603501'), ('京东方A', 'sz000725'),
        ],
        '计算机': [
            ('海光信息', 'sh688041'), ('中科曙光', 'sh603019'),
            ('紫光股份', 'sz000938'), ('科大讯飞', 'sz002230'), ('浪潮信息', 'sz000977'),
        ],
        '汽车': [
            ('比亚迪', 'sz002594'), ('赛力斯', 'sh601127'),
            ('长安汽车', 'sz000625'), ('长城汽车', 'sh601633'), ('北汽蓝谷', 'sh600733'),
        ],
        '锂电池': [
            ('宁德时代', 'sz300750'), ('亿纬锂能', 'sz300014'),
            ('赣锋锂业', 'sz002460'), ('欣旺达', 'sz300207'), ('德赛电池', 'sz000049'),
        ],
        '军工': [
            ('航发动力', 'sh600893'), ('中航沈飞', 'sh600760'),
            ('中航西飞', 'sz000768'), ('紫光国微', 'sz002049'), ('中航高科', 'sh600862'),
        ],
    }
    return LEADERS.get(sector_name, LEADERS.get('电力设备', []))


# ==================== 工具函数 ====================

def _market_code(std_code: str) -> Tuple[str, str]:
    """将 'sz300438' / 'sh600519' 转换为 (market, num)"""
    market = std_code[:2]
    num = std_code[2:]
    return market, num


def _to_float(v: Any, default: float = 0.0) -> float:
    """安全转换为 float"""
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _normalize_code(raw_code: str) -> str:
    """
    标准化股票代码为 'szXXXXXX' 或 'shXXXXXX' 格式
    兼容: 300438 / sz300438 / 0830008 等
    """
    code = str(raw_code).strip().lower()
    # 去掉 sh/sz 前缀
    for prefix in ['sh', 'sz', 'bj']:
        if code.startswith(prefix):
            code = code[2:]
            break
    # 判断市场
    if code.startswith(('6', '5', '9', '8')):
        return f"sh{code}"
    else:
        return f"sz{code}"


def clear_cache() -> None:
    """清除 NeoData 查询缓存（每次审核开始时调用）"""
    _ND_CACHE.clear()


# ==================== NeoData 今日实时行情提取 ====================

def nd_extract_quote(result: Optional[Dict]) -> Optional[Dict]:
    """
    从 NeoData 结果中提取A股实时行情数据。
    正确路径: raw.apiData.apiRecall（不是 entity 层）

    Returns:
        {
            'price': float,     # 最新价格（元）
            'chg': float,       # 涨跌幅（%，正数=涨）
            'open': float,      # 开盘价
            'high': float,      # 最高价
            'low': float,       # 最低价
            'prev_close': float, # 昨日收盘
            'vol': float,       # 成交量（手）
            'amount': float,    # 成交额（万元）
            'turnover': float,  # 换手率（%）
            'pe': float,        # 市盈率TTM
            'pb': float,        # 市净率
            'mkt_cap': float,   # 总市值（亿元）
            'chg5d': float,     # 近5日涨跌幅（%）
            'chg20d': float,    # 近20日涨跌幅（%）
            'update_time': str, # 更新时间
            'raw': str,         # 原始文本
        }
        或 None（提取失败）
    """
    if not result:
        return None
    import re
    raw = result.get('raw', {})
    api_data = raw.get('apiData', {})
    recalls = api_data.get('apiRecall', [])
    
    content = ''
    for r in recalls:
        c = r.get('content', '')
        if '最新价格' in c and '涨跌幅' in c and 'A股' in c:
            content = c
            break
    if not content:
        # 降级：只要有价格和涨跌幅即可
        for r in recalls:
            c = r.get('content', '')
            if '最新价格' in c and '涨跌幅' in c and ('元' in c or '美元' in c):
                # 排除纯美股数据
                if 'A股' not in c and 'sh6' not in c and 'sz0' not in c and 'sz3' not in c:
                    continue
                content = c
                break
    
    if not content:
        return None

    def f(pat, default=None):
        m = re.search(pat, content)
        if m:
            val = m.group(1).replace(',', '').replace('%', '').strip()
            try:
                return float(val)
            except ValueError:
                return val
        return default

    # 提取更新时间
    time_m = re.search(r'数据更新时间[:：]\s*(\d{4}[/\-]\d{2}[/\-]\d{2}\s+\d{2}:\d{2}:\d{2})', content)
    update_time = time_m.group(1) if time_m else ''

    # 提取所属板块（【所属板块】段落）
    sector_m = re.search(r'【所属板块】\s*(.+?)(?=\n【|$)', content, re.DOTALL)
    sector_name = ''
    if sector_m:
        sec_block = sector_m.group(1).strip()
        # 提取板块名和涨跌幅：所属的XXX板块涨跌幅为X.XX%
        sectors = []
        for m in re.finditer(r'所属的([^板块]+)板块涨跌幅为([-\d.]+)%', sec_block):
            sectors.append({'name': m.group(1), 'chg': float(m.group(2))})
        if sectors:
            sector_name = sectors[0]['name']
        else:
            # 降级：取第一行板块名
            sector_name = re.sub(r'涨跌幅[为：:][-\d.]+[%‰]', '', sec_block).strip().split('\n')[0]
            sector_name = sector_name.replace('该股票所属的', '').strip()
    else:
        sectors = []

    return {
        'price':      f(r'最新价格[:：]\s*([-\d.]+)'),
        'chg':        f(r'涨跌幅[:：]\s*([-\d.]+)'),
        'open':       f(r'今日开盘价格[:：]\s*([-\d.]+)'),
        'high':       f(r'最高价[:：]\s*([-\d.]+)'),
        'low':        f(r'最低价[:：]\s*([-\d.]+)'),
        'prev_close': f(r'昨日收盘价格[:：]\s*([-\d.]+)'),
        'vol':        f(r'成交数量[(（](?:手|股)[)）][:：]\s*([-\d,.]+)'),
        'amount':     f(r'成交金额[(（](?:万元|元|万)[)）][:：]\s*([-\d,.]+)'),
        'turnover':   f(r'换手率[:：]\s*([-\d.]+)'),
        'pe':         f(r'市盈率[(（]TTM[)）][:：]\s*([-\d.]+)'),
        'pb':         f(r'市净率[:：]\s*([-\d.]+)'),
        'mkt_cap':    f(r'总市值[(（]亿元[)）][:：]\s*([-\d.]+)'),
        'chg5d':      f(r'5日涨跌幅[:：]\s*([-\d.]+)'),
        'chg20d':     f(r'20日涨跌幅[:：]\s*([-\d.]+)'),
        'update_time': update_time,
        'raw':        content,
        'sector':     sector_name,   # 主要板块名（如"电力设备"）
        'sectors':    sectors,        # [{name, chg}, ...] 所有板块
    }


def get_realtime_neodata(code: str) -> Tuple[Optional[Dict], str, str]:
    """
    通过 NeoData 获取个股今日实时行情。
    替代腾讯API（腾讯今日数据被屏蔽）。

    Returns: (quote_dict, source, error_msg)
    """
    # 构造查询语句：同时查询数字代码和名称，提升命中率
    name_map = {
        '300438': '鹏辉能源', '300750': '宁德时代', '300274': '阳光电源',
        '300014': '亿纬锂能', '002460': '赣锋锂业', '002050': '三花智控',
        '600519': '贵州茅台', '000858': '五粮液',
    }
    name = name_map.get(code.upper().replace('SZ', '').replace('SH', ''), code)
    query = f"{name} {code} 今日最新行情：最新价格、涨跌幅、开盘价、最高价、最低价、昨日收盘、成交量、成交额、换手率、市盈率、市净率、总市值、近5日涨跌幅、近20日涨跌幅"
    result, src, err = query_neodata(query)
    if err or not result:
        return None, "neodata", err or "查询无结果"
    quote = nd_extract_quote(result)
    if not quote or quote.get('price') is None:
        return None, "neodata", "行情提取失败"
    return quote, "neodata", ""
