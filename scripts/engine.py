# -*- coding: utf-8 -*-
"""
铁血哨兵 v2 - A股买点审核引擎
===========================
单一调用入口，整合 skills 版和 workspace 版优势：
  - skills 版优势：三层数据降级、完整类架构、高数据可用率
  - workspace 版优势：筹码集中审核、龙头双维度判断

使用方式:
  python engine.py <股票代码> [--json] [--format]
  python -c "from engine import IronSentinelEngine; print(IronSentinelEngine('300438').audit().to_dict())"
"""

import sys
import os
import re
import json
import time
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

# 确保当前目录在 path 中（支持直接运行）
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from data_source import (
    query_neodata, nd_extract_text, nd_extract_number, nd_extract_quote,
    get_realtime_tencent, get_realtime_neodata, get_daily_bars_tencent,
    get_daily_bars_sina,  # 新浪备用日K线（腾讯API今日被屏蔽）
    get_index_tencent, get_index_bars_tencent,
    get_money_flow_akshare, get_financial_akshare,
    get_industry_info_neodata, get_sector_index_akshare,
    get_board_leaders_fixed, get_realtime_akshare, get_daily_bars_akshare,
    _normalize_code, _market_code, _to_float, clear_cache,
)
from checks import (
    CheckResult, WEIGHTS, NAMES,
    check_macd, check_volume, check_mainforce,
    check_trend, check_fundamental, check_minute_price,
    check_market_day, check_market_trend,
    check_sector_trend, check_sector_leaders, check_chip_concentration,
    get_level, get_suggestion,
)

# ==================== 主引擎类 ====================

class IronSentinelEngine:
    """
    A股买点审核引擎 v2

    数据获取策略（三层降级）:
      优先: NeoData → 备用: 腾讯/新浪 → 降级: akshare
    """

    def __init__(self, stock_code: str, quiet: bool = False):
        self.stock_code = _normalize_code(stock_code)
        self.stock_name = ""
        self._quiet = quiet
        self.results: Dict[str, CheckResult] = {}
        self.data_sources: Dict[str, str] = {}
        self._raw: Dict[str, Any] = {}

    # ---- 数据获取阶段 ----

    def _fetch_all(self) -> bool:
        """获取所有审核所需数据，返回是否至少有基本数据"""
        print("📥 正在获取数据...")

        self._fetch_quote()          # 实时行情 + 股票名称
        self._fetch_daily_bars()     # 日K线
        self._fetch_money_flow()     # 资金流向
        self._fetch_financial()      # 财务数据
        self._fetch_market()         # 大盘指数实时
        self._fetch_market_bars()    # 大盘K线
        self._fetch_industry()        # 行业归属
        self._fetch_sector_trend()   # 板块趋势
        self._fetch_chip()           # 筹码集中度

        ok_count = sum(1 for v in self.data_sources.values()
                       if v not in ("none", "?"))
        print(f"\n✅ 数据获取完成: {ok_count}/9 项 | {self.stock_name or self.stock_code}")
        return ok_count >= 4  # 至少4项有数据才继续

    def _fetch_quote(self) -> None:
        """获取实时行情和股票名称"""
        # 优先：腾讯实时
        data, src, err = get_realtime_tencent(self.stock_code)
        if err or not data:
            data, src, err = get_realtime_akshare(self.stock_code)

        if data:
            self._raw['quote'] = data
            self.data_sources['quote'] = src
            self.stock_name = data.get('name', '')
            print(f"  [行情] ✅ {src} ({self.stock_name})")
        else:
            self.data_sources['quote'] = "none"
            print(f"  [行情] ❌ {err[:50] if err else '行情获取失败'}")

    def _fetch_realtime_quote(self) -> None:
        """获取今日实时行情（NeoData优先 → 腾讯备用），
        结果存入 self._raw['realtime_quote']，
        同时将今日数据注入 bars[-1]，确保 MACD/量能计算包含今天。
        """
        # NeoData 优先
        quote, src, err = get_realtime_neodata(self.stock_code)
        
        # 腾讯备用（腾讯被屏蔽时可能返回 None）
        if not quote:
            qt, qt_src, qt_err = get_realtime_tencent(self.stock_code)
            if qt and not qt_err:
                quote = {
                    'price': qt.get('price'),
                    'chg': qt.get('pct_chg'),
                    'open': qt.get('open'),
                    'high': qt.get('high'),
                    'low': qt.get('low'),
                    'prev_close': qt.get('prev_close'),
                    'vol': qt.get('vol') if qt.get('vol') else qt.get('volumn'),
                    'amount': qt.get('amount'),
                    'turnover': qt.get('turnover_rate'),
                    'pe': qt.get('pe'),
                    'pb': qt.get('pb'),
                    'mkt_cap': qt.get('market_cap'),
                }
                src = qt_src

        if quote and quote.get('price') is not None:
            self._raw['realtime_quote'] = quote
            self.data_sources['realtime_quote'] = src
            price = quote.get('price', '?')
            chg = quote.get('chg', 0) or 0
            upd = quote.get('update_time', '')
            print(f"  [实时] ✅ {src}（价={price} {chg:+.2f}%）")
        else:
            self.data_sources['realtime_quote'] = "none"
            print(f"  [实时] ⚠️ 今日实时行情不可用（{err or ''}）")

    def _inject_today_into_bars(self) -> None:
        """将今日实时数据注入 bars[-1]，使 MACD/量能计算包含今天"""
        bars = self._raw.get('daily_bars', [])
        quote = self._raw.get('realtime_quote', {})
        if not bars or not quote or quote.get('price') is None:
            return

        today_date = datetime.now().strftime('%Y-%m-%d')
        # 用实时数据覆盖 bars[-1] 的收盘/开高低/成交量
        bars[-1] = {
            'date':   today_date,
            'open':   quote.get('open') or quote.get('prev_close') or bars[-1].get('open'),
            'high':   quote.get('high') or bars[-1].get('high'),
            'low':    quote.get('low')  or bars[-1].get('low'),
            'close':  quote.get('price'),
            'vol':    quote.get('vol')   or bars[-1].get('vol'),
            'amount': quote.get('amount') or bars[-1].get('amount'),
            'is_today': True,  # 标记这是今日实时数据
        }
        # 标记数据来源
        rt_src = self.data_sources.get('realtime_quote', '')
        if rt_src and rt_src != 'none':
            self.data_sources['daily_bars'] = f"{self.data_sources.get('daily_bars','?')}+{rt_src}(今日)"

    def _fetch_daily_bars(self) -> None:
        """获取日K线（新浪优先 → 腾讯备用 → akshare降级），获取后注入今日实时数据"""
        bars, src, err = get_daily_bars_sina(self.stock_code, count=80)
        if err or not bars:
            bars, src, err = get_daily_bars_tencent(self.stock_code, count=60)
        if err or not bars:
            bars, src, err = get_daily_bars_akshare(self.stock_code, count=60)

        if bars:
            self._raw['daily_bars'] = bars
            self.data_sources['daily_bars'] = src
            print(f"  [K线] ✅ {src} ({len(bars)}根)")
            # 注入今日实时数据（使 MACD/量能包含今天）
            self._fetch_realtime_quote()
            self._inject_today_into_bars()
        else:
            self.data_sources['daily_bars'] = "none"
            print(f"  [K线] ❌ {err[:50]}")

    def _fetch_money_flow(self) -> None:
        """获取资金流向（NeoData优先 → akshare降级）"""
        flow = None
        src = "none"

        # 优先 NeoData（同时搜索两种可能的type_hint）
        try:
            result, _, err = query_neodata(
                f"查询{self.stock_code}主力资金流向、超大单净流入、大单净流入、中单净流入、小单净流入"
            )
            if not err and result:
                # 尝试多种type_hint（NeoData版本不同返回不同）
                for hint in ["今日资金流向", "资金流向与龙虎榜", "资金流向"]:
                    txt = nd_extract_text(result, hint)
                    if txt and '净流入' in txt:
                        break

                if txt and '净流入' in txt:
                    # 提取各档位净流入（NeoData返回单位为"元"，需转换）
                    def extract_net(keyword, text):
                        """匹配 关键词 数字 元/万/亿，返回元为单位的数值"""
                        # NeoData 主要返回"元"单位
                        patterns = [
                            rf'{re.escape(keyword)}[:：]?\s*([-\d,]+\.?\d*)\s*元',
                            rf'{re.escape(keyword)}[:：]?\s*([-\d,]+\.?\d*)\s*万',
                            rf'{re.escape(keyword)}[:：]?\s*([-\d,]+\.?\d*)\s*亿',
                            rf'{re.escape(keyword)}[:：]?\s*([-\d,]+\.?\d*)',
                        ]
                        for pat in patterns:
                            m = re.search(pat, text)
                            if m:
                                val = float(m.group(1).replace(',', ''))
                                if '万' in pat and '亿' not in pat: val *= 10000
                                elif '亿' in pat: val *= 100000000
                                return val
                        return None

                    # NeoData 返回格式举例：
                    #   主力净流入-49906995元（净流出为负）
                    #   超大单流入-12746730元（大单流出为负，关键词不含"净"）
                    super_net  = extract_net('超大单净流入', txt) or extract_net('超大单流入', txt)
                    big_net    = extract_net('大单净流入', txt) or extract_net('大单流入', txt)
                    mid_net    = extract_net('中单净流入', txt) or extract_net('中单流入', txt)
                    small_net  = extract_net('小单净流入', txt) or extract_net('小单流入', txt)
                    main_net   = extract_net('主力净流入', txt) or extract_net('主力净流入', txt)

                    if any(v is not None for v in [super_net, big_net, main_net]):
                        flow = {
                            'super_net': super_net or 0,
                            'big_net':   big_net   or 0,
                            'mid_net':   mid_net   or 0,
                            'small_net': small_net or 0,
                            'main_net':  main_net  or (super_net or 0) + (big_net or 0),
                        }
                        src = "neodata"
        except Exception:
            pass

        # akshare 降级
        if not flow:
            flow, src, _ = get_money_flow_akshare(self.stock_code)

        if flow:
            self._raw['money_flow'] = flow
            self.data_sources['money_flow'] = src
            # 格式化输出
            main = flow.get('main_net', 0)
            if abs(main) >= 100000000:
                s = f"{main/100000000:.2f}亿"
            else:
                s = f"{main/10000:.2f}万"
            print(f"  [资金] ✅ {src}（主力净流入{s}）")
        else:
            self.data_sources['money_flow'] = "none"
            print(f"  [资金] ❌ 资金流向不可用")

    def _fetch_financial(self) -> None:
        """获取财务数据（NeoData优先 → akshare降级）"""
        fin = None
        src = "none"

        # NeoData 优先（尝试多个type_hint）
        try:
            result, _, err = query_neodata(
                f"查询{self.stock_code}市盈率PE、市净率PB、净资产收益率ROE、每股收益、营业总收入、净利润"
            )
            if not err and result:
                # 尝试多个可能的 type_hint
                txt = ""
                for hint in ["基本面", "估值数据与基本面分析", "财务指标", "估值"]:
                    t = nd_extract_text(result, hint)
                    if t and ('市盈' in t or '市净' in t):
                        txt = t
                        break

                if txt and ('市盈' in txt or '市净' in txt):
                    def extract_num(pat, text):
                        m = re.search(pat, text)
                        if m:
                            try: return float(m.group(1))
                            except: return None
                        return None

                    # 市盈率：支持"市盈TTM为负值"、"市盈率(PE)"、"市盈TTM为"等
                    pe_raw = None
                    if '为负值' in txt or '为负' in txt:
                        pe_raw = None  # 亏损股PE无意义
                    else:
                        for pat in [
                            r'市盈TTM[^0-9\uff1a:]*([-\d.]+)',
                            r'市盈率\(PE\)[^0-9\uff1a:]*([-\d.]+)',
                            r'市盈率[^0-9\uff1a:]*([-\d.]+)',
                        ]:
                            v = extract_num(pat, txt)
                            if v is not None:
                                pe_raw = v
                                break

                    pb = extract_num(r'市净率[^0-9\uff1a:]*([-\d.]+)', txt)
                    roe  = extract_num(r'净资产收益率TTM[^0-9\uff1a:]*([-\d.]+)', txt)
                    eps  = extract_num(r'每股收益[^0-9\uff1a:]*([-\d.]+)', txt)

                    if any(v is not None for v in [pe_raw, pb, roe, eps]):
                        fin = {
                            'pe': pe_raw, 'pb': pb, 'roe': roe,
                            'eps': eps,
                        }
                        src = "neodata"
        except Exception:
            pass

        # akshare 降级
        if not fin:
            fin, src, _ = get_financial_akshare(self.stock_code)

        if fin:
            self._raw['financial'] = fin
            self.data_sources['financial'] = src
            pe_v = fin.get('pe', 'N/A')
            pb_v = fin.get('pb', 'N/A')
            print(f"  [财务] ✅ {src}（PE={pe_v} PB={pb_v}）")
        else:
            self.data_sources['financial'] = "none"
            print(f"  [财务] ❌ 财务数据不可用")

    def _fetch_market(self) -> None:
        """获取大盘指数实时数据"""
        sh_data, sh_src, sh_err = get_index_tencent("sh000001")
        sz_data, sz_src, sz_err = get_index_tencent("sz399001")

        if sh_data or sz_data:
            self._raw['market'] = {
                'sh': sh_data or {},
                'sz': sz_data or {},
            }
            self.data_sources['market'] = sh_src if sh_data else (sz_src if sz_data else "none")
            sh_flag = '✅' if sh_data else '⚠️'
            sz_flag = '✅' if sz_data else '⚠️'
            sh_str  = f"{sh_data.get('chg_pct', 0):+.2f}%" if sh_data else "N/A"
            sz_str  = f"{sz_data.get('chg_pct', 0):+.2f}%" if sz_data else "N/A"
            print(f"  [大盘] {sh_flag}上证{sh_str} {sz_flag}深证{sz_str}")
        else:
            self.data_sources['market'] = "none"
            print(f"  [大盘] ❌ 大盘数据不可用")

    def _fetch_market_bars(self) -> None:
        """获取大盘指数K线（腾讯备用 → 新浪备用）"""
        bars, src, err = get_index_bars_tencent("sh000001", count=10)
        if err or not bars:
            # 新浪大盘K线
            try:
                import requests
                params = {'symbol': 'sh000001', 'scale': 240, 'ma': 'no', 'datalen': 15}
                r = requests.get(
                    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData",
                    params=params, timeout=10
                )
                data = r.json()
                if data:
                    bars = [{
                        'date': d['day'],
                        'open':  _to_float(d.get('open')),
                        'high':  _to_float(d.get('high')),
                        'low':   _to_float(d.get('low')),
                        'close': _to_float(d.get('close')),
                        'vol':   _to_float(d.get('volume')),
                    } for d in data]
                    src = "sina"
                    err = ""
            except Exception as e:
                err = str(e)

        if bars:
            self._raw['market_bars'] = bars
            self.data_sources['market_bars'] = src
            print(f"  [大盘K] ✅ {src} ({len(bars)}根)")
        else:
            self.data_sources['market_bars'] = "none"
            print(f"  [大盘K] ❌ 大盘K线不可用")

    def _fetch_industry(self) -> None:
        """获取行业归属（NeoData优先 → akshare备用）"""
        industry_name = ""
        src = "none"

        # NeoData 优先
        try:
            ind_data, ind_src, _ = get_industry_info_neodata(self.stock_code)
            if ind_data and ind_data.get('industry_name'):
                industry_name = ind_data['industry_name']
                src = "neodata"
        except Exception:
            pass

        # akshare 备用（从主营产品关键字推断）
        if not industry_name:
            try:
                import akshare as ak
                _, num = _market_code(self.stock_code)
                df = ak.stock_individual_info_em(symbol=num)
                if df is not None:
                    for _, row in df.iterrows():
                        if '行业' in str(row.get('item', '')):
                            industry_name = str(row.get('value', '')).strip()
                            src = "akshare"
                            break
            except Exception:
                pass

        # 从腾讯行情中取行业（如果有）
        quote = self._raw.get('quote', {})
        if not industry_name and quote.get('sector'):
            industry_name = quote['sector']
            src = "tencent"

        self._raw['sector_name'] = industry_name or "电力设备"
        self.data_sources['industry'] = src
        if src != "none":
            print(f"  [行业] ✅ {src} → {self._raw['sector_name']}")
        else:
            print(f"  [行业] ⚠️ 待推断 → {self._raw['sector_name']}")

    def _fetch_sector_trend(self) -> None:
        """获取板块今日涨跌（通过板块内成分股判断）"""
        sector_name = self._raw.get('sector_name', '')
        chg_pct = 0.0
        src = "none"

        if sector_name:
            # 通过腾讯实时获取板块涨跌（如果有板块指数）
            # 先尝试从日K线数据获取当日行业平均涨幅
            bars = self._raw.get('daily_bars', [])
            if len(bars) >= 2:
                close_t = _to_float(bars[-1].get('close'))
                close_y = _to_float(bars[-2].get('close'))
                if close_y > 0:
                    chg_pct = (close_t - close_y) / close_y * 100
                    src = "self"
            # akshare 板块数据
            if src == "none":
                try:
                    sd, ss, _ = get_sector_index_akshare(sector_name)
                    if sd and sd.get('chg_pct') is not None:
                        chg_pct = sd['chg_pct']
                        src = ss
                except Exception:
                    pass

        self._raw['sector_trend'] = {
            'sector_name': sector_name,
            'chg_pct': chg_pct,
        }
        self.data_sources['sector_trend'] = src
        flag = '✅' if chg_pct > 0 else '❌'
        print(f"  [板块] {flag if src != 'none' else '⚠️'} {sector_name} {'↑' if chg_pct > 0 else '↓'}{abs(chg_pct):.2f}%")

    def _fetch_chip(self) -> None:
        """获取筹码集中度数据（NeoData优先 → akshare降级）"""
        holder_chg = None
        top10_float = None
        holder_count = None
        holder_date = ""
        src_used = "none"

        # NeoData 优先
        try:
            result, _, err = query_neodata(
                f"查询{self.stock_code}股东户数、股东人数环比变化(%)、前十大流通股东持股合计比例(%)、前十大流通股东占比(%)、筹码集中度及近季度变化趋势"
            )
            if not err and result:
                # 尝试多种type_hint（NeoData版本不同返回不同）
                for hint in ["股东", "筹码", "股东股本与机构持仓情况", "基本面"]:
                    txt = nd_extract_text(result, hint)
                    if txt and ('股东' in txt or '筹码' in txt):
                        break
                if txt:
                    def extract_float(pattern, text):
                        m = re.search(pattern, text)
                        return float(m.group(1)) if m else None

                    # 股东户数（万户）
                    hc_m = re.search(r'股东人数\s*([\d.]+)\s*万户', txt)
                    if hc_m:
                        holder_count = float(hc_m.group(1)) * 10000

                    # 环比变化（%，支持括号内格式：股东人数环比变化10.87%））
                    chg_m = re.search(r'股东人数环比变化[（(]?[^)\d]*([-\d.]+)%?', txt)
                    if chg_m:
                        holder_chg = float(chg_m.group(1))

                    # 前十流通股（%，支持多种格式）
                    for pat in [
                        r'前十大流通股东[^占比\d]*占比[^:\d]*[:：]?\s*([\d.]+)%',
                        r'前十大流通股东持股[^占比\d]*占比[^:\d]*[:：]?\s*([\d.]+)%',
                        r'前十[流大]?通股东[^占比\d]*占比[^:\d]*[:：]?\s*([\d.]+)%',
                        r'前十大流通股东[^持比]*持[^:\d]*[:：]?\s*([\d.]+)%',
                        r'前十流通股占比[:：]?\s*([\d.]+)%',
                    ]:
                        t10_m = re.search(pat, txt)
                        if t10_m:
                            top10_float = float(t10_m.group(1))
                            break

                    # 更新日期
                    date_m = re.search(r'根据([\d\-]+)更新', txt)
                    if date_m:
                        holder_date = date_m.group(1)

                    if holder_chg is not None or top10_float is not None:
                        src_used = "neodata"
        except Exception:
            pass

        # akshare 降级（前十流通股）
        if top10_float is None:
            try:
                import akshare as ak
                _, num = _market_code(self.stock_code)
                top10_df = ak.stock_gdfx_free_top_10_em(symbol=num)
                if top10_df is not None and len(top10_df) > 0:
                    lt_df = top10_df[top10_df['股票代码'] == num]
                    if not lt_df.empty:
                        top10_float = float(lt_df.iloc[-1].get('占流通盘比例', 0))
                        src_used = "akshare"
            except Exception:
                pass

        chip_data = {
            'holder_chg': holder_chg,
            'top10_float_pct': top10_float,
            'holder_count': holder_count,
            'data_date': holder_date,
            'source': src_used,
        }

        self._raw['chip'] = chip_data
        self.data_sources['chip'] = src_used

        if src_used != "none":
            hc_str = f"{holder_chg:+.1f}%" if holder_chg is not None else "N/A"
            t10_str = f"{top10_float:.1f}%" if top10_float is not None else "N/A"
            print(f"  [筹码] ✅ {src_used}（环比{hc_str}，前十流通{t10_str}）")
        else:
            print(f"  [筹码] ❌ 筹码数据不可用")

    # ---- 审核阶段 ----

    def _audit(self) -> List[CheckResult]:
        """执行全部11项审核"""
        if not self._quiet: print("\n🔍 审核中...")

        bars       = self._raw.get('daily_bars', [])
        quote      = self._raw.get('quote')
        flow       = self._raw.get('money_flow')
        fin        = self._raw.get('financial')
        mkt        = self._raw.get('market')
        mkt_bars   = self._raw.get('market_bars')
        sector_trd = self._raw.get('sector_trend')
        chip       = self._raw.get('chip')
        sector_name = self._raw.get('sector_name', '')

        results: List[CheckResult] = []

        # 1. MACD
        r = check_macd(bars, quote)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 2. 量能
        r = check_volume(bars, quote)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 3. 主力净流入
        r = check_mainforce(flow)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 4. 上升趋势
        r = check_trend(bars)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 5. 基本面
        r = check_fundamental(fin, quote or {})
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 6. 分时均线
        r = check_minute_price(quote)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 7. 大盘日内
        r = check_market_day(mkt)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 8. 大盘趋势
        r = check_market_trend(mkt_bars)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 9. 板块趋势
        r = check_sector_trend(sector_trd)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 10. 龙头活跃（双维度）
        print(f"\n🏢 龙头详情（近5日 / 今日盘中）:")
        leader_details, src_used = self._fetch_leader_details(sector_name)
        r = check_sector_leaders(sector_name, leader_details, src_used)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        # 11. 筹码集中
        r = check_chip_concentration(chip)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:60]}")

        total_score = sum(r.score for r in results)
        passed      = sum(1 for r in results if r.passed)
        failed      = sum(1 for r in results if not r.passed and r.available)
        unavailable = sum(1 for r in results if not r.available)

        print(f"\n📊 最终评分: {int(total_score)}分 — {get_level(total_score)}")

        return results, leader_details

    def _fetch_leader_details(self, sector_name: str) -> Tuple[List[Dict], str]:
        """获取龙头成分股涨幅数据"""
        leaders = get_board_leaders_fixed(sector_name)
        leader_details = []
        src_used = "fixed"

        for name, code in leaders:
            gain_5d = 0.0
            gain_today = None

            # 近5日涨幅（新浪日K线）
            bars, src, _ = get_daily_bars_sina(code, count=10)
            if bars and len(bars) >= 2:
                c0 = _to_float(bars[0].get('close'))
                c1 = _to_float(bars[-1].get('close'))
                gain_5d = (c1 - c0) / c0 * 100 if c0 else 0

            # 今日实时涨幅（腾讯）
            rt, _, _ = get_realtime_tencent(code)
            if rt:
                gain_today = _to_float(rt.get('chg_pct'))

            leader_details.append({
                'name': name,
                'code': code,
                'gain_5d': gain_5d,
                'gain_today': gain_today,
            })

        return leader_details, src_used

    # ---- 公开接口 ----

    def audit(self) -> 'AuditReport':
        """
        执行完整买点审核，返回 AuditReport 对象。

        使用示例:
            report = IronSentinelEngine('300438').audit()
            print(report.total_score)          # 85
            print(report.level)               # "✅ 良好买点"
            print(report.suggestion)          # "良好买点，可以考虑买入"
            print(report.to_dict())            # 完整结果（JSON）
        """
        import io
        _orig_stdout = sys.stdout
        if self._quiet:
            sys.stdout = io.StringIO()

        try:
            clear_cache()

            if not self._fetch_all():
                print("\n⚠️ 数据不足，无法完成审核")

            results, leader_details = self._audit()

            total_score = sum(r.score for r in results)
            passed      = sum(1 for r in results if r.passed)
            failed      = sum(1 for r in results if not r.passed and r.available)
            unavailable = sum(1 for r in results if not r.available)

            report = AuditReport(
                stock_code=self.stock_code,
                stock_name=self.stock_name,
                total_score=total_score,
                passed_count=passed,
                failed_count=failed,
                unavailable_count=unavailable,
                results=results,
                suggestion=get_suggestion(total_score),
                level=get_level(total_score),
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                data_sources=self.data_sources,
                leader_details=leader_details,
            )
        finally:
            sys.stdout = _orig_stdout

        return report


# ==================== 报告类 ====================

class AuditReport:
    def __init__(
        self,
        stock_code: str,
        stock_name: str,
        total_score: float,
        passed_count: int,
        failed_count: int,
        unavailable_count: int,
        results: List[CheckResult],
        suggestion: str,
        level: str,
        timestamp: str,
        data_sources: Dict[str, str] = None,
        leader_details: List[Dict] = None,
    ):
        self.stock_code        = stock_code
        self.stock_name        = stock_name
        self.total_score       = total_score
        self.passed_count      = passed_count
        self.failed_count      = failed_count
        self.unavailable_count = unavailable_count
        self.results           = results
        self.suggestion        = suggestion
        self.level             = level
        self.timestamp         = timestamp
        self.data_sources      = data_sources or {}
        self.leader_details    = leader_details or []

    def to_dict(self) -> Dict:
        return {
            'stock_code':        self.stock_code,
            'stock_name':        self.stock_name,
            'total_score':       self.total_score,
            'level':             self.level,
            'passed_count':      self.passed_count,
            'failed_count':      self.failed_count,
            'unavailable_count': self.unavailable_count,
            'suggestion':        self.suggestion,
            'timestamp':         self.timestamp,
            'data_sources':      self.data_sources,
            'results':           [r.to_dict() for r in self.results],
            'leader_details':    self.leader_details,
        }


# ==================== 报告格式化 ====================

def format_report(report: AuditReport) -> str:
    """生成美化的审核报告：按顺序11项 + 龙头详情 + 专家总结"""
    def badge(r):
        if r.passed:    return "✅"
        if r.available: return "❌"
        return "⚠️ "

    W = 60

    def clean_reason(reason):
        reason = re.sub(r'（数据源[:：][^）]+）', '', reason)
        return reason.strip()

    lines = []

    # ── 头部 ──
    lines.append("╔" + "═" * (W - 2) + "╗")
    name_str = f"  {report.stock_name or ''}({report.stock_code}) 买点审核报告"
    lines.append("║" + name_str.center(W - 2) + "║")
    lines.append("║" + f"  审核时间：{report.timestamp}".ljust(W - 2) + "║")
    lines.append("╚" + "═" * (W - 2) + "╝")

    # ── 评分 ──
    lines.append(f"  评分 {int(report.total_score)}/100  {report.level}")
    lines.append(f"  建议  {report.suggestion}")

    # ── 11项审核（按顺序，无分组标题） ──
    lines.append("")
    lines.append("┌──────────────────────────────────────────────────────────────┐")
    for r in report.results:
        status = badge(r)
        name   = f"[{r.rule_num:02d}] {r.rule_name}"
        reason = clean_reason(r.reason)
        lines.append(f"│ {status}  {name}")
        if len(reason) > 52:
            lines.append(f"│      {reason[:52]}")
            lines.append(f"│      {reason[52:]}")
        else:
            lines.append(f"│      {reason}")
        lines.append("│")
    lines.append("└──────────────────────────────────────────────────────────────┘")

    # ── 专家总结（自动生成） ──
    passed   = [r for r in report.results if r.passed]
    failed   = [r for r in report.results if not r.passed and r.available]
    unavail  = [r for r in report.results if not r.available]
    score    = int(report.total_score)

    obs = []
    # 通过项 → 亮点
    for r in passed:
        rn = r.rule_num
        reason = r.reason
        if rn == 1 and '红柱' in reason:
            obs.append("MACD 红柱扩张，短线动能向好")
        elif rn == 2 and '均量' in reason:
            obs.append("量能放大，上涨有资金支撑")
        elif rn == 3 and '净流入' in reason:
            obs.append("主力净流入积极，机构在吸筹")
        elif rn == 4 and '多头' in reason:
            obs.append("均线多头排列，短期趋势向好")
        elif rn == 5:
            obs.append("基本面数据良好，估值合理")
        elif rn == 6 and '多头' in reason:
            obs.append("分时均价上方运行，多头控盘")
        elif rn == 7:
            obs.append("大盘日内走势稳健，指数安全边际较高")
        elif rn == 8:
            obs.append("大盘趋势向上，中期环境偏多")
        elif rn == 9:
            obs.append("板块走势强劲，景气度较高")
        elif rn == 10:
            obs.append("板块龙头表现活跃，龙头效应明显")
        elif rn == 11:
            obs.append("筹码集中度高，主力控盘能力强")

    # 失败项 → 风险
    risks = []
    for r in failed:
        rn = r.rule_num
        reason = r.reason
        if rn == 1 and '绿柱' in reason:
            risks.append("MACD 绿柱，空头动能尚未结束")
        elif rn == 2:
            risks.append("量能不足，上涨动力有限")
        elif rn == 3:
            risks.append("主力资金持续流出，机构在减仓")
        elif rn == 4:
            risks.append("价格跌破均线，空头排列尚未扭转")
        elif rn == 5:
            risks.append("基本面数据不佳，估值偏高或业绩下滑")
        elif rn == 6:
            risks.append("股价在均价下方运行，空头控盘")
        elif rn == 7:
            risks.append("大盘日内走势偏弱")
        elif rn == 8:
            risks.append("大盘趋势向下，中期环境偏空")
        elif rn == 9:
            risks.append("板块走势偏弱，拖累个股")
        elif rn == 10:
            risks.append("龙头表现疲软，板块整体低迷")
        elif rn == 11:
            risks.append("筹码分散，主力控盘能力弱")

    lines.append("")
    lines.append("  🔍 金融专家总结")
    lines.append("  " + "-" * 50)

    if risks:
        for risk in risks[:3]:
            lines.append(f"  ⚠️ {risk}")
    if obs:
        for ob in obs[:3]:
            lines.append(f"  ✅ {ob}")
    if score >= 70:
        lines.append(f"  📈 综合评估：买点条件较好，可考虑轻仓介入")
    elif score >= 50:
        lines.append(f"  📊 综合评估：买点条件一般，建议耐心等待更佳时机")
    elif score >= 30:
        lines.append(f"  📉 综合评估：买点条件不足，暂不推荐入场")
    else:
        lines.append(f"  🚫 综合评估：风险较高，不建议入场")

    lines.append("")

    # ── 龙头详情（表格下方） ──
    if report.leader_details:
        lines.append("  🐉 板块龙头详情")
        lines.append("  " + "-" * 50)
        lines.append(f"  {'名称':<10} {'近5日':>8}  {'今日盘中':>8}")
        lines.append("  " + "-" * 50)
        for d in report.leader_details:
            g5   = f"{d['gain_5d']:+.2f}%" if d['gain_5d'] != 0 else "  --  "
            gt   = f"{d['gain_today']:+.2f}%" if d['gain_today'] is not None else "  --  "
            flag = "📈" if d['gain_today'] and d['gain_today'] > 0 else "📉"
            lines.append(f"  {d['name']:<10} {g5:>8}  {flag} {gt:>8}")
        lines.append("")

    # ── 底部 ──
    lines.append("  " + "─" * 50)
    lines.append("  🔔 请到海通确认KD点后再做决策")
    lines.append("  " + "─" * 50)

    return "\n".join(lines)


# ==================== CLI 入口 ====================

def main():
    p = argparse.ArgumentParser(description="铁血哨兵 v2 - A股买点审核引擎")
    p.add_argument('stock_code', help='股票代码，如 300438 或 sz300438')
    p.add_argument('--json',   action='store_true', help='JSON输出（干净，无进度）')
    p.add_argument('--format',  action='store_true', help='完整格式化报告（旧版）')
    p.add_argument('--report',  action='store_true', help='完整三段式报告（新标准输出）')
    args = p.parse_args()

    try:
        report = IronSentinelEngine(args.stock_code, quiet=args.json or args.report).audit()
        if args.json:
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        elif args.report:
            import build_report as br
            print(br.build_report(report.to_dict()))
        elif args.format:
            print(format_report(report))
        else:
            print(f"📊 {report.stock_name or report.stock_code}({report.stock_code}): "
                  f"{int(report.total_score)}分 {report.level}")
            print(f"💡 {report.suggestion}")
    except Exception as e:
        import traceback
        print(f"❌ 审核失败: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
