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
    query_neodata, nd_extract_text, nd_extract_number,
    get_realtime_tencent, get_daily_bars_tencent,
    get_daily_bars_sina,  # 新浪备用日K线
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

    def __init__(self, stock_code: str):
        self.stock_code = _normalize_code(stock_code)
        self.stock_name = ""
        self.results: Dict[str, CheckResult] = {}
        self.data_sources: Dict[str, str] = {}

        # 原始数据缓存（供审核函数使用）
        self._raw: Dict[str, Any] = {}

    # ---- 数据获取阶段 ----

    def _fetch_all(self) -> bool:
        """获取所有审核所需数据，返回是否至少有基本数据"""
        print("📥 正在获取数据...")

        # Step 1: 基本行情 + 股票名称（NeoData优先，腾讯备用）
        self._fetch_quote()
        # Step 2: 日K线（NeoData优先，腾讯备用，akshare降级）
        self._fetch_daily_bars()
        # Step 3: 资金流向（NeoData优先，akshare降级）
        self._fetch_money_flow()
        # Step 4: 基本面（akshare）
        self._fetch_financial()
        # Step 5: 大盘指数（腾讯备用）
        self._fetch_market()
        # Step 6: 大盘K线（腾讯备用）
        self._fetch_market_bars()
        # Step 7: 行业归属（NeoData优先，akshare备用）
        self._fetch_industry()
        # Step 8: 筹码集中度（NeoData优先，akshare备用）
        self._fetch_chip()

        ok_count = sum(1 for v in self.data_sources.values() if v != "none")
        print(f"\n✅ 数据获取完成: {ok_count}/8 项 | {self.stock_name or self.stock_code}")
        return ok_count >= 4  # 至少4项有数据才继续

    def _fetch_quote(self) -> None:
        """获取实时行情和股票名称"""
        # 优先：NeoData查询股票名称
        try:
            result, _, err = query_neodata(f"查询{self.stock_code}的实时行情、当前价格、涨跌幅、成交量")
            if not err and result:
                txt = nd_extract_text(result, '实时行情数据')
                if txt:
                    name_m = re.search(r'股票名称[:：]\s*([^\n,，。]+)', txt)
                    if name_m:
                        self.stock_name = name_m.group(1).strip()
        except Exception:
            pass

        # 腾讯实时备用
        data, src, err = get_realtime_tencent(self.stock_code)
        if err:
            data, src, err = get_realtime_akshare(self.stock_code)

        if data:
            self._raw['quote'] = data
            self.data_sources['quote'] = src
            if not self.stock_name and data.get('name'):
                self.stock_name = data['name']
            print(f"  [行情] {'✅' if data else '❌'} {src} ({self.stock_name})")
        else:
            self.data_sources['quote'] = "none"
            print(f"  [行情] ❌ 失败: {err[:40]}")

    def _fetch_daily_bars(self) -> None:
        """获取日K线（新浪优先 → 腾讯备用 → akshare降级）"""
        # 优先：新浪财经
        bars, src, err = get_daily_bars_sina(self.stock_code, count=80)
        if err or not bars:
            # 腾讯备用
            bars, src, err = get_daily_bars_tencent(self.stock_code, count=60)
        if err or not bars:
            # akshare 降级
            bars, src, err = get_daily_bars_akshare(self.stock_code, count=60)

        if bars:
            self._raw['daily_bars'] = bars
            self.data_sources['daily_bars'] = src
            print(f"  [K线] ✅ {src} ({len(bars)}根)")
        else:
            self.data_sources['daily_bars'] = "none"
            print(f"  [K线] ❌ {err[:40]}")

    def _fetch_money_flow(self) -> None:
        """获取资金流向"""
        # 优先 NeoData
        try:
            result, _, err = query_neodata(f"查询{self.stock_code}主力资金流向、超大单、大单净流入")
            if not err and result:
                txt = nd_extract_text(result, '资金流向与龙虎榜')
                if txt:
                    main_net = nd_extract_number(result, '资金流向与龙虎榜', r'主力净流入[:：]\s*([-\d.]+)')
                    super_net = nd_extract_number(result, '资金流向与龙虎榜', r'超大单净流入[:：]\s*([-\d.]+)')
                    big_net = nd_extract_number(result, '资金流向与龙虎榜', r'大单净流入[:：]\s*([-\d.]+)')
                    if any(v is not None for v in [main_net, super_net, big_net]):
                        self._raw['money_flow'] = {
                            'main_net': main_net or 0,
                            'super_net': super_net or 0,
                            'big_net': big_net or 0,
                        }
                        self.data_sources['money_flow'] = "neodata"
                        print(f"  [资金] ✅ neodata")
                        return
        except Exception:
            pass

        # akshare 降级
        flow, src, err = get_money_flow_akshare(self.stock_code)
        if flow:
            self._raw['money_flow'] = flow
            self.data_sources['money_flow'] = src
            print(f"  [资金] ✅ {src}")
        else:
            self.data_sources['money_flow'] = "none"
            print(f"  [资金] ❌ 资金流向不可用")

    def _fetch_financial(self) -> None:
        """获取财务数据"""
        fin, src, err = get_financial_akshare(self.stock_code)
        if fin:
            self._raw['financial'] = fin
            self.data_sources['financial'] = src
            print(f"  [财务] ✅ {src}")
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
            ok = f"{'✅' if sh_data else '⚠️'}上证 {'✅' if sz_data else '⚠️'}深证"
            print(f"  [大盘] {ok}")
        else:
            self.data_sources['market'] = "none"
            print(f"  [大盘] ❌ 大盘数据不可用")

    def _fetch_market_bars(self) -> None:
        """获取大盘指数K线"""
        bars, src, err = get_index_bars_tencent("sh000001", count=10)
        if bars:
            self._raw['market_bars'] = bars
            self.data_sources['market_bars'] = src
            print(f"  [大盘K] ✅ {src} ({len(bars)}根)")
        else:
            self.data_sources['market_bars'] = "none"
            print(f"  [大盘K] ❌ 大盘K线不可用")

    def _fetch_industry(self) -> None:
        """获取行业归属"""
        # 优先：直接从日K线数据推断（腾讯数据包含板块信息）
        industry_name = ""

        # akshare 查板块成分股时顺便获取
        sector_data, src, err = get_sector_index_akshare("电力设备")
        if sector_data:
            industry_name = sector_data.get('sector_name', '')

        # NeoData 查行业归属
        ind_data, ind_src, ind_err = get_industry_info_neodata(self.stock_code)
        if ind_data and ind_data.get('industry_name'):
            industry_name = ind_data['industry_name']

        self._raw['sector_name'] = industry_name or "电力设备"
        self.data_sources['industry'] = "neodata" if ind_data else ("akshare" if sector_data else "none")
        print(f"  [板块] {'✅ ' + self.data_sources['industry'] if self.data_sources['industry'] != 'none' else '⚠️ 待推断'} {self._raw['sector_name']}")

    def _fetch_chip(self) -> None:
        """获取筹码集中度数据"""
        holder_chg = None
        top10_float = None
        holder_count = None
        holder_date = ""
        src_used = "none"

        # 优先 NeoData
        try:
            result, _, err = query_neodata(
                f"查询{self.stock_code}股东户数、股东人数环比变化、前十大流通股东占比及近季度变化趋势"
            )
            if not err and result:
                txt = nd_extract_text(result, '股东股本与机构持仓情况')
                if txt:
                    hc_m = re.search(r'股东人数([\d.]+)万户', txt)
                    if hc_m:
                        holder_count = float(hc_m.group(1)) * 10000
                    chg_m = re.search(r'股东人数环比变化([-\d.]+)%', txt)
                    if chg_m:
                        holder_chg = float(chg_m.group(1))
                    t10_m = re.search(r'前十大流通股东占比([\d.]+)%', txt)
                    if t10_m:
                        top10_float = float(t10_m.group(1))
                    date_m = re.search(r'根据([\d\-]+)更新', txt)
                    if date_m:
                        holder_date = date_m.group(1)
                    if holder_chg is not None or top10_float is not None:
                        src_used = "neodata"
                        self._raw['chip'] = {
                            'holder_chg': holder_chg,
                            'top10_float_pct': top10_float,
                            'holder_count': holder_count,
                            'data_date': holder_date,
                            'source': 'neodata',
                        }
                        self.data_sources['chip'] = "neodata"
                        print(f"  [筹码] ✅ neodata")
                        return
        except Exception:
            pass

        # akshare 降级（获取前十流通股）
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

        if src_used != "none":
            self._raw['chip'] = {
                'holder_chg': holder_chg,
                'top10_float_pct': top10_float,
                'holder_count': holder_count,
                'data_date': holder_date,
                'source': src_used,
            }
            self.data_sources['chip'] = src_used
            print(f"  [筹码] ✅ {src_used}")
        else:
            self.data_sources['chip'] = "none"
            print(f"  [筹码] ❌ 筹码数据不可用")

    # ---- 审核阶段 ----

    def _audit(self) -> List[CheckResult]:
        """执行全部11项审核"""
        print("\n🔍 审核中...")
        bars = self._raw.get('daily_bars', [])
        quote = self._raw.get('quote')
        flow = self._raw.get('money_flow')
        fin = self._raw.get('financial')
        mkt = self._raw.get('market')
        mkt_bars = self._raw.get('market_bars')
        sector_name = self._raw.get('sector_name', '')

        results: List[CheckResult] = []

        # 1. MACD
        r = check_macd(bars, quote)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 2. 量能
        r = check_volume(bars, quote)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 3. 主力净流入
        r = check_mainforce(flow)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 4. 上升趋势
        r = check_trend(bars)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 5. 基本面
        r = check_fundamental(fin, quote or {})
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 6. 分时均线
        r = check_minute_price(quote)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 7. 大盘日内
        r = check_market_day(mkt)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 8. 大盘趋势
        r = check_market_trend(mkt_bars)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 9. 板块趋势
        sector_data = {'sector_name': sector_name, 'chg_pct': 0.0}  # 简化，如需要可扩展
        r = check_sector_trend(sector_data)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        # 10. 龙头活跃（双维度）
        leader_details, src_used = self._fetch_leader_details(sector_name)
        print(f"\n🏢 龙头详情（近5日 / 今日盘中）:")
        r = check_sector_leaders(sector_name, leader_details, src_used)
        results.append(r)

        # 11. 筹码集中
        chip_data = self._raw.get('chip')
        r = check_chip_concentration(chip_data)
        results.append(r)
        print(f"  [{r.rule_num:02d}] {r.rule_name}: {'✅' if r.passed else '❌'} {r.reason[:50]}")

        return results

    def _fetch_leader_details(self, sector_name: str) -> Tuple[List[Dict], str]:
        """获取龙头成分股涨幅数据"""
        leaders = get_board_leaders_fixed(sector_name)
        leader_details = []
        src_used = "fixed"

        for name, code in leaders:
            gain_5d = 0.0
            gain_today = None

            # 近5日涨幅
            bars, src, _ = get_daily_bars_tencent(code, count=10)
            if bars and len(bars) >= 2:
                c0 = _to_float(bars[0].get('close'))
                c1 = _to_float(bars[-1].get('close'))
                gain_5d = (c1 - c0) / c0 * 100 if c0 else 0

            # 今日实时涨幅
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
            print(report.total_score)
            print(report.to_dict())
        """
        clear_cache()

        if not self._fetch_all():
            print("\n⚠️ 数据不足，无法完成审核")

        results = self._audit()

        total_score = sum(r.score for r in results)
        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed and r.available)
        unavailable = sum(1 for r in results if not r.available)

        print(f"\n📊 最终评分: {int(total_score)}分 — {get_level(total_score)}")
        print(f"   ✅通过 {passed}/11 | ❌失败 {failed}/11 | ⚠️不可用 {unavailable}/11")

        return AuditReport(
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
        )


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
    ):
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.total_score = total_score
        self.passed_count = passed_count
        self.failed_count = failed_count
        self.unavailable_count = unavailable_count
        self.results = results
        self.suggestion = suggestion
        self.level = level
        self.timestamp = timestamp
        self.data_sources = data_sources or {}

    def to_dict(self) -> Dict:
        return {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'total_score': self.total_score,
            'level': self.level,
            'passed_count': self.passed_count,
            'failed_count': self.failed_count,
            'unavailable_count': self.unavailable_count,
            'suggestion': self.suggestion,
            'timestamp': self.timestamp,
            'data_sources': self.data_sources,
            'results': [r.to_dict() for r in self.results],
        }


# ==================== 报告格式化 ====================

def format_report(report: AuditReport) -> str:
    """生成格式化的审核报告"""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  📊 {report.stock_name or report.stock_code}({report.stock_code}) 买点审核报告")
    lines.append(f"  审核时间：{report.timestamp}")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"  🎯 综合评分：{int(report.total_score)}/100 | {report.level}")
    lines.append(f"  💡 建议：{report.suggestion}")
    lines.append("")

    # 通过/失败/不可用统计
    passed_items = [r for r in report.results if r.passed]
    failed_items = [r for r in report.results if not r.passed and r.available]
    unavail_items = [r for r in report.results if not r.available]

    if passed_items:
        lines.append("✅ 通过项：")
        for r in passed_items:
            lines.append(f"  • {r.rule_name}: {r.reason[:60]}")
        lines.append("")

    if failed_items:
        lines.append("❌ 未通过项：")
        for r in failed_items:
            lines.append(f"  • {r.rule_name}: {r.reason[:60]}")
        lines.append("")

    if unavail_items:
        lines.append("⚠️ 数据不可用：")
        for r in unavail_items:
            lines.append(f"  • {r.rule_name}: {r.reason[:60]}")
        lines.append("")

    lines.append("-" * 60)
    lines.append("💡 请到海通确认KD点后再做决策")
    lines.append("-" * 60)
    return "\n".join(lines)


# ==================== CLI 入口 ====================

def main():
    p = argparse.ArgumentParser(description="铁血哨兵 v2 - A股买点审核引擎")
    p.add_argument('stock_code', help='股票代码，如 300438 或 sz300438')
    p.add_argument('--json', action='store_true', help='JSON输出')
    p.add_argument('--format', action='store_true', help='格式化报告输出')
    args = p.parse_args()

    try:
        report = IronSentinelEngine(args.stock_code).audit()
        if args.json:
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        elif args.format:
            print(format_report(report))
        else:
            # 默认：简洁输出
            print(f"📊 {report.stock_name or report.stock_code}({report.stock_code}): "
                  f"{int(report.total_score)}分 {report.level}")
            print(f"💡 {report.suggestion}")
    except Exception as e:
        print(f"❌ 审核失败: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
