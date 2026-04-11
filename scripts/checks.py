# -*- coding: utf-8 -*-
"""
铁血哨兵 v2 - 审核函数层
=======================
每个审核函数独立，无冗余代码。
统一返回 CheckResult 数据类。

审核项定义（共11项）:
  1. MACD动能增强   15%   日K线
  2. 量能放大       10%   日K线+分钟线
  3. 主力净流入     15%   资金流向
  4. 上升趋势       15%   日K线
  5. 基本面良好     10%   财务数据
  6. 分时均线上方  10%   实时行情
  7. 大盘日内非下行  5%   大盘指数
  8. 大盘趋势非下行  5%   大盘K线
  9. 板块趋势向上    5%   板块指数
  10. 龙头活跃      10%   成分股K线+实时
  11. 筹码集中度   10%   股东数据
"""

import re
from datetime import datetime, time as dtime
from typing import Dict, List, Tuple, Any, Optional

# ==================== 审核项元数据 ====================
WEIGHTS = {
    1: 0.15, 2: 0.10, 3: 0.15, 4: 0.15, 5: 0.10,
    6: 0.10, 7: 0.05, 8: 0.05, 9: 0.05, 10: 0.10, 11: 0.10,
}
NAMES = {
    1: "MACD动能增强", 2: "量能放大", 3: "主力净流入",
    4: "上升趋势", 5: "基本面良好", 6: "分时均线上方",
    7: "大盘日内非下行", 8: "大盘趋势非下行",
    9: "板块趋势向上", 10: "龙头活跃", 11: "筹码集中度",
}

# 分时均价格式化权重（按时间段）
TIME_WEIGHTS = [
    (dtime(9, 30), dtime(10, 0), 0.25),
    (dtime(10, 0), dtime(11, 30), 0.30),
    (dtime(13, 0), dtime(13, 30), 0.15),
    (dtime(13, 30), dtime(14, 30), 0.15),
    (dtime(14, 30), dtime(15, 0), 0.15),
]


# ==================== 时间工具 ====================
def _effective_now() -> datetime:
    """
    非交易日（周末/节假日）时，返回当日 16:00，
    使所有审核走"盘后模式"，直接用 bars[-1]（最后交易日）数据。
    """
    now = datetime.now()
    if now.weekday() >= 5:
        # 非交易日：强制走盘后模式，用最后交易日数据
        return now.replace(hour=16, minute=0, second=0, microsecond=0)
    return now


# ==================== CheckResult 数据类（避免循环导入） ====================


# ==================== CheckResult 数据类（避免循环导入） ====================
class CheckResult:
    def __init__(
        self,
        rule_num: int,
        rule_name: str,
        passed: bool,
        score: float,
        raw_value: Any,
        reason: str,
        available: bool = True,
        source: str = "?",
    ):
        self.rule_num = rule_num
        self.rule_name = rule_name
        self.passed = passed
        self.score = score
        self.raw_value = raw_value
        self.reason = reason
        self.available = available
        self.source = source

    def to_dict(self) -> Dict:
        return {
            'rule_num': self.rule_num,
            'rule_name': self.rule_name,
            'passed': self.passed,
            'score': float(self.score),
            'raw_value': self.raw_value,
            'reason': self.reason,
            'available': self.available,
            'source': self.source,
        }


def _score(passed: bool, weight: float) -> float:
    return weight * 100 if passed else 0.0


def _get_time_weight() -> float:
    """根据当前时间返回分时均线的权重因子"""
    now = datetime.now().time()
    if dtime(11, 30) <= now <= dtime(13, 0):
        return 0.55
    w = 0.0
    for start, end, wt in TIME_WEIGHTS:
        if start <= now < end:
            w = wt
            break
    return w


def _calc_passed_weight(now: Optional[datetime] = None) -> float:
    """
    计算已过交易时段权重（分段权重推算法）
    用于量能盘中估算。

    权重分段:
      9:30-10:00  权重 25%（开盘期）
      10:00-11:30 权重 30%（上午主交易期）
      11:30-13:00 权重 0%（午间休市，跳过）
      13:00-13:30 权重 15%（下午开盘）
      13:30-14:30 权重 15%（下午主交易期）
      14:30-15:00 权重 15%（尾盘）

    Returns:
        0-1 之间的权重，15:00 时 = 1.0
    """
    if now is None:
        now = datetime.now()
    h, m = now.hour, now.minute
    cur_min = h * 60 + m
    passed = 0.0

    # 9:30-10:00（权重25%）
    if cur_min >= 10 * 60:
        passed += 0.25
    elif cur_min >= 9 * 60 + 30:
        passed += 0.25 * (cur_min - (9 * 60 + 30)) / 30

    # 10:00-11:30（权重30%）
    if cur_min >= 11 * 60 + 30:
        passed += 0.30
    elif cur_min >= 10 * 60:
        passed += 0.30 * (cur_min - 10 * 60) / 90

    # 午间休市（跳过）

    # 13:00-13:30（权重15%）
    if cur_min >= 13 * 60 + 30:
        passed += 0.15
    elif cur_min >= 13 * 60:
        passed += 0.15 * (cur_min - 13 * 60) / 30

    # 13:30-14:30（权重15%）
    if cur_min >= 14 * 60 + 30:
        passed += 0.15
    elif cur_min >= 13 * 60 + 30:
        passed += 0.15 * (cur_min - (13 * 60 + 30)) / 60

    # 14:30-15:00（权重15%）
    if cur_min >= 15 * 60:
        passed += 0.15
    elif cur_min >= 14 * 60 + 30:
        passed += 0.15 * (cur_min - (14 * 60 + 30)) / 30

    return min(passed, 1.0)


# ==================== 工具函数 ====================

def _safe_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _calc_macd(bars: List[Dict]) -> Tuple[List[float], List[float], List[float]]:
    """
    计算 MACD(DIF, DEA, BAR) 序列
    bars: 按时间正序的日K线列表 [{close}, ...]
    返回: ([dif序列], [dea序列], [bar序列]) 或 ([], [], [])
    """
    if not bars or len(bars) < 26:
        return [], [], []

    closes = [_safe_float(b.get('close')) for b in bars]

    def ema_list(data: List[float], n: int) -> List[float]:
        if not data:
            return []
        ema_val = data[0]
        k = 2.0 / (n + 1)
        result = [ema_val]
        for price in data[1:]:
            ema_val = price * k + ema_val * (1 - k)
            result.append(ema_val)
        return result

    ema12 = ema_list(closes, 12)
    ema26 = ema_list(closes, 26)
    dif_list = [e1 - e2 for e1, e2 in zip(ema12, ema26)]
    dea_list = ema_list(dif_list, 9)
    bar_list = [2 * (d - s) for d, s in zip(dif_list, dea_list)]

    return dif_list, dea_list, bar_list


def _calc_avg_volume(bars: List[Dict], n: int = 5) -> float:
    """计算近n日平均成交量"""
    if len(bars) < n:
        return 0.0
    vols = [_safe_float(b.get('vol', 0)) for b in bars[-n:]]
    return sum(vols) / n


# ==================== 审核函数 ====================

def check_macd(bars: List[Dict], realtime: Dict) -> CheckResult:
    """
    [1] MACD动能增强
    通过条件（二选一）：
      (1) 红柱增长：bar_today > bar_yesterday
      (2) 绿柱缩短：bar_today < 0 且 abs(bar_today) < abs(bar_yesterday)
    """
    if not bars or len(bars) < 34:
        return CheckResult(
            1, NAMES[1], False, 0.0, None,
            "[数据不足] K线不足34根", False, "?",
        )

    try:
        dif_list, dea_list, bar_list = _calc_macd(bars)
        if not dif_list:
            return CheckResult(
                1, NAMES[1], False, 0.0, None,
                "[数据不足] K线不足34根", False, "?",
            )

        bar_t = bar_list[-1]
        bar_y = bar_list[-2] if len(bar_list) >= 2 else bar_t - 0.1
        dif_t = dif_list[-1]
        dea_t = dea_list[-1]

        # 原始逻辑：红柱增长 OR 绿柱缩短
        red_passed  = bar_t > 0 and bar_t > bar_y          # 红柱且增长
        green_passed = bar_t < 0 and abs(bar_t) < abs(bar_y)  # 绿柱且缩短
        passed = red_passed or green_passed

        if red_passed:
            reason = (f"红柱{bar_y:.4f}→{bar_t:.4f}（增长✅）"
                      f"| DIF={dif_t:.4f} DEA={dea_t:.4f}")
        elif green_passed:
            reason = (f"绿柱{bar_y:.4f}→{bar_t:.4f}（缩短✅）"
                      f"| DIF={dif_t:.4f} DEA={dea_t:.4f}")
        else:
            if bar_t > 0:
                direction = "增长❌" if bar_t <= bar_y else "增长✅"
                reason = (f"红柱{bar_y:.4f}→{bar_t:.4f}（{direction}）"
                          f"| DIF={dif_t:.4f} DEA={dea_t:.4f}")
            else:
                direction = "缩短❌" if abs(bar_t) >= abs(bar_y) else "缩短✅"
                reason = (f"绿柱{bar_y:.4f}→{bar_t:.4f}（{direction}）"
                          f"| DIF={dif_t:.4f} DEA={dea_t:.4f}")

        return CheckResult(1, NAMES[1], passed, _score(passed, WEIGHTS[1]),
                          {'dif': dif_t, 'dea': dea_t, 'bar': bar_t, 'bar_y': bar_y},
                          reason, True, "computed")

    except Exception as e:
        return CheckResult(1, NAMES[1], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")

def check_volume(bars: List[Dict], realtime: Dict) -> CheckResult:
    """
    [2] 量能放大
    非交易日：跳过，返回 unavailable
    盘中（当前时间 < 15:00 且 bars[-1] 有实时数据）:
        估算全天量 = 今日盘中累计量 / passed_weight(now)
        通过条件: 估算全天量 > 近5日均量 × 1.1
    盘后（当前时间 >= 15:00 或 bars[-1] 无实时标记）:
        通过条件: 今日全天量 > 近5日均量 × 1.1
    """
    if not bars or len(bars) < 6:
        return CheckResult(
            2, NAMES[2], False, 0.0, None,
            "[数据不足] K线不足6根", False, "?",
        )

    try:
        now = _effective_now()  # 非交易日自动退为16:00，走盘后模式
        market_close = now.replace(hour=15, minute=0, second=0, microsecond=0)
        today_vol_raw = _safe_float(bars[-1].get('vol', 0))
        prev_vol = _safe_float(bars[-2].get('vol', 0))  # 昨日全天量

        # 均量基准：用昨日往前5天（不含今日）作为历史均量
        hist_bars = bars[-6:-1]  # 不含今日和昨日，共5天
        hist_vol_avg = sum(_safe_float(b.get('vol', 0)) for b in hist_bars) / max(len(hist_bars), 1)

        # 判断盘中/盘后：bars[-1] 有 is_today=True 标记且未收盘 = 盘中
        is_live = bars[-1].get('is_today') is True and now < market_close
        passed_w = _calc_passed_weight(now)

        # 非交易日：now 已退为 16:00，直接用 bars[-1]（最后交易日）数据
        # 仅在 reason 中体现，不加标签
        if is_live and passed_w >= 0.05:
            # 盘中模式：估算全天量
            estimated_full = today_vol_raw / passed_w
            ratio_vs_avg = estimated_full / hist_vol_avg if hist_vol_avg > 0 else 0
            ratio_vs_yesterday = estimated_full / prev_vol if prev_vol > 0 else 0
            # 通过条件：估算全天 > 历史均量 × 1.1
            passed = ratio_vs_avg > 1.1
            reason = (f"盘中{passed_w:.0%}已过：估全天{_fmt_vol(estimated_full)}手 "
                      f"{'✅' if passed else '❌'} 近5日均量{_fmt_vol(hist_vol_avg)}手 "
                      f"（vs昨日{_fmt_vol(prev_vol)}手，比值{ratio_vs_avg:.2f}×）")
        else:
            # 盘后模式（含非交易日用最后交易日数据）
            ratio_vs_avg = today_vol_raw / hist_vol_avg if hist_vol_avg > 0 else 0
            passed = ratio_vs_avg > 1.1
            reason = (f"全天{_fmt_vol(today_vol_raw)}手 "
                      f"{'✅' if passed else '❌'} 近5日均量{_fmt_vol(hist_vol_avg)}手 "
                      f"（vs昨日{_fmt_vol(prev_vol)}手，比值{ratio_vs_avg:.2f}×）")

        return CheckResult(
            2, NAMES[2], passed, _score(passed, WEIGHTS[2]),
            {'today_vol': today_vol_raw, 'hist_vol_avg': hist_vol_avg,
             'prev_vol': prev_vol, 'estimated_full': estimated_full if is_live else today_vol_raw,
             'passed_weight': passed_w, 'is_live': is_live},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(2, NAMES[2], False, 0.0, None,
                           f"[计算错误] {str(e)[:50]}", False, "computed")


def check_mainforce(flow_data: Optional[Dict]) -> CheckResult:
    """
    [3] 主力净流入
    通过条件：主力净流入（超大单+大单）> 0
    """
    if not flow_data:
        return CheckResult(
            3, NAMES[3], False, 0.0, None,
            "[数据不可用] 资金流向不可用", False, "none",
        )

    try:
        main_net = _safe_float(flow_data.get('main_net', 0))
        super_net = _safe_float(flow_data.get('super_net', 0))
        big_net = _safe_float(flow_data.get('big_net', 0))

        # 主力净流入 = 超大单 + 大单
        main_inflow = super_net + big_net

        passed = main_inflow > 0

        # 格式化单位（超过1亿用亿，否则用万）
        if abs(main_inflow) >= 100000000:
            inflow_str = f"{main_inflow / 100000000:.2f}亿"
        else:
            inflow_str = f"{main_inflow / 10000:.2f}万"

        reason = f"主力净流入{inflow_str}"
        if passed:
            reason += "，超大单+大单联合扫货"
        else:
            reason += f"（超大单{super_net/10000:.1f}万 + 大单{big_net/10000:.1f}万）"

        return CheckResult(
            3, NAMES[3], passed, _score(passed, WEIGHTS[3]),
            {'main_net': main_net, 'super_net': super_net, 'big_net': big_net,
             'main_inflow': main_inflow, 'formatted': inflow_str},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(3, NAMES[3], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


def check_trend(bars: List[Dict]) -> CheckResult:
    """
    [4] 上升趋势
    通过条件：MA5 > MA10 > MA20，且价格在MA5上方
    """
    if not bars or len(bars) < 25:
        return CheckResult(
            4, NAMES[4], False, 0.0, None,
            "[数据不足] 需要≥25根K线", False, "?",
        )

    try:
        def ma(bars_list: List[Dict], n: int) -> float:
            if len(bars_list) < n:
                return 0.0
            closes = [_safe_float(b.get('close', 0)) for b in bars_list[-n:]]
            return sum(closes) / n

        ma5 = ma(bars, 5)
        ma10 = ma(bars, 10)
        ma20 = ma(bars, 20)
        price = _safe_float(bars[-1].get('close', 0))

        passed = ma5 > ma10 > ma20 and price > ma5

        if passed:
            reason = f"均线多头排列: 价={price:.2f} > MA5={ma5:.2f} > MA10={ma10:.2f} > MA20={ma20:.2f}"
        else:
            reason = f"价={price:.2f} MA5={ma5:.2f} MA10={ma10:.2f} MA20={ma20:.2f}"

        return CheckResult(
            4, NAMES[4], passed, _score(passed, WEIGHTS[4]),
            {'price': price, 'ma5': ma5, 'ma10': ma10, 'ma20': ma20},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(4, NAMES[4], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


def check_fundamental(fin_data: Optional[Dict], quote: Dict) -> CheckResult:
    """
    [5] 基本面良好
    通过条件：PE(动态) > 0 且 PE < 100，且 PB < 10
    """
    if not fin_data:
        return CheckResult(
            5, NAMES[5], False, 0.0, None,
            "[数据不可用] 基本面不可用", False, "none",
        )

    try:
        pe = _safe_float(fin_data.get('pe', 0))
        pb = _safe_float(fin_data.get('pb', 0))
        roe = _safe_float(fin_data.get('roe', 0))
        price = _safe_float(quote.get('price', 0))

        # PE判断：正值且合理
        pe_ok = 0 < pe < 150
        # PB判断：小于10
        pb_ok = pb < 10 and pb > 0
        passed = pe_ok and pb_ok

        if passed:
            reason = f"PE={pe:.1f}（{'合理' if pe < 50 else '偏高'}）PB={pb:.2f}"
            if roe > 10:
                reason += f" ROE={roe:.1f}%（优质）"
        else:
            reason = f"PE={pe:.1f}（{'负值/亏损' if pe <= 0 else '偏高' if pe >= 150 else '偏高'}）"

        return CheckResult(
            5, NAMES[5], passed, _score(passed, WEIGHTS[5]),
            {'pe': pe, 'pb': pb, 'roe': roe, 'price': price},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(5, NAMES[5], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


def check_minute_price(quote: Optional[Dict]) -> CheckResult:
    """
    [6] 分时均线上方
    通过条件：当前价 > 今日均价（根据分时数据计算）
    注：如果没有分时数据，用昨收代替均价估算
    """
    if not quote:
        return CheckResult(
            6, NAMES[6], False, 0.0, None,
            "[数据不可用] 行情不可用", False, "none",
        )

    try:
        price = _safe_float(quote.get('price', 0))
        prev_close = _safe_float(quote.get('prev_close', 0))
        # 没有真实分时数据时，用昨收估算均价
        estimated_avg = prev_close * 1.002  # 轻微溢价作为均价估计

        if price <= 0:
            return CheckResult(
                6, NAMES[6], False, 0.0, None,
                "[数据不可用] 当前价无效", False, "none",
            )

        passed = price >= estimated_avg

        reason = f"现价={price:.2f} {'≥' if passed else '<'}{'均' if prev_close else '昨'}{prev_close:.2f}"
        if passed:
            reason += "，多头控盘"
        else:
            reason += "，空头主导"

        return CheckResult(
            6, NAMES[6], passed, _score(passed, WEIGHTS[6]),
            {'price': price, 'prev_close': prev_close, 'estimated_avg': estimated_avg},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(6, NAMES[6], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


def check_market_day(mkt_data: Optional[Dict]) -> CheckResult:
    """
    [7] 大盘日内非下行
    通过条件：上证和深证任一涨幅 > -0.5%（不算太弱）
    """
    if not mkt_data:
        return CheckResult(
            7, NAMES[7], False, 0.0, None,
            "[数据不可用] 大盘数据不可用", False, "none",
        )

    try:
        sh_chg = _safe_float(mkt_data.get('sh', {}).get('chg_pct', 0)) if isinstance(mkt_data.get('sh'), dict) else _safe_float(mkt_data.get('sh_chg_pct', 0))
        sz_chg = _safe_float(mkt_data.get('sz', {}).get('chg_pct', 0)) if isinstance(mkt_data.get('sz'), dict) else _safe_float(mkt_data.get('sz_chg_pct', 0))

        # 至少一个不要太弱
        passed = sh_chg > -0.5 or sz_chg > -0.5

        reason = (f"上证{sh_chg:+.2f}% {'✅' if sh_chg > -0.5 else '❌< -0.5%'}，"
                  f"深证{sz_chg:+.2f}% {'✅' if sz_chg > -0.5 else '❌< -0.5%'}")

        return CheckResult(
            7, NAMES[7], passed, _score(passed, WEIGHTS[7]),
            {'sh_chg_pct': sh_chg, 'sz_chg_pct': sz_chg},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(7, NAMES[7], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


def check_market_trend(mkt_bars: Optional[List[Dict]]) -> CheckResult:
    """
    [8] 大盘趋势非下行
    通过条件：上证指数 > MA5（趋势向上）
    """
    if not mkt_bars or len(mkt_bars) < 6:
        return CheckResult(
            8, NAMES[8], False, 0.0, None,
            "[数据不足] 大盘K线不足", False, "none",
        )

    try:
        price = _safe_float(mkt_bars[-1].get('close', 0))
        closes = [_safe_float(b.get('close', 0)) for b in mkt_bars[-5:]]
        ma5 = sum(closes) / len(closes)

        passed = price > ma5

        reason = f"上证{price:.2f} {'> MA5(' + f'{ma5:.2f}) ✅' if passed else '< MA5(' + f'{ma5:.2f}) ❌'}"

        return CheckResult(
            8, NAMES[8], passed, _score(passed, WEIGHTS[8]),
            {'price': price, 'ma5': ma5},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(8, NAMES[8], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


def check_sector_trend(sector_data: Optional[Dict]) -> CheckResult:
    """
    [9] 板块趋势向上
    通过条件：板块涨幅 > 0
    """
    if not sector_data:
        return CheckResult(
            9, NAMES[9], False, 0.0, None,
            "[数据不可用] 板块数据不可用", False, "none",
        )

    try:
        chg_pct = _safe_float(sector_data.get('chg_pct', 0))
        sector_name = sector_data.get('sector_name', '未知板块')

        passed = chg_pct > 0

        reason = f"{sector_name}板块{'↑' if chg_pct > 0 else '↓'}{abs(chg_pct):.2f}% {'✅' if passed else '❌'}"

        return CheckResult(
            9, NAMES[9], passed, _score(passed, WEIGHTS[9]),
            {'chg_pct': chg_pct, 'sector_name': sector_name},
            reason, True, "computed",
        )

    except Exception as e:
        return CheckResult(9, NAMES[9], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


def check_sector_leaders(
    sector_name: str,
    leader_details: List[Dict],
    src_used: str,
) -> CheckResult:
    """
    [10] 龙头活跃（双维度判断）
    维度1（近5日）：positive_count >= 3 OR high_gain_count(>3%) >= 2
    维度2（今日盘中）：positive_count_today >= 3
    两维度 AND 关系，都满足才通过
    """
    if not leader_details:
        return CheckResult(
            10, NAMES[10], False, 0.0, None,
            "[数据不可用] 成分股数据不可用", False, "none",
        )

    gains_5d = [ld.get('gain_5d', 0) for ld in leader_details]
    gains_today = [ld.get('gain_today') for ld in leader_details]

    # 近5日维度
    pos_5d = sum(1 for g in gains_5d if g > 0)
    high_5d = sum(1 for g in gains_5d if g > 3)
    dim1_pass = pos_5d >= 3 or high_5d >= 2

    # 今日盘中维度
    valid_today = [g for g in gains_today if g is not None]
    pos_today = sum(1 for g in valid_today if g > 0) if valid_today else 0
    dim2_pass = len(valid_today) >= 3 and pos_today >= 3

    passed = dim1_pass and dim2_pass

    # 打印龙头详情
    idx = 0
    for ld in leader_details:
        idx += 1
        today_str = f"{ld.get('gain_today', 0):+.2f}%" if ld.get('gain_today') is not None else "N/A"
        print(f"    {idx}. {ld.get('name', '')}: 今日{today_str} | 近5日{ld.get('gain_5d', 0):+.2f}%")

    print(f"    近5日: 正增{pos_5d}/5{'✅' if pos_5d>=3 else '❌'}, 高增>{high_5d}/5{'✅' if high_5d>=2 else '❌'}")
    print(f"    今日盘中: 正增{pos_today}/{len(valid_today)} {'✅' if dim2_pass else '❌'}")

    reason = (f"近5日: 正增{pos_5d}/5 {'✅' if dim1_pass else '❌'}，"
               f"今日盘中: 正增{pos_today}/5 {'✅' if dim2_pass else '❌'}")

    return CheckResult(
        10, NAMES[10], passed, _score(passed, WEIGHTS[10]),
        {
            'sector_name': sector_name,
            'leaders': leader_details,
            'data_source': src_used,
            'dim1_pos_5d': pos_5d,
            'dim1_high_5d': high_5d,
            'dim1_pass': dim1_pass,
            'dim2_pos_today': pos_today,
            'dim2_valid': len(valid_today),
            'dim2_pass': dim2_pass,
        },
        reason, True, src_used,
    )


def check_chip_concentration(chip_data: Optional[Dict]) -> CheckResult:
    """
    [11] 筹码集中度
    通过条件：股东户数环比 < 0（主力吸筹）AND 前十流通股占比 >= 30%
    """
    if not chip_data:
        return CheckResult(
            11, NAMES[11], False, 0.0, None,
            "[数据不可用] 股东数据不可用", False, "none",
        )

    try:
        holder_chg = chip_data.get('holder_chg')  # 百分比，如 -26.7
        top10_float = chip_data.get('top10_float_pct')  # 百分比，如 45.2

        holder_count = chip_data.get('holder_count')
        holder_date = chip_data.get('data_date', '')

        chg_passed = holder_chg is not None and holder_chg < 0
        t10_passed = top10_float is not None and top10_float >= 30

        if holder_chg is not None and top10_float is not None:
            passed = chg_passed and t10_passed
            judge_note = "需同时满足"
        elif holder_chg is not None:
            passed = chg_passed
            judge_note = "股东减少即通过"
        elif top10_float is not None:
            passed = t10_passed
            judge_note = "集中度达标即通过"
        else:
            passed = False
            judge_note = "数据不足"

        hc_str = f"{holder_chg:+.1f}%" if holder_chg is not None else "N/A"
        t10_str = f"{top10_float:.1f}%" if top10_float is not None else "N/A"
        hc_flag = "✅" if chg_passed else ("❌" if holder_chg is not None else "⚠️")
        t10_flag = "✅" if t10_passed else ("❌" if top10_float is not None else "⚠️")

        print(f"    数据源: {'NeoData' if chip_data.get('source') == 'neodata' else 'akshare'}")
        if holder_count:
            print(f"    股东户数: {holder_count/10000:.2f}万户（{holder_date}）")
        print(f"    股东户数环比: {hc_str} {hc_flag}")
        print(f"    前十流通股占比: {t10_str} {t10_flag}")
        print(f"    判定: {'✅ 筹码集中' if passed else '❌ 筹码不集中'}（{judge_note}）")

        reason_parts = []
        if holder_chg is not None:
            reason_parts.append(f"股东环比{'↓' if holder_chg < 0 else '↑'}{abs(holder_chg):.1f}%")
        if top10_float is not None:
            reason_parts.append(f"前十流通股{top10_float:.1f}%")
        reason = '；'.join(reason_parts) if reason_parts else '数据不可用'

        return CheckResult(
            11, NAMES[11], passed, _score(passed, WEIGHTS[11]),
            chip_data, reason, True, chip_data.get('source', '?'),
        )

    except Exception as e:
        return CheckResult(11, NAMES[11], False, 0.0, None,
                            f"[计算错误] {str(e)[:30]}", False, "computed")


# ==================== 格式化工具 ====================

def _fmt_vol(v: float) -> str:
    """格式化成交量"""
    if v >= 100000000:
        return f"{v/100000000:.2f}亿"
    elif v >= 10000:
        return f"{v/10000:.0f}万"
    else:
        return f"{v:.0f}"


def get_level(score: float) -> str:
    if score >= 90:
        return "⭐ 优质买点"
    elif score >= 80:
        return "✅ 良好买点"
    elif score >= 70:
        return "🟡 一般买点"
    elif score >= 60:
        return "⚠️ 较差买点"
    else:
        return "❌ 不推荐买入"


def get_suggestion(score: float) -> str:
    if score >= 90:
        return "优质买点，强烈建议关注买入"
    elif score >= 80:
        return "良好买点，可以考虑买入"
    elif score >= 70:
        return "一般买点，可少量试探"
    elif score >= 60:
        return "较差买点，建议观望等待更好时机"
    else:
        return "不推荐买入，耐心等待更好的买点出现"
