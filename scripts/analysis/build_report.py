"""
铁血哨兵 v2 - 固定展示层
=================================
所有模型输出统一使用此模块的 build_report() 生成报告。
表格格式、颜色、布局全部固化，任何 LLM 输出的分析结果
经过此层格式化后保持一致的展示效果。

数据输入：analyze() 输出的分析结果（规则引擎或LLM）
"""

import json
import hashlib
from typing import Dict, Any, Optional, List


# ═══════════════════════════════════════════════════════════════════════════
#  数据完整性校验（防止展示层被篡改）
# ═══════════════════════════════════════════════════════════════════════════
_VERIFY_LOG = []  # 记录校验结果，供后续输出

def _verify_fingerprint(data: Dict) -> bool:
    """
    验证 engine.py 原始数据是否被篡改。
    严格锁定：results、leader_details 两个字段，任何层不得修改。
    """
    stored  = data.get('_data_hash', '')
    content = json.dumps({
        'results':        data.get('results', []),
        'leader_details': data.get('leader_details', []),
    }, ensure_ascii=False, sort_keys=True, default=str)
    current = hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    ok = (stored == current)
    _VERIFY_LOG.clear()
    _VERIFY_LOG.append(('原始数据指纹', stored))
    _VERIFY_LOG.append(('当前数据指纹', current))
    _VERIFY_LOG.append(('校验结果', '✅ 未篡改' if ok else '❌ 被篡改'))
    return ok

# ═══════════════════════════════════════════════════════════════════════════
#  11项审核项中文名称（固定顺序）
# ═══════════════════════════════════════════════════════════════════════════
ITEMS = [
    (1,  "MACD动能增强",    10),
    (2,  "分时均线上方",    10),
    (3,  "量能放大",        10),
    (4,  "上升趋势",        10),
    (5,  "主力净流入",      10),
    (6,  "基本面良好",      10),
    (7,  "大盘日内非下行",   5),
    (8,  "大盘趋势非下行",   5),
    (9,  "板块趋势向上",     8),   # v3: 5→8
    (10, "龙头活跃",        15),  # v3: 12→15
    (11, "筹码集中度",       7),  # v3: 13→7
]
MAX_SCORE = sum(s for _, _, s in ITEMS)  # 100


# ═══════════════════════════════════════════════════════════════════════════
#  表格样式（固定）
# ═══════════════════════════════════════════════════════════════════════════
DIVIDER = "  " + "─" * 56


def _bar(passed: bool, available: bool = True) -> str:
    """通过率小条形图"""
    if not available:
        return "▫▫▫▫▫"
    if passed:
        return "█" * 5
    return "░" * 5


def _rule_icon(passed: bool, available: bool = True) -> str:
    if not available:
        return "⚪"
    return "✅" if passed else "❌"


def _icon2(cond: bool) -> str:
    return "✅" if cond else "❌"


# ═══════════════════════════════════════════════════════════════════════════
#  核心构建函数
# ═══════════════════════════════════════════════════════════════════════════

def build_report(
    raw_data: Dict,
    analysis: Optional[Dict] = None,
    include_raw: bool = False,
) -> str:
    """
    统一报告构建入口。
    
    Args:
        raw_data:    engine.py 输出的原始 JSON（包含 results, leader_details 等）
        analysis:    analyze.py 输出的分析结果（可选，不传则用规则引擎）
        include_raw: 是否在末尾附加原始 JSON（用于调试）
    
    Returns:
        格式化报告字符串（固定表格格式）
    """
    # ── 数据完整性校验（第一道关）────────────────────────────────
    # ⚠️ engine.py 原始数据(results/leader_details)受 SHA256 指纹保护
    # ⚠️ 任何层修改这两个字段将导致校验失败，拒绝生成报告
    if not _verify_fingerprint(raw_data):
        raise ValueError(
            "❌ 数据指纹校验失败：results / leader_details 已被篡改。"
            "请确保原始数据未被修改，直接使用 engine.py --json 的输出。"
        )

    # 兼容：无 analysis 时自动用规则引擎生成
    if analysis is None:
        from analyze import analyze_by_rules
        analysis = analyze_by_rules(raw_data)

    return _build(
        raw_data=raw_data,
        analysis=analysis,
        include_raw=include_raw,
    )


def _build(
    raw_data: Dict,
    analysis: Dict,
    include_raw: bool,
) -> str:
    results        = raw_data.get("results", [])
    leader_details = raw_data.get("leader_details", [])
    data_sources   = raw_data.get("data_sources", {})

    score      = analysis.get("total_score", raw_data.get("total_score", 0))
    level      = analysis.get("level",       raw_data.get("level", ""))
    suggestion = analysis.get("suggestion",  raw_data.get("suggestion", ""))
    conclusion = analysis.get("conclusion",  "结论待定")
    risks      = analysis.get("risks",       [])
    obs        = analysis.get("observations", [])
    thresholds = analysis.get("key_thresholds", {})
    summary    = analysis.get("expert_summary", "")

    # 把 results 按 rule_num 索引
    by_num = {r.get("rule_num"): r for r in results}

    # ── 数据完整性校验 ──────────────────────────────────────────
    # ⚠️ 表格数据(results/leader_details)来自engine.py，任何层不得修改
    # 此处校验指纹，若被篡改则拒绝生成报告
    _verify_fingerprint(raw_data)
    ok_verify = all(k for k, v in _VERIFY_LOG if '结果' in k)

    lines = []

    # ── 头部 ──
    code = raw_data.get("stock_code", "")
    name = raw_data.get("stock_name", "")
    ts   = raw_data.get("timestamp", "")
    fp   = raw_data.get("_data_hash", "N/A")
    lines.append("")
    lines.append(f"  🩸 铁血哨兵 v2 · A股买点审核报告")
    lines.append(f"  {code} {name}    {ts}    数据指纹: {fp}")
    lines.append(DIVIDER)

    # ── 总评分 ──
    pct = score / MAX_SCORE * 100
    bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
    lines.append(f"  综合评分   {int(score):>3}/{MAX_SCORE}  [{bar}] {pct:.0f}%")
    lines.append(f"  评级       {level}")
    lines.append(f"  结论       {conclusion}")
    lines.append(DIVIDER)

    # ── 11项审核明细（直接用 results 动态渲染，不依赖 ITEMS 硬编码标签） ──
    lines.append("  审核明细")
    lines.append(f"  {'#':<2}  {'审核项':<16} {'状态':^7}  详情摘要")
    lines.append("  " + "-" * 56)
    for r in results:
        num       = r.get("rule_num", 0)
        name_text = r.get("rule_name", "?")
        icon      = _rule_icon(r.get("passed"), r.get("available"))
        avbl      = r.get("available", True)

        if not avbl:
            lines.append(f"  {num:<2}  {name_text:<16} {icon:^7}  ⚠️ 数据不可用")
        else:
            # 原始 reason 完整保留，不做任何截断或清洗
            raw_detail = r.get("reason", "无详情")
            # 第一行：截取合理长度
            lines.append(f"  {num:<2}  {name_text:<16} {icon:^7}  {raw_detail[:44]}")
            # 超长内容换行（缩进对齐）
            if len(raw_detail) > 44:
                indent = "  " + " " * 20 + "  "  # 对齐详情列
                for i in range(44, len(raw_detail), 44):
                    lines.append(indent + raw_detail[i:i+44])

    lines.append(DIVIDER)

    # ── 龙头详情 ──
    if leader_details:
        lines.append("")
        lines.append("  🏢 板块龙头详情")
        lines.append("  " + "-" * 64)
        lines.append(f"  {'序号':<4} {'名称':<10} {'角色':<10} {'今日涨跌幅':>10}  {'近5日涨跌幅':>10}")
        lines.append("  " + "-" * 64)
        for i, d in enumerate(leader_details, 1):
            gt  = f"{d.get('gain_today'):+.2f}%" if d.get('gain_today') is not None else "  N/A  "
            g5  = f"{d.get('gain_5d'):+.2f}%"    if d.get('gain_5d')    is not None else "  N/A  "
            flag_t = "📈" if d.get('gain_today') and d.get('gain_today') > 0 else "📉"
            flag_5 = "📈" if d.get('gain_5d')    and d.get('gain_5d')    > 0 else "📉"
            role = d.get('role', '')
            role_str = f"[{role}]" if role else ''
            lines.append(f"  {i:<4} {d.get('name',''):<10} {role_str:<10} {flag_t} {gt:>10}  {flag_5} {g5:>10}")
        # 维度统计（从 raw_value 中取）
        r10 = by_num.get(10, {})
        extra = r10.get("raw_value", {}) if isinstance(r10.get("raw_value"), dict) else {}
        pos_5d    = extra.get("dim1_pos_5d", 0)
        pos_today = extra.get("dim2_pos_today", 0)
        leader_cnt = extra.get("leader_count", len(leader_details))
        lines.append(f"  近5日正增: {pos_5d}/{leader_cnt}  今日盘中正增: {pos_today}/{leader_cnt}")

    # ── 关键阈值 ──
    if thresholds:
        lines.append("")
        lines.append("  📊 关键阈值")
        lines.append("  " + "-" * 54)
        if "macd_dif" in thresholds:
            lines.append(f"  MACD(DIF): {thresholds['macd_dif']:.4f} "
                         f"{'(正值，红区)' if thresholds['macd_dif'] > 0 else '(负值，绿区)'}")
        if "vol_current" in thresholds and "vol_required" in thresholds:
            ratio = thresholds['vol_current'] / thresholds['vol_required'] * 100 if thresholds['vol_required'] else 0
            lines.append(f"  量能: {thresholds['vol_current']:.0f}万手 / {thresholds['vol_required']:.0f}万手 ({ratio:.0f}%)")

    # ── 风险与机会 ──
    if risks or obs:
        lines.append("")
        lines.append("  🔍 专家分析")
        lines.append("  " + "-" * 54)
        if risks:
            for risk in risks[:4]:
                lines.append(f"  ⚠️ {risk}")
        if obs:
            for ob in obs[:4]:
                lines.append(f"  ✅ {ob}")
        if summary:
            lines.append(f"  📌 {summary}")

    # ── 专家建议 ──
    lines.append("")
    lines.append(DIVIDER)
    lines.append(f"  💡 建议：{suggestion}")
    lines.append(DIVIDER)
    lines.append("")

    # ── 数据来源 ──
    if data_sources:
        lines.append("  📡 数据来源状态")
        lines.append("  " + "-" * 54)
        for src, status in data_sources.items():
            ok = "✅" if status and status != "❌" else "❌"
            lines.append(f"  {ok} {src}: {status}")
        lines.append("")

    # ── 原始 JSON（调试用）──────────────────────────────────────────
    if include_raw:
        import json
        lines.append(DIVIDER)
        lines.append("  📋 原始数据（JSON）")
        lines.append("  " + "-" * 54)
        lines.append(json.dumps(raw_data, ensure_ascii=False, indent=2))
        lines.append("")

    lines.append("  🔔 请到海通确认KD点后再做决策")
    lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
#  快速入口（直接传入 engine.py AuditReport 对象）
# ═══════════════════════════════════════════════════════════════════════════

def build_from_report(report, analysis: Optional[Dict] = None) -> str:
    """build_report 的便捷包装，接收 AuditReport 对象"""
    return build_report(report.to_dict(), analysis=analysis)


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    import json, argparse, sys

    parser = argparse.ArgumentParser(description="铁血哨兵报告生成器")
    parser.add_argument("--data",   required=True, help="engine.py 输出的 JSON 文件")
    parser.add_argument("--analysis", help="analyze.py 输出的 JSON 文件（可选）")
    parser.add_argument("--raw",    action="store_true", help="包含原始 JSON 调试信息")
    args = parser.parse_args()

    with open(args.data) as f:
        raw_data = json.load(f)

    analysis = None
    if args.analysis:
        with open(args.analysis) as f:
            analysis = json.load(f)

    print(build_report(raw_data, analysis=analysis, include_raw=args.raw))


if __name__ == "__main__":
    main()
