"""
铁血哨兵 v2 - LLM 智能分析层
=================================
接收结构化 JSON 数据，执行专家级分析判断。
当前使用内置模型，后续可替换为任意 LLM API。
"""

import json
import sys
import os
import hashlib
from typing import Dict, Any, Optional

# ═══════════════════════════════════════════════════════════════════════════════
#  数据完整性校验（防止 JSON 被篡改）
# ═══════════════════════════════════════════════════════════════════════════════

def _fingerprint(raw: Dict) -> str:
    content = json.dumps({
        'results':        raw.get('results', []),
        'leader_details': raw.get('leader_details', []),
    }, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]

def _verify(data: Dict) -> bool:
    """验证 engine.py 原始数据是否被篡改"""
    stored  = data.get('_data_hash', '')
    current = _fingerprint(data)
    return stored == current

# ═══════════════════════════════════════════════════════════════════════════════
#  评分规则定义（与 checks.py 保持一致）
# ═══════════════════════════════════════════════════════════════════════════════
WEIGHTS = {
    1: 10,   # MACD动能增强
    2: 10,   # 分时均线上方
    3: 10,   # 量能放大
    4: 10,   # 上升趋势
    5: 10,   # 主力净流入
    6: 10,   # 基本面良好
    7: 5,    # 大盘日内非下行
    8: 5,    # 大盘趋势非下行
    9: 5,    # 板块趋势向上
    10: 12,  # 龙头活跃
    11: 13,  # 筹码集中度 (多1分凑100)
}
WEIGHTS_TOTAL = sum(WEIGHTS.values())  # 100

NAMES = {
    1: "MACD动能增强", 2: "分时均线上方", 3: "量能放大", 4: "上升趋势",
    5: "主力净流入", 6: "基本面良好", 7: "大盘日内非下行", 8: "大盘趋势非下行",
    9: "板块趋势向上", 10: "龙头活跃", 11: "龙头活跃(双维)",
}


def get_level(score: float) -> str:
    if score >= 80: return "🟢 优秀买点"
    if score >= 70: return "🟡 较好买点"
    if score >= 50: return "⚠️ 一般买点"
    if score >= 30: return "🔴 较差买点"
    return "⚫ 极差买点"


def get_suggestion(score: float) -> str:
    if score >= 80: return "买点极佳，建议积极关注"
    if score >= 70: return "买点较好，可考虑轻仓介入"
    if score >= 50: return "买点一般，建议耐心等待更佳时机"
    if score >= 30: return "买点较差，暂不推荐入场"
    return "买点极差，风险较高，不建议入场"


# ─── 专家规则分析 ───────────────────────────────────────────────────────────

def analyze_by_rules(data: Dict) -> Dict:
    """
    纯规则引擎分析（不依赖 LLM API，纯本地执行）。
    与 checks.py 逻辑一致，保证评分一致。
    """
    if not _verify(data):
        raise ValueError("❌ 数据指纹校验失败：results / leader_details 被篡改，拒绝分析。")
    results = data.get("results", [])
    score = data.get("total_score", 0)

    # 构建分析结果
    analysis = {
        "method": "rules",
        "total_score": score,
        "level": get_level(score),
        "suggestion": get_suggestion(score),
        "passed_count": data.get("passed_count", 0),
        "failed_count": data.get("failed_count", 0),
        "unavailable_count": data.get("unavailable_count", 0),
        "conclusion": _make_conclusion(score),
        "risks": _extract_risks(results),
        "observations": _extract_observations(results),
        "key_thresholds": _analyze_thresholds(results),
        "expert_summary": _expert_summary(score, results),
    }
    return analysis


def analyze_by_llm(data: Dict, model: str = "glm-4", api_key: str = None) -> Dict:
    """
    通过 LLM API 进行专家级分析（支持 GLM/Qwen 等）。
    
    Args:
        data: engine.py 输出的完整 JSON
        model: 模型名称，如 "glm-4", "qwen-plus", "gpt-4"
        api_key: API 密钥，不传则使用环境变量
    """
    if not _verify(data):
        raise ValueError("❌ 数据指纹校验失败：results / leader_details 被篡改，拒绝分析。")
    # TODO: 实现 LLM API 调用
    # 目前回退到规则引擎
    print(f"[analyze] 模型 '{model}' 暂未实现，回退到规则引擎", file=sys.stderr)
    return analyze_by_rules(data)


def _make_conclusion(score: float) -> str:
    if score >= 70: return "✅ 推荐买入"
    if score >= 50: return "⚠️ 谨慎观望"
    return "❌ 不推荐买入"


def _extract_risks(results: list) -> list:
    risks = []
    for r in results:
        if r.get("passed"):
            continue
        rn = r.get("rule_num")
        reason = r.get("reason", "")
        if rn == 3:  risks.append(f"量能不足：{reason[:30]}")
        elif rn == 4: risks.append(f"均线空头排列：{reason[:30]}")
        elif rn == 5: risks.append(f"主力净流出：{reason[:30]}")
        elif rn == 6: risks.append(f"基本面偏差：{reason[:30]}")
        elif rn == 10: risks.append(f"龙头疲软：{reason[:30]}")
        elif rn == 11: risks.append(f"筹码分散：{reason[:30]}")
    return risks[:5]


def _extract_observations(results: list) -> list:
    obs = []
    for r in results:
        if not r.get("passed"):
            continue
        rn = r.get("rule_num")
        reason = r.get("reason", "")
        if rn == 1: obs.append(f"MACD 动能改善：{reason[:30]}")
        elif rn == 2: obs.append(f"分时多头控盘：{reason[:30]}")
        elif rn == 7: obs.append(f"大盘走强：{reason[:30]}")
        elif rn == 8: obs.append(f"大盘趋势向上：{reason[:30]}")
        elif rn == 9: obs.append(f"板块强势：{reason[:30]}")
    return obs[:5]


def _analyze_thresholds(results: list) -> Dict:
    """提取关键阈值数据"""
    thresholds = {}
    for r in results:
        rn = r.get("rule_num")
        if rn == 1:
            # MACD 绿柱缩短数据
            import re
            m = re.search(r'[-−]?[\d.]+→([-−]?[\d.]+)', r.get("reason", ""))
            if m:
                thresholds["macd_dif"] = float(m.group(1))
        elif rn == 3:
            # 量能对比
            import re
            m = re.search(r'量(\d+)万手.*?需(\d+)万手', r.get("reason", ""))
            if m:
                thresholds["vol_current"] = float(m.group(1))
                thresholds["vol_required"] = float(m.group(2))
        elif rn == 10:
            # 龙头活跃双维度
            extra = r.get("extra_data", {})
            thresholds["leader_pos_5d"] = extra.get("dim1_pos_5d", 0)
            thresholds["leader_pos_today"] = extra.get("dim2_pos_today", 0)
    return thresholds


def _expert_summary(score: float, results: list) -> str:
    """生成专家级一句话总结"""
    if score >= 70:
        return f"买点条件较好({int(score)}分)，可考虑轻仓介入"
    elif score >= 50:
        return f"买点条件一般({int(score)}分)，建议耐心等待更佳时机"
    elif score >= 30:
        return f"买点条件不足({int(score)}分)，暂不推荐入场"
    else:
        return f"买点极差({int(score)}分)，风险较高，不建议入场"


# ─── 主入口 ───────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="铁血哨兵 LLM 分析层")
    parser.add_argument("--json", required=True, help="engine.py 输出的 JSON 文件路径或 - 表示 stdin")
    parser.add_argument("--model", default="rules", help="分析模型: rules（默认）/ glm-4 / qwen-plus / gpt-4")
    parser.add_argument("--out", help="输出文件路径，默认打印到 stdout")
    args = parser.parse_args()

    # 读取 JSON
    if args.json == "-":
        raw = sys.stdin.read()
    else:
        with open(args.json) as f:
            raw = f.read()

    data = json.loads(raw)

    # 执行分析
    if args.model == "rules":
        result = analyze_by_rules(data)
    else:
        result = analyze_by_llm(data, model=args.model)

    # 输出
    output = json.dumps(result, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w") as f:
            f.write(output)
        print(f"[analyze] 分析完成 → {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()
