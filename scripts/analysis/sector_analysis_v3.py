# -*- coding: utf-8 -*-
"""
铁血哨兵 v3 - 板块分析模块（重构版）
====================================
按设计文档 V3 实现：
  1. 多维度板块归属（申万L1/L2/L3 + EM概念 + EM行业）
  2. 双轨并行数据获取（NeoData指数 + 成分股实时计算）
  3. 动态龙头 + 个股定位
  4. 新版审核函数（check_sector_trend_v3 / check_sector_leaders_v3）

Author: Agent
Date: 2026-05-02
"""

import os
import sys
import re
import json
import sqlite3
import time
from typing import Dict, List, Tuple, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# 确保当前目录在 path 中
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from data_source import (
    query_neodata, nd_extract_text, nd_extract_number,
    get_realtime_tencent, get_daily_bars_sina, _normalize_code, _to_float,
)
from checks import CheckResult, WEIGHTS, NAMES, _safe_float

# ==================== 工具函数 ====================

def _standardize_code(num_code: str) -> str:
    """将数字代码转为 sz/sh 标准格式"""
    num = str(num_code).strip()
    if num.startswith(('6', '5', '9', '8')):
        return f"sh{num}"
    return f"sz{num}"


# ==================== 配置 ====================

# 数据路径
DB_PATH = os.path.expanduser("~/.qclaw/skills/iron-sentinel/data/stock_data.db")

# 板块归属数据源优先级（可配置）
BOARD_SOURCE_PRIORITY = ["em", "sina"]  # 优先EM，回退SINA

# 成分股计算限制
MAX_CONSTITUENTS = 15  # 限制成分股数量，避免过慢
CONCURRENT_WORKERS = 8  # 并发请求数

# NeoData 查询缓存
_ND_BOARD_CACHE: Dict[str, Tuple[dict, float]] = {}
_ND_BOARD_CACHE_TTL = 180


# ==================== DB 工具 ====================

def _get_db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ==================== P1: 获取个股板块归属 ====================

def _get_stock_boards(stock_code: str, max_concepts: int = 8) -> List[Dict]:
    """
    Step 1: 获取个股多维度板块归属。

    返回: [{name, source_type, level, weight, board_code, stock_count}, ...]
           按权重降序排列
    """
    boards = []
    conn = _get_db_conn()
    cur = conn.cursor()

    # --- 1. 申万行业链 (L1/L2/L3) ---
    cur.execute("""
        SELECT si.level, si.industry_code,
               il1.name as l1_name, il2.name as l2_name, il3.name as l3_name
        FROM stock_industry si
        LEFT JOIN industry_l1 il1 ON si.industry_code = il1.code AND si.level = 'L1'
        LEFT JOIN industry_l2 il2 ON si.industry_code = il2.code AND si.level = 'L2'
        LEFT JOIN industry_l3 il3 ON si.industry_code = il3.code AND si.level = 'L3'
        WHERE si.stock_code = ?
    """, (stock_code,))

    for row in cur.fetchall():
        level = row['level']
        name = row['l1_name'] or row['l2_name'] or row['l3_name'] or ''
        if not name:
            continue
        weight_map = {'L1': 0.90, 'L2': 0.85, 'L3': 0.80}
        boards.append({
            'name': name,
            'source_type': '申万',
            'level': level,
            'weight': weight_map.get(level, 0.80),
            'board_code': row['industry_code'],
            'stock_count': None,
        })

    # --- 2. EM概念板块 (按成分股数量排序取Top) ---
    # 策略：优先EM，没有则回退SINA
    concept_sources = BOARD_SOURCE_PRIORITY.copy()
    concept_data = []
    for src in concept_sources:
        cur.execute("""
            SELECT sc.board_code, cb.board_name, cb.stock_count
            FROM stock_concept sc
            LEFT JOIN concept_boards cb ON sc.board_code = cb.board_code
            WHERE sc.stock_code = ? AND sc.source = ?
            ORDER BY cb.stock_count ASC
        """, (stock_code, src))
        rows = cur.fetchall()
        if rows:
            concept_data = rows
            break

    for row in concept_data[:max_concepts]:
        name = row['board_name'] or ''
        count = row['stock_count'] or 9999
        if not name:
            continue
        # 成分股越少权重越高
        if count <= 20:
            w = 0.88
        elif count <= 50:
            w = 0.78
        elif count <= 100:
            w = 0.70
        elif count <= 200:
            w = 0.65
        else:
            w = 0.50
        boards.append({
            'name': name,
            'source_type': 'EM概念',
            'level': 'concept',
            'weight': w,
            'board_code': row['board_code'],
            'stock_count': count,
        })

    # --- 3. EM行业板块 ---
    ib_sources = BOARD_SOURCE_PRIORITY.copy()
    for src in ib_sources:
        cur.execute("""
            SELECT sib.board_code, ib.board_name
            FROM stock_industry_board sib
            LEFT JOIN industry_boards ib ON sib.board_code = ib.board_code
            WHERE sib.stock_code = ? AND sib.source = ?
        """, (stock_code, src))
        rows = cur.fetchall()
        if rows:
            for row in rows:
                name = row['board_name'] or ''
                if not name:
                    continue
                boards.append({
                    'name': name,
                    'source_type': 'EM行业',
                    'level': 'industry',
                    'weight': 0.65,
                    'board_code': row['board_code'],
                    'stock_count': None,
                })
            break

    conn.close()

    # 按权重降序排列
    boards.sort(key=lambda x: x['weight'], reverse=True)
    return boards


# ==================== P2: 轨道A - NeoData板块指数 ====================

def _fetch_from_neodata(board_name: str) -> Optional[Dict]:
    """
    轨道A: 通过NeoData查询板块指数数据。

    返回: {
        'chg_pct': float,      # 今日涨跌幅%
        'chg_5d': float,       # 5日涨幅%
        'chg_20d': float,      # 20日涨幅%
        'turnover': float,     # 换手率%
        'volume_ratio': float, # 量比
        'amount': float,       # 成交额(万元)
        'source': 'neodata',
    } 或 None
    """
    cache_key = f"neodata_board:{board_name}"
    now = time.time()
    if cache_key in _ND_BOARD_CACHE:
        cached_data, cached_ts = _ND_BOARD_CACHE[cache_key]
        if now - cached_ts < _ND_BOARD_CACHE_TTL:
            return cached_data.copy()

    query = f"{board_name}板块今日行情：涨跌幅、5日涨幅、20日涨幅、成交额、换手率、量比"
    try:
        result, src, err = query_neodata(query)
        if err or not result:
            return None

        # 尝试多个type_hint
        txt = ""
        for hint in ["板块行情", "板块走势", "板块数据"]:
            t = nd_extract_text(result, hint)
            if t and ('涨跌幅' in t or '涨幅' in t):
                txt = t
                break

        if not txt:
            return None

        def _extract(pat: str, text: str) -> Optional[float]:
            m = re.search(pat, text)
            if m:
                try:
                    return float(m.group(1).replace(',', ''))
                except (ValueError, IndexError):
                    pass
            return None

        data = {
            'chg_pct': _extract(r'涨跌幅[:：]?\s*([-\d.]+)', txt),
            'chg_5d': _extract(r'5日涨幅[:：]?\s*([-\d.]+)', txt),
            'chg_20d': _extract(r'20日涨幅[:：]?\s*([-\d.]+)', txt),
            'turnover': _extract(r'换手率[:：]?\s*([-\d.]+)', txt),
            'volume_ratio': _extract(r'量比[:：]?\s*([-\d.]+)', txt),
            'amount': _extract(r'成交额[:：]?\s*([-\d,.]+)', txt),
            'source': 'neodata',
        }

        # 过滤掉None值过多的结果
        valid_count = sum(1 for v in data.values() if v is not None and v != 'neodata')
        if valid_count < 2:
            return None

        _ND_BOARD_CACHE[cache_key] = (data, now)
        return data.copy()

    except Exception:
        return None


# ==================== P3: 轨道B - 成分股实时计算 ====================

def _get_board_constituents(board_name: str, source_priority: List[str] = None,
                            sw_level: str = None, sw_code: str = None) -> List[Dict]:
    """
    从DB获取板块成分股列表。

    返回: [{stock_code, stock_name}, ...]
    """
    if source_priority is None:
        source_priority = BOARD_SOURCE_PRIORITY

    conn = _get_db_conn()
    cur = conn.cursor()

    # 策略1: 申万行业 — 通过 stock_industry 表直接查（最可靠）
    if sw_level and sw_code:
        cur.execute("""
            SELECT si.stock_code, s.stock_name
            FROM stock_industry si
            LEFT JOIN stocks s ON si.stock_code = s.stock_code
            WHERE si.level = ? AND si.industry_code = ?
        """, (sw_level, sw_code))
        constituents = []
        for r in cur.fetchall():
            constituents.append({
                'stock_code': r['stock_code'],
                'stock_name': r['stock_name'] or '',
            })
        if constituents:
            conn.close()
            return constituents

    # 策略2: EM/SINA概念/行业 — 通过 board_name 匹配 concept_boards / industry_boards
    for src in source_priority:
        # 概念板块
        cur.execute("""
            SELECT cb.board_code FROM concept_boards cb
            WHERE cb.board_name = ? AND cb.source = ?
            LIMIT 1
        """, (board_name, src))
        row = cur.fetchone()
        if row:
            board_code = row['board_code']
            cur.execute("""
                SELECT sc.stock_code, s.stock_name
                FROM stock_concept sc
                LEFT JOIN stocks s ON sc.stock_code = s.stock_code
                WHERE sc.board_code = ? AND sc.source = ?
            """, (board_code, src))
            constituents = []
            for r in cur.fetchall():
                constituents.append({
                    'stock_code': r['stock_code'],
                    'stock_name': r['stock_name'] or '',
                })
            if constituents:
                conn.close()
                return constituents

        # 行业板块
        cur.execute("""
            SELECT ib.board_code FROM industry_boards ib
            WHERE ib.board_name = ? AND ib.source = ?
            LIMIT 1
        """, (board_name, src))
        row = cur.fetchone()
        if row:
            board_code = row['board_code']
            cur.execute("""
                SELECT sib.stock_code, s.stock_name
                FROM stock_industry_board sib
                LEFT JOIN stocks s ON sib.stock_code = s.stock_code
                WHERE sib.board_code = ? AND sib.source = ?
            """, (board_code, src))
            constituents = []
            for r in cur.fetchall():
                constituents.append({
                    'stock_code': r['stock_code'],
                    'stock_name': r['stock_name'] or '',
                })
            if constituents:
                conn.close()
                return constituents

    conn.close()
    return []


def _fetch_constituent_data(stock_code: str) -> Optional[Dict]:
    """
    获取单只成分股的实时数据（今日涨幅、市值、5日涨幅、20日涨幅）。

    返回: {
        'stock_code': str,
        'chg_pct': float,     # 今日涨跌幅%
        'gain_5d': float,     # 近5日涨幅%
        'gain_20d': float,    # 近20日涨幅%
        'mkt_cap': float,     # 市值(亿元)
        'amount': float,      # 成交额(万元)
    } 或 None
    """
    std_code = _standardize_code(stock_code)

    # 今日实时行情（腾讯）
    rt, src, err = get_realtime_tencent(std_code)
    if not rt:
        return None

    chg_pct = _safe_float(rt.get('chg_pct', 0))
    mkt_cap = _safe_float(rt.get('market_cap', 0))
    amount = _safe_float(rt.get('amount', 0))

    # 日K线（新浪，取25根同时算5日/20日涨幅 + 量比）
    gain_5d = 0.0
    gain_20d = 0.0
    volume_ratio = 0.0
    bars, bsrc, berr = get_daily_bars_sina(std_code, count=25)
    if bars and len(bars) >= 2:
        c_last = _safe_float(bars[-1].get('close'))
        # 5日涨幅
        idx_5 = max(0, len(bars) - 5)
        c_5 = _safe_float(bars[idx_5].get('close'))
        if c_5 > 0:
            gain_5d = (c_last - c_5) / c_5 * 100
        # 20日涨幅
        if len(bars) >= 20:
            c_20 = _safe_float(bars[0].get('close'))
            if c_20 > 0:
                gain_20d = (c_last - c_20) / c_20 * 100
        # 量比 = 今日成交量 / 近5日平均成交量
        if len(bars) >= 6:
            today_vol = _safe_float(bars[-1].get('vol', 0))
            hist_vols = [_safe_float(b.get('vol', 0)) for b in bars[-6:-1]]
            hist_avg = sum(hist_vols) / len(hist_vols) if hist_vols else 0
            if hist_avg > 0:
                volume_ratio = today_vol / hist_avg

    return {
        'stock_code': stock_code,
        'chg_pct': chg_pct,
        'gain_5d': gain_5d,
        'gain_20d': gain_20d,
        'volume_ratio': volume_ratio,
        'mkt_cap': mkt_cap,
        'amount': amount,
    }


def _fetch_from_constituents(board_name: str, max_stocks: int = MAX_CONSTITUENTS,
                              sw_level: str = None, sw_code: str = None) -> Optional[Dict]:
    """
    轨道B: 通过成分股实时计算板块数据。

    返回: {
        'chg_pct': float,        # 成分股平均涨跌幅%
        'up_ratio': float,       # 上涨占比%
        'limit_up_count': int,   # 涨停家数
        'avg_gain_5d': float,    # 平均5日涨幅%
        'constituents': List,    # 成分股详细数据
        'source': 'constituents',
    } 或 None
    """
    constituents = _get_board_constituents(board_name, sw_level=sw_level, sw_code=sw_code)
    if not constituents:
        return None

    # 限制数量
    constituents = constituents[:max_stocks]

    # 批量获取实时数据（并发，避免串行累积超时）
    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        future_map = {
            executor.submit(_fetch_constituent_data, c['stock_code']): c
            for c in constituents
        }
        for future in as_completed(future_map):
            c = future_map[future]
            try:
                data = future.result(timeout=12)
                if data:
                    data['stock_name'] = c['stock_name']
                    results.append(data)
            except Exception:
                pass

    if not results:
        return None

    chg_pcts = [r['chg_pct'] for r in results]
    gain_5ds = [r['gain_5d'] for r in results]
    gain_20ds = [r['gain_20d'] for r in results]
    volume_ratios = [r.get('volume_ratio', 0) for r in results if r.get('volume_ratio', 0) > 0]

    up_count = sum(1 for c in chg_pcts if c > 0)
    limit_up_count = sum(1 for c in chg_pcts if c >= 9.9)

    return {
        'chg_pct': sum(chg_pcts) / len(chg_pcts) if chg_pcts else 0,
        'up_ratio': (up_count / len(results) * 100) if results else 0,
        'limit_up_count': limit_up_count,
        'avg_gain_5d': sum(gain_5ds) / len(gain_5ds) if gain_5ds else 0,
        'avg_gain_20d': sum(gain_20ds) / len(gain_20ds) if gain_20ds else 0,
        'avg_volume_ratio': sum(volume_ratios) / len(volume_ratios) if volume_ratios else 0,
        'constituents': results,
        'source': 'constituents',
    }


# ==================== P4: 双轨合并 + 核心板块筛选 ====================

def _fetch_board_data_dual(board: Dict) -> Dict:
    """
    Step 2: 双轨获取板块数据并合并。

    轨道A优先（指数级数据更权威），轨道B补充（情绪数据）。

    返回: board + 合并后的数据字段
    """
    board_name = board['name']

    # 申万行业额外参数
    sw_level = board.get('level') if board.get('source_type') == '申万' else None
    sw_code = board.get('board_code') if board.get('source_type') == '申万' else None

    # 轨道A: NeoData
    track_a = _fetch_from_neodata(board_name)

    # 轨道B: 成分股计算
    track_b = _fetch_from_constituents(board_name, sw_level=sw_level, sw_code=sw_code)

    # 合并策略：取并集，轨道A优先
    merged = {
        'name': board_name,
        'source_type': board['source_type'],
        'level': board['level'],
        'weight': board['weight'],
        'board_code': board['board_code'],
        'stock_count': board.get('stock_count'),
        # 涨跌幅: A优先
        'chg_pct': track_a.get('chg_pct') if track_a else (track_b.get('chg_pct') if track_b else 0),
        # 5日/20日: A优先；B成分股有avg_gain_5d/avg_gain_20d可回填
        'chg_5d': track_a.get('chg_5d') if track_a else (track_b.get('avg_gain_5d') if track_b else None),
        'chg_20d': track_a.get('chg_20d') if track_a else (track_b.get('avg_gain_20d') if track_b else None),
        # 成交额/换手率: 仅A有
        'turnover': track_a.get('turnover') if track_a else None,
        'amount': track_a.get('amount') if track_a else None,
        # 量比: A优先；B成分股有avg_volume_ratio可回填
        'volume_ratio': track_a.get('volume_ratio') if track_a else (track_b.get('avg_volume_ratio') if track_b else None),
        # 涨停/上涨占比: 仅B有
        'up_ratio': track_b.get('up_ratio') if track_b else None,
        'limit_up_count': track_b.get('limit_up_count') if track_b else None,
        # 成分股列表: 仅B有
        'constituents': track_b.get('constituents') if track_b else None,
        # 数据源标记
        'track_a_ok': track_a is not None,
        'track_b_ok': track_b is not None,
    }

    return merged


def _select_core_boards(boards_data: List[Dict], top_n: int = 5) -> List[Dict]:
    """
    Step 3: 筛选核心板块。

    综合得分 = 权重基础分 + 趋势加分 + 排名加分 + 涨停加分
    确保多样性: 至少1个申万 + 1个EM概念
    """
    scored = []
    for b in boards_data:
        # 基础分
        base_score = b.get('weight', 0.5) * 100

        # 趋势加分
        chg = b.get('chg_pct', 0) or 0
        trend_bonus = 0
        if chg > 5:
            trend_bonus = 25
        elif chg > 3:
            trend_bonus = 20
        elif chg > 1:
            trend_bonus = 10
        elif chg > 0:
            trend_bonus = 5

        # 涨停加分
        limit_up = b.get('limit_up_count', 0) or 0
        limit_bonus = min(limit_up * 3, 15)

        total = base_score + trend_bonus + limit_bonus
        scored.append({**b, '_score': total})

    # 按得分降序
    scored.sort(key=lambda x: x['_score'], reverse=True)

    # 确保多样性
    selected = []
    has_sw = False
    has_em_concept = False

    for b in scored:
        if len(selected) >= top_n:
            break
        st = b.get('source_type', '')
        if st == '申万':
            has_sw = True
        elif st == 'EM概念':
            has_em_concept = True
        selected.append(b)

    # 如果缺少申万，从剩余中补一个
    if not has_sw:
        for b in scored:
            if b not in selected and b.get('source_type') == '申万':
                selected.append(b)
                break

    # 如果缺少EM概念，从剩余中补一个
    if not has_em_concept:
        for b in scored:
            if b not in selected and b.get('source_type') == 'EM概念':
                selected.append(b)
                break

    # 最终取top_n
    selected = selected[:top_n]
    return selected


# ==================== P5: 动态龙头 + 个股定位 ====================

def _fetch_board_leaders(board_data: Dict, top_n: int = 8) -> List[Dict]:
    """
    Step 4: 获取板块动态龙头。

    按 近5日涨幅(60%) + 今日涨幅(40%) 排序。
    标注角色：情绪龙头 / 中军 / 补涨候选。
    """
    constituents = board_data.get('constituents', [])
    if not constituents:
        return []

    # 计算综合得分并排序
    for c in constituents:
        g5 = c.get('gain_5d', 0) or 0
        gt = c.get('chg_pct', 0) or 0
        c['_score'] = g5 * 0.6 + gt * 0.4
        c['_mkt_cap'] = c.get('mkt_cap', 0) or 0

    sorted_cons = sorted(constituents, key=lambda x: x['_score'], reverse=True)
    top = sorted_cons[:top_n]

    # 标注角色
    if top:
        # 情绪龙头：涨幅最高
        top[0]['role'] = '情绪龙头'
        # 中军：市值大+涨幅稳（在前半段里找市值最大的）
        mid_idx = len(top) // 2
        mid_candidates = top[:max(mid_idx, 2)]
        if mid_candidates:
            zhongjun = max(mid_candidates, key=lambda x: x.get('_mkt_cap', 0))
            if zhongjun.get('role') != '情绪龙头':
                zhongjun['role'] = '中军'
        # 补涨候选：涨幅滞后但放量（涨幅低但成交额高）
        for c in top:
            if c.get('role'):
                continue
            if c.get('chg_pct', 0) < 2 and c.get('amount', 0) > 5000:
                c['role'] = '补涨候选'
            else:
                c['role'] = '活跃股'

    return top


def _calc_stock_position(stock_code: str, board_data: Dict, leaders: List[Dict]) -> Dict:
    """
    Step 5: 计算个股在板块内的定位。

    返回: {'role': str, 'rank': int, 'vs_avg': float}
    角色: 龙头 / 中军 / 跟风 / 滞涨 / 后排
    """
    stock_code_norm = stock_code.lower().replace('sz', '').replace('sh', '')
    constituents = board_data.get('constituents') or []

    if not constituents:
        return {'role': '后排', 'rank': -1, 'vs_avg': 0}

    # 按今日涨幅排序，计算排名
    sorted_cons = sorted(constituents, key=lambda x: x.get('chg_pct', 0) or 0, reverse=True)

    # 找个股在排序后成分股中的位置
    stock_data = None
    rank = -1
    for i, c in enumerate(sorted_cons):
        sc = c.get('stock_code', '').lower().replace('sz', '').replace('sh', '')
        if sc == stock_code_norm:
            stock_data = c
            rank = i + 1
            break

    # 如果没找到（被截断导致），单独获取目标个股数据并补充
    if not stock_data:
        extra = _fetch_constituent_data(stock_code_norm)
        if extra:
            # 从板块基础信息补个股票名（引擎层 quote 可能已有）
            extra['stock_name'] = extra.get('stock_name') or board_data.get('stock_name') or ''
            constituents.append(extra)
            sorted_cons = sorted(constituents, key=lambda x: x.get('chg_pct', 0) or 0, reverse=True)
            for i, c in enumerate(sorted_cons):
                sc = c.get('stock_code', '').lower().replace('sz', '').replace('sh', '')
                if sc == stock_code_norm:
                    stock_data = c
                    rank = i + 1
                    break

    if not stock_data:
        return {'role': '后排', 'rank': -1, 'vs_avg': 0}

    # 计算板块平均涨幅
    avg_chg = sum(c.get('chg_pct', 0) for c in constituents) / len(constituents) if constituents else 0
    vs_avg = (stock_data.get('chg_pct', 0) or 0) - avg_chg

    # 判定角色（基于涨幅排序后的排名 + 相对板块平均表现）
    total = len(sorted_cons)
    top_pct = rank / total if total > 0 else 1
    chg_today = stock_data.get('chg_pct', 0) or 0

    if rank <= 3 and vs_avg > 2:
        role = '龙头'
    elif top_pct <= 0.4 and vs_avg >= 0:
        role = '中军'
    elif top_pct <= 0.7 and vs_avg >= -2:
        role = '跟风'
    elif chg_today > 0 and top_pct <= 0.8:
        role = '跟风'  # 正涨幅至少给跟风
    elif vs_avg < -2 or top_pct > 0.8 or (chg_today < 0 and top_pct > 0.5):
        role = '滞涨'
    else:
        role = '跟风'

    return {'role': role, 'rank': rank, 'vs_avg': vs_avg}


# ==================== P6: 审核函数V3 ====================

def check_sector_trend_v3(core_boards: List[Dict]) -> CheckResult:
    """
    [9] 板块趋势审核 V3

    评分维度（0-100分）:
      1. 今日涨幅 (0-25分)
      2. 5日趋势 (0-20分) — NeoData独有
      3. 20日趋势 (0-10分) — NeoData独有
      4. 情绪指标 (0-15分) — 成分股计算
      5. 量能指标 (0-10分) — NeoData独有
      6. 相对强度 (0-10分)
      7. 板块多样性 (0-10分)

    通过阈值: 综合评分 >= 55分
    """
    if not core_boards:
        return CheckResult(
            9, "板块趋势向上", False, 0.0, None,
            "[数据不可用] 无板块数据", False, "none"
        )

    # 取最佳板块的数据进行评分
    best = core_boards[0] if core_boards else {}

    # 1. 今日涨幅 (0-25)
    chg = best.get('chg_pct', 0) or 0
    if chg > 5:
        score_chg = 25
    elif chg > 3:
        score_chg = 20
    elif chg > 1:
        score_chg = 15
    elif chg > 0:
        score_chg = 10
    else:
        score_chg = max(0, 5 + int(chg))

    # 2. 5日趋势 (0-20)
    chg5 = best.get('chg_5d')
    if chg5 is not None:
        if chg5 > 10:
            score_5d = 20
        elif chg5 > 5:
            score_5d = 15
        elif chg5 > 2:
            score_5d = 10
        elif chg5 > 0:
            score_5d = 5
        else:
            score_5d = max(0, 5 + int(chg5))
    else:
        score_5d = 0

    # 3. 20日趋势 (0-10)
    chg20 = best.get('chg_20d')
    if chg20 is not None:
        if chg20 > 15:
            score_20d = 10
        elif chg20 > 5:
            score_20d = 7
        elif chg20 > 0:
            score_20d = 4
        else:
            score_20d = max(0, 3 + int(chg20 / 5))
    else:
        score_20d = 0

    # 4. 情绪指标 (0-15)
    up_ratio = best.get('up_ratio')
    limit_up = best.get('limit_up_count', 0) or 0
    if up_ratio is not None:
        score_emotion = min(up_ratio / 100 * 10 + limit_up * 1.5, 15)
    else:
        # 只有涨跌幅时，用涨跌幅估算
        score_emotion = max(0, min(chg * 2, 15))

    # 5. 量能指标 (0-10)
    vr = best.get('volume_ratio')
    if vr is not None:
        if vr > 2:
            score_vol = 10
        elif vr > 1.5:
            score_vol = 8
        elif vr > 1:
            score_vol = 6
        elif vr > 0.8:
            score_vol = 4
        else:
            score_vol = 2
    else:
        score_vol = 0

    # 6. 相对强度 (0-10)
    # 个股在板块中的相对位置
    # 简化：用最佳板块的涨幅 vs 所有板块平均
    avg_chg_all = sum(b.get('chg_pct', 0) or 0 for b in core_boards) / len(core_boards)
    rel_strength = chg - avg_chg_all if len(core_boards) > 1 else 0
    score_rel = max(0, min(5 + rel_strength * 2, 10))

    # 7. 板块多样性 (0-10)
    positive_boards = sum(1 for b in core_boards if (b.get('chg_pct', 0) or 0) > 0)
    if len(core_boards) >= 3 and positive_boards >= 2:
        score_diversity = 10
    elif positive_boards >= 1:
        score_diversity = 5
    else:
        score_diversity = 0

    total = score_chg + score_5d + score_20d + score_emotion + score_vol + score_rel + score_diversity
    passed = total >= 55

    # 构建reason（展示全部7个维度得分）
    board_names = [b['name'] for b in core_boards[:3]]
    reason_parts = [
        f"核心板块: {', '.join(board_names)}",
        f"今日涨幅{chg:+.2f}%({score_chg}分)",
    ]
    if chg5 is not None:
        reason_parts.append(f"5日{chg5:+.2f}%({score_5d}分)")
    if chg20 is not None:
        reason_parts.append(f"20日{chg20:+.2f}%({score_20d}分)")
    # 情绪指标
    emotion_desc = f"上涨比{up_ratio:.0f}%" if up_ratio is not None else f"涨幅估算"
    reason_parts.append(f"情绪{emotion_desc}({round(score_emotion, 1)}分)")
    # 量能
    vol_desc = f"量比{vr:.2f}" if vr is not None else "量比N/A"
    reason_parts.append(f"量能{vol_desc}({score_vol}分)")
    # 相对强度
    reason_parts.append(f"相对强度({round(score_rel, 1)}分)")
    # 多样性
    reason_parts.append(f"多样性({score_diversity}分)")
    reason_parts.append(f"总分{round(total, 1)}分{'✅' if passed else '❌'}")

    return CheckResult(
        9, "板块趋势向上", passed, total * 0.08,  # 新权重8%
        {
            'core_boards': board_names,
            'chg_pct': chg,
            'chg_5d': chg5,
            'chg_20d': chg20,
            'up_ratio': up_ratio,
            'limit_up': limit_up,
            'volume_ratio': vr,
            'score_detail': {
                'chg': score_chg, '5d': score_5d, '20d': score_20d,
                'emotion': score_emotion, 'vol': score_vol,
                'rel': score_rel, 'diversity': score_diversity,
            },
            'total_score': total,
        },
        ' | '.join(reason_parts),
        True, "dual_track",
    )


def check_sector_leaders_v3(core_boards: List[Dict], stock_code: str) -> CheckResult:
    """
    [10] 龙头活跃审核 V3

    评分结构:
      - 板块活跃度 (60%): Top8成分股平均5日涨幅 + 今日涨幅 + 涨停数
      - 个股地位 (40%): 龙头(40分) / 中军(30分) / 跟风(20分) / 滞涨(10分)

    通过条件:
      - 最佳板块活跃度 >= 30分
      - 且个股不是滞涨/后排
      - 若个股是龙头，放宽活跃度要求
    """
    if not core_boards:
        return CheckResult(
            10, "龙头活跃", False, 0.0, None,
            "[数据不可用] 无板块数据", False, "none"
        )

    # 找个股所在的最佳板块（包含成分股数据的）
    best_board = None
    stock_position = None
    best_leaders = []

    for board in core_boards:
        leaders = _fetch_board_leaders(board)
        if not leaders:
            continue
        pos = _calc_stock_position(stock_code, board, leaders)
        if pos['role'] != '后排':
            best_board = board
            best_leaders = leaders
            stock_position = pos
            break

    # 如果没找到有成分的板块，用第一个有成分的
    if not best_board:
        for board in core_boards:
            leaders = _fetch_board_leaders(board)
            if leaders:
                best_board = board
                best_leaders = leaders
                stock_position = _calc_stock_position(stock_code, board, leaders)
                break

    if not best_board or not best_leaders:
        return CheckResult(
            10, "龙头活跃", False, 0.0, None,
            "[数据不可用] 无成分股数据", False, "none"
        )

    # 计算板块活跃度
    avg_5d = sum(l.get('gain_5d', 0) for l in best_leaders) / len(best_leaders)
    avg_today = sum(l.get('chg_pct', 0) for l in best_leaders) / len(best_leaders)
    limit_up = sum(1 for l in best_leaders if (l.get('chg_pct', 0) or 0) >= 9.9)

    # 活跃度评分 (0-60)
    activity = 0
    if avg_5d > 5:
        activity += 25
    elif avg_5d > 2:
        activity += 18
    elif avg_5d > 0:
        activity += 10
    else:
        activity += max(0, 5 + int(avg_5d))

    if avg_today > 3:
        activity += 20
    elif avg_today > 1:
        activity += 14
    elif avg_today > 0:
        activity += 8
    else:
        activity += max(0, 5 + int(avg_today))

    activity += min(limit_up * 5, 15)
    activity = min(activity, 60)

    # 个股地位评分 (0-40)
    role_scores = {'龙头': 40, '中军': 30, '跟风': 20, '滞涨': 10, '后排': 0}
    role = stock_position['role'] if stock_position else '后排'
    position_score = role_scores.get(role, 0)

    total = activity + position_score

    # 通过条件
    if role == '龙头':
        passed = activity >= 20  # 龙头放宽
    elif role in ('中军', '跟风'):
        passed = activity >= 30
    else:
        passed = False

    # 龙头详情输出
    leader_lines = []
    for i, l in enumerate(best_leaders, 1):
        role_tag = f"[{l.get('role', '')}]" if l.get('role') else ''
        leader_lines.append(
            f"{i}. {l.get('stock_name', '')}{role_tag} "
            f"今日{l.get('chg_pct', 0):+.2f}% 5日{l.get('gain_5d', 0):+.2f}%"
        )

    reason = (f"板块活跃度{activity}/60(5日{avg_5d:+.1f}% 今日{avg_today:+.1f}% 涨停{limit_up}家) | "
              f"个股地位[{role}]{position_score}/40 | 总分{total}")

    return CheckResult(
        10, "龙头活跃", passed, total * 0.15,  # 新权重15%
        {
            'board_name': best_board.get('name', ''),
            'activity': activity,
            'position_score': position_score,
            'role': role,
            'rank': stock_position.get('rank', -1) if stock_position else -1,
            'vs_avg': stock_position.get('vs_avg', 0) if stock_position else 0,
            'leaders': leader_lines,
            'limit_up_count': limit_up,
        },
        reason,
        True, "dual_track",
    )


# ==================== 完整分析入口 ====================

def analyze_stock_sectors(stock_code: str) -> Dict:
    """
    完整板块分析入口（Steps 1-5）。

    返回: {
        'boards': List[Dict],         # Step1: 板块归属
        'core_boards': List[Dict],    # Step3: 核心板块
        'leaders': Dict[str, List],   # Step4: 各板块龙头
        'position': Dict,             # Step5: 个股定位
    }
    """
    stock_code = _normalize_code(stock_code)
    num_code = stock_code.lower().replace('sz', '').replace('sh', '')

    # Step 1: 板块归属
    boards = _get_stock_boards(num_code)

    # Step 2: 双轨数据获取
    boards_with_data = []
    for b in boards:
        data = _fetch_board_data_dual(b)
        boards_with_data.append(data)

    # Step 3: 筛选核心板块
    core_boards = _select_core_boards(boards_with_data)

    # Step 4 & 5: 龙头 + 定位（对每个核心板块）
    leaders_map = {}
    position_map = {}
    for board in core_boards:
        name = board['name']
        leaders = _fetch_board_leaders(board)
        leaders_map[name] = leaders
        pos = _calc_stock_position(num_code, board, leaders)
        position_map[name] = pos

    return {
        'boards': boards,
        'boards_with_data': boards_with_data,
        'core_boards': core_boards,
        'leaders': leaders_map,
        'position': position_map,
        'stock_code': stock_code,
    }


# ==================== CLI 测试 ====================

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='板块分析V3测试')
    p.add_argument('stock_code', help='股票代码')
    p.add_argument('--step', type=int, default=0, help='仅测试指定step (1-5)')
    args = p.parse_args()

    code = args.stock_code

    if args.step == 1 or args.step == 0:
        print("=" * 60)
        print("Step 1: 板块归属")
        boards = _get_stock_boards(code)
        for i, b in enumerate(boards, 1):
            print(f"  {i}. {b['name']} ({b['source_type']}-{b['level']}) 权重:{b['weight']}")
        print()

    if args.step == 0:
        print("=" * 60)
        print("完整分析...")
        result = analyze_stock_sectors(code)
        print(f"核心板块: {len(result['core_boards'])}")
        for b in result['core_boards']:
            print(f"  - {b['name']}: 涨{b.get('chg_pct', 0):+.2f}% 5日{b.get('chg_5d')}")
        print()
        for name, pos in result['position'].items():
            print(f"  在[{name}]中: {pos['role']} 排名{pos['rank']}")
