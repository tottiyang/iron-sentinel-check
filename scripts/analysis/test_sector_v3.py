# -*- coding: utf-8 -*-
"""
铁血哨兵 v3 - 板块分析模块单元测试
==================================
运行: python test_sector_v3.py
"""

import sys
sys.path.insert(0, '/Users/totti/.qclaw/skills/iron-sentinel/scripts/analysis')

from sector_analysis_v3 import check_sector_trend_v3, check_sector_leaders_v3
from checks import WEIGHTS


def test_weights():
    total = sum(v * 100 for v in WEIGHTS.values())
    assert abs(total - 100) < 0.01, f'权重总和={total}, 期望100'
    print('✅ 权重校验')


def test_strong_trend():
    boards = [{'name': '锂电池', 'source_type': '申万', 'level': 'L3', 'weight': 0.80,
               'chg_pct': 6.5, 'chg_5d': 12.0, 'chg_20d': 25.0,
               'turnover': 3.5, 'volume_ratio': 2.2, 'up_ratio': 85, 'limit_up_count': 5}]
    r = check_sector_trend_v3(boards)
    assert r.passed, f'强趋势应通过, score={r.score}'
    assert r.score > 0
    print('✅ 强趋势板块')


def test_weak_trend():
    boards = [{'name': '锂电池', 'source_type': '申万', 'level': 'L3', 'weight': 0.80,
               'chg_pct': -2.5, 'chg_5d': -5.0, 'chg_20d': -10.0,
               'turnover': 1.0, 'volume_ratio': 0.6, 'up_ratio': 20, 'limit_up_count': 0}]
    r = check_sector_trend_v3(boards)
    assert not r.passed, f'弱势不应通过, score={r.score}'
    print('✅ 弱势板块')


def test_leader_role():
    boards = [{'name': '锂电池', 'source_type': '申万', 'level': 'L3', 'weight': 0.80, 'chg_pct': 3.5,
               'constituents': [
                   {'stock_code': '300438', 'chg_pct': 8.5, 'gain_5d': 25, 'mkt_cap': 300},
                   {'stock_code': '300750', 'chg_pct': 5.2, 'gain_5d': 18, 'mkt_cap': 8000},
                   {'stock_code': '300014', 'chg_pct': 4.1, 'gain_5d': 15, 'mkt_cap': 1500},
                   {'stock_code': '002460', 'chg_pct': 2.8, 'gain_5d': 12, 'mkt_cap': 600}]}]
    r = check_sector_leaders_v3(boards, '300438')
    role = r.raw_value.get('role', '')
    assert role == '龙头' and r.passed, f'应为龙头,实际={role}'
    print('✅ 龙头角色')


def test_stagnant_role():
    boards = [{'name': '锂电池', 'source_type': '申万', 'level': 'L3', 'weight': 0.80, 'chg_pct': 3.5,
               'constituents': [
                   {'stock_code': '300438', 'chg_pct': -1.5, 'gain_5d': -5, 'mkt_cap': 300},
                   {'stock_code': '300750', 'chg_pct': 5.2, 'gain_5d': 18, 'mkt_cap': 8000},
                   {'stock_code': '300014', 'chg_pct': 4.1, 'gain_5d': 15, 'mkt_cap': 1500},
                   {'stock_code': '002460', 'chg_pct': 2.8, 'gain_5d': 12, 'mkt_cap': 600}]}]
    r = check_sector_leaders_v3(boards, '300438')
    role = r.raw_value.get('role', '')
    assert role == '滞涨' and not r.passed, f'应为滞涨,实际={role}'
    print('✅ 滞涨角色')


def test_zhongjun_role():
    boards = [{'name': '锂电池', 'source_type': '申万', 'level': 'L3', 'weight': 0.80, 'chg_pct': 3.5,
               'constituents': [
                   {'stock_code': '300750', 'chg_pct': 8.5, 'gain_5d': 25, 'mkt_cap': 8000},
                   {'stock_code': '300438', 'chg_pct': 4.2, 'gain_5d': 15, 'mkt_cap': 300},
                   {'stock_code': '300014', 'chg_pct': 3.1, 'gain_5d': 12, 'mkt_cap': 1500},
                   {'stock_code': '002460', 'chg_pct': 1.8, 'gain_5d': 8, 'mkt_cap': 600},
                   {'stock_code': '002050', 'chg_pct': 0.5, 'gain_5d': 3, 'mkt_cap': 900}]}]
    r = check_sector_leaders_v3(boards, '300438')
    role = r.raw_value.get('role', '')
    assert role == '中军', f'应为中军,实际={role}'
    print('✅ 中军角色')


def test_follower_role():
    boards = [{'name': '锂电池', 'source_type': '申万', 'level': 'L3', 'weight': 0.80, 'chg_pct': 3.5,
               'constituents': [
                   {'stock_code': '300750', 'chg_pct': 8.5, 'gain_5d': 25, 'mkt_cap': 8000},
                   {'stock_code': '300014', 'chg_pct': 5.2, 'gain_5d': 18, 'mkt_cap': 1500},
                   {'stock_code': '002460', 'chg_pct': 4.1, 'gain_5d': 15, 'mkt_cap': 600},
                   {'stock_code': '300438', 'chg_pct': 2.0, 'gain_5d': 8, 'mkt_cap': 300},
                   {'stock_code': '002050', 'chg_pct': 0.5, 'gain_5d': 3, 'mkt_cap': 900}]}]
    r = check_sector_leaders_v3(boards, '300438')
    role = r.raw_value.get('role', '')
    assert role == '跟风', f'应为跟风,实际={role}'
    print('✅ 跟风角色')


def test_backseat_role():
    boards = [{'name': '锂电池', 'source_type': '申万', 'level': 'L3', 'weight': 0.80, 'chg_pct': 3.5,
               'constituents': [
                   {'stock_code': '300750', 'chg_pct': 8.5, 'gain_5d': 25, 'mkt_cap': 8000}]}]
    r = check_sector_leaders_v3(boards, '300438')
    role = r.raw_value.get('role', '')
    assert role == '后排', f'应为后排,实际={role}'
    print('✅ 后排角色')


if __name__ == '__main__':
    tests = [
        test_weights,
        test_strong_trend,
        test_weak_trend,
        test_leader_role,
        test_stagnant_role,
        test_zhongjun_role,
        test_follower_role,
        test_backseat_role,
    ]
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f'❌ {t.__name__}: {e}')
        except Exception as e:
            print(f'💥 {t.__name__}: {e}')

    print()
    print(f'结果: {passed}/{len(tests)} 通过')
    if passed == len(tests):
        print('🎉 全部通过！')
        sys.exit(0)
    else:
        sys.exit(1)
