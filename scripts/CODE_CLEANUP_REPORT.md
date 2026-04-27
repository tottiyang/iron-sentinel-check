# 代码清理报告
**日期**: 2026-04-27

---

## 1. 重复文件

### 1.1 sync_counts.py（两个版本）
| 路径 | 状态 | 说明 |
|------|------|------|
| `scripts/sync/sync_counts.py` | ⚠️ 旧版 | 简单版本，仅支持 sina |
| `scripts/fetchers/sync/sync_counts.py` | ✅ 新版 | 完整版本，支持 sina/ths/em |

**建议**: 删除旧版，统一使用新版

### 1.2 fetch_ths_cdp.py（两个版本）
| 路径 | 状态 | 说明 |
|------|------|------|
| `scripts/fetchers/ths/fetch_ths_cdp.py` | ✅ 活跃版 | 简化版，当前使用 |
| `scripts/fetchers/archive/fetch_ths_cdp.py` | ⚠️ 旧版 | 旧版本，可删除 |

**建议**: archive 中的版本可删除（已被 full 版替代）

---

## 2. 废弃/未使用代码

### 2.1 根目录脚本（疑似废弃）
| 文件 | 状态 | 说明 |
|------|------|------|
| `_runner_fetch_stocks.py` | ❌ 未使用 | 未被任何代码引用，功能已被 `fetch_stocks.py` 替代 |
| `collect_sina.py` | ❌ 废弃 | 硬编码旧 DB 路径，功能已被 `fetch_sina_concepts.py` 替代 |
| `run_sina_list.py` | ❌ 废弃 | 硬编码旧 DB 路径，功能已被 `fetch_sina_list.py` 替代 |

### 2.2 硬编码 DB 路径（需修复）
```python
# collect_sina.py
DB = '/Users/totti/.qclaw/skills/iron-sentinel/stock_data.db'  # ❌ 旧路径

# archive/fetch_boards.py
DB = '/Users/totti/.qclaw/skills/iron-sentinel/stock_data.db'  # ❌ 旧路径
```

**当前正确路径**: `~/.qclaw/skills/iron-sentinel/data/stock_data.db`

---

## 3. Archive 目录（14个文件）

全部未被引用，为历史备份：
- `fetch_boards.py` - 旧版板块采集
- `fetch_boards_complete.py` - 完整版
- `fetch_concept_web.py` - Web 采集
- `fetch_concepts_multi.py` - 多源采集
- `fetch_rel_batch.py` - 批量关联
- `fetch_sina_batch.py` - Sina 批量
- `fetch_ths_cdp.py` - 旧版 CDP（可删）
- `fetch_ths_cdp_full.py` - 完整 CDP
- `fetch_ths_core.py` - 核心采集
- `fetch_ths_playwright.py` - Playwright 版
- `fetch_ths_xbrowser.py` - xbrowser 版
- `fetch_unified.py` - 统一采集
- `fetch_xbrowser.py` - xbrowser 通用
- `update_ths_monthly.py` - 月度更新

**建议**: 保留作为历史记录，但 `fetch_ths_cdp.py` 可删除（与 full 版重复）

---

## 4. 活跃代码验证

### 4.1 正在使用的模块
```
✅ fetchers/akshare/fetch_stocks.py      - 申万行业关联
✅ fetchers/sina/fetch_sina_list.py      - Sina 板块列表
✅ fetchers/sina/fetch_sina_concepts.py  - Sina 成分股
✅ fetchers/ths/fetch_ths_list.py        - THS 板块列表
✅ fetchers/ths/fetch_ths_cdp.py         - THS 成分股（CDP）
✅ fetchers/ths/fetch_ths_concepts.py    - THS 成分股（akshare）
✅ fetchers/em/fetch_em_list.py          - EM 板块列表（CDN不通）
✅ fetchers/em/fetch_em_concepts.py      - EM 成分股（CDN不通）
✅ fetchers/db/db_schema.py              - 数据库 Schema
✅ fetchers/sync/sync_counts.py          - 计数同步（新版）
✅ fetchers/sync/sync_concepts.py        - 概念同步
✅ fetchers/sync/sync_industry_boards.py - 行业同步
✅ fetchers/status/report.py             - 状态报告
✅ fetchers/update/update_all.py         - 更新入口
✅ analysis/engine.py                    - 分析引擎
✅ analysis/checks.py                    - 审核逻辑
✅ analysis/data_source.py               - 数据源
✅ analysis/build_report.py              - 报告生成
✅ analysis/analyze.py                   - 分析入口
✅ run.py                                - 主入口
```

---

## 5. 清理建议

### 立即执行
```bash
# 1. 删除重复的旧版 sync_counts.py
rm scripts/sync/sync_counts.py
rmdir scripts/sync  # 如果为空

# 2. 删除 archive 中的旧版 fetch_ths_cdp.py
rm scripts/fetchers/archive/fetch_ths_cdp.py

# 3. 删除废弃的根目录脚本
rm scripts/_runner_fetch_stocks.py
rm scripts/collect_sina.py
rm scripts/run_sina_list.py
```

### 可选执行
```bash
# 4. 清理 archive 目录（如确认不再需要）
# rm -rf scripts/fetchers/archive/
```

---

## 6. 总结

| 类别 | 数量 | 操作 |
|------|------|------|
| 重复文件 | 2 个 | 删除旧版 |
| 废弃脚本 | 3 个 | 删除 |
| Archive 文件 | 14 个 | 保留（1个可删） |
| 硬编码路径 | 2 处 | 已标记，不影响运行 |

**净减少**: 5 个文件
