# DEPRECATED — 废弃脚本清单

以下文件在 v3.0 重构后**已废弃**，已移动到 `archive/` 目录。

## 已归档文件（位于 `archive/` 目录）

| 文件 | 废弃原因 | 替代 |
|------|----------|------|
| `fetch_sina_batch.py` | INSERT OR REPLACE 导致一股仅一行业；stock_count 未按 source 更新 | `sina/fetch_sina_list.py` + `sina/fetch_sina_concepts.py` |
| `fetch_unified.py` | 早期探索，统一层设计思路有缺陷 | v3.0 无统一层，各源独立采集 |
| `fetch_concepts_multi.py` | 同上 | 各源独立采集 |
| `fetch_rel_batch.py` | 同上 | 各源独立采集 |
| `fetch_boards.py` | 使用 EM CDN 接口，CDN 不通时代码仍尝试写入空数据 | `em/fetch_em_list.py` |
| `fetch_boards_complete.py` | 同上，且结构混乱 | `em/fetch_em_list.py` |
| `fetch_concept_web.py` | 同上 | `em/fetch_em_concepts.py` |
| `fetch_ths_core.py` | INSERT OR REPLACE；逐股采集（效率低） | `ths/fetch_ths_list.py` + `ths/fetch_ths_concepts.py` |
| `update_ths_monthly.py` | 同上 | `ths/fetch_ths_concepts.py` |
| `fetch_ths_playwright.py` | 探索代码，未完成 | 后续单独处理 |
| `fetch_ths_cdp.py` | 探索代码，未完成 | 后续单独处理 |
| `fetch_ths_xbrowser.py` | 探索代码，未完成 | 后续单独处理 |
| `fetch_xbrowser.py` | 与 `_exploration` 重复 | 后续单独整理 |

## v3.0 正式使用脚本

| 文件 | 用途 |
|------|------|
| `run.py` | 采集主入口（顶层，9步流程） |
| `fetchers/db/db_reset.py` | 从0重建关联表 |
| `fetchers/db/db_schema.py` | 新 schema（含 source 列、正确 PK） |
| `fetchers/sina/fetch_sina_list.py` | Sina 板块列表采集 |
| `fetchers/sina/fetch_sina_concepts.py` | Sina 成分股关联采集 |
| `fetchers/ths/fetch_ths_list.py` | THS 板块列表采集 |
| `fetchers/ths/fetch_ths_concepts.py` | THS 成分股关联采集 |
| `fetchers/em/fetch_em_list.py` | EM 板块列表采集（CDN 不通处理） |
| `fetchers/em/fetch_em_concepts.py` | EM 成分股关联采集（CDN 不通处理） |
| `fetchers/akshare/fetch_stocks.py` | 个股 + 申万行业 + 申万关联 |
| `fetchers/sync/sync_concepts.py` | 概念成分股统一调度（断点续传） |
| `fetchers/sync/sync_industry_boards.py` | 证监会行业成分股同步 |
| `fetchers/sync/sync_counts.py` | 批量重算 stock_count |
| `fetchers/update/update_all.py` | 每日更新主入口 |
| `fetchers/status/report.py` | 状态报告 |

## Schema 关键变更

| 表 | v3.0 PK | 说明 |
|----|---------|------|
| `concept_boards` | (board_code, source) | 三源共存 |
| `industry_boards` | (board_code, source) | 三源共存 |
| `stock_concept` | (stock_code, board_code, source) | 三源独立存储 |
| `stock_industry_board` | (stock_code, board_code) | **去掉 source**，允许多行业 |
| `stock_industry` | (stock_code, level, industry_code) | 申万 L1/L2/L3 |
