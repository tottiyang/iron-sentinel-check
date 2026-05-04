-- ============================================================
-- 数据库数据修复脚本 - 2026-05-04
-- 修复人: WorkBuddy
-- 关联 Issue: 铁血哨兵数据库数据质量审计
-- ============================================================

-- --------------------------------------------------------
-- Fix 1: concept_boards.stock_count 修复
-- 问题: stock_count 全为 NULL，从未被正确填充
-- 原因: 导入时未从 stock_concept 统计成分股数量
-- --------------------------------------------------------
UPDATE concept_boards 
SET stock_count = (
    SELECT COUNT(*) 
    FROM stock_concept sc 
    WHERE sc.board_code = concept_boards.board_code 
    AND sc.source = concept_boards.source
)
WHERE stock_count IS NULL;

-- --------------------------------------------------------
-- Fix 2: industry_boards 无效 source 数据清理
-- 问题: source 混入数字（15, 3, 5 等）
-- 原因: SINA_hangye_* 的 source 被错误设置为数字
-- --------------------------------------------------------
DELETE FROM industry_boards 
WHERE source NOT IN ('em', 'sina', 'ths', 'em_industry');

-- --------------------------------------------------------
-- Fix 3: industry_boards.stock_count 修复 (source='em')
-- 问题: 496 条 source='em' 的板块 stock_count 为 NULL
-- 原因: 导入时未统计成分股数量
-- --------------------------------------------------------
UPDATE industry_boards 
SET stock_count = (
    SELECT COUNT(*) 
    FROM stock_industry_board sib 
    WHERE sib.board_code = industry_boards.board_code 
    AND sib.source = 'em_industry'
)
WHERE source = 'em' AND stock_count IS NULL;

-- --------------------------------------------------------
-- Fix 4: stocks.exchange 北交所修复
-- 问题: 308 只 920 开头的北交所股票 exchange='UNKNOWN'
-- 原因: 数据导入时未识别北交所代码规则
-- --------------------------------------------------------
UPDATE stocks 
SET exchange = 'BJ' 
WHERE stock_code LIKE '92%' AND exchange = 'UNKNOWN';

-- --------------------------------------------------------
-- Fix 5: industry_boards.stock_count 不匹配修复
-- 问题: 84 条记录的 stock_count 与实际成分股数量不符
-- --------------------------------------------------------
UPDATE industry_boards 
SET stock_count = (
    SELECT COUNT(*) 
    FROM stock_industry_board sib 
    WHERE sib.board_code = industry_boards.board_code 
    AND sib.source = 'em_industry'
)
WHERE stock_count != (
    SELECT COUNT(*) 
    FROM stock_industry_board sib 
    WHERE sib.board_code = industry_boards.board_code 
    AND sib.source = 'em_industry'
);
