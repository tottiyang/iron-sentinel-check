# 铁血哨兵数据目录规范 v1.0

## 核心原则

1. **单一数据源**：所有数据必须写入统一的目录
2. **版本控制**：数据目录路径写入代码和文档，禁止随意变更
3. **备份收拢**：所有备份文件统一存放，禁止散落

## 数据目录结构

```
~/.qclaw/skills/iron-sentinel/
├── data/                          # 主数据目录（唯一）
│   ├── stock_data.db              # 主数据库（生产环境）
│   ├── stock_data.db.backup       # 自动备份（保留最近5个）
│   ├── stock_data.db.YYYYMMDD     # 日期备份
│   │
│   └── backups/                   # 备份目录（统一收拢）
│       ├── pre-rebuild/           # 重建前备份
│       ├── daily/                 # 每日自动备份
│       └── manual/                # 手动备份
│
└── stock_data.db                  # 软链接 → data/stock_data.db（兼容旧代码）
```

## 路径规范（代码中必须遵守）

### Python 代码
```python
# db_schema.py 中定义（唯一源头）
import os

SKILL_DIR = os.path.expanduser("~/.qclaw/skills/iron-sentinel")
DATA_DIR = os.path.join(SKILL_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "stock_data.db")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
```

### 环境变量（可选覆盖）
```bash
export IRON_SENTINEL_DATA_DIR="~/.qclaw/skills/iron-sentinel/data"
```

## 备份策略

| 类型 | 触发条件 | 保留数量 | 位置 |
|------|----------|----------|------|
| 自动备份 | 每次重建前 | 5个 | `data/stock_data.db.backup.N` |
| 日期备份 | 每日首次写入 | 30天 | `data/backups/daily/YYYYMMDD.db` |
| 重建备份 | 手动触发重建 | 永久 | `data/backups/pre-rebuild/YYYYMMDD_HHMMSS.db` |
| 手动备份 | 用户触发 | 按需 | `data/backups/manual/` |

## 清理现有分散的 db 文件

执行以下命令统一收拢：

```bash
# 1. 创建统一目录（所有数据+备份都在 data/ 下）
mkdir -p ~/.qclaw/skills/iron-sentinel/data/backups/{pre-rebuild,daily,manual}

# 2. 备份现有数据（以最大的为准）
cp ~/.qclaw/skills/iron-sentinel/scripts/fetchers/db/stock_data.db \
   ~/.qclaw/skills/iron-sentinel/data/backups/pre-rebuild/stock_data.db.$(date +%Y%m%d_%H%M%S)

# 3. 迁移到统一位置
mv ~/.qclaw/skills/iron-sentinel/scripts/fetchers/db/stock_data.db \
   ~/.qclaw/skills/iron-sentinel/data/stock_data.db

# 4. 创建软链接（兼容旧代码）
ln -sf ~/.qclaw/skills/iron-sentinel/data/stock_data.db \
       ~/.qclaw/skills/iron-sentinel/stock_data.db
ln -sf ~/.qclaw/skills/iron-sentinel/data/stock_data.db \
       ~/.qclaw/skills/iron-sentinel/scripts/fetchers/db/stock_data.db

# 5. 删除其他位置的重复文件
rm -f ~/.qclaw/skills/iron-sentinel/scripts/fetchers/stock_data.db
```

## 验证清单

- [ ] 所有代码使用 `db_schema.DB_PATH`
- [ ] 不存在多个 `stock_data.db` 文件
- [ ] 备份目录结构正确
- [ ] 软链接指向正确

## 生效日期

2026-04-27
