# Event Sourcing 系统部署指南

## 前置要求

### 系统环境
- **OS**: Linux (推荐 Ubuntu 20.04+) 或 macOS
- **Go**: 1.21+
- **PostgreSQL**: 14+ 
- **Kafka**: 3.0+ (可选，如果使用异步事件发布)

### 数据库要求
```bash
PostgreSQL 14.0+
  ├─ Max Connections: 至少 100 (推荐 200)
  ├─ Shared Buffers: 256MB+ (推荐 25% 物理内存)
  ├─ Effective Cache Size: 1GB+ (推荐 50% 物理内存)
  └─ Synchronous Commit: off (推荐用于高吞吐)
```

### 硬件建议
```
开发/测试环境:
  ├─ CPU: 2 cores
  ├─ Memory: 4GB
  ├─ Disk: 20GB SSD

生产环境:
  ├─ CPU: 8+ cores
  ├─ Memory: 16GB+
  ├─ Disk: 500GB+ SSD (根据数据量调整)
  └─ 磁盘 IOPS: 5000+ (对于高并发)
```

---

## 1️⃣ 环境准备

### 1.1 PostgreSQL 安装和初始化

#### macOS (Homebrew)
```bash
# 安装
brew install postgresql@14
brew services start postgresql@14

# 创建数据库和用户
createdb cex_hertz_backend
createuser cex_backend -P
```

#### Linux (Ubuntu/Debian)
```bash
# 安装
sudo apt update
sudo apt install postgresql-14 postgresql-contrib-14
sudo systemctl start postgresql
sudo systemctl enable postgresql

# 创建数据库和用户
sudo -u postgres createdb cex_hertz_backend
sudo -u postgres createuser cex_backend -P
sudo -u postgres psql -c "ALTER USER cex_backend WITH CREATEDB;"
```

#### 数据库权限设置
```sql
-- 以 postgres 用户连接
psql -U postgres

-- 创建用户和数据库
CREATE USER cex_backend WITH PASSWORD 'your_secure_password';
ALTER ROLE cex_backend WITH CREATEDB;

CREATE DATABASE cex_hertz_backend OWNER cex_backend;

-- 授予权限
GRANT ALL PRIVILEGES ON DATABASE cex_hertz_backend TO cex_backend;

-- 连接到数据库
\c cex_hertz_backend cex_backend

-- 创建 schema
CREATE SCHEMA IF NOT EXISTS public;
GRANT USAGE ON SCHEMA public TO cex_backend;
GRANT CREATE ON SCHEMA public TO cex_backend;
```

### 1.2 PostgreSQL 性能配置

编辑 `postgresql.conf` (通常在 `/etc/postgresql/14/main/` 或 Homebrew 的安装目录):

```ini
# 连接和内存
max_connections = 200
shared_buffers = 4GB          # 物理内存的 25%
effective_cache_size = 16GB   # 物理内存的 50%
work_mem = 20MB               # shared_buffers / max_connections
maintenance_work_mem = 1GB

# 性能优化
random_page_cost = 1.1        # SSD 优化
effective_io_concurrency = 200
wal_buffers = 16MB

# 提交和同步
synchronous_commit = off      # 高吞吐优化（牺牲部分持久性）
wal_level = minimal           # 如果不需要 PITR
checkpoint_timeout = 10min
checkpoint_completion_target = 0.9

# 日志（可选）
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql.log'
log_min_duration_statement = 1000  # 记录 >1s 的查询
```

重启 PostgreSQL 使配置生效：
```bash
# macOS
brew services restart postgresql@14

# Linux
sudo systemctl restart postgresql
```

---

## 2️⃣ 数据库 Schema 迁移

### 2.1 运行 Schema SQL 脚本

```bash
# 进入项目目录
cd cex-hertz-backend

# 连接到数据库并运行 SQL 脚本
psql -U cex_backend -d cex_hertz_backend -f biz/dal/pg/schema_outbox.sql
psql -U cex_backend -d cex_hertz_backend -f biz/dal/pg/schema_events.sql
psql -U cex_backend -d cex_hertz_backend -f biz/dal/pg/schema_checkpoint.sql
```

### 2.2 验证 Schema 创建

```bash
psql -U cex_backend -d cex_hertz_backend

-- 验证表
\dt

-- 应该看到以下表：
-- outbox
-- events
-- checkpoint
-- orders
-- trades
-- positions

-- 验证索引
\di

-- 应该看到 6+ 个 events 表的索引
```

### 2.3 初始化检查点

```sql
-- 初始化每个 symbol 的检查点
INSERT INTO checkpoint (symbol, last_seq, last_timestamp, created_at, updated_at)
VALUES 
  ('BTC/USDT', 0, 0, NOW(), NOW()),
  ('ETH/USDT', 0, 0, NOW(), NOW()),
  ('SOL/USDT', 0, 0, NOW(), NOW())
ON CONFLICT (symbol) DO NOTHING;
```

---

## 3️⃣ 应用配置

### 3.1 环境变量设置

```bash
# 数据库连接
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=cex_backend
export DB_PASSWORD=your_secure_password
export DB_NAME=cex_hertz_backend
export DB_SSL_MODE=disable  # 开发环境；生产环境改为 require

# 应用配置
export APP_PORT=8080
export APP_ENV=production
export LOG_LEVEL=info

# Kafka（可选）
export KAFKA_BROKERS=localhost:9092
export KAFKA_TOPIC_EVENTS=events-orderbook
export KAFKA_TOPIC_TRADES=trades-orderbook

# 监控（可选）
export METRICS_ENABLED=true
export METRICS_PORT=9090
```

### 3.2 配置文件

创建 `conf/prod.yaml`:

```yaml
# 数据库
database:
  host: ${DB_HOST:localhost}
  port: ${DB_PORT:5432}
  user: ${DB_USER:cex_backend}
  password: ${DB_PASSWORD}
  name: ${DB_NAME:cex_hertz_backend}
  sslmode: ${DB_SSL_MODE:disable}
  max_connections: 100
  max_idle_time: 300

# 应用
app:
  port: ${APP_PORT:8080}
  env: ${APP_ENV:production}
  log_level: ${LOG_LEVEL:info}

# Event Sourcing
event_sourcing:
  batch_size: 1000
  flush_interval_ms: 100
  recovery_batch_size: 5000

# Kafka
kafka:
  brokers: ${KAFKA_BROKERS:localhost:9092}
  topic_events: ${KAFKA_TOPIC_EVENTS:events-orderbook}
  topic_trades: ${KAFKA_TOPIC_TRADES:trades-orderbook}
  retry_max: 3
  retry_backoff_ms: 100

# 监控
monitoring:
  enabled: ${METRICS_ENABLED:true}
  port: ${METRICS_PORT:9090}
  scrape_interval: 10s
```

---

## 4️⃣ 构建和启动

### 4.1 编译

```bash
cd cex-hertz-backend

# 清理旧的构建
go clean

# 下载依赖
go mod download

# 构建
go build -o cex-hertz-backend .

# 验证编译成功
./cex-hertz-backend --version
```

### 4.2 启动应用

```bash
# 开发环境
go run main.go -config conf/dev.yaml

# 生产环境
./cex-hertz-backend -config conf/prod.yaml

# 或使用 systemd（Linux）
sudo systemctl start cex-hertz-backend
sudo systemctl status cex-hertz-backend
```

### 4.3 Docker 部署（可选）

```dockerfile
# Dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /build
COPY . .
RUN go build -o cex-hertz-backend .

FROM alpine:latest
RUN apk --no-cache add ca-certificates postgresql-client
COPY --from=builder /build/cex-hertz-backend /app/
COPY --from=builder /build/conf /app/conf/
COPY --from=builder /build/biz/dal/pg/schema*.sql /app/schema/
WORKDIR /app
EXPOSE 8080
CMD ["./cex-hertz-backend", "-config", "conf/prod.yaml"]
```

Docker Compose:
```yaml
version: '3.8'
services:
  postgres:
    image: postgres:14-alpine
    environment:
      POSTGRES_USER: cex_backend
      POSTGRES_PASSWORD: your_secure_password
      POSTGRES_DB: cex_hertz_backend
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./biz/dal/pg/schema_*.sql:/docker-entrypoint-initdb.d/
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cex_backend"]
      interval: 10s
      timeout: 5s
      retries: 5

  cex-backend:
    build: .
    environment:
      DB_HOST: postgres
      DB_USER: cex_backend
      DB_PASSWORD: your_secure_password
      DB_NAME: cex_hertz_backend
      APP_PORT: 8080
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy

  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

volumes:
  postgres_data:
```

---

## 5️⃣ 健康检查和验证

### 5.1 启动验证

```bash
# 1. 检查应用是否启动
curl -v http://localhost:8080/ping

# 2. 检查数据库连接
psql -U cex_backend -d cex_hertz_backend -c "SELECT COUNT(*) FROM events;"

# 3. 检查 Kafka 连接（如果使用）
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 4. 检查日志
tail -f logs/cex-hertz-backend.log
```

### 5.2 数据库健康检查 SQL

```sql
-- 检查表大小
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- 检查索引效率
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- 检查活跃连接
SELECT 
    datname,
    usename,
    application_name,
    state,
    COUNT(*) as count
FROM pg_stat_activity
WHERE datname = 'cex_hertz_backend'
GROUP BY datname, usename, application_name, state;

-- 检查长运行查询
SELECT 
    pid,
    usename,
    application_name,
    state,
    query,
    query_start,
    NOW() - query_start as duration
FROM pg_stat_activity
WHERE datname = 'cex_hertz_backend'
    AND state != 'idle'
ORDER BY query_start ASC;
```

---

## 6️⃣ 监控和日志

### 6.1 日志配置

```yaml
# 应用日志位置
logs/
├── cex-hertz-backend.log    # 主应用日志
├── error.log                 # 错误日志
├── event-sourcing.log        # 事件溯源相关
├── database.log              # 数据库操作
└── kafka.log                 # Kafka 发布日志
```

### 6.2 关键日志检查

```bash
# 检查启动日志
grep "PartitionAwareMatchEngine\|PostgresEventLog\|CheckpointManager" logs/cex-hertz-backend.log

# 检查错误
grep "ERROR\|FATAL" logs/error.log

# 检查事件处理性能
grep "EventPipeline\|EventStore" logs/event-sourcing.log

# 实时日志跟踪
tail -f logs/cex-hertz-backend.log | grep "EventLog\|Outbox\|Recovery"
```

### 6.3 监控指标

详见 [MONITORING-METRICS.md](MONITORING-METRICS.md)

---

## 7️⃣ 故障恢复和回滚

### 7.1 故障转移

如果主节点故障：

```bash
# 1. 停止故障节点
systemctl stop cex-hertz-backend

# 2. 在备份节点启动
# 应用会自动从最后的 checkpoint 恢复
systemctl start cex-hertz-backend

# 3. 验证恢复
# 检查 checkpoint 是否更新
psql -U cex_backend -d cex_hertz_backend \
  -c "SELECT symbol, last_seq, updated_at FROM checkpoint ORDER BY updated_at DESC LIMIT 5;"
```

### 7.2 数据回滚

如果需要回滚到特定时间点：

```bash
# 1. 备份当前数据
pg_dump -U cex_backend cex_hertz_backend > backup_$(date +%Y%m%d_%H%M%S).sql

# 2. 从事件历史恢复
-- 找到回滚时间点的 seq
SELECT id, global_seq, created_at, event_type FROM events 
WHERE created_at <= '2026-09-19 12:00:00'
ORDER BY global_seq DESC LIMIT 1;

-- 删除之后的事件（谨慎！应在测试环境先验证）
DELETE FROM events WHERE created_at > '2026-09-19 12:00:00';

-- 更新 checkpoint
UPDATE checkpoint SET last_seq = <上面查询的 seq> WHERE symbol = 'BTC/USDT';

# 3. 重启应用
systemctl restart cex-hertz-backend
```

### 7.3 应用回滚

如果应用版本有问题：

```bash
# 1. 停止当前版本
systemctl stop cex-hertz-backend

# 2. 切换到上一个版本的二进制文件
cp cex-hertz-backend.old cex-hertz-backend

# 3. 启动旧版本（数据库 schema 兼容性必须保证）
systemctl start cex-hertz-backend

# 4. 验证应用状态
curl http://localhost:8080/ping
```

---

## 8️⃣ 性能调优

### 8.1 PostgreSQL 查询优化

```sql
-- 分析表统计
ANALYZE events;
ANALYZE outbox;
ANALYZE checkpoint;

-- 重新建立索引（可选，如果性能下降）
REINDEX TABLE events;
REINDEX TABLE outbox;

-- 查看执行计划
EXPLAIN ANALYZE 
SELECT * FROM events 
WHERE symbol = 'BTC/USDT' 
AND created_at > NOW() - INTERVAL '1 hour'
ORDER BY global_seq DESC
LIMIT 100;
```

### 8.2 连接池优化

```yaml
# 在应用配置中调整
database:
  max_connections: 100
  min_idle: 10
  max_lifetime: 1800
  connection_timeout: 30
  idle_timeout: 300
```

### 8.3 事件批处理优化

```yaml
# 在应用配置中调整
event_sourcing:
  batch_size: 1000        # 批量写入大小
  flush_interval_ms: 100  # 批处理刷新间隔
  recovery_batch_size: 5000  # 恢复时的批处理大小
```

---

## 9️⃣ 故障排查

### 问题 1: 数据库连接失败
```bash
# 检查连接字符串
echo "postgresql://cex_backend:password@localhost:5432/cex_hertz_backend"

# 测试连接
psql postgresql://cex_backend:password@localhost:5432/cex_hertz_backend

# 检查 PostgreSQL 日志
tail -f /var/log/postgresql/postgresql.log
```

### 问题 2: 高延迟查询
```sql
-- 找出慢查询
SELECT query, calls, mean_time, max_time 
FROM pg_stat_statements 
ORDER BY mean_time DESC 
LIMIT 10;

-- 检查表扫描情况
SELECT schemaname, tablename, seq_scan, seq_tup_read, idx_scan
FROM pg_stat_user_tables
ORDER BY seq_tup_read DESC;
```

### 问题 3: 恢复超时
```bash
# 增加恢复批处理大小
export RECOVERY_BATCH_SIZE=10000

# 检查恢复进度
grep "Recovery\|Checkpoint" logs/cex-hertz-backend.log
```

---

## 🔟 生产检查清单

部署前必须完成：

- [ ] PostgreSQL 14+ 已安装并配置
- [ ] 创建了 `cex_hertz_backend` 数据库和 `cex_backend` 用户
- [ ] 所有 schema SQL 脚本已执行
- [ ] 环境变量已设置
- [ ] 应用能成功编译 (`go build . SUCCESS`)
- [ ] 数据库健康检查通过
- [ ] 应用启动后 `/ping` 端点响应正常
- [ ] 监控系统已配置
- [ ] 日志系统已就位
- [ ] 备份策略已制定
- [ ] 故障转移测试已完成
- [ ] 回滚计划已制定

---

## 延阅资源

- [IMPLEMENTATION-COMPLETE.md](IMPLEMENTATION-COMPLETE.md) - 完整实现总结
- [SEQUENCER-DESIGN.md](SEQUENCER-DESIGN.md) - 分布式序列设计
- [PHASE2.2-EVENTLOG-MIGRATION.md](PHASE2.2-EVENTLOG-MIGRATION.md) - EventLog 迁移细节
- [MONITORING-METRICS.md](MONITORING-METRICS.md) - 监控和性能指标
- [OPERATIONS-RUNBOOK.md](OPERATIONS-RUNBOOK.md) - 运维手册
