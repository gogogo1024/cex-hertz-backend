# Event Sourcing 系统运维手册

## 目录

1. [常见问题 (FAQ)](#常见问题-faq)
2. [日志分析指南](#日志分析指南)
3. [性能调优](#性能调优)
4. [备份和恢复](#备份和恢复)
5. [故障排查](#故障排查)
6. [操作命令速查表](#操作命令速查表)

---

## 常见问题 (FAQ)

### Q1: 应用启动慢，如何加速？

**症状**: 启动时日志显示长时间的恢复过程

**原因**: 
- EventStore 需要从数据库加载历史事件
- 恢复批处理大小配置过小
- 数据库查询性能下降

**解决方案**:

```bash
# 1. 增加恢复批处理大小（在配置文件中）
event_sourcing:
  recovery_batch_size: 10000  # 从默认 5000 增加到 10000

# 2. 预热索引
psql -U cex_backend -d cex_hertz_backend << EOF
ANALYZE events;
ANALYZE checkpoint;
REINDEX TABLE events;
EOF

# 3. 启用并行恢复
app:
  recovery_workers: 4  # 使用 4 个并发恢复线程

# 4. 检查数据库连接
psql -U cex_backend -d cex_hertz_backend -c "SHOW max_connections;"
```

### Q2: 内存占用不断增加，如何排查？

**症状**: `top` 或 `ps` 显示内存持续增长

**原因**:
- EventLog 缓存未清理
- 连接池泄漏
- 事件批处理堆积

**排查步骤**:

```bash
# 1. 检查应用内存状态
curl http://localhost:8080/debug/pprof/heap | head -30

# 2. 检查数据库连接数
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT datname, count(*) as connection_count
FROM pg_stat_activity
GROUP BY datname;
EOF

# 3. 检查 Outbox 积压
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT COUNT(*) as pending_messages 
FROM outbox 
WHERE published = false;
EOF

# 4. 清理过期数据
# 保留最近 30 天的数据，删除更早的
DELETE FROM events 
WHERE created_at < NOW() - INTERVAL '30 days'
AND symbol = 'OLD_SYMBOL';  -- 只删除特定 symbol 的旧数据

VACUUM FULL;  -- 回收磁盘空间
```

### Q3: 数据库查询突然变慢，如何处理？

**症状**: 查询延迟从 10ms 跳到 100ms+

**原因**:
- 索引 fragmentation
- 统计数据过期
- 并发查询过多

**解决方案**:

```bash
# 1. 更新表统计
psql -U cex_backend -d cex_hertz_backend << EOF
ANALYZE events;
ANALYZE checkpoint;
ANALYZE outbox;
EOF

# 2. 检查索引健康度
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;
EOF

# 3. 如果某个索引未被使用，可以删除它
-- DROP INDEX CONCURRENTLY idx_name;  -- 在线删除，不锁表

# 4. 重建碎片化的索引
REINDEX INDEX CONCURRENTLY idx_events_global_seq;
REINDEX INDEX CONCURRENTLY idx_events_symbol_created;

# 5. 检查慢查询日志
tail -100 /var/log/postgresql/postgresql.log | grep "duration:"
```

### Q4: Outbox 消息积压（待发布消息过多），如何清理？

**症状**: 

```
WARN: Outbox pending messages > 10000
```

**原因**:
- Kafka broker 连接失败或缓慢
- 发布线程被阻塞
- 消息体过大导致发送慢

**解决方案**:

```bash
# 1. 检查 Kafka 连接
kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# 2. 检查 Outbox 状态
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT 
  event_type,
  COUNT(*) as count,
  MAX(created_at) as latest,
  COUNT(CASE WHEN published = false THEN 1 END) as unpublished
FROM outbox
GROUP BY event_type;
EOF

# 3. 手动触发发布
curl -X POST http://localhost:8080/admin/outbox/dispatch

# 4. 查看发布错误
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT event_id, retry_count, last_error, updated_at
FROM outbox
WHERE published = false
ORDER BY retry_count DESC
LIMIT 20;
EOF

# 5. 如果某些消息发送失败过多次，可以标记为死信
UPDATE outbox 
SET published = true 
WHERE retry_count > 10 
AND published = false;
```

### Q5: 恢复卡住了，怎么办？

**症状**: 应用卡在恢复阶段，日志不更新

**原因**:
- 数据库查询超时或挂起
- 内存不足导致 OOM
- Checkpoint 表损坏

**解决方案**:

```bash
# 1. 检查数据库连接
psql -U cex_backend -d cex_hertz_backend -c "SELECT COUNT(*) FROM pg_stat_activity;"

# 2. 如果有长时间运行的查询，kill 它
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT pid, query, query_start, NOW() - query_start as duration
FROM pg_stat_activity
WHERE datname = 'cex_hertz_backend' 
AND state != 'idle'
ORDER BY query_start ASC;

-- Kill 长时间查询（谨慎！）
SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
WHERE pid != pg_backend_pid() 
AND duration > INTERVAL '5 minutes';
EOF

# 3. 增加超时时间
# 在应用配置中：
database:
  statement_timeout: 60000  # 60 秒

# 4. 重启应用
systemctl restart cex-hertz-backend

# 5. 从特定 checkpoint 恢复（跳过问题区间）
# 首先检查 checkpoint
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT symbol, last_seq, last_timestamp, updated_at FROM checkpoint;
EOF

# 如果需要重置：
DELETE FROM checkpoint WHERE symbol = 'BTC/USDT';
INSERT INTO checkpoint (symbol, last_seq, last_timestamp, created_at, updated_at)
VALUES ('BTC/USDT', 50000, EXTRACT(EPOCH FROM NOW())::BIGINT, NOW(), NOW());
EOF
```

### Q6: 磁盘空间快满了，如何处理？

**症状**: `df -h` 显示磁盘使用率 >85%

**原因**:
- 事件数据累积
- 日志文件未轮转
- 临时文件未清理

**解决方案**:

```bash
# 1. 分析磁盘占用
du -sh /var/lib/postgresql/* | sort -hr

# 2. 存档旧事件（推荐方案：保留 3 个月数据）
psql -U cex_backend -d cex_hertz_backend << EOF
-- 导出旧事件到文件（备份）
\COPY events TO '/tmp/events_archive_2024_06.csv' (FORMAT csv, HEADER true)
WHERE created_at < '2024-06-01';

-- 删除旧事件
DELETE FROM events 
WHERE created_at < '2024-06-01' 
AND symbol NOT IN ('BTC/USDT', 'ETH/USDT');  -- 保留关键交易对

-- 清理日志
VACUUM FULL events;
REINDEX TABLE events;
EOF

# 3. 清理 PostgreSQL 日志
find /var/log/postgresql -name "*.log" -mtime +30 -delete

# 4. 清理应用日志（如果日志文件存储在本地）
find /var/log/cex-hertz-backend -name "*.log.*" -mtime +7 -delete

# 5. 检查磁盘现状
df -h
du -sh /var/lib/postgresql
```

### Q7: 网络突然中断后数据不一致，怎么恢复？

**症状**: 节点间数据不同步，checkpoint 不一致

**原因**:
- 网络分区导致的脑裂
- Kafka 分区丢失
- 节点间心跳超时

**恢复步骤**:

```bash
# 1. 识别主节点（看哪个节点 checkpoint 最新）
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT symbol, last_seq, updated_at, hostname 
FROM checkpoint 
ORDER BY updated_at DESC;
EOF

# 2. 停止所有节点
for node in node1 node2 node3; do
  ssh $node "systemctl stop cex-hertz-backend"
done

# 3. 从主节点同步数据到从节点
pg_dump -U cex_backend cex_hertz_backend > /tmp/primary.sql

for node in secondary-node1 secondary-node2; do
  scp /tmp/primary.sql $node:/tmp/
  ssh $node "psql -U cex_backend cex_hertz_backend < /tmp/primary.sql"
done

# 4. 重启所有节点
for node in node1 node2 node3; do
  ssh $node "systemctl start cex-hertz-backend"
done

# 5. 验证一致性
for node in node1 node2 node3; do
  ssh $node "psql -U cex_backend cex_hertz_backend -c 'SELECT COUNT(*) FROM events;'"
done
```

---

## 日志分析指南

### 日志文件位置

```
/var/log/cex-hertz-backend/
├── cex-hertz-backend.log       # 主应用日志
├── error.log                    # 错误日志
├── event-sourcing.log           # 事件溯源相关
├── database.log                 # 数据库操作
└── kafka.log                    # Kafka 发布
```

### 常见日志模式识别

#### 1. 正常启动日志

```
2024-12-19T10:30:00.123Z INFO   Application starting...
2024-12-19T10:30:01.456Z INFO   Loading configuration from conf/prod.yaml
2024-12-19T10:30:02.789Z INFO   Connecting to PostgreSQL at localhost:5432
2024-12-19T10:30:03.012Z INFO   Database schema verified
2024-12-19T10:30:04.345Z INFO   Loading checkpoint for BTC/USDT at seq 1000000
2024-12-19T10:30:05.678Z INFO   EventLog recovered: 1000 events loaded
2024-12-19T10:30:06.901Z INFO   PartitionAwareMatchEngine initialized
2024-12-19T10:30:07.234Z INFO   Server listening on :8080
```

#### 2. 高延迟警告

```
# 查询慢
2024-12-19T10:35:00.123Z WARN  EventLogQuery: SELECT * FROM events WHERE symbol='BTC/USDT' took 150ms (threshold: 100ms)

# 写入慢
2024-12-19T10:35:10.456Z WARN  EventStoreWrite: INSERT into events took 200ms for 500 events

# Outbox 发布慢
2024-12-19T10:35:20.789Z WARN  OutboxPublish: Kafka send took 1500ms for 100 messages
```

#### 3. 错误和异常

```
# 数据库连接错误
2024-12-19T10:40:00.123Z ERROR EventStore: Failed to connect to database: connection refused

# Kafka 发送失败
2024-12-19T10:40:10.456Z ERROR Outbox: Failed to send to Kafka: broker unavailable

# 幂等性冲突
2024-12-19T10:40:20.789Z ERROR EventStore: Duplicate event_id detected: TRADE-12345 (retrying)
```

### 日志搜索命令

```bash
# 查找所有错误
grep -i "error\|failed\|fatal" /var/log/cex-hertz-backend/cex-hertz-backend.log

# 查找高延迟操作
grep -E "took [0-9]{3,}ms" /var/log/cex-hertz-backend/event-sourcing.log

# 查找恢复相关日志
grep -i "recovery\|checkpoint\|recovering" /var/log/cex-hertz-backend/cex-hertz-backend.log

# 查找 Kafka 问题
grep -i "kafka" /var/log/cex-hertz-backend/kafka.log

# 查找过去 1 小时的错误
find /var/log/cex-hertz-backend -name "*.log" -mtime -0 -exec grep -l "ERROR" {} \;

# 统计错误频率
grep -c "ERROR" /var/log/cex-hertz-backend/error.log

# 查看最后 100 行
tail -100 /var/log/cex-hertz-backend/cex-hertz-backend.log
```

---

## 性能调优

### 1. PostgreSQL 查询优化

#### 查看执行计划

```sql
EXPLAIN ANALYZE 
SELECT * FROM events 
WHERE symbol = 'BTC/USDT' 
AND created_at > NOW() - INTERVAL '1 hour'
ORDER BY global_seq DESC
LIMIT 1000;
```

**优化策略**:

| 症状 | 原因 | 解决方案 |
|------|------|---------|
| Seq Scan | 索引未使用 | 确保索引存在：`CREATE INDEX idx_events_symbol ON events(symbol)` |
| 高成本 | 选择性差 | 添加覆盖索引：`CREATE INDEX idx_full ON events(symbol, created_at, global_seq)` |
| 排序操作 | 无索引排序 | 添加排序索引：`CREATE INDEX idx_seq_order ON events(global_seq)` |

#### 批量操作优化

```sql
-- 不好的做法：逐行插入
INSERT INTO events VALUES (...);
INSERT INTO events VALUES (...);
INSERT INTO events VALUES (...);

-- 好的做法：批量插入
INSERT INTO events VALUES (...), (...), (...);

-- 最好的做法：使用 COPY
COPY events FROM STDIN;
...
\.
```

### 2. 应用级优化

#### 连接池配置

```yaml
database:
  # 初始连接数
  min_connections: 10
  # 最大连接数
  max_connections: 100
  # 连接最大生命周期（秒）
  max_lifetime: 1800
  # 连接空闲超时（秒）
  idle_timeout: 300
  # 连接获取超时（秒）
  acquisition_timeout: 30
```

#### 批处理配置

```yaml
event_sourcing:
  # 批写入大小
  batch_size: 1000
  # 批处理刷新间隔（毫秒）
  flush_interval_ms: 100
  # 恢复批处理大小
  recovery_batch_size: 5000
  # 查询分页大小
  query_page_size: 1000
```

#### 缓存优化

```go
// 只缓存热数据
const CacheSize = 10000       // 缓存条目数
const CacheTTL = 5 * time.Minute  // 缓存过期时间

// 使用分层缓存
L1Cache (内存) -> L2Cache (Redis) -> L3Cache (PostgreSQL)
```

### 3. 监控关键指标

```bash
# 实时监控应用吞吐量
watch -n 1 'curl -s http://localhost:8080/metrics | grep throughput'

# 实时监控数据库连接
watch -n 1 'psql -U cex_backend -d cex_hertz_backend -c "SELECT COUNT(*) FROM pg_stat_activity;"'

# 监控磁盘 I/O
iostat -xm 1 5

# 监控内存使用
free -h && top -b -n 1 | grep cex-hertz-backend
```

---

## 备份和恢复

### 1. 定期备份策略

```bash
#!/bin/bash
# backup.sh - 每日备份脚本

BACKUP_DIR="/backup/cex-hertz"
RETENTION_DAYS=30
DATE=$(date +%Y%m%d_%H%M%S)

# 全库备份
pg_dump -U cex_backend cex_hertz_backend | gzip > $BACKUP_DIR/full_$DATE.sql.gz

# 仅备份事件表（用于快速恢复）
pg_dump -U cex_backend -t events cex_hertz_backend | gzip > $BACKUP_DIR/events_$DATE.sql.gz

# 删除过期备份
find $BACKUP_DIR -name "*.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: $BACKUP_DIR/full_$DATE.sql.gz"
```

### 2. 增量备份

```sql
-- 仅备份过去 24 小时的事件
\COPY (
  SELECT * FROM events 
  WHERE created_at > NOW() - INTERVAL '1 day'
  ORDER BY global_seq
) TO '/backup/cex-hertz/incremental_2024_12_19.csv' (FORMAT csv, HEADER true);
```

### 3. 恢复过程

```bash
# 方法 1：全库恢复
gunzip -c /backup/cex-hertz/full_20241219_120000.sql.gz | \
  psql -U cex_backend cex_hertz_backend

# 方法 2：仅恢复事件表
gunzip -c /backup/cex-hertz/events_20241219_120000.sql.gz | \
  psql -U cex_backend cex_hertz_backend

# 方法 3：从 CSV 恢复（选择性恢复）
psql -U cex_backend -d cex_hertz_backend << EOF
COPY events FROM '/backup/cex-hertz/incremental_2024_12_19.csv' (FORMAT csv, HEADER true);
EOF
```

---

## 故障排查

### 故障树诊断

```
应用无法启动
├─ 数据库连接失败
│  ├─ 检查 DB_HOST, DB_PORT, DB_USER, DB_PASSWORD
│  ├─ 检查网络连通性: ping <DB_HOST>
│  └─ 检查 PostgreSQL 状态: systemctl status postgresql
│
├─ Schema 初始化失败
│  ├─ 检查 schema 文件: ls -la biz/dal/pg/schema_*.sql
│  ├─ 手动执行 SQL: psql -U cex_backend -d cex_hertz_backend -f schema_events.sql
│  └─ 检查权限: SELECT * FROM information_schema.tables WHERE table_schema='public';
│
└─ 应用启动卡住
   ├─ 检查日志: tail -f logs/cex-hertz-backend.log
   ├─ 检查 CPU: top -p $(pgrep cex-hertz-backend)
   └─ 检查内存: ps aux | grep cex-hertz-backend
```

### 快速诊断命令集

```bash
#!/bin/bash
# diagnose.sh - 快速诊断脚本

echo "=== System Health Check ==="
echo "1. Service Status:"
systemctl status cex-hertz-backend

echo -e "\n2. Database Connection:"
psql -U cex_backend -d cex_hertz_backend -c "SELECT version();"

echo -e "\n3. Application Logs:"
tail -50 logs/cex-hertz-backend.log | tail -20

echo -e "\n4. Event Count:"
psql -U cex_backend -d cex_hertz_backend -c \
  "SELECT symbol, COUNT(*) FROM events GROUP BY symbol;"

echo -e "\n5. Checkpoint Status:"
psql -U cex_backend -d cex_hertz_backend -c \
  "SELECT symbol, last_seq, updated_at FROM checkpoint;"

echo -e "\n6. Outbox Status:"
psql -U cex_backend -d cex_hertz_backend -c \
  "SELECT COUNT(*) as total, COUNT(CASE WHEN published=false THEN 1 END) as unpublished FROM outbox;"

echo -e "\n7. Resource Usage:"
ps aux | grep cex-hertz-backend | grep -v grep

echo -e "\n=== Diagnostics Complete ==="
```

---

## 操作命令速查表

### 应用管理

```bash
# 启动
systemctl start cex-hertz-backend

# 停止
systemctl stop cex-hertz-backend

# 重启
systemctl restart cex-hertz-backend

# 状态检查
systemctl status cex-hertz-backend

# 查看日志
journalctl -u cex-hertz-backend -f

# 优雅关闭（等待当前请求完成）
systemctl stop cex-hertz-backend --timeout=30
```

### 数据库管理

```bash
# 连接数据库
psql -U cex_backend -d cex_hertz_backend

# 执行 SQL 脚本
psql -U cex_backend -d cex_hertz_backend -f query.sql

# 导出数据
pg_dump -U cex_backend cex_hertz_backend > backup.sql

# 导入数据
psql -U cex_backend cex_hertz_backend < backup.sql

# 检查表大小
psql -U cex_backend -d cex_hertz_backend -c \
  "SELECT tablename, pg_size_pretty(pg_total_relation_size('public.'||tablename)) \
   FROM pg_tables WHERE schemaname='public' ORDER BY pg_total_relation_size('public.'||tablename) DESC;"

# 清理和优化
psql -U cex_backend -d cex_hertz_backend -c "VACUUM FULL; ANALYZE;"
```

### 性能监控

```bash
# 查看慢查询
psql -U cex_backend -d cex_hertz_backend << EOF
SELECT query, calls, mean_time, max_time 
FROM pg_stat_statements 
ORDER BY mean_time DESC LIMIT 10;
EOF

# 查看表扫描情况
psql -U cex_backend -d cex_hertz_backend -c \
  "SELECT schemaname, tablename, seq_scan, seq_tup_read \
   FROM pg_stat_user_tables ORDER BY seq_tup_read DESC LIMIT 10;"

# 查看索引使用情况
psql -U cex_backend -d cex_hertz_backend -c \
  "SELECT schemaname, tablename, indexname, idx_scan \
   FROM pg_stat_user_indexes ORDER BY idx_scan DESC;"
```

### 监控和告警

```bash
# 检查应用可用性
curl -f http://localhost:8080/ping || echo "Application DOWN"

# 检查指标端点
curl http://localhost:9090/metrics | head -30

# 检查 Prometheus 告警
curl http://localhost:9090/api/v1/alerts

# Grafana 仪表板
# 打开浏览器访问 http://localhost:3000
# 默认用户/密码: admin/admin
```

---

## 相关文档

- [DEPLOYMENT-GUIDE.md](DEPLOYMENT-GUIDE.md) - 部署指南
- [MONITORING-METRICS.md](MONITORING-METRICS.md) - 监控和性能指标
- [IMPLEMENTATION-COMPLETE.md](IMPLEMENTATION-COMPLETE.md) - 完整实现总结
