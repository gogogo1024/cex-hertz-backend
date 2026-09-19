# Event Sourcing 系统监控和性能指标

## 概览

本文档定义了用于监控高并发交易所 Event Sourcing 系统的关键指标、告警规则和最佳实践。

---

## 1. 核心性能指标

### 1.1 事件存储性能 (EventStore)

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **写入吞吐量** | events/sec | <1000 | <500 | EventStore 每秒写入事件数 |
| **写入延迟** (p50) | ms | >10 | >50 | 事件写入数据库的中位延迟 |
| **写入延迟** (p95) | ms | >50 | >200 | 事件写入数据库的 95 分位延迟 |
| **写入延迟** (p99) | ms | >100 | >500 | 事件写入数据库的 99 分位延迟 |
| **批处理大小** | events | - | - | 单个批处理的事件数 |
| **批处理延迟** | ms | >100 | >500 | 批处理的端到端延迟 |

**告警规则**:
```yaml
AlertEventStoreWriteLatencyHigh:
  condition: event_store_write_latency_p99_ms > 500
  for: 5m
  severity: critical
  annotation: "EventStore write latency is critically high (p99 > 500ms)"

AlertEventStoreThroughputLow:
  condition: event_store_write_throughput_per_sec < 500
  for: 2m
  severity: warning
  annotation: "EventStore throughput below expected (< 500 events/sec)"
```

### 1.2 查询性能 (EventLog)

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **按符号查询延迟** (p50) | ms | >5 | >20 | 按 symbol 查询事件的延迟 |
| **按符号查询延迟** (p95) | ms | >20 | >100 | 按 symbol 查询事件的 95% 延迟 |
| **按时间查询延迟** | ms | >10 | >50 | 按时间范围查询的延迟 |
| **全局流查询延迟** | ms | >50 | >200 | 获取全局事件流的延迟 |
| **查询命中率** | % | <80 | <60 | 缓存命中率（如适用） |

**告警规则**:
```yaml
AlertEventLogQueryLatencyHigh:
  condition: eventlog_query_latency_p95_ms > 100
  for: 5m
  severity: warning
  annotation: "EventLog query latency is high (p95 > 100ms)"

AlertEventLogQueryTimeout:
  condition: rate(eventlog_query_timeout_total[1m]) > 0.1
  for: 2m
  severity: critical
  annotation: "EventLog queries timing out"
```

### 1.3 Checkpoint 管理

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **更新延迟** | ms | >100 | >500 | Checkpoint 更新的延迟 |
| **更新成功率** | % | <99 | <95 | Checkpoint 更新成功的比例 |
| **落后事件数** | events | >10000 | >50000 | 未处理的事件数（最后处理 seq 与最新 seq 的差） |
| **恢复时间** (RTO) | sec | >30 | >60 | 从故障恢复到可用的时间 |

**告警规则**:
```yaml
AlertCheckpointLagging:
  condition: checkpoint_lag_events > 50000
  for: 10m
  severity: warning
  annotation: "Checkpoint is lagging by {{ $value }} events"

AlertCheckpointUpdateFailure:
  condition: rate(checkpoint_update_failures_total[1m]) > 0.01
  for: 5m
  severity: critical
  annotation: "Checkpoint update failures detected"
```

---

## 2. 事务一致性指标 (Outbox Pattern)

### 2.1 Outbox 管理

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **待发布消息数** | messages | >1000 | >10000 | Outbox 中未发布的消息数 |
| **发布延迟** | ms | >1000 | >5000 | 消息进入 outbox 到发布的延迟 |
| **发布成功率** | % | <99 | <95 | 消息成功发布的比例 |
| **重试次数** | count | >10 | >50 | 平均重试次数 |
| **重试失败率** | % | >5 | >20 | 重试失败的比例 |

**告警规则**:
```yaml
AlertOutboxBacklogHigh:
  condition: outbox_pending_messages > 10000
  for: 5m
  severity: critical
  annotation: "Outbox backlog is high ({{ $value }} pending messages)"

AlertOutboxPublishFailure:
  condition: rate(outbox_publish_failures_total[1m]) > 0.1
  for: 3m
  severity: critical
  annotation: "Outbox publish failures rate is high"

AlertOutboxPublishLatencyHigh:
  condition: outbox_publish_latency_p99_ms > 5000
  for: 5m
  severity: warning
  annotation: "Outbox publish latency is high (p99 > 5s)"
```

### 2.2 Kafka 发布 (如适用)

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **发送吞吐量** | msgs/sec | <100 | <50 | 每秒发送到 Kafka 的消息数 |
| **发送延迟** (p95) | ms | >500 | >1000 | 发送到 Kafka 的 95% 延迟 |
| **发送失败率** | % | >1 | >5 | 发送到 Kafka 的失败比例 |
| **Broker 连接状态** | status | - | disconnected | 与 Kafka broker 的连接状态 |

**告警规则**:
```yaml
AlertKafkaBrokerDisconnected:
  condition: kafka_broker_connection_status == 0
  for: 1m
  severity: critical
  annotation: "Kafka broker connection lost"

AlertKafkaSendFailure:
  condition: rate(kafka_send_failures_total[1m]) > 0.05
  for: 3m
  severity: warning
  annotation: "Kafka send failure rate is high"
```

---

## 3. 数据库性能指标

### 3.1 连接和会话

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **活跃连接** | connections | >80 | >100 | 当前数据库活跃连接数 |
| **空闲连接** | connections | - | - | 闲置但保留的连接数 |
| **连接创建率** | conn/sec | >10 | >20 | 每秒新建连接数 |
| **连接失败率** | % | >1 | >5 | 连接失败的比例 |

**告警规则**:
```yaml
AlertDatabaseConnectionPoolExhausted:
  condition: database_active_connections > 100
  for: 2m
  severity: critical
  annotation: "Database connection pool nearly exhausted"

AlertDatabaseConnectionCreationSpike:
  condition: rate(database_new_connections_total[1m]) > 20
  for: 2m
  severity: warning
  annotation: "High rate of database connection creation"
```

### 3.2 缓存和内存

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **缓存命中率** | % | <80 | <60 | 缓存命中的比例 |
| **缓存大小** | MB | >1000 | >2000 | 当前缓存占用内存 |
| **内存使用率** | % | >80 | >90 | 数据库进程内存使用率 |

**告警规则**:
```yaml
AlertDatabaseMemoryUsageHigh:
  condition: database_memory_usage_percent > 90
  for: 5m
  severity: critical
  annotation: "Database memory usage is critically high"
```

### 3.3 磁盘 I/O

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **读吞吐量** | MB/s | - | - | 磁盘读取速率 |
| **写吞吐量** | MB/s | - | - | 磁盘写入速率 |
| **IOPS** | ops/s | <5000 | <2000 | 每秒输入/输出操作数 |
| **磁盘空间使用率** | % | >80 | >90 | 磁盘使用比例 |

**告警规则**:
```yaml
AlertDiskSpaceAlmostFull:
  condition: disk_usage_percent > 90
  for: 5m
  severity: critical
  annotation: "Disk space is almost full"

AlertIOPSBottleneck:
  condition: database_iops < 2000
  for: 10m
  severity: critical
  annotation: "Database I/O is bottlenecked"
```

---

## 4. 应用级指标

### 4.1 撮合引擎

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **订单处理延迟** (p99) | ms | >100 | >500 | 订单处理的 99% 延迟 |
| **交易执行延迟** (p99) | ms | >50 | >200 | 交易执行的 99% 延迟 |
| **处理吞吐量** | orders/sec | <1000 | <500 | 每秒处理的订单数 |
| **撮合成功率** | % | <99 | <95 | 订单成功撮合的比例 |

**告警规则**:
```yaml
AlertOrderProcessingLatencyHigh:
  condition: order_processing_latency_p99_ms > 500
  for: 5m
  severity: critical
  annotation: "Order processing latency is critically high"

AlertMatchingThroughputLow:
  condition: matching_throughput_orders_per_sec < 500
  for: 5m
  severity: warning
  annotation: "Matching throughput is below expected"
```

### 4.2 错误和异常

| 指标 | 单位 | 警告 | 严重 | 说明 |
|------|------|------|------|------|
| **错误率** | % | >0.5 | >2 | 总请求中的错误比例 |
| **异常数** | exceptions/min | >10 | >50 | 每分钟的异常发生数 |
| **恐慌恢复** | count | >0 | >5 | Panic 恢复次数 |
| **超时次数** | timeouts/min | >5 | >20 | 每分钟的超时次数 |

**告警规则**:
```yaml
AlertErrorRateHigh:
  condition: rate(app_errors_total[1m]) / rate(app_requests_total[1m]) > 0.02
  for: 3m
  severity: critical
  annotation: "Application error rate is high"

AlertPanicRecovery:
  condition: increase(app_panic_recoveries_total[5m]) > 0
  severity: critical
  annotation: "Application panic detected and recovered"
```

---

## 5. 恢复和可靠性指标

### 5.1 故障恢复

| 指标 | 单位 | 目标 | 说明 |
|------|------|------|------|
| **RTO** (Recovery Time Objective) | sec | <30 | 故障后恢复到可用的时间 |
| **RPO** (Recovery Point Objective) | events | 0 | 允许丢失的最大事件数 |
| **恢复成功率** | % | >99.9 | 恢复成功的比例 |
| **数据一致性** | % | 100 | 恢复后的数据一致性 |

**监控查询**:
```sql
-- 监控最后一次恢复
SELECT 
    symbol,
    last_seq,
    last_timestamp,
    NOW() - updated_at as time_since_update,
    updated_at
FROM checkpoint
ORDER BY updated_at DESC;

-- 检查未处理的事件
SELECT 
    symbol,
    COUNT(*) as unprocessed_count,
    MAX(global_seq) - MAX(last_seq) as lag
FROM events e
LEFT JOIN checkpoint c ON e.symbol = c.symbol
WHERE e.created_at > NOW() - INTERVAL '1 hour'
GROUP BY symbol;
```

### 5.2 数据一致性

| 指标 | 单位 | 目标 | 说明 |
|------|------|------|------|
| **快照一致性** | % | 100 | 快照中的一致性 |
| **幂等性验证** | % | 100 | 幂等性检查通过率 |
| **事件完整性** | % | 100 | 事件历史的完整性 |

---

## 6. Prometheus 配置

### 6.1 Scrape 配置

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'cex-hertz-backend'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 10s
    scrape_timeout: 5s

  - job_name: 'postgres'
    static_configs:
      - targets: ['localhost:9187']  # postgres_exporter
    scrape_interval: 15s

  - job_name: 'kafka'
    static_configs:
      - targets: ['localhost:9308']  # kafka_exporter
    scrape_interval: 15s
```

### 6.2 告警规则配置

```yaml
# alert_rules.yml
groups:
  - name: cex_hertz_backend
    interval: 30s
    rules:
      - alert: EventStoreLatencyHigh
        expr: event_store_write_latency_p99_ms > 500
        for: 5m
        annotations:
          summary: "EventStore latency is critically high"
          description: "p99 latency: {{ $value }}ms"

      - alert: OutboxBacklogHigh
        expr: outbox_pending_messages > 10000
        for: 5m
        annotations:
          summary: "Outbox has high backlog"
          description: "Pending messages: {{ $value }}"

      - alert: CheckpointLagging
        expr: checkpoint_lag_events > 50000
        for: 10m
        annotations:
          summary: "Checkpoint is significantly lagging"
          description: "Lag: {{ $value }} events"

      - alert: DatabaseConnectionPoolExhausted
        expr: database_active_connections > 100
        for: 2m
        annotations:
          summary: "Database connection pool nearly exhausted"
          description: "Active connections: {{ $value }}"

      - alert: MatchingLatencyHigh
        expr: order_processing_latency_p99_ms > 500
        for: 5m
        annotations:
          summary: "Order matching latency is critically high"
          description: "p99 latency: {{ $value }}ms"
```

---

## 7. 仪表板示例 (Grafana)

### 7.1 关键指标仪表板

创建 Grafana dashboard 显示以下内容：

**第 1 行: 事件存储**
```
┌─────────────────┬─────────────────┬─────────────────┐
│ Write Throughput│ Write Latency p99│ Batch Size      │
│ (events/sec)    │ (ms)            │ (events)        │
└─────────────────┴─────────────────┴─────────────────┘
```

**第 2 行: 查询性能**
```
┌─────────────────┬─────────────────┬─────────────────┐
│ Query Latency   │ Cache Hit Rate  │ Query Timeout   │
│ p95 (ms)        │ (%)             │ (per min)       │
└─────────────────┴─────────────────┴─────────────────┘
```

**第 3 行: Outbox 和一致性**
```
┌─────────────────┬─────────────────┬─────────────────┐
│ Pending Messages│ Publish Success │ Retry Failures  │
│ (count)         │ Rate (%)        │ (per min)       │
└─────────────────┴─────────────────┴─────────────────┘
```

**第 4 行: 数据库**
```
┌─────────────────┬─────────────────┬─────────────────┐
│ Active Conn     │ Memory Usage    │ Disk Usage      │
│ (connections)   │ (%)             │ (%)             │
└─────────────────┴─────────────────┴─────────────────┘
```

**第 5 行: 应用**
```
┌─────────────────┬─────────────────┬─────────────────┐
│ Error Rate      │ Processing      │ Matching        │
│ (%)             │ Latency (ms)    │ Throughput (o/s)│
└─────────────────┴─────────────────┴─────────────────┘
```

### 7.2 JSON 配置示例

```json
{
  "dashboard": {
    "title": "CEX Hertz Backend - Event Sourcing",
    "panels": [
      {
        "title": "Event Store Write Throughput",
        "targets": [
          {"expr": "rate(event_store_write_total[1m])"}
        ]
      },
      {
        "title": "Event Store Write Latency (p99)",
        "targets": [
          {"expr": "histogram_quantile(0.99, event_store_write_latency_ms)"}
        ]
      }
    ]
  }
}
```

---

## 8. 日志收集和分析

### 8.1 ELK Stack 配置 (Elasticsearch, Logstash, Kibana)

```yaml
# logstash.conf
input {
  file {
    path => "/var/log/cex-hertz-backend/cex-hertz-backend.log"
    start_position => "beginning"
  }
}

filter {
  grok {
    match => { "message" => "%{TIMESTAMP_ISO8601:timestamp} \[%{DATA:component}\] %{LOGLEVEL:level} %{GREEDYDATA:message}" }
  }
  date {
    match => [ "timestamp", "ISO8601" ]
    target => "@timestamp"
  }
}

output {
  elasticsearch {
    hosts => ["localhost:9200"]
    index => "cex-hertz-%{+YYYY.MM.dd}"
  }
}
```

### 8.2 关键日志搜索

```
# Kibana 搜索查询

# 所有错误
component: "EventStore" AND level: "ERROR"

# 高延迟事件
component: "EventPipeline" AND latency_ms: [500 TO *]

# Outbox 发布失败
component: "OutboxDispatcher" AND level: "ERROR" AND operation: "publish"

# 恢复进度
component: "RecoveryExecutor" AND message: "Recovery*"

# 数据库连接问题
component: "Database" AND (level: "ERROR" OR message: "connection")
```

---

## 9. SLA 和告警级别

### 9.1 SLA 目标

| SLA | 目标 | 监控指标 |
|-----|------|---------|
| 可用性 | 99.9% | 应用可用时间比例 |
| 吞吐量 | >1000 orders/sec | matching_throughput |
| 延迟 (p99) | <500ms | order_processing_latency_p99 |
| 数据丢失 | 0 events | RPO = 0 |
| 恢复时间 | <30s | RTO < 30 |

### 9.2 告警级别定义

| 级别 | 响应时间 | 常见原因 | 处理步骤 |
|------|---------|---------|---------|
| **Info** | 无需立即响应 | 正常操作信息 | 记录和监控 |
| **Warning** | 30分钟 | 轻微性能下降 | 观察趋势，准备扩容 |
| **Critical** | 5分钟 | 服务受阻 | 立即调查和恢复 |
| **Emergency** | 即时 | 数据丢失风险 | 立即停止并回滚 |

---

## 10. 最佳实践

### 10.1 关键指标组合

- **系统健康**: 可用性 + 错误率 + 延迟 p99
- **性能**: 吞吐量 + 延迟 + 缓存命中率
- **可靠性**: 数据一致性 + RTO + RPO
- **容量**: 连接数 + 内存使用 + 磁盘使用

### 10.2 告警策略

- **避免告警风暴**: 使用 group_wait 和 group_interval 聚合告警
- **根因分析**: 每个告警都应该包含调试信息
- **自动化恢复**: 对于已知问题使用自动恢复脚本
- **定期测试**: 每月进行一次告警有效性测试

---

## 相关文档

- [DEPLOYMENT-GUIDE.md](DEPLOYMENT-GUIDE.md) - 部署指南
- [OPERATIONS-RUNBOOK.md](OPERATIONS-RUNBOOK.md) - 运维手册
- [IMPLEMENTATION-COMPLETE.md](IMPLEMENTATION-COMPLETE.md) - 完整实现总结
