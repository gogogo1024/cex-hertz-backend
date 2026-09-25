# Backfill 回填运维清单

目的：以安全、可恢复、可观测的方式把 `positions` 的 bigint 列回填上线，并最终切换读取到新列。

> 适用场景：已部署“写双列”（旧列 + 新 bigint 列）的应用，需把历史数据填到新列以完成迁移。

---

## 1. 总体流程（高层）

- 准备：代码、镜像、备份、Kubernetes 清单准备好。
- 预检（staging）：先 dry-run 验证逻辑与样例；再做小规模真实写入（canary）。
- 生产回填：分片/分批执行，持续监控并保存 checkpoint；遇异常可短路回滚。
- 切换读取：在验证无误后灰度切换服务读取新列，最终删除旧列并清理。

---

## 2. 术语与关键参数

- `batch`：每批处理行数（示例 100–1000），影响事务大小与锁持续时间。
- `throttle`：批间等待毫秒数，降低对 DB 的瞬时压力。
- `checkpoint_table`：保存进度的表（示例 `backfill_checkpoints`）。
- `dry-run`：只打印转换结果，不写入 DB。
- `lock`：Postgres advisory lock，用于避免并发运行。

---

## 3. 准备阶段（必须）

1. 代码确认

   - 确认业务服务在写入时同时写旧列与 `volume_bigint` / `avg_price_bigint`（双写）。
   - 确认 `cmd/migrate/backfill_positions.go` 支持 `-dry-run`、`-batch`、`-throttle`、`-max`、checkpoint，以及 advisory lock。

2. 数据备份（强制）

```bash
# 逻辑备份（示例）
PG_DSN="postgres://user:pass@host:5432/db?sslmode=disable"
pg_dump -Fc "$PG_DSN" -f /tmp/positions_prebackfill.dump
```

3. 镜像与清单准备

```bash
docker build -f cmd/migrate/Dockerfile -t snoy/cex-backfill:latest .
docker push snoy/cex-backfill:latest
```

- 审核 k8s 清单：`k8s/backfill-cronjob.yaml`、`k8s/backfill-job-manual.yaml`、`k8s/backfill-rbac.yaml`。

4. 环境准备

- 确保 k8s 集群可用，并且有权限创建 `ServiceAccount`、`Secret`、`CronJob/Job`。
- 创建 Secret（示例）：

```bash
kubectl create secret generic db-credentials --from-literal=pg_dsn="$PG_DSN" -n default
```

---

## 4. 预检（在 staging）

1. 部署 RBAC 与 Secret：

```bash
kubectl apply -f k8s/backfill-rbac.yaml
kubectl create secret generic db-credentials --from-literal=pg_dsn="$PG_DSN" -n default
```

2. 部署 CronJob（先 dry-run）

编辑 `k8s/backfill-cronjob.yaml`，把 args 中 `-dry-run=false` 改为 `-dry-run=true`，然后：

```bash
kubectl apply -f k8s/backfill-cronjob.yaml
```

3. 手动触发一次 Job（dry-run）并查看日志：

```bash
kubectl create job --from=cronjob/positions-backfill positions-backfill-test-$(date +%s)
kubectl get pods -l job-name=positions-backfill-test-<ts> -w
kubectl logs -l job-name=positions-backfill-test-<ts>
```

4. 验证输出（抽样）：

```sql
-- 在 psql 中执行（示例）
SELECT id, volume, volume_bigint, avg_price, avg_price_bigint
FROM positions
WHERE id IN (1,2,3,4,5);
```

5. 小规模真实写入（Canary，`-max` 控制）

```bash
# 直接在集群中一次性运行（手动 Job）
kubectl run backfill-canary --rm -it --restart=Never \
  --image=snoy/cex-backfill:latest -- /usr/local/bin/backfill \
  -dsn "$PG_DSN" -dry-run=false -max=500 -batch=100 -throttle=200
```

验证 canary 成功后再考虑扩大量级。

---

## 5. Canary 流程与监控要点

- 选择低峰窗口并通知 on-call。 
- 监控点：checkpoint 进度、Job 日志中的失败数、Postgres slow queries、连接数、replica lag。 
- 抽样校验：对比 `(volume::numeric * 1e8)::bigint` 与 `volume_bigint` 的一致性。 

示例检测 SQL：

```sql
SELECT id FROM positions WHERE (volume::numeric * 1e8)::bigint != volume_bigint LIMIT 10;
```

若发现不一致，暂停并排查 parse/scale 逻辑。

---

## 6. 全量回填（生产执行）

1. 切换策略

- **不要** 在回填未稳定前切换服务读取。始终保持读旧列直到验证完成。

2. 执行（分片执行推荐）

示例：使用 k8s Job 按 id 范围批次运行（或直接用 CronJob 调度多次）：

```bash
# 手动触发一次 full run（注意资源与时间窗）
kubectl create job --from=cronjob/positions-backfill positions-backfill-run-$(date +%s)
```

或从容器中直接运行（运维节点）：

```bash
docker run --rm -e PG_DSN="$PG_DSN" snoy/cex-backfill:latest \
  -dsn "$PG_DSN" -dry-run=false -batch=500 -throttle=100
```

3. 期间检查点（checkpoint）与抽样校验

- 每完成若干批次检查 `backfill_checkpoints` 的 `last_id`。
- 随机抽样、对账并记录审计日志。

---

## 7. 监控与告警建议

- 导出指标（Prometheus）：
  - `backfill_rows_processed_total`、`backfill_rows_failed_total`、`backfill_last_processed_id`、`backfill_batch_duration_seconds`。
- 告警阈值建议：
  - 失败率 > 1% → 告警；
  - 单批耗时 > 2x 历史基线 → 告警；
  - replica lag > 30s → 停止回填并调查。

---

## 8. 回滚与恢复

1. 短路回滚（首选）

```bash
kubectl delete cronjob positions-backfill -n default
kubectl delete job -l app=positions-backfill -n default
```

- 立即把业务读取逻辑从 bigint 切回旧列（部署回退版）。

2. 数据级恢复（复杂/仅在严重事故）

- 从预先备份还原数据库，或根据情况把 `volume_bigint` 清空并重跑回填（谨慎）。

---

## 9. 切换读取与清理旧列

1. 灰度切换

- 部署代码版本以先在 1–2 个实例/小流量上读 `volume_bigint`，监控业务行为。无异常后逐渐扩大流量。

2. 删除旧列

- 等全部验证与历史回填完成并备份后，在维护窗口执行 `ALTER TABLE DROP COLUMN`。

```sql
-- 仅在确认完成后执行
ALTER TABLE positions DROP COLUMN IF EXISTS volume;
ALTER TABLE positions DROP COLUMN IF EXISTS avg_price;
```

---

## 10. 运行后复盘（必做）

- 记录：执行时间窗、镜像 tag/digest、processed rows、failed rows、checkpoints、异常与解决方案。
- 更新文档与 playbook，列出改进点。

---

## 11. 可选增强（后续优化）

- 把失败行写入 `backfill_errors` 表以便人工排查。 
- 在容器内暴露 Prometheus 指标并接入集群监控。 
- 增强 checkpoint 支持按 `symbol` 或更细粒度分片并行回填（需额外竞态控制）。

---

## 附录：常用命令速查

```bash
# 创建 secret
kubectl create secret generic db-credentials --from-literal=pg_dsn="$PG_DSN" -n default

# 应用 RBAC 与 CronJob
kubectl apply -f k8s/backfill-rbac.yaml
kubectl apply -f k8s/backfill-cronjob.yaml

# 手动触发 job
kubectl create job --from=cronjob/positions-backfill positions-backfill-run-$(date +%s)

# 查看 job pod 日志
kubectl logs -l job-name=positions-backfill-run-<ts> -n default

# 检查 checkpoint
psql "$PG_DSN" -c "SELECT * FROM backfill_checkpoints WHERE job_name = 'positions_backfill';"

# 备份
pg_dump -Fc "$PG_DSN" -f /tmp/positions_prebackfill.dump
```

---

请在 staging 先执行 dry-run 并把结果反馈给我，我可以协助你把 `k8s` 清单应用到集群，或把该 checklist 转为运维 runbook（含通知模板）。
