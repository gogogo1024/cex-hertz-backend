# Positions 双写与回填指南

目标：平滑将 `positions.volume` / `positions.avg_price`（字符串）迁移到新列 `volume_bigint` / `avg_price_bigint`（bigint 纳单位），保证零停机与可回滚。

整体步骤：

1) 添加新列（已提供 SQL）：
   - 运行文件：`migrations/20260925_add_positions_bigint.up.sql`

2) 代码双写阶段（线上部署）
   - 在业务代码中同时写入旧列（`volume` / `avg_price`）和新列（`volume_bigint` / `avg_price_bigint`）。
   - 保持读取仍然来自旧列，直到回填并验证完毕。

3) Backfill（填充历史数据）
   - 使用本仓库中的回填程序（Go）：

```bash
# dry run: 打印将要写入的 bigint 值
PG_DSN="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
  go run ./cmd/migrate/backfill_positions.go -dsn "$PG_DSN" -batch 500 -dry-run

# 真正写入
PG_DSN="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
  go run ./cmd/migrate/backfill_positions.go -dsn "$PG_DSN" -batch 500
```

4) 验证
   - 在 DB 中检查若干随机行：
     - `SELECT id, volume, volume_bigint, avg_price, avg_price_bigint FROM positions WHERE id IN (...)`。
     - 用 `volume_bigint = (volume::numeric * 1e8)::bigint` 验证转换一致性。

5) 切换读取与变更 API
   - 在确认回填无误后，切换代码读取新列 `volume_bigint` / `avg_price_bigint` 并在写入阶段只写新列（或者继续双写若需回滚窗口）。

6) 清理（可选、严格验证后）
   - 迁移完成且运行稳定一段时间后，可通过 DOWN migration 或 ALTER TABLE DROP COLUMN 移除旧列。

回滚计划
- 在任一步若需回滚：
  - 如果回填出错，停止写入新列（回滚到只写旧列），修复问题并重新运行回填。
  - 如果必须回退迁移（删除新列）：运行 `migrations/20260925_add_positions_bigint.down.sql`。

注意事项
- 大表回填可能耗时，请在低峰期运行并按 batch 分段执行；建议先使用 `-dry-run` 验证若干 batch。
- 确保有数据库备份与快照，以便出现问题时回滚。
