# 分区所有权（Partition Ownership）迁移设计文档

## 背景与目标
- 目标：实现可靠的分区所有权迁移（源节点 → 目标节点），保证迁移期间数据一致性和系统可恢复性。
- 要求：支持逐步切换（Freeze → Flush → Transfer → Commit），迁移信息持久化（DB），并提供可替换的协调器（TransferCoordinator），以便未来用 gRPC/Kafka control-topic 替换 HTTP+Consul 方案。

## 约束
- 需要兼容现有测试套件：默认情况下 FSM 行为应保持与当前测试向后兼容。
- 最小破坏性：在没有 coordinator 的情况下，FSM 应回退到原有的 permissive 行为（guards 返回 true）。
- 首次实现使用方案 A（可短期落地）：DB 持久化 + HTTP 调用（可选） + Consul 服务发现（如果可用）。

## 方案对比

方案 A — DB + HTTP + Consul（短期可交付）
- 描述：在源节点持久化一条 `ownership_transfers` 记录；发送 HTTP 请求到目标节点的内部接收端点（可通过 Consul 查找目标地址）；目标节点确认并将状态写回 DB（或通过 API 调用通知）。
- 优点：实现简单、可在现有 infra（Postgres + Consul）上运行、易于调试。
- 缺点：依赖目标节点运行 HTTP 接口，存在网络调用延迟。

方案 B — Kafka control-topic / gRPC（长期方案）
- 描述：通过 Kafka control topic 或 gRPC 做控制命令广播与确认；迁移操作以事件形式在控制通道上发布，目标节点订阅并确认完成。
- 优点：更解耦、可扩展、与已有消息总线对齐。
- 缺点：实现复杂度更高，需额外考虑控制通道的幂等与回放语义。

决定：先实现方案 A（快速落地、低风险），保留接口以便以后替换为方案 B。

## 数据模型（SQL）
在 `biz/dal/pg/schema_ownership_transfers.sql` 中添加：

```sql
CREATE TABLE IF NOT EXISTS ownership_transfers (
  id SERIAL PRIMARY KEY,
  partition_id TEXT NOT NULL,
  from_owner TEXT NOT NULL,
  to_owner TEXT NOT NULL,
  checkpoint_seq BIGINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ownership_transfers_partition ON ownership_transfers(partition_id);
CREATE INDEX IF NOT EXISTS idx_ownership_transfers_status ON ownership_transfers(status);
```

说明：使用简单的 `id` 自增主键避免在环境中依赖 uuid 扩展。

## 代码设计

1) `model.TransferCoordinator`（接口）
- 方法：
  - `InitiateTransfer(partitionID, fromOwner, toOwner string, checkpointSeq int64) (string, error)` — 在 DB 中创建转移记录并返回 transfer id。
  - `IsTargetReady(transferID string) (bool, error)` — 检查目标节点是否已确认接收（比如 status == 'completed'）。
  - `GetTransfer(transferID string) (*OwnershipTransfer, error)` — 读取转移记录。
  - `MarkTransferCompleted(transferID string) error` — 标记完成。

2) `DBTransferCoordinator`（`biz/service` 实现）
- 使用 `gorm.DB` 持久化 `ownership_transfers`，只在 `InitiateTransfer` 中写记录；后续可扩展为同时通过 Consul 查找并向目标发送 HTTP 请求。

3) FSM 集成（`biz/model/partition_ownership.go`）
- 新增可选字段 `transferCoordinator TransferCoordinator`（默认 nil，不影响现有测试）。
- `Flushing -> Transferring`：guard 首先检查 `CheckpointSeq ≤ LastProcessedSeq`（本地检查）。当 `transferCoordinator != nil` 时，在 action 中调用 `InitiateTransfer`，并将返回的 `transferID` 写入 metadata（`TransferID` 字段）。
- `Transferring -> Standby`：guard 在 `transferCoordinator != nil` 时调用 `IsTargetReady(transferID)`，等待目标确认。

## API Contract（目标节点 HTTP 接口，方案 A 可选）
- POST /internal/v1/ownership/transfer
  - 请求体：{ partition_id, from_owner, to_owner, checkpoint_seq, transfer_id }
  - 返回：{ accepted: true }
  - 目标节点收到请求后：
    1. 在本地准备好 checkpoint（基于 checkpoint_seq）并启动自身 recovery/回放流程；
    2. 回复 accepted；并将转移状态更新为 `completed`（写 DB 或通过 coordinator 回调）。

## FSM 变更要点（实现步骤）
1. 在 `model.OwnershipMetadata` 增加 `TransferID string` 字段。
2. 在 `OwnershipStateMachine` 中增加可选 `transferCoordinator TransferCoordinator`，并提供 `SetTransferCoordinator(tc TransferCoordinator)`。
3. 修改 `registerTransitions` 中 Flushing->Transferring 的 action：如果 coordinator 存在则 `InitiateTransfer`，并在 metadata 中写入 `TransferID`。
4. 修改 Transferring->Standby 的 guard：如果 coordinator 存在则基于 `IsTargetReady(TransferID)` 做判断，否则保留老行为。

## 测试计划
- 单元测试：
  - `model` 层：保持原有 `partition_ownership_test.go` 通过（默认 coordinator 为 nil）。
  - `service` 层：为 `DBTransferCoordinator` 添加单元测试（内存 sqlite 或 Postgres docker-compose）。
  - 快速集成：在本地 docker-compose 启动 Postgres，运行 `go test ./biz/service -run TestDBTransferCoordinator`。

- 集成测试场景：
  1. 正常迁移：source 发起 transfer → DB 写入 record → 模拟目标节点将 record 标记为 completed → 源节点通过 guard 成功切换到 Standby。
  2. 目标不可达：record 保持 pending → 源节点应阻塞或超时回滚（基于 migrationTimeout）。

## 任务清单（Issue checklist）
- [ ] 添加 SQL migration `schema_ownership_transfers.sql`。
- [ ] 增加 `model.TransferCoordinator` 接口与 `OwnershipTransfer` model。
- [ ] 实现 `service.DBTransferCoordinator`（仅持久化，HTTP 调用为可选/扩展）。
- [ ] 在 `OwnershipStateMachine` 中注入可选 coordinator，修改 guards/actions。确保在 coordinator 为 nil 时回退到原行为。
- [ ] 新增单元/集成测试：`DBTransferCoordinator`、迁移流程模拟测试。
- [ ] 文档更新：`doc/PARTITION_TRANSFER_DESIGN.md`、`doc/PARTITION_TRANSFER_PROTOCOL.md`（如需同步）。

## 风险与迭代策略
- 先以最小可用实现（persist-only）上线，观察迁移日志与回放成功率。
- 下一步（方案 B）：替换为 Kafka control-topic 或 gRPC，使控制面事件化并保证幂等与可回放。

---
作者: 自动生成（请审阅并补充运营/部署细节）
