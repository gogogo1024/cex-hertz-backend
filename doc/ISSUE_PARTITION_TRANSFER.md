# Issue: 实现分区所有权迁移（方案 A：DB + HTTP + Consul）

## 概述
实现一个可落地的分区所有权迁移协调器（TransferCoordinator），使源节点在迁移流程中将迁移信息持久化到 PostgreSQL（`ownership_transfers`），并在有条件时通过 HTTP 通知目标节点。首选方案为方案 A（DB + HTTP + Consul），后续可替换为 Kafka control-topic（方案 B）。

## 为什么要做
- 目前 `OwnershipStateMachine` 中多处 guards/actions 为占位实现，需要真实的转移协调路径与持久化支持，才能在生产环境安全执行分区迁移。

## 接受标准
- 新增数据库表 `ownership_transfers` 并提供迁移 SQL。
- 新增 `model.TransferCoordinator` 接口与 `model.OwnershipTransfer` model。
- 提供 `service.DBTransferCoordinator`：在 DB 中创建迁移记录并能查询状态。
- `OwnershipStateMachine` 在 coordinator 存在时会调用 `InitiateTransfer` 并将 `TransferID` 写入 metadata；在 `Transferring->Standby` guard 会检查 coordinator 状态以决定是否完成迁移。
- 保持向后兼容：在没有注入 coordinator 的情况下，现有的 `partition_ownership_test.go` 必须通过。

## 实现步骤（任务清单）
- [ ] 添加 SQL migration: `biz/dal/pg/schema_ownership_transfers.sql`。
- [ ] 新增 `biz/model/transfer_coordinator.go`（接口 + GORM model）。
- [ ] 新增 `biz/service/db_transfer_coordinator.go`（实现，持久化到 Postgres）。
- [ ] 修改 `biz/model/partition_ownership.go`：新增 `TransferID`、`SetTransferCoordinator`，并在相关 transition 中调用 coordinator（如果存在）。
- [ ] 为 `DBTransferCoordinator` 添加单元测试（可使用 sqlite 或 docker-compose 的 Postgres）。
- [ ] 在 CI / 本地运行：`go test ./biz/model -v` 以及 `go test ./biz/service -run TestDBTransferCoordinator -v`。

## 风险与回退策略
- 若目标节点不可达或确认延迟，迁移可依赖 `migrationTimeout` 超时机制回滚。
- 初始版仅做持久化（persist-only）；如果发现需更高实时性，再迭代加入 HTTP 主动通知与 Consul 服务发现。

## 备注（PR 描述模板）
请在 PR 中包含：
- SQL migration 文件路径与简要说明。
- 代码修改列表（文件级）。
- 本地测试步骤与必要的 docker-compose 服务。
