# 测试运行指南

## 前置条件

项目的测试需要以下服务可用：
- PostgreSQL 15
- Redis
- Kafka
- Consul（可选，用于服务注册）

## 快速启动测试环境

使用提供的脚本启动所有依赖服务：

```bash
# 使 shell 脚本可执行
chmod +x scripts/start-test-env.sh

# 启动测试环境
scripts/start-test-env.sh
```

这个脚本会：
1. 使用 docker-compose 启动所有依赖服务
2. 等待 PostgreSQL 就绪
3. 打印所有服务的连接信息

## 手动启动服务

或者，你也可以手动启动服务：

```bash
# 启动所有服务
docker-compose -f docker-compose-base.yaml up -d

# 查看服务状态
docker-compose -f docker-compose-base.yaml ps

# 停止所有服务
docker-compose -f docker-compose-base.yaml down
```

## 运行测试

### 运行所有测试

```bash
go test -v ./...
```

### 运行特定包的测试

```bash
go test -v ./biz/service
```

### 运行 Event Sourcing 测试

```bash
# 使用 no_rocksdb build tag 运行（避免 RocksDB 依赖）
go test -v ./biz/service -run '^Test(Integer|OrderBook|Depth|Event|Position)' -tags no_rocksdb
```

### 运行基准测试

```bash
go test -v ./biz/service -bench BenchmarkOrderBook -tags no_rocksdb
```

## 故障排查

### PostgreSQL 连接失败

如果看到 "connection refused" 错误：

1. 检查 PostgreSQL 是否正在运行：
   ```bash
   docker-compose -f docker-compose-base.yaml ps postgres
   ```

2. 检查日志：
   ```bash
   docker-compose -f docker-compose-base.yaml logs pg
   ```

3. 确保端口 5432 未被占用：
   ```bash
   lsof -i :5432
   ```

4. 完全重启服务：
   ```bash
   docker-compose -f docker-compose-base.yaml down
   docker-compose -f docker-compose-base.yaml up -d
   ```

### 配置文件找不到

如果看到 "读取配置文件失败" 错误：

1. 确保从项目根目录运行测试
2. 检查 `conf/test/conf.yaml` 文件存在
3. 如果从其他目录运行，指定完整路径或设置 `GO_ENV` 环境变量

## CI/CD 集成

在 CI/CD 中运行测试时：

1. 使用 docker-compose 启动依赖服务
2. 等待服务健康检查通过
3. 运行测试命令
4. 清理资源

示例（GitHub Actions）：

```yaml
services:
  postgres:
    image: postgres:15
    env:
      POSTGRES_PASSWORD: postgres
    options: >-
      --health-cmd pg_isready
      --health-interval 10s
      --health-timeout 5s
      --health-retries 5

steps:
  - name: Run tests
    run: go test -v ./biz/service -tags no_rocksdb
```

## 编写新测试

- Event Sourcing 相关测试应该在 `event_sourcing_test.go` 中
- 数据库相关测试应该在 `order_service_test.go` 或其他 `*_test.go` 文件中
- 确保不修改 TestMain 的数据库初始化逻辑
- 使用 `assert` 或 `require` 库（testify）编写断言

## 测试覆盖率

生成测试覆盖率报告：

```bash
go test -v ./biz/service -coverprofile=coverage.out -tags no_rocksdb
go tool cover -html=coverage.out
```
