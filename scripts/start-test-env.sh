#!/bin/bash
# 启动测试环境
# 使用 docker-compose 启动所有依赖服务（PostgreSQL、Redis、Kafka、Consul）

set -e

echo "Starting test environment with docker-compose..."

# 检查 docker-compose 是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    exit 1
fi

# 使用现有的 docker-compose-base.yaml 启动服务
docker-compose -f docker-compose-base.yaml up -d

# 等待 PostgreSQL 健康检查通过
echo "Waiting for PostgreSQL to be ready..."
sleep 5
max_attempts=30
attempt=1
until docker-compose -f docker-compose-base.yaml exec -T pg pg_isready -U postgres > /dev/null 2>&1 || [ $attempt -eq $max_attempts ]; do
    echo "PostgreSQL is not ready yet... ($attempt/$max_attempts)"
    sleep 2
    ((attempt++))
done

if [ $attempt -eq $max_attempts ]; then
    echo "Error: PostgreSQL did not become ready in time"
    exit 1
fi

echo "✓ Test environment is ready!"
echo ""
echo "Service endpoints:"
echo "  PostgreSQL: localhost:5432 (user: postgres, password: postgres)"
echo "  Redis: localhost:6379"
echo "  Kafka: localhost:9092"
echo "  Consul: localhost:8500"
echo ""
echo "To run tests:"
echo "  go test -v ./biz/service -tags no_rocksdb"
echo ""
echo "To stop the test environment:"
echo "  docker-compose -f docker-compose-base.yaml down"
