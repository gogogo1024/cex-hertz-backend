#!/usr/bin/env bash
set -euo pipefail
docker compose -f docker-compose-base.yaml up -d
echo "等待 Kafka/pg 就绪（手动观察或 30s）..."
sleep 10
gofmt -w .
go test -v -tags=integration ./biz/service -run TestOutboxKafkaFailureScenarios -timeout 15m