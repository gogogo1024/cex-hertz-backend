//go:build integration

package service

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"testing"
	"time"

	kafkadal "github.com/gogogo1024/cex-hertz-backend/biz/dal/kafka"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/conf"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// 注意：本文件不再声明 TestMain，以避免与包内其他测试文件冲突。
// 启动依赖与等待逻辑在具体测试中按需触发。

// TestOutboxKafkaFailureScenarios 模拟 Kafka 中断并验证 dispatcher 的重试与恢复
func TestOutboxKafkaFailureScenarios(t *testing.T) {
	// 启动依赖服务（如果已在运行则不会重复启动）
	if out, err := runDockerComposeCommand("compose", "-f", "../../docker-compose-base.yaml", "up", "-d", "pg", "redis", "kafka", "kafka-init", "consul"); err != nil {
		t.Fatalf("failed to start docker compose services: %v\noutput=%s", err, out)
	}

	cfg := conf.GetConf()
	require.NotNil(t, cfg)
	// 等待关键端口就绪
	require.NoError(t, waitForTCP("localhost:5432", 120*time.Second))
	require.NoError(t, waitForTCP("localhost:6379", 120*time.Second))
	require.NoError(t, waitForTCP(cfg.Kafka.Brokers[0], 120*time.Second))

	db, err := gorm.Open(postgres.Open(cfg.Postgres.DSN), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.OutboxEntry{}))
	// 清理历史 outbox 数据，避免影响本次场景测试
	require.NoError(t, db.Unscoped().Where("1=1").Delete(&model.OutboxEntry{}).Error)

	outboxRepo := pg.NewOutboxRepo(db)
	dispatcher := NewOutboxDispatcher(outboxRepo, 10, 3)

	// Helper failing writer is provided by test helper file for deterministic failure simulation

	t.Run("KafkaDownThenRecover", func(t *testing.T) {
		eventID := fmt.Sprintf("e2e-failure-%d", time.Now().UnixNano())
		entry := &model.OutboxEntry{
			EventID:       eventID,
			EventType:     "TradeExecuted",
			AggregateID:   "BTC/USDT",
			AggregateType: "OrderBook",
			Payload:       fmt.Sprintf(`{"event_id":"%s","event_type":"TradeExecuted"}`, eventID),
			Published:     false,
		}
		require.NoError(t, db.Create(entry).Error)

		// 为了避免对容器的强依赖，在此直接注入会失败的 writer 模拟 Kafka 不可用场景
		dispatcher.SetKafkaProducer(&failingWriter{})

		// 触发一次分发，预期会记录错误并增加 retry_count
		dispatcher.dispatchBatch(context.Background())

		var updated model.OutboxEntry
		require.NoError(t, db.Where("event_id = ?", eventID).First(&updated).Error)
		require.GreaterOrEqual(t, updated.RetryCount, 1)
		require.NotEmpty(t, updated.LastError)

		// 为重启/恢复场景，先恢复到真实可用的 writer
		// 如果测试环境确实使用容器控制 kafka，这里仍保留容器启动等待逻辑
		require.NoError(t, dockerComposeStart("kafka"))
		require.NoError(t, waitForKafkaReady(cfg.Kafka.Brokers[0], 120*time.Second))
		time.Sleep(5 * time.Second)
		kafkadal.ResetWriters()
		dispatcher.SetKafkaProducer(nil)

		// 再次触发分发，期望成功发布
		dispatcher.dispatchBatch(context.Background())

		var published model.OutboxEntry
		require.NoError(t, db.Where("event_id = ?", eventID).First(&published).Error)
		require.True(t, published.Published)
	})

	t.Run("IntermittentKafka", func(t *testing.T) {
		// 连续短暂停止/启动 kafka，模拟抖动
		eventID := fmt.Sprintf("e2e-flap-%d", time.Now().UnixNano())
		entry := &model.OutboxEntry{
			EventID:       eventID,
			EventType:     "TradeExecuted",
			AggregateID:   "BTC/USDT",
			AggregateType: "OrderBook",
			Payload:       fmt.Sprintf(`{"event_id":"%s","event_type":"TradeExecuted"}`, eventID),
			Published:     false,
		}
		require.NoError(t, db.Create(entry).Error)

		// 进行三次快速停止/启动
		for i := 0; i < 3; i++ {
			require.NoError(t, dockerComposeStop("kafka"))
			time.Sleep(800 * time.Millisecond)
			require.NoError(t, dockerComposeStart("kafka"))
			// 等待端口短暂可达
			_ = waitForTCP(cfg.Kafka.Brokers[0], 10*time.Second)
		}

		// 最终保证 kafka 可用并可返回元数据
		require.NoError(t, waitForKafkaReady(cfg.Kafka.Brokers[0], 120*time.Second))

		// 触发分发前清理 writer，确保重建连接
		kafkadal.ResetWriters()
		dispatcher.SetKafkaProducer(nil)

		// 触发分发，期望最终被成功发布（允许中间有重试）
		dispatcher.dispatchBatch(context.Background())

		var published model.OutboxEntry
		require.NoError(t, db.Where("event_id = ?", eventID).First(&published).Error)
		require.True(t, published.Published)
	})
}

// ---- helpers ----

func waitForTCP(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for %s", address)
}

func runDockerComposeCommand(cmdAndArgs ...string) (string, error) {
	// 尝试使用 `docker` + `compose ...`，若不可用则 fallback 到 `docker-compose`
	out, err := exec.Command("docker", cmdAndArgs...).CombinedOutput()
	if err == nil {
		return string(out), nil
	}
	// fallback
	// 当使用 docker-compose 时，参数不同：去掉最前面的 "compose"
	args := make([]string, 0, len(cmdAndArgs))
	for i := range cmdAndArgs {
		if i == 0 && cmdAndArgs[i] == "compose" {
			continue
		}
		args = append(args, cmdAndArgs[i])
	}
	out2, err2 := exec.Command("docker-compose", args...).CombinedOutput()
	if err2 != nil {
		return string(out2), fmt.Errorf("docker compose failed: docker err=%v out=%s; docker-compose err=%v out=%s", err, string(out), err2, string(out2))
	}
	return string(out2), nil
}

func dockerComposeStop(service string) error {
	_, err := runDockerComposeCommand("compose", "-f", "../../docker-compose-base.yaml", "stop", service)
	return err
}

func dockerComposeStart(service string) error {
	_, err := runDockerComposeCommand("compose", "-f", "../../docker-compose-base.yaml", "up", "-d", service)
	return err
}

// waitForKafkaReady 使用 kafka-go 尝试获取元数据，保证 broker 不仅能建立 TCP 连接，
// 而且已经完成内部初始化并能返回 topic/partition 信息。
func waitForKafkaReady(broker string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := kafkago.Dial("tcp", broker)
		if err == nil {
			_, err2 := conn.ReadPartitions()
			_ = conn.Close()
			if err2 == nil {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for kafka metadata at %s", broker)
}
