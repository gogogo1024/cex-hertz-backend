//go:build integration

package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/conf"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// TestOutboxDispatcherPublishFailAndRetry 模拟 Kafka 不可用时，dispatcher 会记录错误并增加 retry_count
func TestOutboxDispatcherPublishFailAndRetry(t *testing.T) {
	cfg := conf.GetConf()
	require.NotNil(t, cfg)
	require.NotEmpty(t, cfg.Postgres.DSN)

	db, err := gorm.Open(postgres.Open(cfg.Postgres.DSN), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.OutboxEntry{}))

	outboxRepo := pg.NewOutboxRepo(db)
	dispatcher := NewOutboxDispatcher(outboxRepo, 10, 3)

	// 模拟 Kafka 不可用：注入一个始终返回错误的 writer，使测试不依赖外部容器状态
	dispatcher.SetKafkaProducer(&failingWriter{})

	eventID := fmt.Sprintf("trade-fail-%d", time.Now().UnixNano())
	entry := &model.OutboxEntry{
		EventID:       eventID,
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
		Payload:       fmt.Sprintf(`{"event_id":"%s","event_type":"TradeExecuted"}`, eventID),
		Published:     false,
	}
	require.NoError(t, db.Create(entry).Error)

	// 触发一次分发；当 Kafka 不可用时，应记录错误并增加 retry_count
	dispatcher.dispatchBatch(context.Background())

	var updated model.OutboxEntry
	require.NoError(t, db.Where("event_id = ?", eventID).First(&updated).Error)
	require.GreaterOrEqual(t, updated.RetryCount, 1)
	require.NotEmpty(t, updated.LastError)

	_ = updated // 避免 go vet 警告
}
