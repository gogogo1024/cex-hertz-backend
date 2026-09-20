//go:build integration

package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/conf"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestOutboxDispatcherKafkaE2E(t *testing.T) {
	cfg := conf.GetConf()
	require.NotNil(t, cfg)
	require.NotEmpty(t, cfg.Postgres.DSN)
	require.NotEmpty(t, cfg.Kafka.Brokers)

	tradeTopic := cfg.Kafka.Topics["trade"]
	if tradeTopic == "" {
		tradeTopic = "trade"
	}

	db, err := gorm.Open(postgres.Open(cfg.Postgres.DSN), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.OutboxEntry{}))

	outboxRepo := pg.NewOutboxRepo(db)
	dispatcher := NewOutboxDispatcher(outboxRepo, 10, 3)

	eventID := fmt.Sprintf("trade-e2e-%d", time.Now().UnixNano())
	entry := &model.OutboxEntry{
		EventID:       eventID,
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
		Payload:       fmt.Sprintf(`{"event_id":"%s","event_type":"TradeExecuted","aggregate_id":"BTC/USDT"}`, eventID),
		Published:     false,
	}
	require.NoError(t, db.Create(entry).Error)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   cfg.Kafka.Brokers,
		Topic:     tradeTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// 在发送前把游标移动到当前末尾，仅消费本次测试新增的消息。
	require.NoError(t, reader.SetOffset(kafka.LastOffset))

	dispatcher.dispatchBatch(context.Background())

	var published model.OutboxEntry
	require.NoError(t, db.Where("event_id = ?", eventID).First(&published).Error)
	require.True(t, published.Published)
	require.NotNil(t, published.PublishedAt)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for {
		msg, readErr := reader.ReadMessage(ctx)
		require.NoError(t, readErr)

		if string(msg.Key) != eventID {
			continue
		}

		require.Contains(t, string(msg.Value), eventID)

		headers := map[string]string{}
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}

		require.Equal(t, eventID, headers["event_id"])
		require.Equal(t, "BTC/USDT", headers["aggregate_id"])
		require.True(t, strings.EqualFold("OrderBook", headers["aggregate_type"]))
		return
	}
}
