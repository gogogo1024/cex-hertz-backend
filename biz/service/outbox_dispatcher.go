package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	kafkadal "github.com/gogogo1024/cex-hertz-backend/biz/dal/kafka"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/conf"
	kafkago "github.com/segmentio/kafka-go"
)

// OutboxDispatcher 异步读取并发布 outbox 中的事件
// 确保 outbox pattern 的可靠性
// 使用 kafkadal.MessageSender 作为通用发送者接口
// 保留本文件内部使用的别名以减少变更范围
type kafkaMessageSender = kafkadal.MessageSender

type OutboxDispatcher struct {
	outboxRepo    *pg.OutboxRepo
	kafkaProducer kafkaMessageSender
	batchSize     int
	maxRetries    int
	ticker        *time.Ticker
	stopCh        chan bool
	isRunning     bool
}

// NewOutboxDispatcher 创建 outbox 分发器
func NewOutboxDispatcher(
	outboxRepo *pg.OutboxRepo,
	batchSize int,
	maxRetries int,
) *OutboxDispatcher {
	od := &OutboxDispatcher{
		outboxRepo: outboxRepo,
		batchSize:  batchSize,
		maxRetries: maxRetries,
		stopCh:     make(chan bool),
	}
	return od
}

func (od *OutboxDispatcher) SetKafkaProducer(writer kafkaMessageSender) {
	od.kafkaProducer = writer
}

func (od *OutboxDispatcher) defaultTopic() string {
	cfg := conf.GetConf()
	if cfg == nil || len(cfg.Kafka.Topics) == 0 {
		return "trade"
	}
	if topic, ok := cfg.Kafka.Topics["trade"]; ok {
		return topic
	}
	for _, topic := range cfg.Kafka.Topics {
		return topic
	}
	return "trade"
}

func (od *OutboxDispatcher) resolveTopic(entry *model.OutboxEntry) string {
	if entry == nil {
		return od.defaultTopic()
	}
	cfg := conf.GetConf()
	if cfg != nil {
		switch strings.ToLower(entry.EventType) {
		case "tradeexecuted", "trade_executed":
			if topic, ok := cfg.Kafka.Topics["trade"]; ok {
				return topic
			}
		case "positionupdated", "position_updated":
			if topic, ok := cfg.Kafka.Topics["order"]; ok {
				return topic
			}
		}
		if entry.AggregateType != "" {
			for key, topic := range cfg.Kafka.Topics {
				if strings.EqualFold(key, strings.ToLower(entry.AggregateType)) || strings.EqualFold(strings.ToLower(key), strings.ToLower(entry.AggregateType)) {
					return topic
				}
			}
		}
	}
	return od.defaultTopic()
}

// Start 启动 outbox dispatcher（异步）
// 定期读取未发布的条目并发送
func (od *OutboxDispatcher) Start(ctx context.Context, interval time.Duration) {
	if od.isRunning {
		hlog.Warnf("[OutboxDispatcher] Already running")
		return
	}

	od.isRunning = true
	od.ticker = time.NewTicker(interval)

	go func() {
		hlog.Infof("[OutboxDispatcher] Started (interval: %v, batchSize: %d)", interval, od.batchSize)

		for {
			select {
			case <-od.stopCh:
				od.ticker.Stop()
				od.isRunning = false
				hlog.Infof("[OutboxDispatcher] Stopped")
				return
			case <-od.ticker.C:
				od.dispatchBatch(ctx)
			}
		}
	}()
}

// Stop 停止 outbox dispatcher
func (od *OutboxDispatcher) Stop() {
	if od.isRunning {
		od.stopCh <- true
	}
}

// dispatchBatch 分发一批未发布的事件
func (od *OutboxDispatcher) dispatchBatch(ctx context.Context) {
	entries, err := od.outboxRepo.GetUnpublished(ctx, od.batchSize)
	if err != nil {
		hlog.Errorf("[OutboxDispatcher] Failed to get unpublished entries: %v", err)
		return
	}

	if len(entries) == 0 {
		return
	}

	hlog.Infof("[OutboxDispatcher] Processing %d entries", len(entries))

	successfulEventIDs := []string{}
	for _, entry := range entries {
		if err := od.publishEntry(ctx, entry); err != nil {
			hlog.Errorf("[OutboxDispatcher] Failed to publish entry %s: %v", entry.EventID, err)
			if err := od.outboxRepo.RecordPublishError(ctx, entry.EventID, err.Error()); err != nil {
				hlog.Errorf("[OutboxDispatcher] Failed to record error: %v", err)
			}
		} else {
			successfulEventIDs = append(successfulEventIDs, entry.EventID)
		}
	}

	// 批量标记为已发布
	if len(successfulEventIDs) > 0 {
		if err := od.outboxRepo.MarkPublishedBatch(ctx, successfulEventIDs); err != nil {
			hlog.Errorf("[OutboxDispatcher] Failed to mark entries as published: %v", err)
		} else {
			hlog.Infof("[OutboxDispatcher] Published %d entries", len(successfulEventIDs))
		}
	}
}

// publishEntry 发布单个条目
// 在实际应用中，应该根据 event_type 和 aggregate_type 选择正确的发布目标
func (od *OutboxDispatcher) publishEntry(ctx context.Context, entry *model.OutboxEntry) error {
	// 解析 payload
	var event interface{}
	if err := json.Unmarshal([]byte(entry.Payload), &event); err != nil {
		hlog.Errorf("[OutboxDispatcher] Failed to unmarshal payload: %v", err)
		return err
	}

	// 根据事件类型和聚合根类型决定发送目标
	// 示例：发送到 Kafka 或其他消息队列
	switch entry.EventType {
	case "TradeExecuted":
		return od.publishToKafka(ctx, entry, event)
	case "PositionUpdated":
		return od.publishToKafka(ctx, entry, event)
	default:
		hlog.Warnf("[OutboxDispatcher] Unknown event type: %s", entry.EventType)
		return nil
	}
}

// publishToKafka 发布到 Kafka，使用 event_id 作为消息 key，保证同一事件的幂等投递。
func (od *OutboxDispatcher) publishToKafka(ctx context.Context, entry *model.OutboxEntry, payload interface{}) error {
	topic := od.resolveTopic(entry)

	// Select producer:
	// - If a custom producer was injected via SetKafkaProducer and it is NOT a *kafkago.Writer,
	//   use it for all topics (tests inject mocks this way).
	// - Otherwise, use per-topic writers from kafkadal.GetWriter(topic).
	var producer kafkaMessageSender
	if od.kafkaProducer != nil {
		if _, isWriter := od.kafkaProducer.(*kafkago.Writer); isWriter {
			// prefer per-topic writer even if kafkaProducer holds a default writer
			producer = kafkadal.GetWriter(topic)
		} else {
			// custom producer (mock) — use it for all topics
			producer = od.kafkaProducer
		}
	} else {
		producer = kafkadal.GetWriter(topic)
	}

	if producer == nil {
		return fmt.Errorf("kafka producer not configured for topic %s", topic)
	}

	msgBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload for event %s: %w", entry.EventID, err)
	}

	msg := kafkago.Message{
		Key:   []byte(entry.EventID),
		Value: msgBytes,
		Headers: []kafkago.Header{
			{Key: "event_id", Value: []byte(entry.EventID)},
			{Key: "aggregate_id", Value: []byte(entry.AggregateID)},
			{Key: "aggregate_type", Value: []byte(entry.AggregateType)},
		},
	}

	// If underlying producer is not a kafka.Writer or the writer has no Topic set,
	// we set the message Topic explicitly. kafkago.Writer will ignore Message.Topic
	// when its own Topic is set.
	if w, ok := producer.(*kafkago.Writer); !ok || w.Topic == "" {
		msg.Topic = topic
	}

	hlog.Infof("[OutboxDispatcher] Publishing event %s to Kafka topic=%s key=%s",
		entry.EventID, topic, entry.EventID)

	if err := producer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write message to kafka topic %s: %w", topic, err)
	}

	return nil
}

// CleanupPublished 清理已发布的旧条目（定期运行）
func (od *OutboxDispatcher) CleanupPublished(ctx context.Context, retentionDays int) error {
	rowsAffected, err := od.outboxRepo.CleanPublished(ctx, retentionDays)
	if err != nil {
		hlog.Errorf("[OutboxDispatcher] Failed to cleanup: %v", err)
		return err
	}
	hlog.Infof("[OutboxDispatcher] Cleaned up %d entries", rowsAffected)
	return nil
}
