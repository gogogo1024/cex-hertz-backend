package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// OutboxDispatcher 异步读取并发布 outbox 中的事件
// 确保 outbox pattern 的可靠性
type OutboxDispatcher struct {
	outboxRepo    *pg.OutboxRepo
	kafkaProducer interface{} // 真实实现中应该是 Kafka producer
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
	return &OutboxDispatcher{
		outboxRepo: outboxRepo,
		batchSize:  batchSize,
		maxRetries: maxRetries,
		stopCh:     make(chan bool),
	}
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

// publishToKafka 发布到 Kafka（占位符，实际应集成 Kafka producer）
func (od *OutboxDispatcher) publishToKafka(ctx context.Context, entry *model.OutboxEntry, payload interface{}) error {
	// 实际实现中应该：
	// 1. 使用 event_id 作为幂等键（Kafka ProducerConfig 设置 EnableIdempotence = true）
	// 2. 根据 aggregate_id 选择分区（确保同一聚合根的事件有序）
	// 3. 设置重试策略
	//
	// 示例：
	// msg := &sarama.ProducerMessage{
	//   Topic: "events-" + entry.AggregateType,
	//   Key:   sarama.StringEncoder(entry.AggregateID),
	//   Value: sarama.StringEncoder(entry.Payload),
	// }
	// _, _, err := od.kafkaProducer.SendMessage(msg)

	hlog.Debugf("[OutboxDispatcher] Publishing event %s to topic events-%s",
		entry.EventID, entry.AggregateType)

	// 模拟发布延迟
	time.Sleep(10 * time.Millisecond)

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
