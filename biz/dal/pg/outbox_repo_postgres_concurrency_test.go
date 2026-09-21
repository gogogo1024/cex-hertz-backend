//go:build integration

package pg

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"time"
)

// TestWriteOutboxEntryIfNotExists_Concurrent_Postgres 并发插入同一 event_id，验证只有一次插入成功
func TestWriteOutboxEntryIfNotExists_Concurrent_Postgres(t *testing.T) {
	cfgDSN := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	db, err := gorm.Open(postgres.Open(cfgDSN), &gorm.Config{})
	require.NoError(t, err)

	require.NoError(t, db.AutoMigrate(&model.OutboxEntry{}))

	repo := NewOutboxRepo(db)
	eventID := "concurrent-test-" + time.Now().Format("20060102150405")

	// 并发 N 个 goroutine 都在事务内尝试插入同一 event
	const N = 8
	var wg sync.WaitGroup
	wg.Add(N)
	// 原子计数器记录实际有多少个 goroutine 返回 inserted=true
	var successCount int32

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			tx := db.Begin()
			entry := &model.OutboxEntry{
				EventID:       eventID,
				EventType:     "TradeExecuted",
				AggregateID:   "BTC/USDT",
				AggregateType: "OrderBook",
				Payload:       "{}",
			}
			inserted, err := repo.WriteOutboxEntryIfNotExists(tx, entry)
			if err != nil {
				_ = tx.Rollback().Error
				return
			}
			if inserted {
				atomic.AddInt32(&successCount, 1)
			}
			// 无论 inserted 与否，都提交事务（插入已由 ON CONFLICT 控制）
			_ = tx.Commit().Error
		}()
	}

	wg.Wait()

	// 精确断言：恰有一次写入被视为 inserted
	require.Equal(t, int32(1), atomic.LoadInt32(&successCount))

	// 验证数据库中只有一条对应的 outbox entry，且字段符合预期
	var saved model.OutboxEntry
	err = db.WithContext(context.Background()).Where("event_id = ?", eventID).First(&saved).Error
	require.NoError(t, err)
	require.Equal(t, eventID, saved.EventID)
	require.Equal(t, "TradeExecuted", saved.EventType)
	require.Equal(t, "BTC/USDT", saved.AggregateID)
	require.Equal(t, "OrderBook", saved.AggregateType)
}
