//go:build integration

package pg

import (
	"context"
	"sync"
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
				_ = tx.Commit().Error
			} else {
				_ = tx.Commit().Error
			}
		}()
	}

	wg.Wait()

	// 验证数据库中只有一条对应的 outbox entry
	var cnt int64
	err = db.WithContext(context.Background()).Model(&model.OutboxEntry{}).Where("event_id = ?", eventID).Count(&cnt).Error
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)
}
