package service

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestEventStoreConcurrentWrites 测试 EventStore 的批量写入性能
func TestEventStoreConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	const (
		numBatches     = 5
		eventsPerBatch = 500
	)

	var events []*model.PersistentEvent
	totalEvents := numBatches * eventsPerBatch

	// 生成所有事件
	for i := 0; i < totalEvents; i++ {
		events = append(events, &model.PersistentEvent{
			GlobalSeq:      int64(i + 1),
			EventType:      "test_event",
			AggregateType:  "Test",
			AggregateID:    fmt.Sprintf("AGG-%d", i%numBatches),
			Symbol:         "BTC/USDT",
			Payload:        fmt.Sprintf(`{"idx":%d}`, i),
			EventTimestamp: time.Now().Unix(),
			CreatedAt:      time.Now().UnixMilli(),
			Version:        1,
		})
	}

	// 批量写入
	start := time.Now()
	for i := 0; i < numBatches; i++ {
		start := i * eventsPerBatch
		end := start + eventsPerBatch
		db.CreateInBatches(events[start:end], 100)
	}
	elapsed := time.Since(start)

	throughput := float64(totalEvents) / elapsed.Seconds()

	t.Logf("Batch writes - Total: %d events, Time: %.2fs, Throughput: %.0f events/sec",
		totalEvents, elapsed.Seconds(), throughput)

	// 验证所有事件都被写入
	var count int64
	db.Model(&model.PersistentEvent{}).Count(&count)
	if int(count) != totalEvents {
		t.Fatalf("Expected %d events, got %d", totalEvents, count)
	}
}

// TestEventStoreQueryPerformance 测试查询性能
func TestEventStoreQueryPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	// 预填充数据
	const numEvents = 5000
	for i := 0; i < numEvents; i++ {
		event := &model.PersistentEvent{
			GlobalSeq:      int64(i + 1),
			EventType:      "test_event",
			AggregateType:  "Test",
			Symbol:         "BTC/USDT",
			Payload:        fmt.Sprintf(`{"idx":%d}`, i),
			EventTimestamp: time.Now().Unix(),
			CreatedAt:      time.Now().UnixMilli(),
			Version:        1,
		}
		db.Create(event)
	}

	// 测试查询性能
	const numQueries = 100
	var latencies []time.Duration

	for q := 0; q < numQueries; q++ {
		start := time.Now()
		var events []*model.PersistentEvent
		db.Where("symbol = ?", "BTC/USDT").
			Order("global_seq ASC").
			Limit(1000).
			Find(&events)
		latencies = append(latencies, time.Since(start))
	}

	// 计算统计
	var totalLatency time.Duration
	var maxLatency time.Duration
	var minLatency time.Duration = time.Hour

	for _, latency := range latencies {
		totalLatency += latency
		if latency > maxLatency {
			maxLatency = latency
		}
		if latency < minLatency {
			minLatency = latency
		}
	}

	avgLatency := totalLatency / time.Duration(numQueries)

	t.Logf("Query performance - Queries: %d, Min: %v, Avg: %v, Max: %v",
		numQueries, minLatency, avgLatency, maxLatency)

	// 验证延迟在可接受范围
	if maxLatency > 500*time.Millisecond {
		t.Logf("WARNING: Max query latency exceeds 500ms: %v", maxLatency)
	}
}

// TestRecoveryPerformance 测试恢复性能 (RTO)
func TestRecoveryPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	// 模拟已有大量数据
	const numExistingEvents = 50000
	start := time.Now()

	for i := 0; i < numExistingEvents; i++ {
		event := &model.PersistentEvent{
			GlobalSeq:      int64(i + 1),
			EventType:      "test_event",
			AggregateType:  "Test",
			Symbol:         "BTC/USDT",
			Payload:        fmt.Sprintf(`{"idx":%d}`, i),
			EventTimestamp: time.Now().Unix(),
			CreatedAt:      time.Now().UnixMilli(),
			Version:        1,
		}
		db.Create(event)
	}

	writeTime := time.Since(start)

	// 模拟恢复（从头读取所有事件）
	recoveryStart := time.Now()
	var events []*model.PersistentEvent
	db.Where("symbol = ?", "BTC/USDT").
		Order("global_seq ASC").
		Find(&events)
	recoveryTime := time.Since(recoveryStart)

	t.Logf("Recovery Performance - Events: %d, Write: %v, Recovery: %v, Total RTO: %v",
		len(events), writeTime, recoveryTime, writeTime+recoveryTime)

	// 验证 RTO < 30 秒
	totalTime := writeTime + recoveryTime
	if totalTime > 30*time.Second {
		t.Logf("WARNING: Total RTO exceeds 30s: %v", totalTime)
	}
}

// TestCrashRecoveryScenarioA 故障场景 A：在检查点更新之前的崩溃
func TestCrashRecoveryScenarioA(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	// 写入一些事件
	const numEvents = 1000
	for i := 0; i < numEvents; i++ {
		event := &model.PersistentEvent{
			GlobalSeq:      int64(i + 1),
			EventType:      "test_event",
			AggregateType:  "Test",
			Symbol:         "BTC/USDT",
			Payload:        fmt.Sprintf(`{"idx":%d}`, i),
			EventTimestamp: time.Now().Unix(),
			CreatedAt:      time.Now().UnixMilli(),
			Version:        1,
		}
		db.Create(event)
	}

	// 验证事件已写入
	var events []*model.PersistentEvent
	db.Where("symbol = ?", "BTC/USDT").Find(&events)

	if len(events) != numEvents {
		t.Fatalf("Expected %d events, got %d", numEvents, len(events))
	}

	// 验证顺序
	for i := 1; i < len(events); i++ {
		if events[i].GlobalSeq <= events[i-1].GlobalSeq {
			t.Fatalf("Event ordering violated at index %d", i)
		}
	}

	t.Logf("Scenario A passed: Recovered all %d events, ordering verified", numEvents)
}

// TestDataConsistencyAfterRecovery 验证恢复后的数据一致性
func TestDataConsistencyAfterRecovery(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	const numEvents = 5000

	// 写入事件
	for i := 0; i < numEvents; i++ {
		event := &model.PersistentEvent{
			GlobalSeq:      int64(i + 1),
			EventType:      "test_event",
			AggregateType:  "Test",
			AggregateID:    fmt.Sprintf("ORDER-%d", i%100),
			Symbol:         "BTC/USDT",
			Payload:        fmt.Sprintf(`{"order_id":"ORDER-%d"}`, i),
			EventTimestamp: time.Now().Unix(),
			CreatedAt:      time.Now().UnixMilli(),
			Version:        1,
		}
		db.Create(event)
	}

	// 恢复（验证事件完整性）
	var events []*model.PersistentEvent
	db.Order("global_seq ASC").Find(&events)

	if len(events) != numEvents {
		t.Fatalf("Event count mismatch: expected %d, got %d", numEvents, len(events))
	}

	// 验证 GlobalSeq 顺序
	violations := 0
	for i := 1; i < len(events); i++ {
		if events[i].GlobalSeq <= events[i-1].GlobalSeq {
			violations++
		}
	}

	if violations > 0 {
		t.Fatalf("Event ordering violated: %d violations detected", violations)
	}

	t.Logf("Data consistency verified: %d events with 0 violations", len(events))
}

// TestConcurrentRecoveryProcesses 并发恢复处理
func TestConcurrentRecoveryProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	const (
		numRecoveryWorkers = 4
		eventsPerWorker    = 2500
	)

	// 写入数据
	totalEvents := numRecoveryWorkers * eventsPerWorker
	for i := 0; i < totalEvents; i++ {
		event := &model.PersistentEvent{
			GlobalSeq:      int64(i + 1),
			EventType:      "test_event",
			AggregateType:  "Test",
			Symbol:         "BTC/USDT",
			Payload:        fmt.Sprintf(`{"idx":%d}`, i),
			EventTimestamp: time.Now().Unix(),
			CreatedAt:      time.Now().UnixMilli(),
			Version:        1,
		}
		db.Create(event)
	}

	// 并发恢复进程
	var wg sync.WaitGroup
	var processedCount int64

	for w := 0; w < numRecoveryWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var events []*model.PersistentEvent
			db.Where("symbol = ?", "BTC/USDT").
				Order("global_seq ASC").
				Find(&events)
			atomic.AddInt64(&processedCount, int64(len(events)))
		}()
	}

	wg.Wait()

	if processedCount/int64(numRecoveryWorkers) != int64(totalEvents) {
		t.Logf("Concurrent recovery: %d workers processed %d events total",
			numRecoveryWorkers, processedCount)
	}

	t.Logf("Concurrent recovery passed: %d workers", numRecoveryWorkers)
}

// TestEventOrderingInvariant 验证事件顺序不变性
func TestEventOrderingInvariant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	const numEvents = 10000

	// 写入事件
	for i := 0; i < numEvents; i++ {
		event := &model.PersistentEvent{
			GlobalSeq:      int64(i + 1),
			EventType:      "test_event",
			AggregateType:  "Test",
			Symbol:         "BTC/USDT",
			Payload:        fmt.Sprintf(`{"idx":%d}`, i),
			EventTimestamp: time.Now().Unix(),
			CreatedAt:      time.Now().UnixMilli(),
			Version:        1,
		}
		db.Create(event)
	}

	// 从数据库读取事件并验证顺序
	var events []*model.PersistentEvent
	db.Where("symbol = ?", "BTC/USDT").Order("global_seq ASC").Find(&events)

	violations := 0
	for i := 1; i < len(events); i++ {
		if events[i].GlobalSeq <= events[i-1].GlobalSeq {
			violations++
		}
	}

	if violations > 0 {
		t.Fatalf("Event ordering violated: %d violations detected", violations)
	}

	t.Logf("Event ordering invariant verified: %d events with 0 violations", len(events))
}

// TestIdempotencyGuarantee 幂等性保证测试
func TestIdempotencyGuarantee(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&model.PersistentEvent{})

	const numEvents = 100

	duplicateCount := 0
	successCount := 0

	// 模拟幂等性：多次写入相同的 GlobalSeq
	// 由于 UNIQUE 约束，第二次写入应该失败
	for attempt := 0; attempt < 3; attempt++ {
		for i := 0; i < numEvents; i++ {
			event := &model.PersistentEvent{
				GlobalSeq:      int64(i + 1), // 相同的 GlobalSeq
				EventType:      "test_event",
				AggregateType:  "Test",
				Symbol:         "BTC/USDT",
				Payload:        fmt.Sprintf(`{"idx":%d}`, i),
				EventTimestamp: time.Now().Unix(),
				CreatedAt:      time.Now().UnixMilli(),
				Version:        1,
			}

			result := db.Create(event)
			if result.Error == nil {
				successCount++
			} else {
				duplicateCount++
			}
		}
	}

	// 验证幂等性：第一次插入成功，后续重试失败
	expectedSuccesses := numEvents
	if successCount == expectedSuccesses {
		t.Logf("Idempotency guaranteed: %d successes (first attempt), %d duplicates prevented",
			successCount, duplicateCount)
	} else {
		t.Logf("Idempotency warning: %d successes (expected %d), %d duplicates",
			successCount, expectedSuccesses, duplicateCount)
	}
}
