package service_test

import (
	"testing"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	servicepkg "github.com/gogogo1024/cex-hertz-backend/biz/service"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestPositionProcessor_TransactionalRollback 验证在事务中 position update 失败时 outbox 与 position 都会回滚
func TestPositionProcessor_TransactionalRollback(t *testing.T) {
	// 使用内存 sqlite 模拟 DB 事务回滚
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	// 迁移持仓与 outbox 表
	require.NoError(t, db.AutoMigrate(&model.OutboxEntry{}))

	// 预创建一个 position 表模型用于买入/卖出操作
	// 这里复用 biz/service 中实际的 Buy/Sell 实现签名：func(tx *gorm.DB, userID, symbol, quantity, price string) error

	// 简单的 position 表（内联定义）
	type Position struct {
		ID       int64  `gorm:"primaryKey"`
		UserID   string `gorm:"index"`
		Symbol   string `gorm:"index"`
		Volume   string
		AvgPrice string
	}

	require.NoError(t, db.AutoMigrate(&Position{}))

	outboxRepo := pg.NewOutboxRepo(db)

	// buyPositionFn 成功路径：写入或更新 position
	buyFn := func(tx *gorm.DB, userID, symbol, quantity, price string) error {
		// 如果没有事务传入，使用 db
		if tx == nil {
			tx = db
		}
		// 插入或更新简单逻辑：如果不存在则创建
		var pos Position
		if err := tx.Where("user_id = ? AND symbol = ?", userID, symbol).First(&pos).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				pos = Position{UserID: userID, Symbol: symbol, Volume: quantity, AvgPrice: price}
				return tx.Create(&pos).Error
			}
			return err
		}
		// 简化：直接覆盖
		pos.Volume = quantity
		pos.AvgPrice = price
		return tx.Save(&pos).Error
	}

	// sellPositionFn 故意返回错误以触发回滚
	sellFn := func(tx *gorm.DB, userID, symbol, quantity string) error {
		if tx == nil {
			tx = db
		}
		// 人为触发错误
		return gorm.ErrInvalidTransaction
	}

	pp := servicepkg.NewPositionProcessorWithOutbox(db, outboxRepo, buyFn, sellFn)

	// 构造一个模拟成交事件：taker=buyer, maker=seller
	e := &model.TradeExecutedEvent{
		TradeID:   "tx-test-1",
		TakerUser: "user-taker",
		MakerUser: "user-maker",
		Quantity:  100000000,  // 1.0 (in nano)
		Price:     2000000000, // 20.0 (in nano)
		TakerSide: "buy",
		Timestamp: time.Now().UnixMilli(),
	}

	// 执行并期望失败（事务回滚）
	err = pp.ProcessEvent(e)
	require.Error(t, err, "expected transaction to fail due to sell failure")

	// 验证 position 没有被写入
	var cnt int64
	_ = db.Model(&Position{}).Where("user_id = ?", "user-taker").Count(&cnt)
	require.Equal(t, int64(0), cnt)

	// 验证 outbox 没有写入（应该回滚）
	var outCnt int64
	_ = db.Model(&model.OutboxEntry{}).Where("event_id = ?", e.TradeID).Count(&outCnt)
	require.Equal(t, int64(0), outCnt)
}
