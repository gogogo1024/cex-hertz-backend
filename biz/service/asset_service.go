package service

import (
	"fmt"
	"math/big"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"

	"gorm.io/gorm"
)

func GetUserBalance(userID string) ([]model.Balance, error) {
	var balances []model.Balance
	err := pg.GormDB.Where("user_id = ?", userID).Find(&balances).Error
	return balances, err
}

func GetUserPositions(userID string) ([]model.Position, error) {
	var positions []model.Position
	err := pg.GormDB.Where("user_id = ?", userID).Find(&positions).Error
	return positions, err
}

func GetUserPositionBySymbol(userID, symbol string) (*model.Position, error) {
	var position model.Position
	err := pg.GormDB.Where("user_id = ? AND symbol = ?", userID, symbol).First(&position).Error
	return &position, err
}

// 买入持仓（加权均价）
func BuyPosition(userID, symbol, buyQty, buyPrice string) error {
	// 兼容旧接口：调用基于全局 DB 的事务版实现
	return BuyPositionTx(pg.GormDB, userID, symbol, buyQty, buyPrice)
}

// 卖出持仓
func SellPosition(userID, symbol, sellQty string) error {
	// 兼容旧接口：调用基于全局 DB 的事务版实现
	return SellPositionTx(pg.GormDB, userID, symbol, sellQty)
}

// BuyPositionTx 在给定事务/DB 对象上执行持仓买入逻辑（便于在事务中调用）
// 使用整数纳单位并用大整数计算加权均价以避免 overflow/浮点误差
func BuyPositionTx(tx *gorm.DB, userID, symbol, buyQtyStr, buyPriceStr string) error {
	var pos model.Position
	queryErr := tx.Where("user_id = ? AND symbol = ?", userID, symbol).First(&pos).Error

	buyQty, err := model.ParseQuantity(buyQtyStr)
	if err != nil {
		return err
	}
	buyPrice, err := model.ParsePrice(buyPriceStr)
	if err != nil {
		return err
	}

	if queryErr == gorm.ErrRecordNotFound {
		// 新持仓，直接创建（双写：旧列字符串 + 新列 bigint）
		pos = model.Position{
			UserID:         userID,
			Symbol:         symbol,
			Volume:         buyQty.String(),
			AvgPrice:       buyPrice.String(),
			VolumeBigint:   buyQty,
			AvgPriceBigint: buyPrice,
		}
		return tx.Create(&pos).Error
	}
	if queryErr != nil {
		return queryErr
	}

	// 已有持仓，计算加权均价：newAvg = (oldQty*oldAvg + buyQty*buyPrice) / (oldQty + buyQty)
	// 使用 bigint 字段计算并更新，同时保持旧列的字符串表现
	oldQty := pos.VolumeBigint
	oldAvg := pos.AvgPriceBigint
	newQty := model.QuantityInNano(int64(oldQty) + int64(buyQty))

	bigOldQty := new(big.Int).SetInt64(int64(oldQty))
	bigOldAvg := new(big.Int).SetInt64(int64(oldAvg))
	bigBuyQty := new(big.Int).SetInt64(int64(buyQty))
	bigBuyPrice := new(big.Int).SetInt64(int64(buyPrice))

	prod1 := new(big.Int).Mul(bigOldQty, bigOldAvg)
	prod2 := new(big.Int).Mul(bigBuyQty, bigBuyPrice)
	sum := new(big.Int).Add(prod1, prod2)
	bigNewQty := new(big.Int).SetInt64(int64(newQty))
	if bigNewQty.Sign() == 0 {
		return fmt.Errorf("new quantity is zero")
	}
	newAvgBig := new(big.Int).Div(sum, bigNewQty)
	if !newAvgBig.IsInt64() {
		return fmt.Errorf("price overflow when computing weighted average")
	}

	pos.VolumeBigint = newQty
	pos.AvgPriceBigint = model.PriceInNano(newAvgBig.Int64())
	// 更新旧列的字符串表示，保证兼容读取旧字段的代码
	pos.Volume = newQty.String()
	pos.AvgPrice = pos.AvgPriceBigint.String()
	return tx.Save(&pos).Error
}

// SellPositionTx 在给定事务/DB 对象上执行持仓卖出逻辑（便于在事务中调用）
// 使用整数纳单位
func SellPositionTx(tx *gorm.DB, userID, symbol, sellQtyStr string) error {
	var pos model.Position
	queryErr := tx.Where("user_id = ? AND symbol = ?", userID, symbol).First(&pos).Error
	if queryErr != nil {
		return queryErr
	}

	sellQty, err := model.ParseQuantity(sellQtyStr)
	if err != nil {
		return err
	}

	oldQty := pos.VolumeBigint
	if int64(oldQty) < int64(sellQty) {
		return fmt.Errorf("持仓不足")
	}
	newQty := model.QuantityInNano(int64(oldQty) - int64(sellQty))
	pos.VolumeBigint = newQty
	// 卖出后均价不变，除非持仓为 0，则重置均价
	if newQty == 0 {
		pos.AvgPriceBigint = model.PriceInNano(0)
		pos.AvgPrice = "0"
	} else {
		// 保持旧列字符串表示同步
		pos.AvgPrice = pos.AvgPriceBigint.String()
	}
	pos.Volume = newQty.String()
	return tx.Save(&pos).Error
}
