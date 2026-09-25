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
		// 新持仓，双写（兼容旧列与新 bigint 列）
		pos = model.Position{
			UserID:      userID,
			Symbol:      symbol,
			Volume:      buyQty,
			AvgPrice:    buyPrice,
			VolumeStr:   buyQtyStr,
			AvgPriceStr: buyPriceStr,
		}
		return tx.Create(&pos).Error
	}
	if queryErr != nil {
		return queryErr
	}

	// 已有持仓，优先使用 bigint 列的值，若不存在则回退解析旧字符串列
	var oldQty model.QuantityInNano
	var oldAvg model.PriceInNano
	if pos.Volume != 0 {
		oldQty = pos.Volume
	} else {
		if pos.VolumeStr != "" {
			if q, err := model.ParseQuantity(pos.VolumeStr); err == nil {
				oldQty = q
			}
		}
	}
	if pos.AvgPrice != 0 {
		oldAvg = pos.AvgPrice
	} else {
		if pos.AvgPriceStr != "" {
			if p, err := model.ParsePrice(pos.AvgPriceStr); err == nil {
				oldAvg = p
			}
		}
	}
	// 计算新值（纳单位）
	oldQtyVal := oldQty
	oldAvgVal := oldAvg
	newQty := model.QuantityInNano(int64(oldQtyVal) + int64(buyQty))

	bigOldQty := new(big.Int).SetInt64(int64(oldQtyVal))
	bigOldAvg := new(big.Int).SetInt64(int64(oldAvgVal))
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

	newAvg := model.PriceInNano(newAvgBig.Int64())

	// 双写：更新 bigint 列和旧字符串列
	pos.Volume = newQty
	pos.AvgPrice = newAvg
	pos.VolumeStr = newQty.String()
	pos.AvgPriceStr = newAvg.String()
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

	// 优先使用 bigint 列，回退解析旧字符串列
	var oldQty model.QuantityInNano
	if pos.Volume != 0 {
		oldQty = pos.Volume
	} else if pos.VolumeStr != "" {
		if q, err := model.ParseQuantity(pos.VolumeStr); err == nil {
			oldQty = q
		}
	}

	if int64(oldQty) < int64(sellQty) {
		return fmt.Errorf("持仓不足")
	}
	newQty := model.QuantityInNano(int64(oldQty) - int64(sellQty))

	pos.Volume = newQty
	pos.VolumeStr = newQty.String()
	// 卖出后均价不变，除非持仓为 0，则重置均价
	if newQty == 0 {
		pos.AvgPrice = model.PriceInNano(0)
		pos.AvgPriceStr = "0"
	}
	return tx.Save(&pos).Error
}
