package model

// 用户余额结构
// 可根据实际业务扩展字段
// 例如币种、冻结余额等

type Balance struct {
	ID     uint   `gorm:"primaryKey"`
	UserID string `gorm:"index;not null"`
	Asset  string `gorm:"index;not null"` // 币种
	Amount string `gorm:"not null"`       // 可用余额
	Frozen string `gorm:"not null"`       // 冻结余额
}

// 用户持仓结构
// 可根据实际业务扩展字段

type Position struct {
	ID     uint   `gorm:"primaryKey"`
	UserID string `gorm:"index;not null"`
	Symbol string `gorm:"index;not null"` // 交易对
	// 兼容双写阶段：保留旧列（字符串）并新增 bigint 列（纳单位）
	Volume   string `gorm:"column:volume;type:text;not null"`    // 旧列：持仓数量（字符串表示）
	AvgPrice string `gorm:"column:avg_price;type:text;not null"` // 旧列：持仓均价（字符串表示）

	// 新列：纳单位整数（用于消除浮点误差）
	VolumeBigint   QuantityInNano `gorm:"column:volume_bigint;type:bigint;default:0"`
	AvgPriceBigint PriceInNano    `gorm:"column:avg_price_bigint;type:bigint;default:0"`
}
