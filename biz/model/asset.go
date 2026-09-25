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
	// 使用整数纳单位表示以避免浮点误差（与 Order/事件模型统一）
	Volume   QuantityInNano `gorm:"column:volume;type:bigint;not null"`    // 持仓数量（纳单位）
	AvgPrice PriceInNano    `gorm:"column:avg_price;type:bigint;not null"` // 持仓均价（纳单位）
}
