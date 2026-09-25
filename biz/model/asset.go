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
	// 兼容旧列（字符串）和新列（bigint 纳单位），当前处于双写阶段：
	// - `volume` / `avg_price` 保留旧字符串表示（兼容旧读）
	// - `volume_bigint` / `avg_price_bigint` 为新的整数纳单位表示
	VolumeStr   string         `gorm:"column:volume;type:text;not null"`             // 旧列：持仓数量（字符串）
	AvgPriceStr string         `gorm:"column:avg_price;type:text;not null"`          // 旧列：持仓均价（字符串）
	Volume      QuantityInNano `gorm:"column:volume_bigint;type:bigint;not null"`    // 新列：持仓数量（纳单位）
	AvgPrice    PriceInNano    `gorm:"column:avg_price_bigint;type:bigint;not null"` // 新列：持仓均价（纳单位）
}
