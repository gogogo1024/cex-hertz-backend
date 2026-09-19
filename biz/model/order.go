package model

import (
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

// ParsePrice 将字符串价格转换为 PriceInNano (int64)
// 例如："65123.45" → 65123450000000 (乘以 10^8)
func ParsePrice(priceStr string) (PriceInNano, error) {
	priceStr = strings.TrimSpace(priceStr)
	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid price format: %s", priceStr)
	}
	return PriceInNano(int64(price * 1e8)), nil
}

// ParseQuantity 将字符串数量转换为 QuantityInNano (int64)
// 例如："0.12345678" → 12345678 (乘以 10^8，假设基础单位是 10^-8)
func ParseQuantity(qtyStr string) (QuantityInNano, error) {
	qtyStr = strings.TrimSpace(qtyStr)
	qty, err := strconv.ParseFloat(qtyStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid quantity format: %s", qtyStr)
	}
	return QuantityInNano(int64(qty * 1e8)), nil
}

// SubmitOrderMsg 订单结构体（API 接收格式，使用字符串表示价格/数量）
type SubmitOrderMsg struct {
	OrderID  string
	Symbol   string
	Side     string
	Price    string // API 输入格式
	Quantity string // API 输入格式
	UserID   string
}

// ToInternal 将外部格式转换为内部格式（int64）
func (s *SubmitOrderMsg) ToInternal() (*SubmitOrderMsgInternal, error) {
	price, err := ParsePrice(s.Price)
	if err != nil {
		return nil, err
	}
	quantity, err := ParseQuantity(s.Quantity)
	if err != nil {
		return nil, err
	}
	return &SubmitOrderMsgInternal{
		OrderID:  s.OrderID,
		Symbol:   s.Symbol,
		Side:     s.Side,
		Price:    price,
		Quantity: quantity,
		UserID:   s.UserID,
	}, nil
}

// SubmitOrderMsgInternal 订单内部表示（使用 int64）
type SubmitOrderMsgInternal struct {
	OrderID  string
	Symbol   string
	Side     string
	Price    PriceInNano
	Quantity QuantityInNano
	UserID   string
}

// Order 订单数据库模型（GORM）
type Order struct {
	OrderID   string         `gorm:"primaryKey;column:order_id" json:"order_id"`
	UserID    string         `gorm:"column:user_id" json:"user_id"`
	Symbol    string         `gorm:"column:symbol" json:"symbol"`
	Side      string         `gorm:"column:side" json:"side"`
	Price     int64          `gorm:"column:price;type:bigint" json:"price"`       // 内部存储（int64）
	Quantity  int64          `gorm:"column:quantity;type:bigint" json:"quantity"` // 内部存储（int64）
	Status    string         `gorm:"column:status" json:"status"`
	CreatedAt int64          `gorm:"column:created_at" json:"created_at"`
	UpdatedAt int64          `gorm:"column:updated_at" json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

func (Order) TableName() string {
	return "orders"
}
