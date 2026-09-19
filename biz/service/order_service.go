package service

import (
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// MatchResultPusher 用于推送撮合结果给客户端的回调函数
// 在主程序中由 cexserver.PushMatchResult 赋值
var MatchResultPusher func(msgType string, symbol string, orderID string, price string, quantity string, status string, ts int64)

// InsertOrder 业务层只做聚合和编排，所有数据操作通过pg.order_repo.go
func InsertOrder(orderID, userID, symbol, side, price, quantity, status string, createdAt, updatedAt int64) error {
	// 将字符串价格和数量转换为 int64
	priceInNano, err := model.ParsePrice(price)
	if err != nil {
		return err
	}
	quantityInNano, err := model.ParseQuantity(quantity)
	if err != nil {
		return err
	}

	order := &model.Order{
		OrderID:   orderID,
		UserID:    userID,
		Symbol:    symbol,
		Side:      side,
		Price:     int64(priceInNano),
		Quantity:  int64(quantityInNano),
		Status:    status,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	return pg.InsertOrder(order)
}

func ListOrders(userID, status string) ([]model.Order, error) {
	return pg.ListOrders(userID, status)
}

func GetOrderByID(orderID string) (*model.Order, error) {
	return pg.GetOrderByID(orderID)
}

func CreateOrder(order *model.Order) error {
	return pg.CreateOrder(order)
}

func UpdateOrderStatus(orderID, status string) error {
	return pg.UpdateOrderStatus(orderID, status)
}
