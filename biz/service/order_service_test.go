package service

import (
	"os"
	"testing"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

func TestMain(m *testing.M) {
	// 初始化数据库连接
	if err := pg.InitGorm(); err != nil {
		panic("GORM DB 初始化失败: " + err.Error())
	}
	if err := pg.AutoMigrate(); err != nil {
		panic("GORM 自动迁移失败: " + err.Error())
	}

	// 清理测试数据（删除测试用的 order ID，使用 Unscoped 硬删除）
	testOrderIDs := []string{"order1", "order2"}
	for _, orderID := range testOrderIDs {
		pg.GormDB.Unscoped().Where("order_id = ?", orderID).Delete(&model.Order{})
	}

	code := m.Run()
	os.Exit(code)
}

func TestInsertOrder(t *testing.T) {
	err := InsertOrder("order1", "user1", "BTCUSDT", "buy", "10000", "1", "open", 1620000000, 1620000000)
	if err != nil {
		t.Errorf("InsertOrder failed: %v", err)
	}
}

func TestListOrders(t *testing.T) {
	orders, err := ListOrders("user1", "open")
	if err != nil {
		t.Errorf("ListOrders failed: %v", err)
	}
	if len(orders) == 0 {
		t.Log("ListOrders returned 0 orders (may be expected if db is empty)")
	}
}

func TestGetOrderByID(t *testing.T) {
	order, err := GetOrderByID("order1")
	if err != nil {
		t.Errorf("GetOrderByID failed: %v", err)
	}
	if order != nil && order.OrderID != "order1" {
		t.Errorf("GetOrderByID returned wrong order: %+v", order)
	}
}

func TestCreateOrder(t *testing.T) {
	price, _ := model.ParsePrice("20000")
	quantity, _ := model.ParseQuantity("2")
	order := &model.Order{
		OrderID:   "order2",
		UserID:    "user2",
		Symbol:    "BTCUSDT",
		Side:      "sell",
		Price:     int64(price),
		Quantity:  int64(quantity),
		Status:    "open",
		CreatedAt: 1620000001,
		UpdatedAt: 1620000001,
	}
	err := CreateOrder(order)
	if err != nil {
		t.Errorf("CreateOrder failed: %v", err)
	}
}

func TestUpdateOrderStatus(t *testing.T) {
	err := UpdateOrderStatus("order1", "closed")
	if err != nil {
		t.Errorf("UpdateOrderStatus failed: %v", err)
	}
}
