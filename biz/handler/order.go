package handler

import (
	"context"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/biz/service"
	"github.com/gogogo1024/cex-hertz-backend/util"
)

type SubmitOrderRequest struct {
	OrderID  string `json:"order_id"`
	Symbol   string `json:"symbol"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

type SubmitOrderResponse struct {
	Type    string `json:"type"`
	OrderID string `json:"order_id"`
	Symbol  string `json:"symbol"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// SubmitOrder RESTful 下单接口
func SubmitOrder(ctx context.Context, c *app.RequestContext) {
	var req model.Order
	if err := c.BindAndValidate(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	// Price 和 Quantity 现在是 int64，检查是否为 0（int64 的零值）
	if req.Symbol == "" || req.Side == "" || req.Price == 0 || req.Quantity == 0 || req.UserID == "" {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "missing required fields"})
		return
	}
	// 后端生成 OrderID
	id, err := util.GenerateOrderID()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": "failed to generate order_id"})
		return
	}
	req.OrderID = fmt.Sprintf("%d", id)
	req.Status = "active"
	req.UpdatedAt = req.CreatedAt
	// 如果全局撮合引擎存在，则将请求转发到撮合引擎（优先走引擎路径）
	if service.GlobalMatchEngine != nil {
		// 将内部表示转换为 SubmitOrderMsg（price/quantity 格式化为字符串）
		priceStr := fmt.Sprintf("%.8f", float64(req.Price)/1e8)
		qtyStr := fmt.Sprintf("%.8f", float64(req.Quantity)/1e8)
		orderMsg := model.SubmitOrderMsg{
			OrderID:  req.OrderID,
			Symbol:   req.Symbol,
			Side:     req.Side,
			Price:    priceStr,
			Quantity: qtyStr,
			UserID:   req.UserID,
		}
		if err := service.GlobalMatchEngine.SubmitOrder(orderMsg); err != nil {
			if err == service.ErrSymbolNotLocallyOwned {
				c.JSON(consts.StatusConflict, map[string]interface{}{"error": "symbol not owned by this node"})
				return
			}
			c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
			return
		}
		c.JSON(consts.StatusOK, map[string]interface{}{"order_id": req.OrderID, "status": "submitted"})
		return
	}

	// 回退到直接写入数据库的旧逻辑（当撮合引擎不可用或未注入时）
	if err := service.CreateOrder(&req); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, map[string]interface{}{"order_id": req.OrderID, "status": "received"})
}

// GetOrder 查询单个订单
func GetOrder(ctx context.Context, c *app.RequestContext) {
	orderID := c.Param("id")
	order, err := pg.GetOrderByID(orderID)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]interface{}{"error": "order not found"})
		return
	}
	c.JSON(consts.StatusOK, order)
}

// ListOrders 查询订单列表
func ListOrders(ctx context.Context, c *app.RequestContext) {
	userID := string(c.Query("user_id"))
	status := string(c.Query("status"))
	orders, err := service.ListOrders(userID, status)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, orders)
}

// CancelOrder 取消订单
func CancelOrder(ctx context.Context, c *app.RequestContext) {
	type CancelOrderRequest struct {
		OrderID string `json:"order_id"`
		UserID  string `json:"user_id"`
	}
	var req CancelOrderRequest
	if err := c.BindAndValidate(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "invalid request"})
		return
	}
	order, err := pg.GetOrderByID(req.OrderID)
	if err != nil || order.UserID != req.UserID {
		c.JSON(consts.StatusNotFound, map[string]interface{}{"error": "order not found or user mismatch"})
		return
	}
	if err := service.UpdateOrderStatus(req.OrderID, "cancelled"); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, map[string]interface{}{"order_id": req.OrderID, "status": "cancelled"})
}

// 查询成交记录（GORM）
func ListTrades(ctx context.Context, c *app.RequestContext) {
	symbol := string(c.Query("symbol"))
	limit := 50
	if l := c.Query("limit"); len(l) > 0 {
		fmt.Sscanf(string(l), "%d", &limit)
	}
	trades, err := pg.ListTrades(symbol, limit)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	c.JSON(consts.StatusOK, trades)
}
