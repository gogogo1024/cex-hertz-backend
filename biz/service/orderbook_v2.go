package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/huandu/skiplist"
)

// OrderBookV2 - 改进的 OrderBook 实现
// 特点：
// 1. 使用 int64 精确表示价格和数量（避免浮点误差）
// 2. 生成事件而不是直接修改状态
// 3. 支持完整的深度聚合
// 4. 确定性撮合（便于回放）
type OrderBookV2 struct {
	symbol string

	// 买单：价格降序
	buys *skiplist.SkipList

	// 卖单：价格升序
	sells *skiplist.SkipList

	// 追踪所有订单（用于处理取消和修改）
	// order_id -> order
	orders map[string]*OrderBookEntry

	// 保护并发访问
	mu sync.RWMutex

	// 事件序列生成器
	sequencer *Sequencer
}

// OrderBookEntry 订单簿中的订单
type OrderBookEntry struct {
	OrderID     string
	UserID      string
	Symbol      string
	Side        string // "buy" or "sell"
	Price       model.PriceInNano
	Quantity    model.QuantityInNano
	FilledQty   model.QuantityInNano // 已成交数量
	SubmitTime  int64                // 提交时间，用于时间优先级
	SequenceNum uint64               // 订单序列号，用于决定优先级
}

// PriceComparator 用于 skiplist 的价格比较器（降序）
type PriceComparatorDesc struct{}

func (c PriceComparatorDesc) Compare(a, b interface{}) int {
	priceA := a.(model.PriceInNano)
	priceB := b.(model.PriceInNano)
	if priceA > priceB {
		return -1
	} else if priceA < priceB {
		return 1
	}
	return 0
}

func (c PriceComparatorDesc) CalcScore(element interface{}) float64 {
	return float64(element.(model.PriceInNano))
}

// PriceComparatorAsc 升序比较器
type PriceComparatorAsc struct{}

func (c PriceComparatorAsc) Compare(a, b interface{}) int {
	priceA := a.(model.PriceInNano)
	priceB := b.(model.PriceInNano)
	if priceA < priceB {
		return -1
	} else if priceA > priceB {
		return 1
	}
	return 0
}

func (c PriceComparatorAsc) CalcScore(element interface{}) float64 {
	return -float64(element.(model.PriceInNano))
}

// NewOrderBook 创建一个新的 OrderBook
func NewOrderBook(symbol string, sequencer *Sequencer) *OrderBookV2 {
	return &OrderBookV2{
		symbol:    symbol,
		buys:      skiplist.New(PriceComparatorDesc{}),
		sells:     skiplist.New(PriceComparatorAsc{}),
		orders:    make(map[string]*OrderBookEntry),
		sequencer: sequencer,
	}
}

// MatchOrder 撮合订单，返回生成的事件列表
func (ob *OrderBookV2) MatchOrder(order *OrderBookEntry) ([]model.MatchingEngineEvent, model.QuantityInNano) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var events []model.MatchingEngineEvent
	remainingQty := order.Quantity

	if order.Side == "buy" {
		// 买单：与卖单撮合
		events = ob.matchBuyOrder(order, &remainingQty)
	} else {
		// 卖单：与买单撮合
		events = ob.matchSellOrder(order, &remainingQty)
	}

	// 如果还有剩余，加入 OrderBook
	if remainingQty > 0 {
		ob.addOrderToBook(order, remainingQty)
	}

	// 记录订单
	ob.orders[order.OrderID] = order

	return events, remainingQty
}

// matchBuyOrder 买单撮合
func (ob *OrderBookV2) matchBuyOrder(order *OrderBookEntry, remainingQty *model.QuantityInNano) []model.MatchingEngineEvent {
	var events []model.MatchingEngineEvent

	for *remainingQty > 0 && ob.sells.Len() > 0 {
		// 获取最低卖价
		elem := ob.sells.Front()
		if elem == nil {
			break
		}

		sellPrice := elem.Key().(model.PriceInNano)

		// 买价低于最低卖价，停止撮合
		if order.Price < sellPrice {
			break
		}

		// 获取该价格的卖单队列
		sellQueue := elem.Value.([]*OrderBookEntry)
		if len(sellQueue) == 0 {
			break
		}

		// 取最早的卖单（时间优先级）
		seller := sellQueue[0]

		// 计算本次成交数量
		tradeQty := model.Min(*remainingQty, seller.Quantity-seller.FilledQty)
		if tradeQty <= 0 {
			sellQueue = sellQueue[1:] // 移除已满成交的订单
			if len(sellQueue) > 0 {
				ob.sells.Set(sellPrice, sellQueue)
			} else {
				ob.sells.Remove(sellPrice)
			}
			continue
		}

		// 生成成交事件
		tradeID := fmt.Sprintf("%s-%s-%d", ob.symbol, order.OrderID, len(events))
		event := &model.TradeExecutedEvent{
			Seq:          ob.sequencer.NextGlobalSeq(),
			Timestamp:    time.Now().UnixMilli(),
			TradeID:      tradeID,
			SymbolStr:    ob.symbol,
			TakerOrderID: order.OrderID,
			MakerOrderID: seller.OrderID,
			TakerUser:    order.UserID,
			MakerUser:    seller.UserID,
			Price:        order.Price, // 以 maker 的报价成交
			Quantity:     tradeQty,
			TakerSide:    "buy",
		}
		events = append(events, event)

		// 更新成交数量
		*remainingQty -= tradeQty
		seller.FilledQty += tradeQty

		// 如果卖方订单已完全成交，移除
		if seller.FilledQty >= seller.Quantity {
			sellQueue = sellQueue[1:]
			if len(sellQueue) == 0 {
				ob.sells.Remove(sellPrice)
			} else {
				ob.sells.Set(sellPrice, sellQueue)
			}
		}
	}

	return events
}

// matchSellOrder 卖单撮合
func (ob *OrderBookV2) matchSellOrder(order *OrderBookEntry, remainingQty *model.QuantityInNano) []model.MatchingEngineEvent {
	var events []model.MatchingEngineEvent

	for *remainingQty > 0 && ob.buys.Len() > 0 {
		// 获取最高买价
		elem := ob.buys.Front()
		if elem == nil {
			break
		}

		buyPrice := elem.Key().(model.PriceInNano)

		// 卖价高于最高买价，停止撮合
		if order.Price > buyPrice {
			break
		}

		// 获取该价格的买单队列
		buyQueue := elem.Value.([]*OrderBookEntry)
		if len(buyQueue) == 0 {
			break
		}

		// 取最早的买单（时间优先级）
		buyer := buyQueue[0]

		// 计算本次成交数量
		tradeQty := model.Min(*remainingQty, buyer.Quantity-buyer.FilledQty)
		if tradeQty <= 0 {
			buyQueue = buyQueue[1:]
			if len(buyQueue) > 0 {
				ob.buys.Set(buyPrice, buyQueue)
			} else {
				ob.buys.Remove(buyPrice)
			}
			continue
		}

		// 生成成交事件
		tradeID := fmt.Sprintf("%s-%s-%d", ob.symbol, order.OrderID, len(events))
		event := &model.TradeExecutedEvent{
			Seq:          ob.sequencer.NextGlobalSeq(),
			Timestamp:    time.Now().UnixMilli(),
			TradeID:      tradeID,
			SymbolStr:    ob.symbol,
			TakerOrderID: order.OrderID,
			MakerOrderID: buyer.OrderID,
			TakerUser:    order.UserID,
			MakerUser:    buyer.UserID,
			Price:        buyPrice, // 以 maker 的报价成交
			Quantity:     tradeQty,
			TakerSide:    "sell",
		}
		events = append(events, event)

		// 更新成交数量
		*remainingQty -= tradeQty
		buyer.FilledQty += tradeQty

		// 如果买方订单已完全成交，移除
		if buyer.FilledQty >= buyer.Quantity {
			buyQueue = buyQueue[1:]
			if len(buyQueue) == 0 {
				ob.buys.Remove(buyPrice)
			} else {
				ob.buys.Set(buyPrice, buyQueue)
			}
		}
	}

	return events
}

// addOrderToBook 将订单加入 OrderBook
func (ob *OrderBookV2) addOrderToBook(order *OrderBookEntry, qty model.QuantityInNano) {
	var book *skiplist.SkipList
	if order.Side == "buy" {
		book = ob.buys
	} else {
		book = ob.sells
	}

	// 获取现有的订单队列
	elem := book.Get(order.Price)
	var queue []*OrderBookEntry
	if elem != nil {
		queue = elem.Value.([]*OrderBookEntry)
	}

	// 创建新的订单条目（带有剩余数量）
	entry := *order
	entry.Quantity = qty
	entry.FilledQty = 0

	// 追加到队列
	queue = append(queue, &entry)

	// 更新回 skiplist
	book.Set(order.Price, queue)
}

// GetDepth 获取深度快照（聚合到 price level）
// 用于生成市场数据
func (ob *OrderBookV2) GetDepth(levels int) (bids []map[string]string, asks []map[string]string) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	// 获取买单深度
	elem := ob.buys.Front()
	for i := 0; i < levels && elem != nil; i, elem = i+1, elem.Next() {
		price := elem.Key().(model.PriceInNano)
		queue := elem.Value.([]*OrderBookEntry)

		// 聚合同一价位的所有订单
		var totalQty model.QuantityInNano
		for _, order := range queue {
			totalQty += order.Quantity - order.FilledQty
		}

		bids = append(bids, map[string]string{
			"price":    fmt.Sprintf("%.8f", float64(price)/1e8),
			"quantity": fmt.Sprintf("%.8f", float64(totalQty)/1e8),
		})
	}

	// 获取卖单深度
	elem = ob.sells.Front()
	for i := 0; i < levels && elem != nil; i, elem = i+1, elem.Next() {
		price := elem.Key().(model.PriceInNano)
		queue := elem.Value.([]*OrderBookEntry)

		// 聚合同一价位的所有订单
		var totalQty model.QuantityInNano
		for _, order := range queue {
			totalQty += order.Quantity - order.FilledQty
		}

		asks = append(asks, map[string]string{
			"price":    fmt.Sprintf("%.8f", float64(price)/1e8),
			"quantity": fmt.Sprintf("%.8f", float64(totalQty)/1e8),
		})
	}

	return
}

// CancelOrder 取消订单
func (ob *OrderBookV2) CancelOrder(orderID string) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	order, ok := ob.orders[orderID]
	if !ok {
		return fmt.Errorf("order not found: %s", orderID)
	}

	// 如果订单已部分或全部成交，不允许取消
	if order.FilledQty > 0 {
		return fmt.Errorf("cannot cancel partially filled order: %s", orderID)
	}

	// 从 OrderBook 中移除
	var book *skiplist.SkipList
	if order.Side == "buy" {
		book = ob.buys
	} else {
		book = ob.sells
	}

	elem := book.Get(order.Price)
	if elem != nil {
		queue := elem.Value.([]*OrderBookEntry)
		// 过滤出不是这个订单的其他订单
		var newQueue []*OrderBookEntry
		for _, q := range queue {
			if q.OrderID != orderID {
				newQueue = append(newQueue, q)
			}
		}
		if len(newQueue) == 0 {
			book.Remove(order.Price)
		} else {
			book.Set(order.Price, newQueue)
		}
	}

	// 从订单记录中移除
	delete(ob.orders, orderID)

	return nil
}
