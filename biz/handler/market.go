package handler

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/redis"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// GetDepth 获取深度（订单簿快照）
type depthItem struct {
	Price    float64
	Amount   float64
	Count    int
	PriceStr string
}

func parseLimit(limitStr string, defaultLimit int) int {
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			return l
		}
	}
	return defaultLimit
}

func aggregateOrders(orders []model.Order, symbol string) (map[string]*depthItem, map[string]*depthItem) {
	bidsAgg := make(map[string]*depthItem)
	asksAgg := make(map[string]*depthItem)
	for _, o := range orders {
		if o.Symbol != symbol {
			continue
		}
		// Price 和 Quantity 已经是 int64 nano 格式，需要转换为 float64
		// PriceInNano = price × 10^8, 所以 price = PriceInNano / 1e8
		price := float64(o.Price) / 1e8
		qty := float64(o.Quantity) / 1e8
		priceStr := strconv.FormatFloat(price, 'f', -1, 64)

		switch o.Side {
		case "buy":
			if bidsAgg[priceStr] == nil {
				bidsAgg[priceStr] = &depthItem{Price: price, PriceStr: priceStr}
			}
			bidsAgg[priceStr].Amount += qty
			bidsAgg[priceStr].Count++
		case "sell":
			if asksAgg[priceStr] == nil {
				asksAgg[priceStr] = &depthItem{Price: price, PriceStr: priceStr}
			}
			asksAgg[priceStr].Amount += qty
			asksAgg[priceStr].Count++
		}
	}
	return bidsAgg, asksAgg
}

func depthItemsToSlice(items map[string]*depthItem) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(items))
	for _, agg := range items {
		result = append(result, map[string]interface{}{
			"price":  agg.PriceStr,
			"amount": strconv.FormatFloat(agg.Amount, 'f', -1, 64),
			"count":  agg.Count,
		})
	}
	return result
}

func sortDepth(items []map[string]interface{}, desc bool) {
	sort.Slice(items, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(items[i]["price"].(string), 64)
		pj, _ := strconv.ParseFloat(items[j]["price"].(string), 64)
		if desc {
			return pi > pj
		}
		return pi < pj
	})
}

func GetDepth(c context.Context, ctx *app.RequestContext) {
	symbol := string(ctx.Query("symbol"))
	if symbol == "" {
		ctx.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "symbol参数不能为空"})
		return
	}
	bidLimit := parseLimit(string(ctx.Query("bid_limit")), 20)
	askLimit := parseLimit(string(ctx.Query("ask_limit")), 20)

	orders, _ := pg.ListOrders("", "active")
	bidsAgg, asksAgg := aggregateOrders(orders, symbol)

	bids := depthItemsToSlice(bidsAgg)
	asks := depthItemsToSlice(asksAgg)

	sortDepth(bids, true)
	sortDepth(asks, false)

	if len(bids) > bidLimit {
		bids = bids[:bidLimit]
	}
	if len(asks) > askLimit {
		asks = asks[:askLimit]
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"symbol":    symbol,
		"bids":      bids,
		"asks":      asks,
		"bid_limit": bidLimit,
		"ask_limit": askLimit,
	})
}

// GetTrades 获取最新成交
func GetTrades(c context.Context, ctx *app.RequestContext) {
	symbol := string(ctx.Query("symbol"))
	limitStr := string(ctx.Query("limit"))
	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	trades, _ := pg.ListTrades(symbol, limit)
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"symbol": symbol,
		"trades": trades,
	})
}

// GetTicker 获取ticker（最新价、24h量等）
func GetTicker(c context.Context, ctx *app.RequestContext) {
	symbol := string(ctx.Query("symbol"))
	trades, _ := pg.ListTrades(symbol, 1)
	var lastPrice string
	if len(trades) > 0 {
		lastPrice = trades[0].Price
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"symbol":     symbol,
		"last_price": lastPrice,
	})
}

// GetKline 获取K线数据（简单示例，实际应从聚合表或缓存获取）
func GetKline(c context.Context, ctx *app.RequestContext) {
	symbol := string(ctx.Query("symbol"))
	period := string(ctx.Query("period"))
	limitStr := string(ctx.Query("limit"))
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	if symbol == "" || period == "" {
		ctx.JSON(consts.StatusBadRequest, map[string]interface{}{"error": "symbol和period参数不能为空"})
		return
	}
	// 1. 优先查 Redis
	redisKey := "kline:" + symbol + ":" + period
	klineData, err := redis.Client.LRange(c, redisKey, int64(-limit), -1).Result()
	if err == nil && len(klineData) > 0 {
		var klines []model.Kline
		for _, v := range klineData {
			var k model.Kline
			if e := json.Unmarshal([]byte(v), &k); e == nil {
				klines = append(klines, k)
			}
		}
		ctx.JSON(consts.StatusOK, map[string]interface{}{
			"symbol": symbol,
			"period": period,
			"kline":  klines,
		})
		return
	}
	// 2. 查数据库聚合表
	var klines []model.Kline
	db := pg.GormDB
	db.Where("symbol = ? AND period = ?", symbol, period).
		Order("timestamp desc").Limit(limit).Find(&klines)
	// 逆序
	for i, j := 0, len(klines)-1; i < j; i, j = i+1, j-1 {
		klines[i], klines[j] = klines[j], klines[i]
	}
	ctx.JSON(consts.StatusOK, map[string]interface{}{
		"symbol": symbol,
		"period": period,
		"kline":  klines,
	})
}
