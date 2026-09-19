package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/gogogo1024/cex-hertz-backend/biz/engine"
	"github.com/gogogo1024/cex-hertz-backend/biz/handler"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/biz/service"
	"github.com/gogogo1024/cex-hertz-backend/conf"
	"github.com/gogogo1024/cex-hertz-backend/middleware"
	"github.com/hertz-contrib/websocket"
	"github.com/panjf2000/ants/v2"
	"github.com/segmentio/kafka-go"
)

const shardNum = 32

var upgrader = websocket.HertzUpgrader{
	CheckOrigin: func(ctx *app.RequestContext) bool {
		return true // 允许所有跨域 WebSocket 连接
	},
} // use default options

type SymbolShard struct {
	Mu     sync.RWMutex
	Subs   map[string]map[*websocket.Conn]struct{}
	MsgBuf map[string]chan []byte // 每个symbol的消息缓冲区
}

var symbolShards [shardNum]*SymbolShard

var broadcastPool *ants.Pool

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

var msgBytePool = sync.Pool{
	New: func() any {
		return make([]byte, 4096)
	},
}

func init() {
	for i := 0; i < shardNum; i++ {
		symbolShards[i] = &SymbolShard{
			Subs:   make(map[string]map[*websocket.Conn]struct{}),
			MsgBuf: make(map[string]chan []byte),
		}
	}
	// 初始化 ants goroutine 池（全局池，供 engine.BroadcastPool 使用）
	if err := engine.InitBroadcastPool(1024); err != nil {
		panic(err)
	}
}

// 启动symbol消息分发 goroutine
func ensureSymbolDispatcher(shard *SymbolShard, symbol string) {
	if _, ok := shard.MsgBuf[symbol]; ok {
		return
	}
	msgBuf := make(chan []byte, 4096)
	shard.MsgBuf[symbol] = msgBuf
	go func() {
		for msg := range msgBuf {
			shard.Mu.RLock()
			conns := shard.Subs[symbol]
			for conn := range conns {
				err := broadcastPool.Submit(func() {
					success := false
					for i := 0; i < 3; i++ {
						if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
							log.Printf("broadcast error: %v, retry %d", err, i+1)
						} else {
							success = true
							break
						}
					}
					if !success {
						log.Printf("conn write failed after retries, will remove from symbol: %v", conn.RemoteAddr())
						shard := GetSymbolShard(symbol)
						shard.Mu.Lock()
						delete(shard.Subs[symbol], conn)
						if len(shard.Subs[symbol]) == 0 {
							delete(shard.Subs, symbol)
						}
						shard.Mu.Unlock()
						cleanConnFromAllSymbols(conn)
						_ = conn.Close()
					}
				})
				if err != nil {
					log.Printf("broadcastPool.Submit error: %v, conn: %v", err, conn.RemoteAddr())
				}
			}
			shard.Mu.RUnlock()
		}
		shard.Mu.Lock()
		delete(shard.MsgBuf, symbol)
		shard.Mu.Unlock()
	}()
}

func GetSymbolShard(symbol string) *SymbolShard {
	h := fnv32(symbol)
	return symbolShards[h%shardNum]
}

func fnv32(key string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// 解析 action/symbol

type Message struct {
	Action string `json:"action"`
	Symbol string `json:"symbol"`
}

func parseAction(msg []byte) (string, string) {
	var m Message
	if err := json.Unmarshal(msg, &m); err != nil {
		return "", ""
	}
	return m.Action, m.Symbol
}

// 清理连接所有symbol订阅
func cleanConnFromAllSymbols(c *websocket.Conn) {
	for i := 0; i < shardNum; i++ {
		shard := symbolShards[i]
		shard.Mu.Lock()
		for sym, conns := range shard.Subs {
			if conns != nil {
				if _, ok := conns[c]; ok {
					delete(conns, c)
					if len(conns) == 0 {
						delete(shard.Subs, sym)
					}
				}
			}
		}
		shard.Mu.Unlock()
	}
}

// Broadcast 广播消息到symbol
func Broadcast(symbol string, msg []byte) {
	shard := GetSymbolShard(symbol)
	shard.Mu.Lock()
	ensureSymbolDispatcher(shard, symbol)
	buf, ok := shard.MsgBuf[symbol]
	shard.Mu.Unlock()
	if ok && buf != nil {
		select {
		case buf <- msg:
			// 写入成功
		default:
			log.Printf("symbol %s ring buffer full, drop message", symbol)
			go saveDroppedMessage(symbol, msg)
		}
	}
}

func safeBroadcast(symbol string, buf *bytes.Buffer) {
	msg := msgBytePool.Get().([]byte)
	if cap(msg) < buf.Len() {
		msg = make([]byte, buf.Len())
	}
	msg = msg[:buf.Len()]
	copy(msg, buf.Bytes())
	Broadcast(symbol, msg)
	msgBytePool.Put(msg)
}

// 丢弃的消息异步写入 Kafka
func saveDroppedMessage(symbol string, msg []byte) {
	go func() {
		topic := "dropped_" + symbol
		w := getDroppedKafkaWriter(topic)
		if w == nil {
			log.Printf("failed to get dropped kafka writer for topic %s", topic)
			return
		}
		_ = w.WriteMessages(context.Background(), kafka.Message{Value: msg})
	}()
}

var droppedKafkaWriters sync.Map // map[topic]*kafka.Writer

func getDroppedKafkaWriter(topic string) *kafka.Writer {
	if w, ok := droppedKafkaWriters.Load(topic); ok {
		return w.(*kafka.Writer)
	}

	brokers := conf.GetConf().Kafka.Brokers
	w := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		Async: true,
	}
	droppedKafkaWriters.Store(topic, w)
	return w
}

// PushMatchResult 复杂消息组装示例：撮合结果推送
func PushMatchResult(msgType string, symbol string, orderID string, price string, quantity string, status string, ts int64) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(`{"symbol":"`)
	buf.WriteString(symbol)
	buf.WriteString(`","type":"`)
	buf.WriteString(msgType)
	buf.WriteString(`","data":{`)
	buf.WriteString(`"order_id":"`)
	buf.WriteString(orderID)
	buf.WriteString(`","price":"`)
	buf.WriteString(price)
	buf.WriteString(`","quantity":"`)
	buf.WriteString(quantity)
	buf.WriteString(`","status":"`)
	buf.WriteString(status)
	buf.WriteString(`","ts":`)
	buf.WriteString(fmt.Sprintf("%d", ts))
	buf.WriteString("}}")
	safeBroadcast(symbol, buf)
	bufferPool.Put(buf)
}

// PushOrderBookSnapshot 复杂消息组装示例：订单簿快照推送
func PushOrderBookSnapshot(symbol string, bids, asks []string, ts int64) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(`{"symbol":"`)
	buf.WriteString(symbol)
	buf.WriteString(`","type":"depth_update","data":{`)
	buf.WriteString(`"bids":[`)
	for i, b := range bids {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("\"")
		buf.WriteString(b)
		buf.WriteString("\"")
	}
	buf.WriteString("],\"asks\":[")
	for i, a := range asks {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("\"")
		buf.WriteString(a)
		buf.WriteString("\"")
	}
	buf.WriteString("],\"ts\":")
	buf.WriteString(fmt.Sprintf("%d", ts))
	buf.WriteString("}}")
	safeBroadcast(symbol, buf)
	bufferPool.Put(buf)
}

var matchEngine engine.Engine

// InjectEngine 注入撮合引擎实例
func InjectEngine(e engine.Engine) {
	matchEngine = e
}

// NewWebSocketServer WebSocket 服务端
func NewWebSocketServer(addr string, pm *service.PartitionManager, localAddr string) *server.Hertz {
	h := server.Default(server.WithHostPorts(addr))
	h.NoHijackConnPool = true

	h.GET("/ws", func(ctx context.Context, c *app.RequestContext) {
		log.Printf("[WS] /ws handler called, path=%s, method=%s", c.Path(), c.Request.Method())
		// 分布式路由中间件逻辑迁移到ws，传递 pm 和 localAddr
		middleware.DistributedRouteMiddleware(pm, localAddr)(ctx, c)
		if c.IsAborted() {
			log.Printf("[WS] connection aborted by DistributedRouteMiddleware, path=%s", c.Path())
			return
		}
		err := upgrader.Upgrade(c, func(conn *websocket.Conn) {
			log.Printf("[WS] connection upgraded: %v", conn.RemoteAddr())
			defer func() {
				cleanConnFromAllSymbols(conn)
				if err := conn.Close(); err != nil {
					log.Printf("close error: %v", err)
				}
				log.Printf("[WS] connection closed: %v", conn.RemoteAddr())
			}()

			for {
				mt, msg, err := conn.ReadMessage()
				if err != nil {
					log.Printf("read error: %v", err)
					break
				}
				log.Printf("[WS] received message: %s", string(msg))

				action, symbol := parseAction(msg)
				log.Printf("[WS] parsed action=%s, symbol=%s", action, symbol)
				if action == "subscribe" && symbol != "" {
					log.Printf("[WS] subscribe action for symbol=%s", symbol)
					shard := GetSymbolShard(symbol)
					shard.Mu.Lock()
					if shard.Subs[symbol] == nil {
						shard.Subs[symbol] = make(map[*websocket.Conn]struct{})
					}
					shard.Subs[symbol][conn] = struct{}{}
					shard.Mu.Unlock()
					ack := []byte(`{"type":"subscription_ack","symbol":"` + symbol + `"}`)
					if err := conn.WriteMessage(mt, ack); err != nil {
						log.Printf("ack error: %v", err)
					}
					ensureSymbolDispatcher(shard, symbol)
					continue
				}
				if action == "unsubscribe" && symbol != "" {
					log.Printf("[WS] unsubscribe action for symbol=%s", symbol)
					shard := GetSymbolShard(symbol)
					shard.Mu.Lock()
					if conns, ok := shard.Subs[symbol]; ok {
						delete(conns, conn)
						if len(conns) == 0 {
							delete(shard.Subs, symbol)
						}
					}
					shard.Mu.Unlock()
					ack := []byte(`{"type":"unsubscription_ack","symbol":"` + symbol + `"}`)
					if err := conn.WriteMessage(mt, ack); err != nil {
						log.Printf("ack error: %v", err)
					}
					continue
				}
				if action == "SubmitOrder" {
					log.Printf("[WS] SubmitOrder action, msg=%s", string(msg))
					var order model.SubmitOrderMsg
					if err := json.Unmarshal(msg, &order); err != nil {
						log.Printf("invalid SubmitOrder: %v", err)
						continue
					}
					// 集成迁移重定向逻辑
					migrated, newPartitionID := handler.SymbolMigrationChecker(symbol)
					if migrated {
						// 迁移期间重定向，统一调用 WebSocket 场景的 MigrateWSRedirectHandler
						handler.MigrateWSRedirectHandler(conn, mt, symbol, newPartitionID)
						continue
					}
					if matchEngine != nil {
						log.Printf("[WS] calling matchEngine.SubmitOrder for symbol=%s", symbol)
						if err := matchEngine.SubmitOrder(order); err != nil {
							log.Printf("[WS] SubmitOrder failed for symbol=%s: %v", symbol, err)
						}
					}
					ack := []byte(`{"type":"order_ack","symbol":"` + symbol + `"}`)
					if err := conn.WriteMessage(mt, ack); err != nil {
						log.Printf("order ack error: %v", err)
					}
					continue
				}
			}
		})
		if err != nil {
			log.Printf("upgrade error: %v", err)
		}
	})

	return h
}

// 用户ID到��接的映射
var userConnMap sync.Map // map[userID]*websocket.Conn

// RegisterUserConn 注册用户和连接的关系（需在用户登录或鉴权后调用）
func RegisterUserConn(userID string, conn *websocket.Conn) {
	userConnMap.Store(userID, conn)
}

// UnregisterUserConn 断开连接时清理
func UnregisterUserConn(userID string) {
	userConnMap.Delete(userID)
}

// Unicast 单播消息到指定 userID
func Unicast(userID string, msg []byte) {
	if v, ok := userConnMap.Load(userID); ok {
		if conn, ok := v.(*websocket.Conn); ok {
			_ = conn.WriteMessage(websocket.TextMessage, msg)
		}
	}
}

// PushUnicastMsg 优化版单播消息组装与发送，复用 bufferPool 和 msgBytePool
func PushUnicastMsg(userID string, msgType string, data map[string]any) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(`{"type":"`)
	buf.WriteString(msgType)
	buf.WriteString(`","data":`)
	jsonData, err := json.Marshal(data)
	if err != nil {
		buf.WriteString(`{}`)
	} else {
		buf.Write(jsonData)
	}
	buf.WriteString("}")
	msg := msgBytePool.Get().([]byte)
	if cap(msg) < buf.Len() {
		msg = make([]byte, buf.Len())
	}
	msg = msg[:buf.Len()]
	copy(msg, buf.Bytes())
	Unicast(userID, msg)
	msgBytePool.Put(msg)
	bufferPool.Put(buf)
}
