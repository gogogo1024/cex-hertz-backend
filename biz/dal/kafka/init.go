package kafka

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/conf"
	"github.com/segmentio/kafka-go"
)

var (
	writers sync.Map // map[string]*kafka.Writer
)

// GetWriter 获取指定 topic 的 kafka.Writer，自动复用
func GetWriter(topic string) *kafka.Writer {
	val, ok := writers.Load(topic)
	if ok {
		return val.(*kafka.Writer)
	}
	kafkaConf := conf.GetConf().Kafka
	brokers := normalizeBrokers(kafkaConf.Brokers)
	if len(brokers) == 0 {
		panic("Kafka brokers not configured")
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Async:        true,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	writers.Store(topic, writer)
	return writer
}

// normalizeBrokers 将配置中的 localhost 映射到 127.0.0.1，避免在某些系统上
// 因 localhost 解析为 IPv6 (::1) 导致 kafka-go 连接失败。
func normalizeBrokers(brokers []string) []string {
	if len(brokers) == 0 {
		return brokers
	}
	out := make([]string, len(brokers))
	for i, b := range brokers {
		host, port, err := net.SplitHostPort(b)
		if err != nil {
			// 非 host:port 格式，保留原样
			out[i] = b
			continue
		}
		if strings.EqualFold(host, "localhost") {
			out[i] = net.JoinHostPort("127.0.0.1", port)
			continue
		}
		out[i] = b
	}
	return out
}

// InitWriters 预初始化所有 topics 的 writer（自动从配置获取）
func InitWriters() {
	topicsMap := conf.GetConf().Kafka.Topics
	for _, topic := range topicsMap {
		GetWriter(topic)
	}
}

// TestKafkaConnection 测试 Kafka 连接
func TestKafkaConnection() {
	kafkaConf := conf.GetConf().Kafka
	brokers := kafkaConf.Brokers
	if len(brokers) == 0 {
		panic("Kafka brokers not configured")
	}
	conn, err := kafka.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		panic(fmt.Sprintf("failed to connect to kafka: %v", err))
	}
	_ = conn.Close()
}

// CloseAllWriters 可选：关闭所有 writer
func CloseAllWriters() {
	writers.Range(func(key, value interface{}) bool {
		if w, ok := value.(*kafka.Writer); ok {
			_ = w.Close()
		}
		return true
	})
}

// ResetWriters 关闭并从缓存中移除所有 writer，方便在 broker 重启后创建新的连接实例
func ResetWriters() {
	writers.Range(func(key, value interface{}) bool {
		if w, ok := value.(*kafka.Writer); ok {
			_ = w.Close()
		}
		writers.Delete(key)
		return true
	})
}

// Init 初始化 Kafka，包含连接测试和 writer 预初始化（自动从配置获取）
func Init() {
	TestKafkaConnection()
	InitWriters()
}
