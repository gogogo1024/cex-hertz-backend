//go:build integration

package service

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// failingWriter 用于在测试中模拟 Kafka 写入失败的确定性场景
type failingWriter struct{}

func (f *failingWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	return fmt.Errorf("simulated kafka failure")
}
