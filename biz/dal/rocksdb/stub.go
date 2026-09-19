//go:build no_rocksdb
// +build no_rocksdb

package rocksdb

import (
	"encoding/json"
)

// CompensateOrder 补偿订单结构体（stub 版本）
type CompensateOrder struct {
	OrderJSON     json.RawMessage `json:"order_json"`
	RetryCount    int             `json:"retry_count"`
	LastRetryTime int64           `json:"last_retry_time"`
}

const MaxRetryCount = 5 // 最大重试次数

// Stub implementations for RocksDB when compiled with no_rocksdb tag

func Init(path string) {
	// No-op
}

func SaveOrderCompensate(orderID string, data interface{}) error {
	return nil
}

func GetAllOrderCompensates() (map[string]*CompensateOrder, error) {
	return make(map[string]*CompensateOrder), nil
}

func DeleteOrderCompensate(orderID string) error {
	return nil
}

func UpdateOrderCompensateRetry(orderID string, data interface{}) error {
	return nil
}
