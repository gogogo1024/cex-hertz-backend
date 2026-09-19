//go:build !no_rocksdb
// +build !no_rocksdb

package rocksdb

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/linxGnu/grocksdb"
)

// CompensateOrder 补偿订单结构体
// OrderJSON: 原始订单JSON
// RetryCount: 重试次数
// LastRetryTime: 上次重试时间戳（秒）
type CompensateOrder struct {
	OrderJSON     json.RawMessage `json:"order_json"`
	RetryCount    int             `json:"retry_count"`
	LastRetryTime int64           `json:"last_retry_time"`
}

const MaxRetryCount = 5 // 最大重试次数，Kafka故障率低，建议设置为5

var (
	compensateDB     *grocksdb.DB
	compensateDBOnce sync.Once
	compensateDBPath = "data/compensate_rocksdb"
)

// Init 初始化RocksDB实例
func Init(path string) error {
	var err error
	compensateDBOnce.Do(func() {
		if path != "" {
			compensateDBPath = path
		} else {
			// 若未指定，优先用环境变量，再用默认
			if envPath := os.Getenv("COMPENSATE_ROCKSDB_PATH"); envPath != "" {
				compensateDBPath = envPath
			}
		}
		opts := grocksdb.NewDefaultOptions()
		opts.SetCreateIfMissing(true)
		compensateDB, err = grocksdb.OpenDb(opts, compensateDBPath)
	})
	if err != nil {
		hlog.Errorf("[RocksDB] 初始化失败: %v", err)
		return err
	}
	hlog.Infof("[RocksDB] 补偿DB初始化成功, path=%s", compensateDBPath)
	return nil
}

// SaveOrderCompensate 写入补偿订单（带重试信息）
func SaveOrderCompensate(orderID string, order interface{}) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	orderJSON, err := json.Marshal(order)
	if err != nil {
		return err
	}
	comp := CompensateOrder{
		OrderJSON:     orderJSON,
		RetryCount:    0,
		LastRetryTime: time.Now().Unix(),
	}
	val, err := json.Marshal(comp)
	if err != nil {
		return err
	}
	return compensateDB.Put(wo, []byte(orderID), val)
}

// UpdateOrderCompensateRetry 更新补偿订单的重试次数和时间
func UpdateOrderCompensateRetry(orderID string, comp *CompensateOrder) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	comp.RetryCount++
	comp.LastRetryTime = time.Now().Unix()
	val, err := json.Marshal(comp)
	if err != nil {
		return err
	}
	return compensateDB.Put(wo, []byte(orderID), val)
}

// GetAllOrderCompensates 遍历所有补偿订单，返回CompensateOrder结构
func GetAllOrderCompensates() (map[string]*CompensateOrder, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := compensateDB.NewIterator(ro)
	defer it.Close()
	result := make(map[string]*CompensateOrder)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		val := make([]byte, len(it.Value().Data()))
		copy(val, it.Value().Data())
		var comp CompensateOrder
		if err := json.Unmarshal(val, &comp); err == nil {
			result[key] = &comp
		}
		it.Key().Free()
		it.Value().Free()
	}
	return result, nil
}

// DeleteOrderCompensate 删除补偿订单
func DeleteOrderCompensate(orderID string) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return compensateDB.Delete(wo, []byte(orderID))
}

// CloseCompensateDB 关闭DB
func CloseCompensateDB() {
	if compensateDB != nil {
		compensateDB.Close()
	}
}
