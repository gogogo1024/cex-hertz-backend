package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

var ErrSymbolNotLocallyOwned = errors.New("symbol is not owned by this node")

// PartitionAwareMatchEngine 支持动态感知分区变更的撮合引擎（基于 Event Sourcing）
// 在 MatchEngine 的基础上，增加对分布式分区的感知和管理
type PartitionAwareMatchEngine struct {
	*MatchEngine
	pm             *PartitionManager
	localAddr      string
	watchedSymbols sync.Map // symbol -> bool
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewPartitionAwareMatchEngine 创建支持动态分区的撮合引擎
func NewPartitionAwareMatchEngine(
	pm *PartitionManager,
	localAddr string,
	broadcaster func(symbol string, data []byte),
	unicaster func(userID string, data []byte),
) *PartitionAwareMatchEngine {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建基础的事件处理基础设施
	sequencer := NewSequencer()
	eventLog := NewInMemoryEventLog()
	eventPipeline := NewEventPipeline(eventLog)

	// 注册所有处理器
	eventPipeline.RegisterProcessor(NewDatabaseProcessor(nil, 1000))
	eventPipeline.RegisterProcessor(NewPositionProcessor(nil, nil))
	eventPipeline.RegisterProcessor(NewWebSocketProcessor(broadcaster, unicaster))

	// 创建基础的 MatchEngine
	matchEngine := NewMatchEngine(eventPipeline, eventLog, sequencer, broadcaster, unicaster)

	pa := &PartitionAwareMatchEngine{
		MatchEngine: matchEngine,
		pm:          pm,
		localAddr:   localAddr,
		ctx:         ctx,
		cancel:      cancel,
	}

	// 启动分区监听
	go pa.watchPartitionChange()

	return pa
}

// watchPartitionChange 持续监听分区表变更，动态订阅/退订 symbol
func (pa *PartitionAwareMatchEngine) watchPartitionChange() {
	pa.pm.WatchPartitionTable(pa.ctx)
	var lastSymbols map[string]struct{}

	for {
		select {
		case <-pa.ctx.Done():
			return
		default:
			pt := pa.pm.GetPartitionTable()
			mySymbols := make(map[string]struct{})

			// 收集本节点负责的所有 symbol
			for _, partition := range pt.Partitions {
				for _, addr := range partition.Workers {
					if addr == pa.localAddr {
						for _, symbol := range partition.Symbols {
							mySymbols[symbol] = struct{}{}
						}
					}
				}
			}

			// 订阅新 symbol（创建 OrderBook 和启动 worker）
			for symbol := range mySymbols {
				if lastSymbols != nil {
					if _, exists := lastSymbols[symbol]; exists {
						continue // 已经在监控中
					}
				}
				// 新增的 symbol，为其创建 OrderBook
				// （懒加载，当收到订单时会自动创建）
				if _, watched := pa.watchedSymbols.LoadOrStore(symbol, true); !watched {
					hlog.Infof("[PartitionAwareMatchEngine] Started watching symbol: %s", symbol)
				}
			}

			// 退订不再负责的 symbol
			if lastSymbols != nil {
				for symbol := range lastSymbols {
					if _, ok := mySymbols[symbol]; !ok {
						pa.watchedSymbols.Delete(symbol)
						hlog.Infof("[PartitionAwareMatchEngine] Stopped watching symbol: %s", symbol)
					}
				}
			}

			lastSymbols = mySymbols

			// 定期检查（避免繁忙轮询）
			select {
			case <-pa.ctx.Done():
				return
			default:
				// 等待一小段时间后再检查
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// SubmitOrder 覆盖基类方法，添加分区检查
func (pa *PartitionAwareMatchEngine) SubmitOrder(order model.SubmitOrderMsg) error {
	// 检查该 symbol 是否由本节点负责
	if _, watched := pa.watchedSymbols.Load(order.Symbol); !watched {
		return ErrSymbolNotLocallyOwned
	}

	// 调用基类的 SubmitOrder
	return pa.MatchEngine.SubmitOrder(order)
}

// Shutdown 优雅关闭引擎
func (pa *PartitionAwareMatchEngine) Shutdown() {
	pa.cancel()
	pa.MatchEngine.Shutdown()
	hlog.Infof("[PartitionAwareMatchEngine] Shutdown completed")
}

// GetLocalSymbols 获取本节点负责的所有 symbol
func (pa *PartitionAwareMatchEngine) GetLocalSymbols() []string {
	var symbols []string
	pa.watchedSymbols.Range(func(key, value interface{}) bool {
		symbols = append(symbols, key.(string))
		return true
	})
	return symbols
}
