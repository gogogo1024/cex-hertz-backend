package service

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/hashicorp/consul/api"
)

const partitionTableConsulKey = "partition/table"

// PartitionManager 管理分区表，支持 Consul 存储和热加载
// 提供分区表的读写、监听和本地缓存

type PartitionManager struct {
	client      *api.Client
	cache       *model.PartitionTable
	lock        sync.RWMutex
	watchCancel context.CancelFunc
	// ownershipFSMs 存放运行时的 OwnershipStateMachine 实例，key 为 partitionID
	fsmLock       sync.RWMutex
	ownershipFSMs map[string]*model.OwnershipStateMachine
	// 注入的 TransferCoordinator（若已设置，则在注册 FSM 时自动注入）
	transferCoord model.TransferCoordinator
}

// NewPartitionManager 创建 PartitionManager
// 支持多个 Consul 地址，自动选择可用节点
func NewPartitionManager(consulAddrs []string) (*PartitionManager, error) {
	var lastErr error
	for _, addr := range consulAddrs {
		cfg := api.DefaultConfig()
		cfg.Address = addr
		cli, err := api.NewClient(cfg)
		if err != nil {
			lastErr = err
			continue
		}
		// 检测本地 Consul Agent 是否可用
		_, err = cli.Agent().Self()
		if err != nil {
			lastErr = err
			continue
		}
		// 检查 Consul 集群 leader 状态，增强健壮性
		status := cli.Status()
		leader, err := status.Leader()
		if err != nil || leader == "" {
			lastErr = err
			continue
		}
		return &PartitionManager{
			client:        cli,
			cache:         model.NewPartitionTable(),
			ownershipFSMs: make(map[string]*model.OwnershipStateMachine),
		}, nil
	}
	return nil, lastErr
}

// LoadFromConsul 加载分区表到本地缓存
func (pm *PartitionManager) LoadFromConsul() error {
	kv := pm.client.KV()
	pair, _, err := kv.Get(partitionTableConsulKey, nil)
	if err != nil {
		return err
	}
	if pair == nil {
		return nil // 未初始化
	}
	var pt model.PartitionTable
	err = json.Unmarshal(pair.Value, &pt)
	if err != nil {
		return err
	}
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.cache = &pt
	return nil
}

// SaveToConsul 保存分区表到 Consul
func (pm *PartitionManager) SaveToConsul() error {
	pm.lock.RLock()
	data, err := json.Marshal(pm.cache)
	pm.lock.RUnlock()
	if err != nil {
		return err
	}
	kv := pm.client.KV()
	_, err = kv.Put(&api.KVPair{Key: partitionTableConsulKey, Value: data}, nil)
	return err
}

// GetPartitionTable 获取本地分区表快照
func (pm *PartitionManager) GetPartitionTable() *model.PartitionTable {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	return pm.cache
}

// WatchPartitionTable 启动分区表变更监听，基于 Consul blocking query 实现事件驱动热加载
func (pm *PartitionManager) WatchPartitionTable(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	pm.watchCancel = cancel
	go func() {
		var lastIndex uint64
		kv := pm.client.KV()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// blocking query: 只有分区表有变更时才返回
				q := &api.QueryOptions{
					WaitIndex: lastIndex,
					WaitTime:  10 * time.Minute, // 最长阻塞10分钟
				}
				pair, meta, err := kv.Get(partitionTableConsulKey, q)
				if err != nil {
					time.Sleep(time.Second) // 避免频繁重试
					continue
				}
				if pair == nil || meta == nil {
					time.Sleep(time.Second)
					continue
				}
				if meta.LastIndex == lastIndex {
					continue // 没有变更
				}
				lastIndex = meta.LastIndex
				var pt model.PartitionTable
				err = json.Unmarshal(pair.Value, &pt)
				if err != nil {
					continue
				}
				pm.lock.Lock()
				pm.cache = &pt
				pm.lock.Unlock()
			}
		}
	}()
}

// StopWatch 停止监听
func (pm *PartitionManager) StopWatch() {
	if pm.watchCancel != nil {
		pm.watchCancel()
	}
}

// UpdatePartitionTable 更新本地分区表并保存到 Consul
func (pm *PartitionManager) UpdatePartitionTable(pt *model.PartitionTable) error {
	pm.lock.Lock()
	pm.cache = pt
	pm.lock.Unlock()
	return pm.SaveToConsul()
}

// GetConsulClient 获取Consul客户端（用于其他服务如NodeIDManager）
func (pm *PartitionManager) GetConsulClient() *api.Client {
	return pm.client
}

// RegisterOwnershipFSM 注册或更新分区对应的 OwnershipStateMachine
// 如果之前已通过 SetupCoordinators 注入了 coordinator，则会在注册时自动注入到 fsm
func (pm *PartitionManager) RegisterOwnershipFSM(partitionID string, fsm *model.OwnershipStateMachine) {
	pm.fsmLock.Lock()
	defer pm.fsmLock.Unlock()
	pm.ownershipFSMs[partitionID] = fsm
	if pm.transferCoord != nil && fsm != nil {
		fsm.SetTransferCoordinator(pm.transferCoord)
	}
}

// UnregisterOwnershipFSM 注销分区对应的 FSM（例如在分区释放时）
func (pm *PartitionManager) UnregisterOwnershipFSM(partitionID string) {
	pm.fsmLock.Lock()
	defer pm.fsmLock.Unlock()
	delete(pm.ownershipFSMs, partitionID)
}

// GetOwnershipFSM 返回已注册的 FSM（如果存在）
func (pm *PartitionManager) GetOwnershipFSM(partitionID string) *model.OwnershipStateMachine {
	pm.fsmLock.RLock()
	defer pm.fsmLock.RUnlock()
	return pm.ownershipFSMs[partitionID]
}

// SetTransferCoordinator 将 coordinator 注入到所有已注册的 FSM，并在后续注册时自动注入
func (pm *PartitionManager) SetTransferCoordinator(tc model.TransferCoordinator) {
	pm.fsmLock.Lock()
	defer pm.fsmLock.Unlock()
	pm.transferCoord = tc
	for _, fsm := range pm.ownershipFSMs {
		if fsm != nil {
			fsm.SetTransferCoordinator(tc)
		}
	}
}

// ListRegisteredFSMs 返回注册的 FSM 的浅拷贝映射（仅供调试/检查）
func (pm *PartitionManager) ListRegisteredFSMs() map[string]*model.OwnershipStateMachine {
	pm.fsmLock.RLock()
	defer pm.fsmLock.RUnlock()
	out := make(map[string]*model.OwnershipStateMachine, len(pm.ownershipFSMs))
	for k, v := range pm.ownershipFSMs {
		out[k] = v
	}
	return out
}
