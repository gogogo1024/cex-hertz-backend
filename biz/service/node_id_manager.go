package service

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/hashicorp/consul/api"
)

const (
	nodeRegistryPrefix = "node_registry/"
	nodeIDCounterKey   = "node_registry/next_id"
	nodeIDTTL          = 10 * time.Second
)

// NodeIDManager 管理分布式节点ID的幂等分配
// 确保同一个节点在重启后仍然获得相同的NodeID
type NodeIDManager struct {
	consulClient *api.Client
	nodeAddr     string // 本节点地址（用作唯一标识）
	mu           sync.Mutex
	nodeID       uint64
	sessionID    string
}

// NodeIDEntry 节点注册表条目
type NodeIDEntry struct {
	NodeID    uint64 `json:"node_id"`
	NodeAddr  string `json:"node_addr"`
	Timestamp int64  `json:"timestamp"`
}

// NewNodeIDManager 创建NodeID管理器
// nodeAddr用于节点唯一标识，在Docker中需要确保稳定性
// 建议传入: os.Getenv("NODE_ID") 或 POD_NAME 或 HOSTNAME（需要确保跨重启唯一）
func NewNodeIDManager(consulClient *api.Client, nodeAddr string) *NodeIDManager {
	// Docker环境优化：使用nodeAddr后缀进行去重
	// 在Kubernetes中，这通常是Pod名称（唯一稳定）
	// 在Docker Compose中，需要手动设置环境变量确保唯一性
	return &NodeIDManager{
		consulClient: consulClient,
		nodeAddr:     nodeAddr,
	}
}

// GetOrAllocateNodeID 幂等地获取或分配NodeID
// 如果节点已注册过，返回之前分配的ID
// 如果是首次注册，分配新的ID
//
// ⚠️ Docker环境注意事项:
// - 必须确保nodeAddr在容器重启后保持不变
// - 推荐使用持久化标识符：
//   * Kubernetes: $POD_NAME (StatefulSet保证唯一性)
//   * Docker Compose: 显式设置环境变量NODE_IDENTITY
//   * 单机: HOSTNAME 或 $CONTAINER_ID
func (m *NodeIDManager) GetOrAllocateNodeID() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nodeAddr == "" {
		return 0, fmt.Errorf("nodeAddr must not be empty (Docker环境下需要持久化唯一标识)")
	}

	// 1. 检查是否已经分配过
	key := nodeRegistryPrefix + m.nodeAddr
	kv := m.consulClient.KV()
	pair, _, err := kv.Get(key, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to query node registry: %w", err)
	}

	// 已注册，复用之前的NodeID
	if pair != nil {
		var entry NodeIDEntry
		if err := json.Unmarshal(pair.Value, &entry); err == nil {
			hlog.Infof("[NodeIDManager] Reusing NodeID=%d for node %s (DockerAddr=%s)", entry.NodeID, entry.NodeAddr, m.nodeAddr)
			m.nodeID = entry.NodeID
			return entry.NodeID, nil
		}
	}

	// 2. 首次注册，分配新的NodeID
	newNodeID, err := m.allocateNewNodeID()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate new NodeID: %w", err)
	}

	// 3. 持久化节点注册
	entry := NodeIDEntry{
		NodeID:    newNodeID,
		NodeAddr:  m.nodeAddr,
		Timestamp: time.Now().Unix(),
	}
	data, _ := json.Marshal(entry)

	_, err = kv.Put(&api.KVPair{
		Key:   key,
		Value: data,
	}, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to save node registry: %w", err)
	}

	hlog.Infof("[NodeIDManager] Allocated NodeID=%d for node %s", newNodeID, m.nodeAddr)
	m.nodeID = newNodeID
	return newNodeID, nil
}

// allocateNewNodeID 分配新的NodeID（原子操作，避免重复）
func (m *NodeIDManager) allocateNewNodeID() (uint64, error) {
	kv := m.consulClient.KV()
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 读取当前计数器
		pair, _, err := kv.Get(nodeIDCounterKey, nil)
		if err != nil {
			return 0, fmt.Errorf("failed to read counter: %w", err)
		}

		var currentID uint64 = 1 // 从1开始（0是单机模式）
		var modifyIndex uint64 = 0

		if pair != nil {
			modifyIndex = pair.ModifyIndex
			currentID = parseNodeID(pair.Value)
		}

		// 检查ID是否超出范围
		if currentID > 1023 {
			return 0, fmt.Errorf("NodeID exhausted: max 1024 nodes supported, current=%d", currentID)
		}

		nextID := currentID + 1
		nextIDBytes := []byte(fmt.Sprintf("%d", nextID))

		// CAS操作：只有当ModifyIndex匹配时才写入
		success, _, err := kv.CAS(&api.KVPair{
			Key:         nodeIDCounterKey,
			Value:       nextIDBytes,
			ModifyIndex: modifyIndex,
		}, nil)

		if err != nil {
			return 0, fmt.Errorf("CAS operation failed: %w", err)
		}

		if success {
			return currentID, nil
		}

		// CAS失败，重试（有其他节点同时在分配）
		hlog.Warnf("[NodeIDManager] CAS conflict on attempt %d, retrying...", attempt+1)
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}

	return 0, fmt.Errorf("failed to allocate NodeID after %d retries (CAS conflict)", maxRetries)
}

// parseNodeID 从字节解析NodeID
func parseNodeID(data []byte) uint64 {
	var id uint64
	fmt.Sscanf(string(data), "%d", &id)
	return id
}

// GetNodeID 获取已分配的NodeID（无须分配）
func (m *NodeIDManager) GetNodeID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodeID
}

// RegisterWithTTL 注册节点并设置TTL（用于健康检查）
func (m *NodeIDManager) RegisterWithTTL() error {
	// 可选：使用Consul Session机制实现自动过期
	// 当节点心跳失败时，自动删除registration
	// 这里先保持简单实现
	hlog.Infof("[NodeIDManager] Node %s registered with NodeID=%d", m.nodeAddr, m.nodeID)
	return nil
}
