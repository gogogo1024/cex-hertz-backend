package service

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/redis"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// OwnershipEpochManager 管理分区所有权的 Epoch 版本号
// 存储在 Redis 中以实现高速访问和分布式一致性
//
// Redis 数据结构：
//
//	Hash Key: "partition:{partitionID}:ownership"
//	Fields:
//	  - epoch: 当前所有权的版本号（单调递增）
//	  - owner: 当前所有者节点ID
//	  - updated_at: 最后更新时间戳
type OwnershipEpochManager struct {
	lockMgr *RedisLockManager
}

// NewOwnershipEpochManager 创建所有权Epoch管理器
func NewOwnershipEpochManager(lockMgr *RedisLockManager) *OwnershipEpochManager {
	return &OwnershipEpochManager{
		lockMgr: lockMgr,
	}
}

// OwnershipEpochInfo 所有权Epoch信息
type OwnershipEpochInfo struct {
	Epoch     int64  `json:"epoch"`
	Owner     string `json:"owner"`
	UpdatedAt int64  `json:"updated_at"`
}

// GetEpoch 获取分区的当前Epoch版本号
// 用于快速检查（毫秒级，直接从Redis读取）
func (oem *OwnershipEpochManager) GetEpoch(ctx context.Context, partitionID string) (int64, error) {
	key := fmt.Sprintf("partition:%s:ownership", partitionID)

	val, err := redis.Client.HGet(ctx, key, "epoch").Result()
	if err != nil {
		if err.Error() == "redis: nil" {
			// 如果不存在，初始化为0
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get epoch: %w", err)
	}

	var epoch int64
	if _, err := fmt.Sscanf(val, "%d", &epoch); err != nil {
		return 0, fmt.Errorf("invalid epoch value: %w", err)
	}

	return epoch, nil
}

// GetOwnershipInfo 获取完整的所有权信息
func (oem *OwnershipEpochManager) GetOwnershipInfo(ctx context.Context, partitionID string) (*OwnershipEpochInfo, error) {
	key := fmt.Sprintf("partition:%s:ownership", partitionID)

	data, err := redis.Client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get ownership info: %w", err)
	}

	if len(data) == 0 {
		return nil, nil // 不存在
	}

	var epoch int64
	if e, ok := data["epoch"]; ok {
		fmt.Sscanf(e, "%d", &epoch)
	}

	var updatedAt int64
	if u, ok := data["updated_at"]; ok {
		fmt.Sscanf(u, "%d", &updatedAt)
	}

	return &OwnershipEpochInfo{
		Epoch:     epoch,
		Owner:     data["owner"],
		UpdatedAt: updatedAt,
	}, nil
}

// UpdateOwnershipEpoch 更新分区的所有权并自动递增Epoch
// 在分区所有者迁移时调用
//
// 使用Redis Lua脚本保证原子性：
//  1. 读取当前epoch
//  2. 检查所有者是否改变
//  3. 如果改变，递增epoch
//  4. 更新所有字段
func (oem *OwnershipEpochManager) UpdateOwnershipEpoch(
	ctx context.Context,
	partitionID string,
	newOwner string,
) (newEpoch int64, err error) {
	// 使用分布式锁保证原子性
	lockKey := fmt.Sprintf("partition:%s:ownership_update", partitionID)

	err = oem.lockMgr.WithLock(ctx, lockKey, func(ctx context.Context) error {
		// 获取当前状态
		info, err := oem.GetOwnershipInfo(ctx, partitionID)
		if err != nil {
			return fmt.Errorf("failed to get current ownership: %w", err)
		}

		var currentEpoch int64
		if info != nil {
			currentEpoch = info.Epoch
			// 如果新所有者与当前所有者相同，不更新
			if info.Owner == newOwner {
				newEpoch = currentEpoch
				return nil
			}
		}

		// 递增epoch
		newEpoch = currentEpoch + 1

		// 写入Redis
		key := fmt.Sprintf("partition:%s:ownership", partitionID)
		pipe := redis.Client.Pipeline()

		pipe.HSet(ctx, key, "epoch", newEpoch)
		pipe.HSet(ctx, key, "owner", newOwner)
		pipe.HSet(ctx, key, "updated_at", time.Now().Unix())
		pipe.Expire(ctx, key, 24*time.Hour) // 24小时过期

		_, err = pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to update ownership: %w", err)
		}

		hlog.Infof("[OwnershipEpochManager] Updated partition %s: owner=%s, epoch=%d",
			partitionID, newOwner, newEpoch)

		return nil
	})

	return newEpoch, err
}

// VerifyOwnershipFencing 验证事件是否可以由当前节点处理
// 用于在处理事件时进行 Fencing 检查
//
// 返回：
//   - isValid: 是否可以处理（当前epoch与事件epoch匹配）
//   - currentEpoch: 当前的epoch
//   - err: 错误
func (oem *OwnershipEpochManager) VerifyOwnershipFencing(
	ctx context.Context,
	partitionID string,
	eventEpoch int64,
) (isValid bool, currentEpoch int64, err error) {
	currentEpoch, err = oem.GetEpoch(ctx, partitionID)
	if err != nil {
		return false, 0, fmt.Errorf("failed to verify fencing: %w", err)
	}

	isValid = (currentEpoch == eventEpoch)
	return
}

// ========== EventContext 集成 ==========

// FillEventContextWithEpoch 填充EventContext中的所有权Epoch信息
// 在从Kafka消费事件时调用
func (oem *OwnershipEpochManager) FillEventContextWithEpoch(
	ctx context.Context,
	eventCtx *model.EventContext,
	partitionID string,
	nodeID string,
) error {
	epoch, err := oem.GetEpoch(ctx, partitionID)
	if err != nil {
		return fmt.Errorf("failed to fill epoch: %w", err)
	}

	eventCtx.WithOwnershipEpoch(epoch, nodeID)
	return nil
}

// ========== 监控和诊断 ==========

// GetOwnershipAuditLog 获取所有权变更的审计日志
// 用于诊断和调试
func (oem *OwnershipEpochManager) GetOwnershipAuditLog(
	ctx context.Context,
	partitionID string,
) (map[string]interface{}, error) {
	info, err := oem.GetOwnershipInfo(ctx, partitionID)
	if err != nil {
		return nil, err
	}

	if info == nil {
		return map[string]interface{}{
			"status": "uninitialized",
		}, nil
	}

	return map[string]interface{}{
		"partition_id": partitionID,
		"epoch":        info.Epoch,
		"owner":        info.Owner,
		"updated_at":   time.Unix(info.UpdatedAt, 0).Format(time.RFC3339),
	}, nil
}

// ========== Cache 本地缓存（可选优化）==========

// LocalEpochCache 本地缓存当前节点的所有权信息
// 减少Redis访问，提高性能
type LocalEpochCache struct {
	// 缓存: partitionID -> epoch
	cache map[string]int64
	// 缓存过期时间
	expiresAt map[string]int64
	// 缓存过期间隔
	ttl time.Duration
}

// NewLocalEpochCache 创建本地缓存
func NewLocalEpochCache(ttl time.Duration) *LocalEpochCache {
	return &LocalEpochCache{
		cache:     make(map[string]int64),
		expiresAt: make(map[string]int64),
		ttl:       ttl,
	}
}

// Get 从缓存读取（如果过期则返回0）
func (lec *LocalEpochCache) Get(partitionID string) (int64, bool) {
	now := time.Now().UnixNano()
	if expireTime, ok := lec.expiresAt[partitionID]; ok && now < expireTime {
		return lec.cache[partitionID], true
	}
	return 0, false
}

// Set 更新缓存
func (lec *LocalEpochCache) Set(partitionID string, epoch int64) {
	lec.cache[partitionID] = epoch
	lec.expiresAt[partitionID] = time.Now().Add(lec.ttl).UnixNano()
}

// Invalidate 使缓存失效
func (lec *LocalEpochCache) Invalidate(partitionID string) {
	delete(lec.cache, partitionID)
	delete(lec.expiresAt, partitionID)
}

// InvalidateAll 清空所有缓存
func (lec *LocalEpochCache) InvalidateAll() {
	lec.cache = make(map[string]int64)
	lec.expiresAt = make(map[string]int64)
}
