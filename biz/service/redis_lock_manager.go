package service

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// RedisLockManager 分布式锁管理器
// 基于 Redlock (Redis Distributed Lock) 实现
// 优雅封装 redsync，提供简洁的API
type RedisLockManager struct {
	rs *redsync.Redsync
}

// NewRedisLockManager 创建锁管理器
func NewRedisLockManager(redisClient *redis.Client) *RedisLockManager {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	return &RedisLockManager{rs: rs}
}

// LockOptions 锁选项
type LockOptions struct {
	// 锁的过期时间（防止死锁）
	ExpireDuration time.Duration

	// 获取锁的尝试次数
	MaxRetries int

	// 重试之间的延迟
	RetryDelay time.Duration
}

// DefaultLockOptions 默认选项
func DefaultLockOptions() *LockOptions {
	return &LockOptions{
		ExpireDuration: 30 * time.Second,
		MaxRetries:     3,
		RetryDelay:     100 * time.Millisecond,
	}
}

// WithLock 执行一个在分布式锁保护下的操作
// 类似 Node.js 中的 redlock.using()
//
// 示例:
//
//	err := lockMgr.WithLock(ctx, "partition:1:owner", func(ctx context.Context) error {
//	    // 在这里进行临界操作
//	    return updatePartitionOwner(ctx, "partition:1", "node-2")
//	})
func (rm *RedisLockManager) WithLock(
	ctx context.Context,
	key string,
	fn func(context.Context) error,
	opts ...*LockOptions,
) error {
	opt := DefaultLockOptions()
	if len(opts) > 0 && opts[0] != nil {
		opt = opts[0]
	}

	// 获取分布式锁
	mutex := rm.rs.NewMutex(
		key,
	)

	// 尝试获取锁
	var err error
	for i := 0; i < opt.MaxRetries; i++ {
		err = mutex.LockContext(ctx)
		if err == nil {
			break
		}

		// 解析错误，如果是上下文超时则直接返回
		if ctx.Err() != nil {
			return fmt.Errorf("lock context expired: %w", ctx.Err())
		}

		// 重试
		if i < opt.MaxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(opt.RetryDelay):
			}
		}
	}

	if err != nil {
		return fmt.Errorf("failed to acquire lock on %s: %w", key, err)
	}

	// 确保解锁
	defer func() {
		if _, unlockErr := mutex.Unlock(); unlockErr != nil {
			// 记录解锁错误，但不影响业务流程
			hlog.Warnf("[RedisLockManager] Failed to unlock %s: %v", key, unlockErr)
		}
	}()

	// 执行业务逻辑
	return fn(ctx)
}

// TryLock 尝试获取锁（非阻塞）
// 返回 (locked, mutex, error)
// 如果 locked=true，调用者需要手动调用 mutex.Unlock()
//
// 示例:
//
//	locked, mutex, err := lockMgr.TryLock(ctx, "event:offset:123")
//	if locked {
//	    defer mutex.Unlock()
//	    // 执行操作
//	}
func (rm *RedisLockManager) TryLock(
	ctx context.Context,
	key string,
	opts ...*LockOptions,
) (bool, *redsync.Mutex, error) {
	mutex := rm.rs.NewMutex(key)

	err := mutex.TryLockContext(ctx)
	if err == redsync.ErrFailed {
		// 锁被其他进程持有
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("lock error: %w", err)
	}

	return true, mutex, nil
}

// IsLocked 检查某个key是否被锁住
// 用于监控和诊断
func (rm *RedisLockManager) IsLocked(ctx context.Context, key string) (bool, error) {
	// 简单实现：尝试获取锁
	locked, mutex, err := rm.TryLock(ctx, key)
	if err != nil {
		return false, err
	}

	if locked {
		// 我们成功获取了锁，说明没有被锁住
		mutex.Unlock()
		return false, nil
	}

	return true, nil
}

// ========== 便利函数 - 针对特定场景的封装 ==========

// WithEventProcessingLock 获取事件处理锁
// 防止同一个事件被并发处理
func (rm *RedisLockManager) WithEventProcessingLock(
	ctx context.Context,
	topic string,
	partition int32,
	offset int64,
	fn func(context.Context) error,
) error {
	lockKey := fmt.Sprintf("event:processing:%s:%d:%d", topic, partition, offset)

	opts := &LockOptions{
		ExpireDuration: 5 * time.Minute, // 事件处理最多5分钟
		MaxRetries:     1,               // 事件不能等待
	}

	return rm.WithLock(ctx, lockKey, fn, opts)
}

// WithPartitionOwnershipUpdate 获取分区所有权更新锁
// 保证分区迁移时的原子性
func (rm *RedisLockManager) WithPartitionOwnershipUpdate(
	ctx context.Context,
	partitionID string,
	fn func(context.Context) error,
) error {
	lockKey := fmt.Sprintf("partition:%s:owner_update", partitionID)

	opts := &LockOptions{
		ExpireDuration: 10 * time.Second, // 所有权更新快速完成
		MaxRetries:     3,
		RetryDelay:     50 * time.Millisecond,
	}

	return rm.WithLock(ctx, lockKey, fn, opts)
}

// GetRecoveryLock 获取恢复操作的锁
// 防止并发恢复同一个symbol
func (rm *RedisLockManager) WithRecoveryLock(
	ctx context.Context,
	processorName string,
	symbol string,
	fn func(context.Context) error,
) error {
	lockKey := fmt.Sprintf("recovery:lock:%s:%s", processorName, symbol)

	opts := &LockOptions{
		ExpireDuration: 30 * time.Minute, // 恢复可能比较长
		MaxRetries:     1,                // 不重试（恢复应该立即执行）
	}

	return rm.WithLock(ctx, lockKey, fn, opts)
}
