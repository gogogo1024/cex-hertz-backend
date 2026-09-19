package handler

import (
	"context"
	"fmt"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/service"
)

// OrderProcessingContext 订单处理上下文
// 包含从 epoch fencing 检查中获得的信息
type OrderProcessingContext struct {
	Symbol           string
	PartitionID      string
	CurrentEpoch     int64
	ProcessingNodeID string
	IsFenced         bool // 是否因为epoch过期而被fencing
	ErrorMsg         string
}

// VerifyOrderOwnershipFencing 验证当前节点是否仍然拥有该分区
//
// 用于防止 stale write 问题：
//   - 订单提交时，节点A拥有分区P
//   - 缓存了epoch=1
//   - 分区迁移到节点B，epoch变为2
//   - 但节点A仍然有老的请求，epoch=1
//   - 此时应该拒绝处理，防止数据损坏
//
// 返回: (context, error)
// 如果 err != nil，说明无法获取owner信息
// 如果 context.IsFenced == true，说明当前节点不再拥有该分区
func VerifyOrderOwnershipFencing(
	ctx context.Context,
	epochMgr *service.OwnershipEpochManager,
	symbol string,
	partitionID string,
	cachedEpoch int64, // 客户端缓存的epoch（通常来自订单请求的metadata）
	nodeID string,
) (*OrderProcessingContext, error) {
	result := &OrderProcessingContext{
		Symbol:           symbol,
		PartitionID:      partitionID,
		ProcessingNodeID: nodeID,
		IsFenced:         false,
	}

	// 快速路径：从Redis获取当前epoch
	currentEpoch, err := epochMgr.GetEpoch(ctx, partitionID)
	if err != nil {
		result.ErrorMsg = fmt.Sprintf("failed to get epoch: %v", err)
		hlog.Warnf("[OrderHandler] Epoch check failed for %s: %v", symbol, err)
		return result, err
	}

	result.CurrentEpoch = currentEpoch

	// 检查 fencing
	if cachedEpoch > 0 && cachedEpoch != currentEpoch {
		// Epoch 不匹配，说明分区所有权已变更
		result.IsFenced = true
		result.ErrorMsg = fmt.Sprintf(
			"ownership epoch mismatch: expected %d, got %d (partition may have migrated)",
			cachedEpoch, currentEpoch,
		)
		hlog.Warnf("[OrderHandler] Order fenced: %s (epoch %d != %d)",
			symbol, cachedEpoch, currentEpoch)
		return result, nil
	}

	return result, nil
}

// OrderOwnershipInterceptor 订单处理的所有权检查拦截器
//
// 典型使用场景：
//  1. 客户端订单请求中包含当前缓存的epoch信息
//  2. 服务器检查epoch是否仍然有效
//  3. 如果epoch过期，返回特殊错误码（例如409 Conflict）
//
// 示例：
//
//	processingCtx, err := handler.OrderOwnershipInterceptor(
//	    ctx, epochMgr,
//	    req.Symbol, req.PartitionID, req.CachedEpoch,
//	    nodeID,
//	)
//	if err != nil {
//	    return handleError(c, err)
//	}
//	if processingCtx.IsFenced {
//	    c.JSON(409, map[string]interface{}{
//	        "error": "PARTITION_MIGRATED",
//	        "message": processingCtx.ErrorMsg,
//	        "current_epoch": processingCtx.CurrentEpoch,
//	    })
//	    return
//	}
func OrderOwnershipInterceptor(
	ctx context.Context,
	epochMgr *service.OwnershipEpochManager,
	symbol string,
	partitionID string,
	cachedEpoch int64,
	nodeID string,
) (*OrderProcessingContext, error) {
	return VerifyOrderOwnershipFencing(ctx, epochMgr, symbol, partitionID, cachedEpoch, nodeID)
}

// ========== 监控和诊断辅助函数 ==========

// GetOwnershipDiagnostics 获取分区所有权的诊断信息
// 用于调试和监控面板
func GetOwnershipDiagnostics(
	ctx context.Context,
	epochMgr *service.OwnershipEpochManager,
	symbols []string,
) map[string]interface{} {
	diagnostics := make(map[string]interface{})

	for _, symbol := range symbols {
		// 这里假设有某种方式从symbol获取partitionID
		// 在真实系统中，可能需要访问partition manager
		partitionID := fmt.Sprintf("partition:%s", symbol) // 简化示例

		info, err := epochMgr.GetOwnershipInfo(ctx, partitionID)
		if err != nil {
			diagnostics[symbol] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}

		if info == nil {
			diagnostics[symbol] = map[string]interface{}{
				"status": "uninitialized",
			}
			continue
		}

		diagnostics[symbol] = map[string]interface{}{
			"epoch":     info.Epoch,
			"owner":     info.Owner,
			"timestamp": info.UpdatedAt,
		}
	}

	return diagnostics
}
