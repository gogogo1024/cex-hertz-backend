-- Persistent Event Store (append-only) for PostgreSQL
-- 用于替代内存中的 InMemoryEventLog
-- 保证事件完整历史、可重放性、崩溃恢复

CREATE TABLE IF NOT EXISTS events (
    -- 主键：全局唯一序列号（严格递增）
    id BIGSERIAL PRIMARY KEY,
    
    -- 分布式全局序列：(NodeID << 40) | LocalSeq
    -- 用途：事件唯一标识 + 跨节点排序
    -- 注意：不是全分布式共识，只是节点内部的排序
    global_seq BIGINT NOT NULL UNIQUE,
    
    -- 事件类型（TradeExecuted、OrderCancelled等）
    event_type VARCHAR(100) NOT NULL,
    
    -- 聚合根ID（按业务分类）
    aggregate_id VARCHAR(255) NOT NULL,
    
    -- 聚合根类型（OrderBook、Position等）
    aggregate_type VARCHAR(100) NOT NULL,
    
    -- 交易对（如BTC/USDT）
    symbol VARCHAR(50),
    
    -- 事件负载（JSON 格式）
    payload JSONB NOT NULL,
    
    -- 事件发生时间戳（毫秒）
    event_timestamp BIGINT,
    
    -- 创建时间（入库时间）
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- 可选：事件版本控制
    version INTEGER DEFAULT 1
);

-- 创建复合索引：按聚合根查询事件流
-- 用途：重放特定聚合根的所有事件
CREATE INDEX IF NOT EXISTS idx_events_aggregate 
    ON events(aggregate_type, aggregate_id, created_at);

-- 创建索引：按符号查询（用于订单簿重建）
CREATE INDEX IF NOT EXISTS idx_events_symbol 
    ON events(symbol, created_at)
    WHERE symbol IS NOT NULL;

-- 创建索引：按事件类型查询
CREATE INDEX IF NOT EXISTS idx_events_type 
    ON events(event_type, created_at);

-- 创建索引：按全局序列查询（用于顺序恢复）
CREATE INDEX IF NOT EXISTS idx_events_global_seq 
    ON events(global_seq);

-- 按时间范围查询
CREATE INDEX IF NOT EXISTS idx_events_created 
    ON events(created_at);

-- JSONB 索引（加速嵌套字段查询）
-- 用于快速查找特定字段值
CREATE INDEX IF NOT EXISTS idx_events_payload_gin 
    ON events USING gin(payload);

-- 不可变表检查（防止修改）
-- 注意：PostgreSQL 12+ 使用 GENERATED ALWAYS AS IDENTITY ALWAYS GENERATED
-- 在实现中配合应用层检查：UPDATE events 应该返回 0 rows

-- 分区策略（可选，用于超大表）
-- 可以按时间分区：PARTITION BY RANGE (created_at)
-- 或按聚合根分区：PARTITION BY LIST (aggregate_type)
-- 示例分区语法（已注释，按需启用）：
/*
-- 按月份分区
CREATE TABLE events_2026_01 PARTITION OF events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
*/

-- 统计表大小的视图（可选）
CREATE OR REPLACE VIEW events_stats AS
SELECT 
    event_type,
    aggregate_type,
    COUNT(*) as event_count,
    MIN(created_at) as oldest_event,
    MAX(created_at) as newest_event,
    pg_size_pretty(pg_total_relation_size(
        'events'::regclass
    )) as table_size
FROM events
GROUP BY event_type, aggregate_type
ORDER BY event_count DESC;
