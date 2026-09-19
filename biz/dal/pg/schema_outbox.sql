-- outbox table for transactional outbox pattern
-- 确保业务变更和事件发布的原子性

CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    
    -- 事件标识（业务层的唯一 ID，如 TradeID）
    event_id VARCHAR(255) NOT NULL UNIQUE,
    
    -- 事件类型
    event_type VARCHAR(100) NOT NULL,
    
    -- 聚合根 ID（如 symbol、user-id）
    aggregate_id VARCHAR(255) NOT NULL,
    
    -- 聚合根类型（如 OrderBook、Position）
    aggregate_type VARCHAR(100) NOT NULL,
    
    -- 事件负载（JSON）
    payload TEXT NOT NULL,
    
    -- 发布状态
    published BOOLEAN NOT NULL DEFAULT FALSE,
    published_at TIMESTAMP NULL,
    
    -- 时间戳
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- 重试相关
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL
);

-- 创建索引以加速查询
CREATE INDEX IF NOT EXISTS idx_outbox_published ON outbox(published);
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate ON outbox(aggregate_id, aggregate_type);
CREATE INDEX IF NOT EXISTS idx_outbox_created ON outbox(created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(created_at) 
    WHERE published = FALSE;

-- 创建更新触发器自动更新 updated_at（如果数据库支持）
-- PostgreSQL 示例：
CREATE OR REPLACE FUNCTION update_outbox_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_outbox_updated_at ON outbox;
CREATE TRIGGER trigger_outbox_updated_at
    BEFORE UPDATE ON outbox
    FOR EACH ROW
    EXECUTE FUNCTION update_outbox_updated_at();
