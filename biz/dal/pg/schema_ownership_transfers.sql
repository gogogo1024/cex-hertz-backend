-- ownership_transfers: 持久化分区迁移记录（方案 A）
CREATE TABLE IF NOT EXISTS ownership_transfers (
  id SERIAL PRIMARY KEY,
  partition_id TEXT NOT NULL,
  from_owner TEXT NOT NULL,
  to_owner TEXT NOT NULL,
  checkpoint_seq BIGINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ownership_transfers_partition ON ownership_transfers(partition_id);
CREATE INDEX IF NOT EXISTS idx_ownership_transfers_status ON ownership_transfers(status);
