-- Up migration: add bigint columns for positions (nullable first)
BEGIN;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS volume_bigint bigint;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS avg_price_bigint bigint;
COMMIT;

-- Backfill should be executed after this migration (see doc/migration/backfill-positions.md)
