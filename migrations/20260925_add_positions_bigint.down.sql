-- Down migration: remove bigint columns added for positions
BEGIN;
ALTER TABLE positions DROP COLUMN IF EXISTS volume_bigint;
ALTER TABLE positions DROP COLUMN IF EXISTS avg_price_bigint;
COMMIT;
