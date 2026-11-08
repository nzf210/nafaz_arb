-- Add a retry-safe natural key for scanner opportunity writes.
-- Separate migration preserves compatibility with projects that already applied
-- 20261107000100_create_nafaz_core_schema.sql.
create unique index if not exists uq_scanner_opportunities_scan_pair
    on public.scanner_opportunities (scan_id, pair_key);
