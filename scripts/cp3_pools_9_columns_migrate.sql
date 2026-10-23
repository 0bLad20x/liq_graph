-- CP3 pool-only migration: 17 columns -> 9. Preserve pool rows and mints.
-- STOP every liq_graph process and BACK UP the database before running.
-- One-time migration for current CP3 schema; no CASCADE, no DROP TABLE.
BEGIN;
SET LOCAL lock_timeout = '10s';
DO $$
DECLARE bad bigint;
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='pools' AND column_name='status'
  ) THEN
    RAISE EXCEPTION 'CP3 old public.pools.status absent: already migrated or wrong database';
  END IF;
  -- Unknown must not carry a durable proof, and validated records must be complete.
  SELECT count(*) INTO bad FROM public.pools
  WHERE (status=0 AND (validated_slot IS NOT NULL OR descriptor IS NOT NULL))
     OR (status IN (1,2) AND (validated_slot IS NULL OR descriptor IS NULL
         OR completion_checked_at IS NULL
         OR mint_a IS NULL OR mint_b IS NULL))
     OR (status=1 AND (jsonb_typeof(descriptor->'manifest')<>'object'
         OR jsonb_typeof(descriptor->'evidence')<>'object'
         OR jsonb_typeof(descriptor->'mints')<>'array'
         OR completion_reason_code IS NOT NULL))
     OR (status=2 AND (descriptor->'manifest' IS DISTINCT FROM 'null'::jsonb
         OR completion_reason_code IS NULL));
  IF bad>0 THEN
    RAISE EXCEPTION 'CP3 migration refused: % inconsistent completion rows; repair before altering',bad;
  END IF;
  SELECT count(*) INTO bad FROM public.pools
  WHERE status IN (1,2) AND descriptor_version<>3;
  IF bad>0 THEN
    RAISE EXCEPTION 'CP3 migration refused: % legacy completion descriptors need explicit conversion',bad;
  END IF;
END $$;
ALTER TABLE public.pools ADD COLUMN completion JSONB NULL;
UPDATE public.pools
SET completion = descriptor ||
    jsonb_build_object(
      'validated_slot', validated_slot,
      'checked_at_ms', (extract(epoch FROM completion_checked_at)*1000)::bigint,
      'reason_code', completion_reason_code)
WHERE status IN (1,2);
ALTER TABLE public.pools
  DROP COLUMN first_seen_at,
  DROP COLUMN last_seen_at,
  DROP COLUMN status,
  DROP COLUMN validated_slot,
  DROP COLUMN descriptor_version,
  DROP COLUMN planner_version,
  DROP COLUMN descriptor,
  DROP COLUMN completion_checked_at,
  DROP COLUMN completion_reason_code;
-- The former status index is removed automatically with its column.
ALTER TABLE public.pools ADD CONSTRAINT pools_completion_object_chk
    CHECK (completion IS NULL OR jsonb_typeof(completion)='object');
ALTER TABLE public.pools ADD CONSTRAINT pools_completion_slot_chk
    CHECK (completion IS NULL OR
        (jsonb_typeof(completion->'validated_slot')='number'
         AND (completion->>'validated_slot')::bigint>=0
         AND jsonb_typeof(completion->'checked_at_ms')='number'
         AND (completion->>'checked_at_ms')::bigint>0));
COMMIT;
