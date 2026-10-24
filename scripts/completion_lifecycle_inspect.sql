-- CP3: lifecycle completion status derived from JSON, not SQL columns.
WITH c AS (
 SELECT venue,completion,
        CASE WHEN completion IS NULL THEN 0
             WHEN jsonb_typeof(completion->'manifest')='object' THEN 1
             ELSE 2 END AS status
 FROM public.pools WHERE venue IN (1,2,3,4,5,7,8,9,10,11)
)
SELECT venue,status,COUNT(*) AS pools,
       COUNT(*) FILTER (WHERE completion->>'checked_at_ms' IS NOT NULL) AS timestamped_proofs,
       MAX((completion->>'checked_at_ms')::bigint) AS last_check_ms
FROM c GROUP BY venue,status ORDER BY venue,status;

SELECT venue,completion->>'reason_code' AS reason_code,COUNT(*) AS pools
FROM public.pools
WHERE venue IN (1,2,3,4,5,7,8,9,10,11)
  AND completion IS NOT NULL AND jsonb_typeof(completion->'manifest') IS DISTINCT FROM 'object'
GROUP BY venue,completion->>'reason_code' ORDER BY venue,reason_code;

SELECT COUNT(*) AS invalid_completion
FROM public.pools
WHERE venue IN (1,2,3,4,5,7,8,9,10,11)
  AND completion IS NOT NULL
  AND (jsonb_typeof(completion->'validated_slot') IS DISTINCT FROM 'number'
    OR jsonb_typeof(completion->'checked_at_ms') IS DISTINCT FROM 'number'
    OR (jsonb_typeof(completion->'manifest')='object' AND completion->>'reason_code' IS NOT NULL)
    OR (jsonb_typeof(completion->'manifest') IS DISTINCT FROM 'object'
        AND (completion->>'reason_code' IS NULL
             OR completion->>'reason_code'='transient_failure')));

-- CP1 read-only proof invariants. A successful Unchanged refresh must not rewrite completion.
SELECT venue,
       COUNT(*) FILTER (WHERE completion IS NOT NULL) AS full_proofs,
       COUNT(*) FILTER (WHERE jsonb_typeof(completion->'manifest')='object') AS prepared,
       COUNT(*) FILTER (WHERE completion->>'reason_code'='dynamic_mode_unsupported') AS dynamic_mode_paused_or_recheckable,
       COUNT(*) FILTER (WHERE jsonb_typeof(completion->'manifest')='object'
           AND (completion->'manifest'->>'validated_slot')::bigint
               <> (completion->>'validated_slot')::bigint) AS invalid_manifest_slots,
       MAX((completion->>'checked_at_ms')::bigint) AS latest_full_check_ms
FROM public.pools
WHERE venue IN (1,2,3,4,5,7,8,9,10,11)
GROUP BY venue ORDER BY venue;
