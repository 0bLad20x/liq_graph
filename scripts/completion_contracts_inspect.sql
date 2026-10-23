-- CP3 nine-column completion cache inspection. Read-only.
WITH c AS (
 SELECT venue, completion,
        CASE WHEN completion IS NULL THEN 0
             WHEN jsonb_typeof(completion->'manifest')='object' THEN 1
             ELSE 2 END AS status
 FROM public.pools
)
SELECT venue,status,COUNT(*) AS pools,
       COUNT(*) FILTER (WHERE completion->>'checked_at_ms' IS NOT NULL) AS timestamped_proofs,
       COUNT(*) FILTER (WHERE completion->>'reason_code' IS NOT NULL) AS typed_failures,
       MAX((completion->>'checked_at_ms')::bigint) AS last_check_ms
FROM c GROUP BY venue,status ORDER BY venue,status;

SELECT venue,completion->>'reason_code' AS reason_code,COUNT(*) AS pools
FROM public.pools
WHERE completion IS NOT NULL AND jsonb_typeof(completion->'manifest') IS DISTINCT FROM 'object'
GROUP BY venue,completion->>'reason_code' ORDER BY venue,reason_code;

SELECT COUNT(*) AS invalid_completion
FROM public.pools
WHERE completion IS NOT NULL
  AND (jsonb_typeof(completion->'validated_slot') IS DISTINCT FROM 'number'
    OR jsonb_typeof(completion->'checked_at_ms') IS DISTINCT FROM 'number'
    OR (jsonb_typeof(completion->'manifest')='object'
        AND (completion->>'reason_code' IS NOT NULL
             OR jsonb_typeof(completion->'mints') IS DISTINCT FROM 'array'
             OR jsonb_typeof(completion->'evidence') IS DISTINCT FROM 'object'))
    OR (jsonb_typeof(completion->'manifest') IS DISTINCT FROM 'object'
        AND (completion->>'reason_code' IS NULL
             OR completion->>'reason_code'='transient_failure')));
