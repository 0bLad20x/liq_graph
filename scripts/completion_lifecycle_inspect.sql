-- Read-only migrated-protocol overview. Exact run matching is in the generated
-- completion_lifecycle_<selection>_verify.sql from completion_lifecycle_live.ps1.
SELECT venue, status, COUNT(*) AS pools,
       COUNT(*) FILTER (WHERE descriptor->>'checked_at_ms' IS NOT NULL) AS timestamped_proofs,
       MAX((descriptor->>'checked_at_ms')::bigint) AS last_check_ms
FROM public.pools WHERE venue IN (1, 2, 3, 4, 5, 7, 8, 9, 10, 11)
GROUP BY venue, status ORDER BY venue, status;

SELECT venue, completion_reason_code AS reason_code, COUNT(*) AS pools
FROM public.pools WHERE venue IN (1, 2, 3, 4, 5, 7, 8, 9, 10, 11) AND status = 2
GROUP BY venue, completion_reason_code ORDER BY venue, reason_code;

SELECT COUNT(*) AS incompatible_new_descriptors
FROM public.pools
WHERE venue IN (1, 2, 3, 4, 5, 7, 8, 9, 10, 11) AND descriptor->>'checked_at_ms' IS NOT NULL
  AND ((status = 1 AND completion_reason_code IS NOT NULL)
    OR (status = 2 AND completion_reason_code IS NULL)
    OR completion_reason_code = 'transient_failure');
