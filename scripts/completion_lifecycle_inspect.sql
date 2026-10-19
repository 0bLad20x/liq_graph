-- Read-only DLMM overview; exact run matching is in the generated
-- completion_lifecycle_verify.sql from completion_lifecycle_live.ps1.
SELECT status, COUNT(*) AS pools,
       COUNT(*) FILTER (WHERE descriptor->>'checked_at_ms' IS NOT NULL) AS timestamped_proofs,
       MAX((descriptor->>'checked_at_ms')::bigint) AS last_check_ms
FROM public.liq_graph_pool_cache WHERE venue = 1
GROUP BY status ORDER BY status;

SELECT descriptor->>'reason_code' AS reason_code, COUNT(*) AS pools
FROM public.liq_graph_pool_cache WHERE venue = 1 AND status = 2
GROUP BY descriptor->>'reason_code' ORDER BY reason_code;

SELECT COUNT(*) AS incompatible_new_descriptors
FROM public.liq_graph_pool_cache
WHERE venue = 1 AND descriptor->>'checked_at_ms' IS NOT NULL
  AND ((status = 1 AND descriptor->>'reason_code' IS NOT NULL)
    OR (status = 2 AND descriptor->>'reason_code' IS NULL)
    OR descriptor->>'reason_code' = 'transient_failure');
