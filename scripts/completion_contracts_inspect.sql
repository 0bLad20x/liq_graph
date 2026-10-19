-- Read-only inspection. Run after completion_contracts_live.ps1.
-- status: 0 Unknown, 1 Prepared, 2 Incomplete. Retryable is an attempt outcome.
SELECT venue, status, COUNT(*) AS pools,
       COUNT(*) FILTER (WHERE descriptor->>'checked_at_ms' IS NOT NULL) AS timestamped_proofs,
       COUNT(*) FILTER (WHERE descriptor->>'reason_code' IS NOT NULL) AS typed_failures,
       MAX((descriptor->>'checked_at_ms')::bigint) AS last_check_ms
FROM public.liq_graph_pool_cache
GROUP BY venue, status ORDER BY venue, status;

SELECT venue, descriptor->>'reason_code' AS reason_code, COUNT(*) AS pools
FROM public.liq_graph_pool_cache
WHERE status = 2
GROUP BY venue, descriptor->>'reason_code' ORDER BY venue, reason_code;

-- Must return zero: current typed contract has no persisted Retryable proof,
-- and successful completion has no machine-readable failure reason.
SELECT COUNT(*) AS incompatible_new_descriptors
FROM public.liq_graph_pool_cache
WHERE descriptor->>'checked_at_ms' IS NOT NULL
  AND ((status = 1 AND descriptor->>'reason_code' IS NOT NULL)
       OR (status = 2 AND descriptor->>'reason_code' IS NULL)
       OR descriptor->>'reason_code' = 'transient_failure');

-- Historical descriptors without new metadata remain readable. Their check time
-- is unknown; restore must not invent one. Current admission is a RAM decision
-- and cannot be reconstructed reliably from historical last_observation alone.
SELECT venue, COUNT(*) AS historical_proofs_without_check_time
FROM public.liq_graph_pool_cache
WHERE status IN (1,2) AND descriptor->>'checked_at_ms' IS NULL
GROUP BY venue ORDER BY venue;
