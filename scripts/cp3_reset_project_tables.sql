-- DESTRUCTIVE CP3 RESET. Run only against the intended disposable liq_graph DB.
-- Stops are manual: shut down every liq_graph process FIRST.
-- All pool manifests, Mint records and existing completion proofs will be lost.
-- Do not CASCADE and never drop unrelated application tables.
BEGIN;
DROP TABLE IF EXISTS public.liq_graph_pool_cache;
DROP TABLE IF EXISTS public.liq_graph_mint_catalog;
DROP TABLE IF EXISTS public.pools;
DROP TABLE IF EXISTS public.mints;
COMMIT;

-- Do not create tables here: persistence.rs is the single DDL source of truth.
-- Launch liq_graph; Persistence::connect creates public.mints and public.pools.
