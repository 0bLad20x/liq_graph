# CP3 — Central Persistence & Dual Registry

Status: **Draft — not accepted without `cargo build --locked` and real live provider evidence.**

## Contract

- Exactly two project tables: `public.mints` and `public.pools`.
- All SQL ownership belongs to `src/workers/persistence.rs`.
- Pool and mint state live in their respective RAM registries, restored on startup.
- Discovery, Jupiter, DexScreener and RPC are data sources; the graph and execution are downstream and are not a database authority.
- Pool/Mint addresses are Base58 TEXT. Standard values are typed SQL columns; protocol-specific pool configuration and versioned V10 manifest/evidence are JSONB.
- Do not silently migrate or reuse the prior bytea/JSONB schema. The database reset is **manual and destructive**, limited to the two project tables; do not drop unrelated tables.

## Live acceptance

1. Verify branch and clean working tree; run `cargo build --locked` on this branch.
2. In a disposable project database, explicitly drop `public.liq_graph_pool_cache` and `public.liq_graph_mint_catalog` after confirming scope. Start with no pool/mint tables.
3. Start full runtime with actual providers; observe real Discovery, Jupiter, Pool Registry, Mint Registry and central Persistence writes.
4. Stop, restart, verify Pool and Mint RAM restores, valid Base58 identities and fresh updates.
5. Verify Prepared/Incompleted statuses, manifest evidence and TVL source timestamps with SQL. Do not mistake stored TVL as live TVL after restart.
6. Scan `src` to ensure only `src/workers/persistence.rs` references PostgreSQL SQL.

A successful build alone is not completion evidence.
