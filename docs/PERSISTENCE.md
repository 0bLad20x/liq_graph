# Persistence and Completion ownership

PostgreSQL is a bootstrap/persistence cache. No price or execution hotpath reads SQL.
Persistence is the single database writer; Registry is the sole in-memory pool owner.
CompletionLifecycle dispatches protocol validation and refresh, and Registry accepts
typed results before Persistence stores them. MintQualification remains the owner of
economic mint qualification and admission inputs.

`public.pools` stores pool_address, venue, mint_a, mint_b, tvl_usd, tvl_source,
tvl_observed_at, pool_config and completion. `public.mints` stores the mint catalog.
The authoritative schema and encode/decode rules live in `workers/persistence.rs`.
Historical TVL is not promoted to current live TVL on restore.

A null completion means Unknown. Prepared completion includes validated manifests,
mint proofs and evidence; Incomplete stores a typed reason and validation metadata.
The persisted `validated_slot` and `checked_at_ms` belong to the outer completion
object. The inner manifest contains account blocks; it does not repeat a slot.
Restoration reconstructs the typed ExecutionManifest with the outer slot. SQL-NULL
comparison against an imaginary inner slot cannot establish proof agreement.

Full validation establishes the persisted proof. A lightweight Unchanged refresh
does not extend that full proof's timestamp or rewrite it. Ongoing refresh remains
active in the shared runtime. Live Completion may be exercised independently with
`--completion-lifecycle-live`; venue and limits use its existing environment settings.
Isolated real Jupiter/DexScreener enrichment remains available via `--cp1-live`.
Historical shell/SQL acceptance and reset tools are removed; there is no parallel
SQL-derived admission or manual manifest producer in the runtime.

Existing migrations are already applied to the current repository's database schema.
This cleanup executes no SQL and changes no persistence format or schema. A new
installation uses the current worker schema, rather than a historical migration script.
