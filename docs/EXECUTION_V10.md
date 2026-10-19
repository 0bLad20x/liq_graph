# V10 transaction simulation

The user generated `Cargo.lock` with Cargo's online resolver. The exact supplied
diff adds only `ring` to the root package dependencies; its Git blob starts with
`3b8ba46`. Dependency versions are unchanged. That generated result is committed
with the signing dependency. The user built committed HEAD `830ad06`, tree
`208441b073ac38767b4fce4d6d5d082a42c693d2`, successfully with
`cargo build --locked --release`. The subsequent live diagnostic run completed
18 cases, including successful signed direct SOL/USDC simulations without DEX
calls. The cleanup after that run changes only documentation and removes its
temporary diagnostic files; the application code and dependencies are unchanged.
The implemented scope is transaction building, signing and simulation. Sending
and the production Signal -> Graph -> ExecutionRequest connection remain open.

`ExecutionRequest` describes exactly one transaction. Its producer selects all groups,
pool alternatives and any conversion pool before handing the job to `execution_v10`.
The worker has no Registry/DB/graph access, ranking, pool replacement, splitting,
manifest refresh, DEX decoding, or send operation. Production `--run` is unchanged.

## Input and resources

The typed/JSON contract lives in `src/core/execution.rs`:

- `id`, expected `wallet`, `settlement` (WSOL/USDC).
- `groups`: homogeneous `direct` or `bridge` groups. Direct: `target`, `pools`.
  Bridge: `target`, `bridge`, one `intermediate`, `bridges`, `direct`.
- Optional `conversion`: the explicitly chosen Q/S pool, on the supported DEX
  represented by its manifest. No fixed Raydium-only selection.
- Every pool contains the existing `manifest` and existing two `mints` proofs.
  The manifest now carries typed `bound_signer` (null for unbound pools).
  PumpSwap requires its real stored binding. Historical descriptors are upgraded
  from their existing wallet evidence when restored; the executor reads no evidence JSON.
  Manifest fields are `pool`, `mint_a`, `mint_b`, `validated_slot`, `bound_signer`, `base_a`, `base_b`.
  Each account has `address` and `writable`. `base_a` quotes mint A, `base_b` quotes B.
- `parameters` is an object. Omitted fields use the documented Rust defaults below;
  unknown fields are rejected. Mint addresses/programs are reused from completion
  data; there is no new mint RPC or proof-generation path.

Signer/keypair and shared RPC handle are startup resources. The private key is not
an input-contract field. Signing uses `ring` Ed25519, verifies the keypair and the
resulting signature, and simulation always uses `sigVerify=true` without replacing
the blockhash. A `BlockhashNotFound` result permits one blockhash refresh/re-sign.

Parameter defaults: `minimum_profit=0`, `trade_size=0` (executor auto sizing),
`use_flashloan=false`, `no_failure=false`, `constant_conversion=false`,
`executor_compute_unit_limit=1000000`, `transaction_compute_unit_limit=1400000`,
`loaded_accounts_data_size_limit=67108864`, `priority_fee_lamports=0`,
`additional_fee_bp=0`, `additional_fee_collector=null`.
Profit/size are settlement base units; priority fee is absolute lamports.
No parameter is overwritten by a capacity-test mode.

The worker validates four groups / sixteen declarations including conversion,
the exact resulting account set (this worker uses a 64-account boundary), instruction
references and 4096-byte v1 wire size. Shared pools across groups are allowed with
consistent proof versions and count once per declaration. An oversized request is
rejected unchanged. Account blocks are never shortened or rebuilt.

## Local real-chain simulation

Use a clean committed checkout and a funded mainnet wallet (or the explicit
flashloan option), with existing `RPC_*` settings in `.env`. New generic transport
capabilities are `RPC_N_GET_LATEST_BLOCKHASH` and `RPC_N_SIMULATE_TRANSACTION`;
both default true like the other ordinary RPC reads. No provider-specific V10 client.

A complete request needs no database:

```powershell
.\scripts\v10_simulation_live.ps1 -WalletPath C:\keys\wallet.json -RequestPath .\request.json
```

For a first direct-group probe, explicitly choose two persisted Prepared pools with
the same target/settlement pair. The helper exports only these two supplied addresses
using the existing `DATABASE_URL` and PostgreSQL client; it does not search routes:

```powershell
.\scripts\v10_simulation_live.ps1 -WalletPath C:\keys\wallet.json -PoolA POOL_ADDRESS_A -PoolB POOL_ADDRESS_B -Settlement WSOL
# Optional: -UseFlashloan -MinimumProfit 0
```

This manual cache source is a historical snapshot, not live Registry admission or a
manifest refresh. A later production producer must supply the current admitted
Prepared data through the same contract. Live signal detection, qualitative graph
selection and deciding how many requests follow one signal are not implemented here.

The helper runs `cargo build --locked --release`, records branch/HEAD, preserves the
exact request, and saves `report.json` plus `worker.log` in a new `v10_live` directory.
The native equivalent is:

```powershell
cargo run --locked --release -- --v10-simulate .\request.json --wallet C:\keys\wallet.json
```

## Evidence and acceptance

The report retains every declared pool/quote/manifest slot, exact resource counts,
blockhash validity, simulated signature, RPC provider/context, raw simulation error,
logs, inner instructions and projected accounts. It never claims a signature was sent.

Distinguish `rejected`, `rpc_failure`, `simulation_failed`,
`simulated_no_dex_call_observed`, `simulated_dex_calls_observed`. A recognized DEX
call requires both a declared program and a declared pool in the inner instruction.
No cause is guessed from `Custom:1` or consumed compute alone. No-op success is not
proof of a swap or profit; observed DEX calls are not a net-profit guarantee either.

Build success is a prerequisite. Functional acceptance requires a real request that
reaches the executor with the expected accounts, and inspection of the simulation
report. Failure reports remain evidence for diagnosis, not successful acceptance.
The direct-group evidence above is the current live record. The diagnostic run
also demonstrated local identity rejection, executor InvalidAccountData responses
to the four tested account mutations, and ComputationalBudgetExceeded at 1000 CU.
Unchanged references before/after and the high-profit-threshold case returned
Custom:1 with no_failure=false and success without trading with no_failure=true.
This supports the No-Trade interpretation for the tested case, not a universal
Custom:1 mapping. The detailed observations and remaining execution evidence are
tracked in [issue #32](https://github.com/0bLad20x/liq_graph/issues/32).

Swaps, bridge/conversion routes, flashloans and other DEX paths remain unproven.
The temporary diagnostic script and its separate guide were removed at the user's
request; the issue links their historical committed version. The ordinary
`scripts/v10_simulation_live.ps1` and native simulation command remain available.

Primary references:
- https://www.solanamevbot.com/docs/onchain-bot/onchain-program/v10
- https://github.com/Cetipoo/solana-onchain-arbitrage-bot/blob/master/src/v10.rs
- https://solana.com/docs/core/transactions/versioned-transactions
- https://solana.com/docs/rpc/http/simulatetransaction
