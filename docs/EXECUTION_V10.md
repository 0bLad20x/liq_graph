# V10 simulation

The common runtime connects Graph -> Signal Engine -> ExecutionCandidates -> V10.
See [RUNTIME.md](RUNTIME.md) for launch and dataflow acceptance.
ExecutionCandidates freezes current Prepared proofs and price observations into
complete requests. The executor independently validates requests because it also
accepts manual file input. It owns transaction packing, signing and simulation;
it has no Registry/DB/graph lookup, route search, manifest refresh or broadcast.

## Input and resources

The typed/JSON contract lives in `src/core/execution.rs`:

- `id`, expected `wallet`, `settlement` (WSOL/USDC).
- `groups`: homogeneous `direct` or `bridge` groups. Direct: `target`, `pools`.
  Bridge: `target`, `bridge`, one `intermediate`, `bridges`, `direct`.
- Optional `conversion`: the explicitly chosen Q/S pool, on the supported DEX
  represented by its manifest. No fixed Raydium-only selection.
- Every pool contains the existing `manifest` and existing two `mints` proofs.
  The manifest carries typed `bound_signer` (null for unbound pools).
  PumpSwap requires its real stored binding. Historical descriptors are upgraded
  from their existing wallet evidence when restored; the executor reads no evidence JSON.
  Manifest fields are `pool`, `mint_a`, `mint_b`, `validated_slot`, `bound_signer`, `base_a`, `base_b`.
  Each account has `address` and `writable`. `base_a` quotes mint A, `base_b` quotes B.
- `parameters` is an object. Omitted fields use the documented Rust defaults below;
  unknown fields are rejected. Mint addresses/programs are reused from completion
  data; there is no new mint RPC or proof-generation path.

Signer/keypair and shared RPC handle are startup resources. The private key is not
an input-contract field. Signing uses `ring` Ed25519, verifies the keypair and the
resulting signature, in the current implementation. `V10_SIMULATION_FAST=true` is the default:
the RPC replaces the blockhash and disables signature verification. Signed mode
(`false`) obtains a recent blockhash and uses signature verification; a
`BlockhashNotFound` result permits one blockhash refresh/re-sign. Skipping redundant
local signing in fast mode and reusing compiled messages remain CP8 work.

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

## Reusable request replay

Replay one existing complete request without a database export or implicit pool selection:

```powershell
.\scripts\v10_simulation_live.ps1 -WalletPath C:\keys\wallet.json -RequestPath .\request.json
```

The helper verifies a clean committed checkout, builds with a locked release build,
preserves the request and saves checkout/report/log artifacts under `v10_live/`.
It restores the caller's working directory. The native entrypoint remains:

```powershell
cargo run --locked --release -- --v10-simulate .\request.json --wallet C:\keys\wallet.json
```

## Results and remaining CP8 scope

Keep rejected input, RPC failure, simulation failure, success without observed DEX
calls and success with observed DEX calls separate. Recognition requires both a
declared program and declared pool in an inner instruction. Error numbers or compute
consumption alone do not identify a swap. No-op success does not prove profit.

Runtime acceptance requires complete validated signal input and a matching real
simulation response for every accepted request. It does not require on-chain success
to claim the request handoff. `execution_integration_passed` currently means at least
one successful simulation; it does not prove DEX calls or all four route forms.

CP8 separately addresses the private alternative-pair index, repeated message building,
redundant fast-mode signing and unused execution fields. Its functional proof must
report packing/rejection/simulation/DEX results per Direct2, Direct3, Triangle3 and
Triangle4, with account counts, CU, error codes and costs. Nothing is broadcast.
