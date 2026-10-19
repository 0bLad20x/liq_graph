# V10 live error diagnosis

This user-requested diagnostic script runs the existing simulation command against
real RPC state. It does not add a Rust test suite or change production validation,
Registry, manifests in persistence, RPC policy, instruction encoding or sending.
Every request is a fresh copy of the supplied JSON. No database is needed.

The first scope is one direct group with exactly two different Meteora DLMM pools,
the same target/settlement pair, and no conversion. Use the saved SOL/USDC request
that already completed with `no_failure=true`:

```powershell
.\scripts\v10_diagnostics_live.ps1 `
  -WalletPath (Join-Path $env:USERPROFILE ".solana\meteora-arb-test.json") `
  -RequestPath .\v10_live\20261019T165716926Z\request.json
```

PowerShell 7.2 or newer and a clean committed draft checkout are required. The script builds
once with `cargo build --locked --release` and records branch, HEAD, tree, source
hash and public signer. Existing `RPC_*` settings are used by the native command.
The script never reads or prints keypair bytes itself.

## Scenarios

Each case runs with `no_failure=false` and `true`. The default is 18 invocations:
two reference pairs surrounding seven mutation pairs. Request IDs are unique;
apart from those tracing IDs and the explicit `no_failure` value, reference inputs
remain unchanged. Each mutation changes one semantic condition; keeping the same
false identity consistent can require updating several JSON fields.

| Scenario | Deliberate change | What is observed |
| --- | --- | --- |
| `baseline_before`, `baseline_after` | No semantic mutation | Reference behavior at the actual simulation slots |
| `mint_proof_mismatch` | First target mint proof names the wallet | Existing local identity guard; not an executor error mapping |
| `pool_is_settlement_mint` | First pool identity and both block references name the settlement mint account | Executor behavior for a wrong account type in the pool role |
| `target_is_wallet` | Group target and both pool mint identities/proofs consistently name the wallet | Behavior for a non-mint account in the target role |
| `foreign_target_vault` | First target vault replaced by the second pool's target vault | Behavior for a foreign vault with the same declared mint |
| `foreign_bin_array` | First final BinArray replaced by the second pool's final BinArray | Behavior for an array declared for another pool |
| `low_transaction_cu` | Transaction CU limit set to 1000 | Runtime budget behavior; executor allowance remains unchanged |
| `high_minimum_profit` | Minimum profit set to 1000000000000 base units | Response to that threshold; a particular result is not asserted |

The original file and database are unchanged. Foreign accounts come from the
second supplied manifest; their current initialization is not independently
assumed. The final account of this repository's DLMM block is a BinArray, after
the optional bitmap. No account decoder or protocol logic is added to the worker.

For a shorter targeted run, references are still included:

```powershell
.\scripts\v10_diagnostics_live.ps1 `
  -WalletPath (Join-Path $env:USERPROFILE ".solana\meteora-arb-test.json") `
  -RequestPath .\v10_live\20261019T165716926Z\request.json `
  -Scenario foreign_target_vault,foreign_bin_array
```

## Evidence and interpretation

`v10_live/diagnostics-<UTC>/` contains the source request, run metadata and
incrementally written `summary.json` / semicolon-delimited `summary.csv`. Each
numbered case folder contains `request.json`, `mutation.json` with changed JSON
paths and old/new values, `report.json`, and `worker.log`. Expected nonzero native
exits are recorded and do not stop the run. If the initial unchanged reference
with `no_failure=true` does not successfully reach the executor, mutations stop;
diagnose that baseline from its saved evidence first.

The summary records the existing worker outcome/phase, exit status, CU, context
slot, provider, executor invocation, recognized DEX calls, first failing program
in log order, raw simulation error and local error. `process_error` means no
readable worker report was produced; its stderr is retained, not guessed.
The first failing CPI can identify the originating program even when its error
later propagates through the executor. Always inspect the full invocation log.

These are empirical observations, not PASS/FAIL assertions or an official error
table. Local rejection does not describe executor behavior. Repeated `Custom:1`
does not establish one universal cause. A successful no-trade result does not
prove that every supplied account was validated or that a swap would succeed.
Live pool state changes between calls; compare both reference pairs and context
slots before attributing differences exclusively to a mutation.

The script has not been executed here: this environment has no PowerShell,
Cargo, RPC credentials or signer. User-run reports are required for live evidence.
