# N64: owned child-result demand in the existing SDK frame

This validates an execution-protocol foundation. The actual Unistore legacy
source migration, full services and final unified performance remain incomplete.
The default old-tree entry regression is deliberately still RED and is reported
as an open new test, never as an inherited baseline failure or an ignored test.

## Why the protocol is needed

Actual integer legacy arithmetic evaluates both child Results before folding
SQL errors. Its RHS runs even after a left infrastructure failure. Division
returns before the RHS after a left failure; Decimal skips the RHS after left
NULL. The default SDK host protocol aborts on a child failure, so a real consumer
needs an explicit way to retain its original child error and continue demand.

FreshResult requests one fresh row child. On failure the same SDK frame loop
cancels descendant tasks, discards their frames and any old success, then moves
one sealed HostArgFailure to the requesting host. The original LocalError and
its boxed cause are neither cloned nor reconstructed. Default Fresh/Reuse and
the default error handler retain immediate failure. Window Result requests are
refused before input effects. Panic remains an unwind with normal task cleanup.

Only three SDK production files change: local/host.rs, local/mod.rs and
types/expr_eval.rs. No second expression driver, native evaluator callback,
value algorithm, SQL declaration, source seal or increased limit is introduced.
An opt-in TaskGuard ledger owns task/index/checkpoint/failure data and charges
actual capacity and retained reason bytes. Normal HostFrame layout, constructor
and storage calculation match the archived originals exactly. Foreign boxed
error heaps remain opaque, as with existing runtime errors; this is no total
statement-memory guarantee. Global step/depth/storage limits are never reset.

## Independent actual legacy observations

Before observers, archive657 files, including all Unistore source/test/build
inputs and signed-current expression/SDK inputs. Four independent sweeps freeze
940248 / B02C0B8B9BDDD00C records over163 actual SimpleSig variants,285 real
signature/metadata choices and9 typed/aggregate/public views. Raw bytes, float
bits, exact errors/panics, context events and reuse are retained. Two complete
record files are byte-identical,69113998 bytes, SHA256
253465873621b2cf473a0314c40a0dd32f55935afb0279561c1ca347a00e5eae.
The executed observer and source hashes are pinned before production changes.

Protocol controls prove actual opaque infrastructure identity, eager RHS versus
division short circuit, Decimal NULL behavior, full i128 MIN from JSON cast,
MIN/-1 panic, poisoned effective-lane reuse and fresh-lane recovery. One setup
incorrectly treated column-out-of-row as infrastructure; its failed prefix is
retained and the complete original uses a real public poisoned-scope failure.
One initial protocol incorrectly assumed the lane healthy after panic; the
actual original poisoning is now asserted. The original digest is never rebased.

Actual wire routing inventory covers218 old signature branches:100 remain
outside the shared catalog priority, fixed digest A6929D86CAE966A1. This is
metadata routing evidence, not execution coverage for fabricated PB inputs.

## Validation

| Gate | Result |
| --- | --- |
| New SDK result protocol | 9 passed |
| Complete SDK | 1162 passed; exact inherited1 failed;1 ignored;0.26s |
| Scoped Makefile clippy | passed; exact prior warning Counter,81 lib/89 lib-test |
| Complete frontend | 2358 passed; exact inherited3 failed;95 ignored;902.63s |
| Projection | 52 passed;119.41s |
| Expand | 6 passed; fresh Projection binary SHA authenticated |
| Actual Session SHOW | 31 passed; exact inherited2 failed;0 ignored;0.04s |
| Production check | passed;149 expression warnings |
| make lint | passed |
| Both formatter versions | passed;8 changed Rust files |
| Source/original/protected audit | passed;657 files;all value kernels unchanged |
| Original legacy matrix/protocol after SDK change | 2 passed;940248 digest unchanged |
| Complete Unistore | 198 passed; inherited3 plus1 explicitly open entry test;13 ignored;2.01s |

The new tests cover nested result barriers, boxed cause identity, RHS effects,
all six child error classes, exact descendant cleanup, stale-success removal,
global budgets, panic cleanup, provider disappearance and window refusal.
Frontend, SDK, caller and clippy warning counters match their previous complete
baselines. Native/Decimal/AST/routing controls pass in the complete frontend.

The open Unistore test observes old typed-tree entries [3,1,3,1,1,4,4]. Main
Unistore production remains unchanged except cfg(test) observation calls and the
observer declaration. No default source-graph migration is claimed. No test is
ignored to conceal this gap. The SDK change is independently reviewable while
actual genuine SimpleExpr/Shared graph composition continues immediately.

Reproduce from TiDB (the scoped clippy helper enters the SDK Makefile environment):

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --manifest-path ../tikv/Cargo.toml --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --manifest-path ../tikv/Cargo.toml --release --locked --offline -j8 -p tidb_query_expr --lib host_result_ -- --nocapture --test-threads=1
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib
    make lint

Workspace evidence uses n64-sdk-result-*, n64-result-final-*,
n64-result-source-audit.py and n64-result-final-check.py. Failed compile/setup
attempts and the superseded larger-frame prototype are retained separately.
Only local DCO checkpoints are authorized; no push, PR, release or deployment.
