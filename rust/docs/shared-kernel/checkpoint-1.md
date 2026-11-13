# Shared expression kernel: first runnable checkpoint

This is an opt-in prototype from original TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d and TiKV548812e1ef57aef077a2062a9cc356640a6347f5. It is not completion of M1 or the full project. Existing production routes and historical experiment worktrees remain separate. The paired SDK checkpoint is52d7be276763f6b527ea52e11005c9e21231bdc7 on expression-shared-kernel-m1.

Native Expression/Chunk MD5 and real protobuf MD5 now call the original TiKV RpnExpression::eval_decoded loop. The native adapter iteratively compiles the admitted tree. Flat IF control edges execute inside that same loop, currently with literal integer/NULL conditions. Full native f64 and expression Decimal constants survive transport without conversion to NotNan or fixed-size storage. The Decimal expression carrier retains coefficient data, stored scale, displayed scale and declared shape. Separate checked numeric-parts accessors support fixed-size Decimal algorithm bridges.

The checked admission is deliberately narrow: default-collation VarString columns, MD5 with explicitly typed byte inputs, numeric literals, and literal-condition numeric IF. Byte-literal output identity, dynamic IF, implicit argument conversions, wide-value arithmetic, invalid JSON/constant regexp guards, invocation mode, RAND/user variables/BENCHMARK and multi-root ordering remain pending. Unsupported native trees fail preparation; no runtime fallback invokes the old evaluator. Existing PB builders never emit native extended values; existing executor matches reject those variants explicitly.

## Validation

On home-gentoo, use the isolated wrapper `/home/agent/tidb/expression-shared-kernel/tools/cargo-tidb`, pinned nightly-2026-08-22, GCC14, one Cargo job, target `/home/agent/tidb/expression-shared-kernel/target-tidb`. From `tidb/rust`:

    ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib shared_kernel --locked --offline -- --list
    ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib shared_kernel --locked --offline -- --nocapture

The initial list contained three tests, all passed. The final checkpoint adds one branch comparison and runs **4 passed, 0 failed, 1323 filtered out**. It covers native/PB MD5, shuffled duplicate selection[7,2,7], NULL, empty selection, out-of-range input, rebinding, original-native MD5 value comparison; IF true/false/NULL against original native evaluation; selected-IF NaN payloads, negative zero and infinities;82-digit Decimal identity and declared shape; hidden8/7 coefficient1142857142 at storage scale9;40-digit exact parts and invalid-parts rejection.

`make lint` passed. `make bazel_prepare` was attempted for the new worktree but failed because bazel is not installed. No Go/Bazel inputs changed. Both diffs pass `git diff --check`. A separate check of the SDK executor crate with its original lockfile is running at checkpoint preparation; SDK full unit tests, clippy and service/SQL/performance checks are not yet verified. Do not infer any throughput, CPU or allocation improvement from these tests.

## Failures and corrections

The first Cargo path had one excess parent traversal; corrected before compiling product code. Initial compilation then found a Vec/SmallVec conversion and missing extended-enum match/imports; corrected. A check launched from the SDK directory loaded a different Cargo configuration and was interrupted before returning to the documented TiDB Rust directory. First test link failed because newly resolved kvproto introduced grpcio0.13 alongside the original0.10. Cargo.lock now pins original SDK kvproto623e58e60fa95c26b356cc6b9c266b0418a08710 and tipb5f9928e91afe4ac82c770cc9cd5fb2c804a7555e; no linker suppression or dependency source rewrite was used. Original failed logs remain in the agent workspace `evidence/shared-kernel-refactor/`.

## Size and coverage

As this is the first checkpoint from the originals, incremental and original-base source counts coincide. TiDB production is +388/-0, tests +209/-0; SDK production is +205/-16. Combined production/tests are **+802/-16, net786, total818 changed lines**. Build manifests/lockfile add+1866/-91 and are counted separately, not ignored. Documentation and generated inventory also count in all-file Git totals; they are not production compatibility code.

The inventory contains exactly245 frozen identities and five explicit exceptions. Its191 matched semantic candidates,48 migration-inspection candidates and one MD5 direct candidate are planning classes, not compatibility credit. Names/aliases and fixed-base file anchors are provided so reviews can replace candidates with verified decisions. It does not establish the full algorithm-migration budget. The provisional file budget remains in inventory-notes.md; all necessary moves and old algorithm deletions must count in the full result.

Next work remains under task33: shared demanded input and dynamic control, wide-value operation policy, explicit scalar/vector invocation and original PB demand, then state/repeat/group execution. Designer reviews this runnable checkpoint in parallel. No completion or performance claim is made for those pending gates.
