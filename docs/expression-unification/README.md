# Expression unification experiment

Checkpoint-ID: `aes-two-51` (previous `div-one-50`).
**166/245 functional families; target221; strict final acceptance0.** New families: AES_ENCRYPT/AES_DECRYPT. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the same Plan; no force-push or automatic PR.

## Shared implementation
24 fixed AES direction/key-width/mode recipes plus an actual NULL witness use existing Bytes carriers and results. The shared crypto module owns original mode loops, padding and key folding; native utilities are facades. A narrow opaque block API preserves the native random-access CTR layer; independent GCM keeps its direct AES dependency. The RustCrypto primitive version remains0.9.1.

Native SQL retains guarded lazy coercion/demand and ECB ignored-IV warnings, not cipher/IV/key algorithms. Only exact typed short-IV causes plus this-call receipts become SQL errors. Cipher-domain failures remain successful NULL; infrastructure failures do not. No new carrier, result type, binding, driver or PB/legacy admission.

## Validation
| Gate | Result |
|---|---|
| Shared crypto / native encrypt | 6 / 21 passed |
| TiKV AES / local | 4 / 293 passed; local1 ignored |
| Native AES / SQL | 5 / 3 passed |
| Full expression | **1494 passed,4 old failures,94 ignored; exit101** |
| Full unistore | **201 passed,1 old failure,13 ignored; exit101** |

Nine test attempts: eight actual runs and one compile failure caused by prematurely removing the GCM dependency; restored and retried successfully. Three offline lock commands. One separate formatter-path failure was corrected before Cargo. Six final focused gates pass; full failure sections equal the previous checkpoint after thread IDs only. No original fixtures changed. Nine new tests include fixed Go/NIST vectors and168 direct-column zero-slot SQL cases.13 Rust sources pass final scoped formatter/diff checks.

Exact commands, incidents and hashes: [summary](logs/aes-two-summary.txt). Review map and limitations: [evidence](evidence/aes-two-checkpoint.md), `checkpoint.json`, and the root Plan.

## Remaining work
79 eligible families remain; target needs55. IntDIV ordered warnings/conversion and six-comparison native JSON closure are next-batch candidates only. Broader request-root integration, physical heap/peak/OOM, differential/M6/TiFlash, release/performance, whole workspace and lint remain unverified. Existing parser/full-suite failures and extreme Decimal release-shape exceptions remain unresolved. No complete Go-package/type-domain or FIPS claim.
