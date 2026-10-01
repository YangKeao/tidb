# Expression unification experiment

Checkpoint-ID: `mod-one-49` (previous: `like-two-48`).
**163/245 functional families, target221; strict final-audited acceptance0.** New family: MOD. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the Plan; no force-push or automatic PR.

## Shared implementation
- Public native Decimal remainder delegates to a checked TiKV Grow-remainder wrapper over the existing division loop. Exact wide values, hidden scales, dividend sign and normalized zero remain intact; no Fixed9 substitution or discarded quotient.
- Eight closed value recipes cover four native integer profiles, full-i128 legacy integer, native/legacy IEEE real and shared Decimal. Existing NULL/missing recipes are reused; no new carrier, result kind, metadata binding or driver.
- Value recipes require two real non-NULL inputs. Their computed NULL alone means zero divisor; native code then applies the original handler, retaining explicit input presence to distinguish genuine SQL NULL. Legacy remains silent. No host zero test or arithmetic fallback.
- Original signedness, nonfinite policies, typed/PB child demand and batch order remain distinct. Three NULL-admission seams are connected. Wire MOD, DIV/IntDIV and coefficient-fast tiers remain unchanged.

## Validation
| Gate | Result |
|---|---|
| Shared / native Decimal | 87 / 24 passed |
| TiKV MOD / local | 20 / 289 passed; local1 ignored |
| Native MOD | 35 passed, 2 ignored |
| Legacy / SQL | 1 / 2 passed |
| Full expression | **1490 passed, 4 old failures, 94 ignored; exit101** |
| Full unistore | **200 passed, 1 old failure, 13 ignored; exit101** |

Nine actual test runs; seven focused gates passed first try. No compile failure, new RED, retry or zero-match. Full failure sections match the prior checkpoint after numeric panic-thread IDs only. Three original complete test modules remain byte-identical; no original SQL expectation/fixture changes. Eleven added focused tests.
17 Rust sources (TiKV7/native10), no dependency/manifest/lock changes; pinned formatter/diff checks.
Exact commands and all9 whole-log hashes: [summary](logs/mod-one-summary.txt), [evidence](evidence/mod-one-checkpoint.md), `checkpoint.json`.

## Remaining work
82 eligible families remain. DIV still needs explicit full-u32 precision and actual Decimal disposition; IntDIV needs warning-before-conversion preservation. These are not replaced with Fixed9 or fake SQL operands. JSON renderer closure, FORMAT/DATE/MICROSECOND, broader request-root integration, physical heap/peak/OOM, differential tests, M6, TiFlash, release, whole workspace and lint remain unverified. Existing compatibility exceptions and parser/full-suite failures remain unresolved. No whole Go-package/type-domain or final performance completion claim.
