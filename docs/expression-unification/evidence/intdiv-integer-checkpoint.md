# Integer DIV evaluator slice — intdiv-integer-68

Round69 follows intdiv-sdk-67. This is a partial evaluator checkpoint, **not complete INTDIV migration**: native integer and legacy i128 execution move to TiKV; native bounded Decimal and legacy exact Decimal policies remain native. Functional count stays208/245, strict0,37 eligible families remain and13 more are needed for221. Latest whole-family functional migration remains tso-timediff-66.

## Actual computation and boundaries

Five fixed profiles use existing carriers/results: `IntDivIntSsNative`, `IntDivIntUsNative`, `IntDivIntSuNative`, `IntDivIntUuNative` take actual Int2 bits and produce OwnSignedInt; native pack restores Int for SS and UInt bits for the others. Nonzero SS/US/SU use existing TiKV codec helpers; UU uses actual u64 division. Actual zero divisor enters the worker and returns None, after which the frontend replays its original division-by-zero policy. It is never disguised as an observed NULL input.

The existing arithmetic-operation enum gains IntDivide with SQL name DIV, distinct from `/`. Native overflow retains the existing typed arithmetic cause, admitted only with exact operation/kind identity. This is not a new cause type, result kind, carrier, binding, driver or wire policy. Existing real/Decimal generic helpers reject the new operation rather than accidentally admitting Decimal DIV through `/`.

`IntDivInt128Legacy` takes actual Int1282 values and returns OwnInt128 through the existing exact16-byte little-endian transport. There is no i64/u64 narrowing or signedness reinterpretation. Zero produces None; MIN/-1 still executes ordinary Rust division and panics. JSON-to-integer saturation makes this panic reachable. Production does not catch or convert it; existing poisoned-worker lifecycle applies if a caller catches it. Output bytes are charged before conversion to the retained scalar.

Native integer business division is removed from `integer_coerce.rs`. Original coercion/unsupported-operand ordering, caller-specific precision pre-reads and NO_UNSIGNED_SUBTRACTION demand remain. Only actually reached NULL exits now use the existing true NullWitness worker, including typed left-NULL and integer-vector NULL. Typed left-NULL still skips the right child; vector evaluation retains whole-left-batch then whole-right-batch order. Non-NULL Decimal computation is unchanged.

All five legacy integer labels share the one i128 profile. The original `(left?,right?)` evaluation occurs before Missing/Null/Pair classification: NULL left still evaluates right; left error stops it; extra children are ignored. The separate legacy Decimal branch is unchanged. No native PB or parser admission is added.

## Ownership and validation

Eight agents work under exclusive leases: H kernel, C four closed CPP files, D native bridge/tests, A native integer/coercion NULL exits, G two typed NULL shims, E legacy integer leaf, F SQL lifecycle tests, B read-only transport/overflow audit. Parent owns formatting, serialized gates, Plan, evidence and publication.

Eight actual serialized Cargo launches:6 green gates and2 unchanged old full-suite REDs. All11 additive tests pass on their first gates; no compile failure, new RED, Cargo retry, interruption or zero-match run. Exact commands, timings and raw hashes are in `../logs/intdiv-integer-summary.txt`.

CPP integer-division4 and local319+1ignored pass; native expression5, typed NULL1, legacy1, SQL2 pass (filters overlap). SQL pins10 integer results across both vector modes=20 observations, eight direct zero-slot probes, and one warning-before-integer-overflow check. This is genuine integer-path takeover evidence without CAST/FMT/WHERE/ORDER masks, not Decimal or whole-family evidence.

Full expression **1558 passed/4 old failures/94 ignored**; unistore **209/1 old/13 ignored**. Entire failure sections and final lists match intdiv-sdk-67 after only numeric panic-thread-ID normalization. Prior canonical digests remain `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637` and `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`.

All12 changed Rust sources (5 CPP/7 native) pass pinned formatter checks; both diffs pass whitespace checks. Original test bodies and the two non-NULL Decimal implementation blocks are byte-exact. An audit script initially guessed a nonexistent next-function anchor and failed after printing valid receipt checks; source grep/read identified `decimal_binary`, and the corrected block comparison passed without any code/test change. This was a source-audit correction, not a Cargo failure or a hidden successful run. No production/test repairs, fixture regeneration or expected-value recording occurred.

## Still unfinished

INTDIV is deliberately absent from credited family IDs. Its remaining non-NULL native Decimal policy needs original0/1/2 precision reads and warning-before-integer-overflow ordering; legacy Decimal needs shared exact quotient/SDK closure. No two-stage or computed-report ABI for these policies is established here. Earlier raw SDK work preserves the complete signed-text projection domain rather than blanket-rejecting i128::MIN.

Whole workspace/lint/dev/bazel_prepare/release, performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns coverage and prior JSON_KEYS/AST/SQL metadata/parser/GB/vector/Decimal gaps remain deferred. Tests never use provider output as an oracle. No fixture recording, dependency/generated/Go/Bazel changes or whole-package/PR-readiness claim.

Publication is TiKV first then paired TiDB, with identical committed Plan copies, no force push or PR. The old untracked client-differential BUILD.bazel stays excluded. Overall goal remains active.
