# Integer division SDK prerequisites — intdiv-sdk-67

Round68 follows tso-timediff-66. This checkpoint removes six native SDK implementations, **not** the INTDIV evaluator. Functional progress stays **208/245**, strict final-audited count **0**;37 eligible families remain and13 more are needed for221. The latest functional migration remains tso-timediff-66. No new family credit, Go-package completion or PR readiness is claimed.

## Scope and ownership

Eight agents first mapped the full evaluator closure. Only B/G received write leases: B owns TiKV datatype `decimal.rs` plus native `decimal/mod.rs`; G owns native `overflow.rs` and `overflow_tests.rs`. All evaluator, scalar/vector, legacy and SQL files remain unchanged. Parent owns formatting, gates, Plan, guides, evidence and paired publication.

Three raw-coefficient APIs move to TiKV Decimal associated helpers: `native_raw_coefficient_i128`, `native_to_i64_trunc`, `native_to_u64_trunc`. Native public methods become thin calls plus exact Res-to-original-warning mapping. They do not use the normalized math bridge or inspect visible scale/declared shape. Original UTF8 panic wording, byte-index string slicing, scale-subtraction panic, saturation and fractional disposition order remain. Unsigned negative values return Overflow(0) before decoding or slicing coefficient bytes, including negative zero and malformed raw storage.

The i128 projection preserves signed text parsing followed by conditional checked negation. Canonical positive magnitude2^127 does not parse as i128, but raw text containing the minus sign can produce i128::MIN when the separate negative flag is false. Blanket rejection of all MIN results would change this API. The new static helper was renamed before compilation because the old private `native_coefficient_i128(&self)` already existed; its body/callers stay unchanged.

Three native division SDKs delegate to the existing public TiKV codec helpers `div_i64`, `div_u64_with_i64`, `div_i64_with_u64`. Native zero-divisor assertions and original OverflowError type/operand wording remain; TiKV's different wire diagnostic text is not forwarded. Unsigned/unsigned and legacy i128 policies are not changed. TiKV's error-string allocation is not claimed allocation-equivalent to the old native error structure.

## Explicitly unfinished INTDIV closure

The native integer leaf remains in `ops/integer_coerce.rs`, Decimal policy in `ops.rs`. Precision getter demand is0/1/2 inside the Decimal helper, with additional caller-specific pre-reads that must not be merged. The bounded warning quotient must be adjusted before warning replay, and a warning callback error must precede any integer overflow. Existing `/` profiles have different increment/error policy and cannot be substituted.

Current DecimalDivision metadata owns only precision and disposition; the actual Decimal stays in the output. It cannot hide a computed integer answer. A future two-stage solution must bind both calls to the same existing scope, including execution-only/NoColumns paths; the existing pack callback does not automatically expose its temporary scope. A single worker with a narrow actual computed report remains another option, not an established ABI.

Typed left-NULL and integer-vector NULL paths still bypass an INTDIV worker and require explicit future closure. Native generic coercion/error ordering must stay. Legacy `cophandler.rs` integer DIV uses raw i128 division for all five labels: MIN/-1 panic is actually reachable through JSON-to-integer saturation. Legacy Decimal DIV instead uses exact division and returns NULL for signed-i64 quotient overflow. Both legacy paths retain `(left?, right?)` evaluation: NULL left still demands right, left error stops it.

Public `div_rem`/`div_rem_unbounded` remain native this round. TiKV's existing exact-pair core should be reused later, not duplicated. Its budgeted entry rejects IntegerPair; the legacy single quotient may use the existing retained-quotient request after domain review. Native digit_divmod also serves div_round and cannot simply be deleted. No new exact-division API is added without a live consumer.

## Validation

Eight actual serialized Cargo launches:6 green gates plus2 unchanged old full-suite REDs. No new RED, compile failure, retry, interruption or zero-match run. All5 new tests pass (2 shared raw-projection tests,2 native facade tests,1 division-adapter test). Exact commands, timings and full raw hashes are in `../logs/intdiv-sdk-summary.txt`.

TiKV raw projections2 and existing local lifecycle317+1ignored pass. Native Decimal97, overflow18 and expression integer-division2 pass; filters overlap, so these are not additive unique totals. The existing SQL division regression passes1 test with both vector modes (four original queries); it is indirect SDK regression evidence, not new worker-root or INTDIV migration evidence.

Full expression **1554 passed/4 old failures/94 ignored**, unistore **208/1 old/13 ignored**. Complete failure sections and final lists equal tso-timediff-66 after only numeric panic-thread-ID normalization. Prior canonical digests remain `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637` and `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`.

All4 changed Rust sources (1 TiKV,3 native) pass pinned formatter checks and both diffs pass whitespace checks. Original test function bodies are byte-exact. The private projection name collision was resolved before compilation by renaming only the new helper and its new calls/tests. No production repair after a test failure, fixture recording or provider-output oracle replacement occurred. The other six agents performed read-only closure analysis; a mistaken RO div_by_u64 label was corrected to div_round after reading the source, and an initially guessed nonexistent path was corrected through actual discovery.

## Deferred and publication

No manifests/locks/generated tables/dependencies/Go/Bazel changes occurred. Original fixtures and tests are immutable. Performance, zero-copy, physical heap/peak/OOM, broad workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns roots and prior compatibility gaps remain deferred. The SDK step does not establish runnable SQL takeover or direct zero-slot INTDIV roots.

Plan copies and the paired TiKV SHA will be verified at publication, TiKV first then TiDB, without force push or PR. The old untracked client-differential BUILD.bazel stays excluded. Overall goal remains active.
