# Six comparison evaluators — compare-six-53

Previous: `compare-substrate-52`. Added frozen families **eq/ne/lt/le/gt/ge**: functional **172/245**, required **221**, strict final acceptance **0**. This is a functional delegation/native-deletion checkpoint, not PR readiness or full compatibility/performance acceptance. Paired commit and Plan hash are in `../checkpoint.json`.

## Review map and ownership

Six exclusive parallel owners; parent-only integration, formatting, serialized Cargo, guides/Plan and paired publication.21 existing Rust files (TiKV7/native14), no source-file, dependency, manifest or lock addition.

| Layer | Files / responsibility |
|---|---|
| Shared kernels | TiKV `components/tidb_query_expr/src/impl_compare.rs`, `lib.rs`: finite predicate identity, fixed kernels, reexport |
| Closed protocol | TiKV `local/{batch,compile,registry}.rs`, `types/{function,expr_eval}.rs`: roles, unit metadata, strict admission, result ownership, ordinary-call refusal |
| Native preparation | `rust/crates/tidb-expr/src/ops.rs`, `ops/{integer_coerce,real_coerce}.rs`: source coercion/demand only; old six-predicate answers removed |
| Native bridge | `tikv/{evaluated_ascii,evaluated_ascii_tests,mod}.rs`: existing pool/materializer, five narrow public legacy SDKs, lifecycle tests |
| Typed/batch/PB/row | `scalar_function.rs`, `scalar_function/pb_builtin.rs`, `evaluator.rs`, `row.rs`, `lib.rs`, `func.rs`: actual-value dispatch and context threading; func change only row-IN import/call |
| Legacy and SQL | `rust/crates/tidb-unistore/src/cophandler.rs`; `rust/crates/tidb-session/src/tests_core/lifecycle.rs`: existing30 signatures and fixed SQL coverage |

Native paths abbreviated under `rust/crates/tidb-expr/src/` where applicable. The preceding JSON/Time/Decimal datatype substrate is unchanged.

## Closed evaluator contract

`ComparisonOp::{Eq,Ne,Lt,Le,Gt,Ge}` is a finite payload on each explicit domain/profile identity. Derived equality/hash includes the payload; native pool, affine scope, factory and result checks retain that complete identity. Each selector picks six **distinct actual generated metadata functions**, not one evaluator with an operation-code operand. Runtime metadata remains `()`.

| Profile | Existing actual-value carrier | Shared policy |
|---|---|---|
| IntSs/Su/Us/UuNative | Int2, preserving raw u64 bits | Existing signedness comparers |
| Int128Legacy | Int1282, LE16 byte slots | Full i128 order, no narrowing |
| RealNative | Ieee754Bits2, LE8 | Literal IEEE predicates: NaN only NE true, signed zero equal |
| RealLegacy | Same physical role, distinct identity | Original total_cmp including NaN payload/sign and signed zero |
| DecimalNative | Decimal2 with real remaining-budget third slot | Existing Grow Ord, all words, no Fixed9 conversion |
| BytesNative | CollatedBytes2 with checked tag third slot | Shared native collation policy |
| VectorNative | NativeVector2 | Existing vector comparer |
| TimeCoreNative | TimeCoreBits2, LE8 byte slots | Shared raw calendar-core comparison |
| DurationNative | Int2 signed nanoseconds | Signed order |
| JsonNative | Bytes2, each actual `[type_code || payload]` | Shared native raw JSON comparator, not wire JSON or canonical text |

Thirteen profiles × six predicates = **78 fixed value kernels**. Successful value results are owned signed0/1. New value profiles require actual nonnull first operands at both facade and official readiness; IEEE Undemanded is refused. Existing nullable operations remain unchanged; budget/tag auxiliary checks remain real checks.

`CompareNullNative` consumes a genuine `NullWitness(None)` and rejects Some. `CompareMissingLegacy` consumes NoArgs. Both return owned NULL. There is no fake RHS, new carrier/result kind, binding lifecycle, driver, ordinary local-call access, or PB/legacy admission. Malformed raw JSON payload policy remains the preceding substrate's policy; missing type-byte transport is infrastructure failure. Count/resource/role failures are not SQL NULL or overflow.

## Native and legacy integration

Native guarded preparation retains sentinel checks, unsigned metadata reinterpretation, vector casting/NULL, JSON string parsing and raw conversion, byte-domain precedence, Raw rejection, temporal and duration domains, Decimal-column/string-constant precision exceptions, mixed-string numeric conversion order and numeric promotion. Temporal preparation returns parsed actual Time/duration values; text-left reverses operands, not a host-computed Ordering. Only the shared wrapper computes the six scalar predicate answers.

Typed integer comparisons still demand both children. Generic typed values retain prior literal casting. Numeric batch eligibility, conversions and physical selection order remain; rows submit actual Integer/Decimal values or genuine NULL, and masks project computed -1/0/1. Real context now reaches batch evaluation. Existing PB admission remains only EqInt/GtInt, including its left-NULL RHS stop.

Exactly30 legacy signatures (six predicates × Int/Real/String/Decimal/Time) use narrow `eval_legacy_*_comparison_in` SDKs through the same pool. Int evaluates both children before left/right SQL folding; other domains preserve left-error stop but demand RHS after NULL. Absent children and evaluated NULL remain distinct. Bytes resolve the original request collation ID only for Values inside the guard, retaining global enable state and unknown-ID fallback. Decimal conversion and calendar-core extraction also stay guarded. Results are validated worker booleans, then widened to legacy i128, never host comparison results. No legacy JSON/vector/duration signature is invented.

## Explicit row compatibility boundary

Rows keep original Eq-then-Lt decision order, including the old NaN behavior where GT/GE use NOT(Lt), not scalar GT. Actual false/NULL/last-true leaf results are projected; NE/GT and all-equal LT reuse shared NOT. Empty rows retain their original structural zero-operand identities without fabricated scalar witnesses. Default derivation-free collation, precision4 and literal operand descriptors are unchanged.

**Intentional context activation:** `row_compare_in` now receives real statement context from AST and row-IN. Mixed text/numeric preparation observes truncate policy; Time/text preparation observes date modes, timezone and warnings. Previously NoColumns supplied defaults and silence. This is a recorded compatibility change, **not complete equivalence to that old contextless behavior**. Two narrow row tests pin activation and composite/NULL/empty behavior. No hidden origin tag or second runtime is introduced to emulate the old omission.

NullEq is outside these six families; its existing malformed-time row panic is not repaired. Pure contextless sorting SDKs, remaining integer comparison utilities used by other families, and broader request-root closure remain explicit separate scopes. No global integer-helper or whole-type deletion claim follows from removing these six runtime calculations.

## Verification and actual corrections

[Exact commands and hashes](../logs/compare-six-summary.txt):12 attempts,11 actual runs and one compile failure. Seven final focused gates: shared kernels29, local295/1ignored, native profiles24, existing comparisons67/5ignored, legacy2, SQL2 and instrumentation1.

Final full expression:1498 pass/4 old failures/94 ignored. Full unistore:203 pass/1 old failure/13 ignored. Both exit101. Complete failure sections equal `compare-substrate-52` after numeric panic-heading thread IDs only, with unchanged `27654f2c…`/`b64dcced…` hashes; no source-address mapping.

Actual incidents, not fabricated RED evidence:
1. A new local test incorrectly used the wire parser for84digit Grow coefficients. Construction alone changed to the existing Grow API; digits, width and fixed expected result stayed unchanged.
2. Three E0603 errors came from private collation-module paths in the new SDK. Existing public root exports fixed them; zero tests ran in that attempt.
3. Full expression exposed one existing instrumentation test: newly shared comparisons add two calls to NOT IN and NOT BETWEEN. Counts were source-derived as Eq+Eq+NOT=3 and Ge+Le+AND+NOT=4; SQL strings/results were not edited. Focused and full reruns confirm the repair.
4. Before compilation, unistore visibility required five narrow typed public SDKs, rather than a nonexistent generic public ready API. This design correction is distinct from the actual compile failure.

Thirteen new tests cover all78 identities, roles/readiness/unit metadata/ordinary refusal, payload cache switching, fixed numeric/NaN/wide/collation/vector/time/JSON policies, legacy demand/presence/infra and SQL. SQL uses11 fixed pairs,180 direct-column zero-slot calls plus36 filter/row calls; no unrelated WHERE-id/ORDER/HEX expression can mask ownership failure. Original SQL expected values and fixtures are unchanged; only the described existing instrumentation counts changed. Scoped21-source pinned formatter and both diff checks pass.

Architecture-index and maintenance-guide updates describe these actual paths and the row compatibility boundary, without adding repository policy. Whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy, physical heap/peak/OOM/M6, exhaustive old-row-context/domain differential,150-row differential/TiFlash/FIPS and prior parser/GB/ignored-vector/extreme Decimal exceptions remain unverified or deferred.73 eligible families remain;49 more are needed for221.
