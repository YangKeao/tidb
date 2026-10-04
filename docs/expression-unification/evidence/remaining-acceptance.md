# Remaining acceptance after the functional threshold

Status: **226/245 functional families; strict0; overall goal active**. The frozen denominator is unchanged. R86–R90 close IFNULL, IF, COALESCE, CASE and NULLIF selection through shared workers; see [IFNULL](ifnull-checkpoint.md), [IF](if-checkpoint.md), [COALESCE](coalesce-checkpoint.md), [CASE](case-checkpoint.md) and [NULLIF](nullif-checkpoint.md) evidence. Existing comparison/return-type/branch-cast adapters remain explicit. The19 remaining families are not approved blanket exceptions. This review is not itself a passing test receipt.

The accepted synchronous/scoped evaluator design stays in place. No universal compiler rewrite or exhaustive performance project is required to make the next functional steps. Conversely, reaching90% does not erase the Plan's remaining core ownership/demand obligations.

R91 adds a [Real/Float32→UNSIGNED CAST slice](cast-real-uint-checkpoint.md), **not a new family**. Its native rounding/wrapping/range/overflow algorithm now belongs to TiKV. Other CAST domains, outer NULL, UNION negative bypass and the native diagnostic formatter remain; legacy wire conversion has a distinct unchanged policy. All226 family objects and the19-family remainder are unchanged.

## Five core families still need closure

| Families | Remaining work / source evidence |
|---|---|
| cast | `cast.rs` and datatype `decimal/mod.rs` still own ordinary parsing/status policy. R83 float-constructor/Display sharing did not migrate general string-to-Decimal parsing. Close ordinary domains over shared SDKs; record truly exceptional domains separately. |
| in | `func.rs` still owns found-match/found-null reduction. Its existing eager candidate/comparison side effects cannot be replaced with wire early return. |
| greatest, least, interval | `builtin_ext/compare2.rs` retains extrema selection and INTERVAL's metadata-selected nullable-linear versus NOT_NULL-binary search. Preserve actual type, collation, precision and getter/search order. |

Observed caller chain: `evaluator.rs::run_with_consumer` → `Expression::eval` → `ScalarFunction::eval` → native control branches or PB dispatch. The R85 parent read the actual CASE/IF/IFNULL/COALESCE branches; R86–R90 removed IFNULL/IF/COALESCE/CASE/NULLIF runtime selection and share choice primitives, retaining distinct truth/error, proof and return-projection policies. NULLIF keeps eager operands and actual Eq before left preparation; its SQL zero-slot refusal is comparison-stage evidence, distinct from selector-isolating direct tests. CASE keeps AST base-once versus rewritten per-WHEN evaluation and original SQL branch casts. COALESCE has catalog facts but no PB/legacy admission; that boundary remains closed. Remaining conclusions are not inferred solely from registry names.

## Eight ordinary families still pending

| Family group | Remaining work |
|---|---|
| convert_charset | Actual encode/decode/replacement/retag policy in `convert_charset.rs`; shared GB leaves are prerequisites, not evaluator closure. |
| date_add, date_sub, extract | Calendar/unit algorithms and warning/type policies remain in `time_fn/calendar.rs`. |
| str_to_date, timestampdiff | Real format scanning and distinct errors, or civil/month difference policy; source in `time_fn/calendar.rs`. |
| tidb_bounded_staleness | Null/invalid-zero, range check, one demanded SafeTS getter, clamp and FSP3 in `time_fn/mod.rs`. Host supplies SafeTS input, not an excuse to keep all computation native. |
| json_sum_crc32 | Existing internal scalar-array algorithm in `builtin_ext/json/report.rs`. SQL ARRAY syntax is still rejected by the baseline; migrate the implemented domain without inventing new SQL admission. |

These are pending implementations, not approved whole-family exceptions merely because they take work.

## Six nontrivial exception candidates, not yet approved exceptions

| Families | Concrete boundary / why leaf-only sharing is insufficient |
|---|---|
| rand | `Columns` exposes a host-computed f64. Evaluator ownership needs actual RNG state, atomic advancement and const-node identity; passing a computed draw through a worker earns no credit. |
| json_schema_valid | Validator retrieval can access file/HTTP references. Document NULL/parse demand precedes construction/I/O; pure validation must not be hidden by relabeling the entire family host-only. |
| tidb_decode_plan, tidb_decode_binary_plan | Real plan codecs, Explain protobuf/tree rendering and distinct fallback/warning/missing-main panic policies, not base64 alone. |
| tidb_encode_sql_digest | Lexer and normalizer ownership in `tidb-parser`, not SHA256 alone. |
| validate_password_strength | Unicode classifications plus demand-driven identity, enable, policy and dictionary getters; native precomputed scoring is not migration. |

A final exception needs its supported signatures/domains, source entrypoints, minimal compatibility/effect example, retained implementation and removal condition. Current next-candidate notes are not that final approval.

## Request-root work

Two bounded R85 integration fixes (fail-before/pass-after receipts in [request-scope-checkpoint.md](request-scope-checkpoint.md)):

1. Literal rewriting formerly called the NoColumns entry despite PlanScopeResolver retaining a live statement context. R85 routes that capability into the existing scoped literal helper, preserving resolver timezone/modes, the old mode default and capture order.
2. `LegacyEvaluator::eval_shared` formerly dropped an available selected parent scope. R85 binds the original semantic context through existing `AsciiScope::with_columns`, preserving row/settings/warnings and active-child-scope priority. With no parent capability, the old standalone path remains.

Neither target creates a live DAG request owner or closes every default/fold/DML/range/aggregate/window wrapper. `RequestEvalContext` currently has no execution capability, and production `LegacyEvaluator::new` defaults raw_columns to NoColumns. Further caller/lifecycle integration remains necessary. Before-owner standalone PREPARE and explicit NoResolver defaults must not acquire a fabricated or stale statement owner.

## Final evidence still required

- Source/deletion and a small cross-entry matrix for the remaining core work: real-column projection/filter, exact PB signatures, legacy, fold/default/DML and aggregate/window parameters, including warning/error order and actual result metadata.
- Precisely classify preserved baseline versus introduced gaps: INTDIV raw-empty/nonzero behavior, zero-date CAST diagnostic, mode forwarding, JSON_KEYS aggregate, AST arity and derived collation. Do not repair original test expectations to obtain green status.
- Keep the known expression4/unistore1 full-suite failures visible until independently resolved. A threshold or independent source review is not a full-suite pass.
- Record dependency compile receipts and owned-transport/width-one costs without claiming benchmarks or zero-copy. Physical heap/peak/OOM, allocator fault injection, release performance, exhaustive differential/TiFlash/FIPS and dual-timezone footprint remain deferred under the user's speed preference.
- Scope-required lint and final integration gates remain separate from these targeted implementation receipts. No whole-package Go transcreation or PR-readiness claim is made.
