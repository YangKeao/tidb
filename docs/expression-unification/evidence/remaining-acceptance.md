# Remaining acceptance after the functional threshold

Status: **228/245 functional families; strict0; overall goal active**. R94 closes [TIMESTAMPDIFF](timestamp-diff-checkpoint.md), including distinct ordinary/Shared PB text and manual legacy raw-core workers, with shared underlying temporal difference types/math. R93 closes [TIDB_BOUNDED_STALENESS](bounded-staleness-checkpoint.md) through actual endpoint/SafeTS workers, retaining original casts and warning delivery. SQL evidence covers the current absent-SafeTS lower-bound behavior; nondefault SafeTS clamps have direct tests, not invented storage evidence. The frozen denominator is unchanged. R86–R90 close IFNULL, IF, COALESCE, CASE and NULLIF selection through shared workers; see [IFNULL](ifnull-checkpoint.md), [IF](if-checkpoint.md), [COALESCE](coalesce-checkpoint.md), [CASE](case-checkpoint.md) and [NULLIF](nullif-checkpoint.md) evidence. Existing comparison/return-type/branch-cast adapters remain explicit. The17 remaining families are not approved blanket exceptions. This review is not itself a passing test receipt.

The accepted synchronous/scoped evaluator design stays in place. No universal compiler rewrite or exhaustive performance project is required to make the next functional steps. Conversely, reaching90% does not erase the Plan's remaining core ownership/demand obligations.

R91 adds a [Real/Float32→UNSIGNED CAST slice](cast-real-uint-checkpoint.md), **not a new family**. Its native rounding/wrapping/range/overflow algorithm now belongs to TiKV. Other CAST domains, outer NULL and UNION negative bypass remain; legacy wire conversion has a distinct unchanged policy. R92 [type deduplication](decimal-policy-checkpoint.md) subsequently moves the full native float-format policy and Decimal precision-cast body to TiKV. LowerExp and Ryu policies remain distinct through a shared layout renderer; CAST warning classification and source preparation are still caller-owned. This adds no C4 profile or family credit. R91–R92 added no family credit; R93 retains all226 prior objects and adds only bounded staleness.

## Five core families still need closure

| Families | Remaining work / source evidence |
|---|---|
| cast | `cast.rs` and datatype `decimal/mod.rs` still own ordinary parsing/status policy. R83 float-constructor/Display sharing did not migrate general string-to-Decimal parsing. Close ordinary domains over shared SDKs; record truly exceptional domains separately. |
| in | Runtime reduction, typed temporal/JSON membership and prepared string cache/probe remain native. AST/typed generic exhaust comparisons, while ready-values/legacy early-stop; typed temporal paths cast everything first. R93 inventoried these differences. The old NOT IN test fixes3facades (Eq,Eq,NOT); adding a real reducer requires explicitly authorized mechanical receipt adjustment, not hiding calls or changing SQL expectations. No IN code/test was changed. |
| greatest, least, interval | `builtin_ext/compare2.rs` retains extrema selection and INTERVAL's metadata-selected nullable-linear versus NOT_NULL-binary search. Preserve actual type, collation, precision and getter/search order. |

Observed caller chain: `evaluator.rs::run_with_consumer` → `Expression::eval` → `ScalarFunction::eval` → native control branches or PB dispatch. The R85 parent read the actual CASE/IF/IFNULL/COALESCE branches; R86–R90 removed IFNULL/IF/COALESCE/CASE/NULLIF runtime selection and share choice primitives, retaining distinct truth/error, proof and return-projection policies. NULLIF keeps eager operands and actual Eq before left preparation; its SQL zero-slot refusal is comparison-stage evidence, distinct from selector-isolating direct tests. CASE keeps AST base-once versus rewritten per-WHEN evaluation and original SQL branch casts. COALESCE has catalog facts but no PB/legacy admission; that boundary remains closed. Remaining conclusions are not inferred solely from registry names.

## Six ordinary families still pending

| Family group | Remaining work |
|---|---|
| convert_charset | Actual encode/decode/replacement/retag policy in `convert_charset.rs`; shared GB leaves are prerequisites, not evaluator closure. |
| date_add, date_sub, extract | Calendar/unit algorithms and warning/type policies remain in `time_fn/calendar.rs`. |
| str_to_date | Real format scanning and distinct error/type policies in `time_fn/calendar.rs`. TIMESTAMPDIFF civil/raw difference policies are now shared. |
| json_sum_crc32 | Existing internal scalar-array algorithm in `builtin_ext/json/report.rs`. SQL ARRAY syntax is still rejected by the baseline; migrate the implemented domain without inventing new SQL admission. |

These are pending implementations, not approved whole-family exceptions merely because they take work. R94 source review found EXTRACT's real selector in `time_fn/extract.rs`, including mixed datetime/duration choice and error policy; moving only `calendar::extract_composite` would not close it. INTERVAL needs a lazy search cursor and separate eager/typed NaN predicates; GREATEST/LEAST retain five domains, including original NoColumns numeric comparison and distinct string-as-time policy. These are scoped findings, not blanket deferrals.

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
