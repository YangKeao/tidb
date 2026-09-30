# Expression unification experiment

Checkpoint-ID: `pi-ip-five-16` (previous: `raw-math-six-15`)

**47/245 families delegate to TiKV with their native evaluator algorithms removed; target 221.** This checkpoint adds PI, IS_IPV4, IS_IPV6, IS_IPV4_COMPAT and IS_IPV4_MAPPED. Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Validated steps push both branches without force-push or automatic PRs.

## PI and IP ownership

PI uses true NoArgs: zero input columns/schema entries, one zero-argument FnCall and one output row. Its private wrapper calls the original TiKV pi function and returns owned IEEE bits. No dummy argument, native constant or evaluator shortcut remains in the admitted AST/typed/PB/legacy routes.

SQL may legally fold PI. SQL tests verify its value and metadata without forcing the optimizer to retain it or demanding a zero-slot runtime refusal. Separate core/helper/legacy tests prove actual invocation and explicit-context rejection.

Four IP predicates use private default-NULL-propagating wrappers. Non-NULL inputs call the existing official algorithms; wire NULL-to-zero behavior remains unchanged. IPv4 alone removes redundant leading ASCII zeros from each existing dot segment before parsing, retaining empty segments, all separators and other characters. This does not decide range or validity. IPv6 and binary COMPAT/MAPPED payloads are not normalized.

Native IPv4/IPv6 parsing, redundant IPv6 pre-check and binary prefix algorithms are deleted. Checked text conversion, raw-byte conversion, metadata and signed boolean packing remain frontend-owned. No additional PB/unistore admission is introduced for these predicates.

ClosedPrivate selects fixed getters through existing common preparation. NoArgs, IEEE and ordinary-value roles are checked separately. The same synchronous driver, evaluator-instance pool and limits remain; Real/NotNan is not widened and there is no native fallback.

## Actual validation

- TiKV local: 203 passed/1 ignored; role/identity guard: 1 passed.
- New IP and PI dispatch: 1 each passed; SQL/lifecycle: 35 passed.
- Legacy infrastructure/inverse-trig tests: 2 passed; existing math fixture including exact PI bits: 1 passed.
- Full expression: 1397 passed/4 unchanged failures/94 ignored, 1495 discovered. Complete failure blocks match15 after only thread-ID normalization.

SQL checks 28 predicate cells plus PI and metadata, and eight direct zero-slot predicate refusals. Existing expected values were not changed. Full unistore was not rerun here; checkpoint15's non-green full suite and scoped HEAD comparison remain documented, not silently promoted to green.

Exact commands and boundaries: `evidence/pi-ip-five-checkpoint.md`, `logs/pi-ip-five-summary.txt`.

## Open work

Next candidates are SPACE, REPEAT, TO_BASE64 and FROM_BASE64. Explicit packet disposition must preserve native1301 policy without fabricating NULL arguments, while private wrappers reuse unique TiKV cores and preserve existing wire behavior. No credit yet.

Go/std trigonometric differences, other string compatibility, SHA2, compression and ORD remain documented gaps. Broad operation-scope guards, release performance, allocator remeasurement, physical peak/OOM safety, paired differential reruns, full workspace and make lint remain unverified. Kernel reuse is not a claim of complete Go-package transcreation.
