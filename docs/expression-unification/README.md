# Expression unification experiment

Checkpoint-ID: `json-introspection-three-30` (previous: `compression-two-29`)

**96/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds JSON_VALID, JSON_TYPE and JSON_DEPTH. Strict final-audited acceptance remains **0**; the experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **Shared parser, type names and depth:** TiKV `native_policy`, `json_type` and `json_depth` own the compatibility parser, common type-name selector and single depth recursion. Text-number distinctions, typed temporal/opaque names and original document coercions remain; no generic lossy JSON codec bridge is introduced.
- **Public helper included after review:** native `binary_json_ops::element_depth` retains its original `to_node` conversion, then delegates child traversal to the narrow `native_json_depth_from_children` helper. Its nested depth algorithm was removed, without a dummy array or codec bridge. Review found this additional owner; the first edit was not already complete.
- **Closed execution:** six fixed operations reuse Bytes/NoArgs and the existing driver. The five-state JSON report carries NULL, bytes, integer, empty-text or invalid-text outcomes. JSON_VALID's Others signature is the only added NoArgs whitelist entry. There is no new input role, four-column allowance or PB/legacy admission; native NoColumns dispatch wrappers are test-only.
- **Explicit error-phase change:** source-type, UTF-8 and numeric-conversion errors remain in guarded coercion before admission. JSON parsing and typed JSON_TYPE validation now happen in the worker: zero-slot refusal therefore precedes bad/empty JSON and malformed typed-payload results. Healthy SQL values and original JSON error codes/messages remain; this is **not** preservation of every former error precedence.

The change covers 24 Rust files (12 per repository), including one new `native_policy` module. Original expected values are unchanged.

## Actual validation

| Final run | Result |
|---|---|
| TiKV datatype | 34 passed |
| TiKV JSON, including two core tests, 19 kernel tests and casts | 32 passed |
| Native datatype binary JSON, including the added helper test | 32 passed |
| Native `builtin_ext::json` | 40 passed |
| Original JSON source tests | 30 passed |
| New native dispatch tests | 3 passed |
| SQL/lifecycle | 63 passed |
| Full native expression library | **1443 passed, 4 unchanged failures, 94 ignored; 1541 total, exit 101; 10.54 s** |

Four earlier runs (TiKV datatype 34, native datatype 31, local evaluator 252 plus one ignored, kernels 19) preceded the public-helper correction. Across all **12 actual runs, 11 passed and one retained the known non-green baseline**; there were no compilation failures, failed-test retries or expected-value edits. The complete full-expression failure section matches checkpoint29 after thread-ID normalization: SHA-256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`.

New SQL coverage checks four rows across three families with result metadata, six typed-DATE/numeric/raw columns, four 3140 diagnostics and ten zero-slot refusals. Exact commands: [summary](logs/json-introspection-summary.txt). Ownership, review correction and compatibility limits: [evidence](evidence/json-introspection-checkpoint.md).

## Remaining work

Next frozen read-only candidates, **not credited**: JSON_STORAGE_FREE, JSON_STORAGE_SIZE and JSON_QUOTE. SIZE must share encoder-layout primitives without introducing binary encoding's u16 limits; QUOTE must share traversal while retaining native serde escaping versus wire `0x07 → \a` / `0x0b → \v` policy. JSON_LENGTH remains deferred.

Operation-scope coverage, allocation/high-water and physical-peak checks, paired differential reruns, release performance, whole-workspace acceptance, `make lint` and TiFlash integration remain unfinished. No whole-JSON-codec or complete Go-package claim is made. Full unistore, parser-charset and datatype suites were not rerun; historical non-green results are not passing evidence.
