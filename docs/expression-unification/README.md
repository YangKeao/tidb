# Expression unification experiment

Checkpoint-ID: `packet-string-four-17` (previous: `pi-ip-five-16`)

**51/245 families delegate to TiKV with their native evaluator algorithms removed; target 221.** This checkpoint adds SPACE, REPEAT, TO_BASE64 and FROM_BASE64. Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror the canonical `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## Packet-string ownership

Native code keeps coercion, demand, packet sizing/1301 policy and packing. Typed Allow/SuppressByPacket accompanies real nullable arguments in an independent role. Actual private TiKV wrappers produce suppressed NULL; original inputs are not replaced by fake NULL, nor packet diagnostics by resource errors.

REPEAT's NULL-left path carries an explicit Undemanded count, accepted only for that operation with NULL bytes and Allow. A checked irrelevant Some(0) is physical transport, not an evaluated SQL NULL. One TiKV repeat core serves both wrappers; its empty-input fast path prevents billions of empty iterations without changing wire values.

One encoder/line-wrapper and one decoder live in TiKV. Native TO_BASE64 may encode above16MiB; wire keeps its old empty-result policy. Native FROM_BASE64 removes four whitespace bytes and returns NULL for invalid multiple-of-four length; wire removes six and retains its empty-result behavior. Padding/trailing-bit rules remain covered. The value-only FROM entry keeps execution context but omits packet and raw-length policy. Silent size overflow skips native packet diagnostics and reaches the appropriate kernel with real arguments and Allow.

Text results remain Text for SPACE/REPEAT/TO_BASE64; FROM stays Binary. No new PB/unistore admission, driver, pool, limits or native fallback is introduced.

## Actual validation

- TiKV local: 207 passed/1 ignored; original string tests: 63 passed.
- Native dispatcher: 3 passed, including actual encoding of16,777,217 bytes, demand and packet/value-only policies.
- SQL/lifecycle retry: 37 passed. Initial run: 36 passed/1 new assertion failure.
- Full expression: 1400 passed/4 unchanged failures/94 ignored, 1498 discovered. Complete failure blocks match16 after only thread-ID normalization.

The initial SQL assertion wrongly expected the returned evaluation-origin1105 error to appear in warnings. Existing session teardown explicitly excludes that error row. Only the new assertion/comment changed: Warning1301 remains in diagnostics, while typed1105/HY000 is checked separately. Runtime and existing expected values were untouched. The initial command chain stopped at SQL; full-expression ran independently later.

SQL uses seven small rows, four1024-byte packet overflow cases and nine direct zero-slot refusals, including suppressed FROM. Full unistore, full workspace, make lint, release performance and allocator remeasurement were not run here. Exact commands: `logs/packet-string-four-summary.txt`; boundaries: `evidence/packet-string-four-checkpoint.md`.

## Next work

LOWER/UPPER/SHA2/ORD are the next parallel batch, not yet credited. Go simple casing, malformed-byte normalization, binary/PB paths, SHA2's quiet native versus warning wire policy, and ORD's argument-charset preparation must remain explicit. Variadic/lazy CONCAT, overlapping TRIM/SUBSTRING_INDEX, padding limits, INSERT offsets and GB collation residuals remain documented gaps.

Broad operation-scope guards, physical peak/OOM safety, paired differential reruns and final acceptance remain open. Kernel reuse is not complete Go-package transcreation.
