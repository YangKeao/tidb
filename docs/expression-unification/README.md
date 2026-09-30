# Expression unification experiment

Checkpoint-ID: `substring-gb-21` (previous: `log-pow-length-insert-six-20`)

**66/245 families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds the complete SUBSTRING/SUBSTR/MID family and moves GB compatibility to shared ownership. The GB foundation adds no evaluator-family credit. Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## Ownership and preserved policies

Eight closed substring recipes cover native/legacy × actual two/three arguments × bytes/UTF8. All admitted NULL, empty and nonempty results reach the synchronous TiKV evaluator. Two arguments are not implemented with a synthetic maximum third argument: native positive-length overflow yields empty, while the genuine two-argument form returns the tail.

Native coercion and Go per-invalid-byte normalization remain frontend-owned. Two-argument SQL retains its complete `NoColumns` cast policy but uses the real execution context for worker admission. PB preserves its earlier NULL child-demand boundary, uncoerced preceding values, source-reader error precedence and existing actual-arity behavior.

Legacy preserves source/position/length demand order, full i128 values, position-zero and width-rejection NULLs, out-of-range empty results, grouped Rust-lossy text and unchecked addition. A shared pure predicate answers only whether length is demanded; the final kernel revalidates that state. Only legacy transports its integer operands as dedicated 16-byte little-endian values. Ordinary byte recipes cannot impersonate this role. No driver, pool, four-column whitelist or general graph admission was expanded.

GB comparison, key emission and required encoding leaves now live in TiKV's `codec/collation/gb.rs`; native callers are facades. Existing wire/native policies remain explicit: key-only PUA NULs do not become comparison bytes, codec differences and nine wire-only overrides remain distinct. TiKV's four canonical tables and 2103-pair mapping retain their original bytes; two duplicate native CI images and two generated override copies are deleted. Native encoding uses pinned registry 0.8.35; wire keeps its original git 0.8.29. Generators verify the shared ownership, not a second generated mirror. See [GB evidence](evidence/gb-shared-foundation.md).

**Historical correction retained:** checkpoint18/19 LOWER/UPPER complete-family claims missed legacy algorithms and NULL bypasses. Checkpoint20 repaired them and restored its temporarily corrected 63 count to65, without adding extra families or rewriting old commits. This checkpoint adds only SUBSTRING, reaching66.

## Actual validation

- TiKV collation:21 passed; local evaluator:224 passed/1 ignored; original strings:63 passed.
- Native datatype library:436 passed; shared collation contract:14 passed.
- Substring dispatcher:3 passed; legacy:2 passed; SQL/lifecycle:45 passed, including nine new direct zero-slot refusals.
- Both generator checks passed; the parser generator check covers GB ownership only.
- Full expression:1413 passed/4 unchanged failures/94 ignored,1511 discovered.
- Full unistore:179 passed/1 unchanged failure/13 ignored,193 discovered.

Both complete failure blocks match checkpoint20 after only thread-ID normalization. Parser-charset is also **not all green**:13 tests produced9 passes and4 failures, starting with the unchanged default-registry assertion (supported7 versus defaults5), followed by three poisoned-test-lock failures. The original assertion also fails alone; excluding it yields12 passes. Its registry, data, flag initialization and old assertion are unchanged, and the new tests do not mutate global mode. This is source attribution plus isolation on the current artifact, not a rerun of the complete old HEAD artifact. No old expected value was changed.

The initial unsupported Cargo test-target invocation ran zero tests; its corrected aggregated target and all later stages are reported separately. Exact commands and sixteen receipts: [summary](logs/substring-gb-summary.txt); [substring and joint-checkpoint evidence](evidence/substring-gb-checkpoint.md).

## Next work and exclusions

STRCMP, LOCATE/INSTR/POSITION and FIND_IN_SET can now consume the shared GB foundation, but their evaluators are not yet migrated or credited. Preserve NoPad/cache policies and do not replace compare with sort-key comparison. Unbounded ELT/FIELD cannot be credited through a four-argument subset.

EXP/LOG10 use distinct native Go algorithms; COMPRESS has distinct encoded bytes; UNCOMPRESS needs typed diagnostic outcomes and bounded inflation. No new PB/unistore admission was added. Full codec-domain equivalence, operation-scope guards, physical peak/OOM safety, allocator remeasurement, paired differential reruns, full workspace, make lint and release performance remain unverified. Kernel reuse is not a new complete Go-package transcreation claim.
