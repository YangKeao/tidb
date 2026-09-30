# Expression unification experiment

Checkpoint-ID: `log-pow-length-insert-six-20` (previous: `trim-subidx-pad-four-19`)

**65/245 families delegate to TiKV with their native evaluator algorithms removed; target 221.** This checkpoint adds LN, LOG (both arities), LOG2, POW/POWER, UNCOMPRESSED_LENGTH and INSERT (`insert_func` in the frozen ledger). Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## Ownership and coverage correction

Raw mathematical primitives live only in TiKV. Native coercion, logarithm warning3020/domain masking and POW finite-result policy stay frontend-owned. Genuine non-NULL domain errors still compute their raw IEEE results before masking; warning timing is unchanged. PB POW can leave either operand uncoerced when the other is actually NULL. Legacy POW evaluates left first and preserves NaN/Inf rather than applying native SQL policy. All admitted NULL paths enter the worker.

UNCOMPRESSED_LENGTH uses one nullable empty/short/full-LE32 kernel. The frontend retains1259 for1..4 bytes; empty and short yield0, NULL remains NULL, and the entire32-bit header is preserved. No packet check or zlib decoding is introduced.

INSERT uses one TiKV range/splice core. Native text uses actual character boundaries, normalizes only its source, and does not decode replacement bytes. Binary retains byte indexing. Existing wire UTF8 behavior is preserved, including its known character-offset-as-byte-offset defect. Packet checking follows the actual computed result and does not read the limit for NULL. Only four PAD and two INSERT operations admit four physical arguments/five compile nodes; other limits, the driver and pool are unchanged.

**Historical correction:** checkpoint18/19 complete-family claims for LOWER/UPPER missed independent unistore algorithms and NULL bypasses. During this checkpoint the corrected count was temporarily63. The legacy paths are now repaired and validated, restoring65 without crediting extra families. Legacy ASCII maps ASCII only, retaining high bytes; legacy UTF8 preserves Rust grouped-lossy preparation before the existing Go-simple TiKV kernels. This correction does not rewrite history or claim the old snapshots were complete.

## Actual validation

Final repaired snapshot:

- TiKV local:220 passed/1 ignored; original string tests:63 passed. Earlier unchanged math/encryption tests:46/8 passed.
- Six-family dispatcher:4 passed; legacy POW:2 passed before the casing-only repair; legacy casing:2 passed after it. Both are covered by the final full unistore run.
- SQL/lifecycle:43 passed, including three stored rows × eleven results, raw replacement bytes, diagnostic order and fourteen new zero-slot calls.
- Full expression:1410 passed/4 unchanged failures/94 ignored,1508 discovered.
- Full unistore:177 passed/1 unchanged failure/13 ignored,191 discovered.

Complete failure blocks match the previous expression and unistore baselines after only thread-ID normalization; both full suites remain non-green. Receipts retain the initial raw-POW return-type compile errors, the new packet-getter assertion correction, and the casing test-helper exhaustiveness compile correction. Existing expected values were not changed. Exact commands and all nineteen stage-specific receipts: [summary](logs/log-pow-length-insert-six-summary.txt); [ownership evidence](evidence/log-pow-length-insert-six-checkpoint.md).

## Next work and exclusions

Complete SUBSTRING requires native, PB and independent legacy demand policies, not just ordinary SQL inputs. STRCMP/search/FIND_IN_SET require moving the remaining GB compatibility kernels/data to shared ownership first; sort keys cannot replace GB compare because their PUA/NUL policies differ. Unbounded ELT/FIELD cannot be credited through a four-argument subset.

EXP/LOG10 use distinct native Go algorithms; COMPRESS has distinct encoded bytes; UNCOMPRESS needs typed diagnostic outcomes and bounded inflation. No new PB/unistore admission was added. Complete operation-scope guards, physical peak/OOM safety, allocator remeasurement, paired differential reruns, full workspace, make lint and release performance remain unverified. Kernel reuse is not complete Go-package transcreation.
